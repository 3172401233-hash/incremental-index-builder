# incremental-index-builder

Node.js 在线二级索引构建库。对一张持续接收插入、更新、删除的大表新建二级索引时，
保证切换前查询只能看到完整的旧索引，切换后只能看到完整的新索引，不会出现
“构建期间的新数据没进索引”或“旧版本文档索引项残留”。

零第三方依赖，基于 Node 内置的 `node:test` 运行测试，要求 Node.js >= 20。

## 核心思路：快照 + 写入日志追赶 + 锁内原子切换

每条写入都会进入一条仅追加的写入日志（oplog），并分配单调递增的 LSN
（Log Sequence Number）。构建分三个阶段：

1. **base（基础索引）**：在固定的快照 LSN 上物化一致性快照，按 key 排序后
   逐条扫描，构建出新索引的基础部分。
2. **catchup（追赶）**：从快照 LSN 的下一条开始，按 LSN 顺序重放 oplog，
   把快照之后发生的插入、更新、删除应用到新索引，直到追平日志头部。
3. **switching（切换）**：在写锁内做最终确认——日志位置连续且没有任何未应用
   写入——然后原子替换 active 索引。旧索引保留并继续接收双写，用于回退。

查询永远只走 active 索引，构建中的索引对外不可见，因此不存在中间态泄漏。

## 目录结构

- `src/write-log.js`：仅追加写入日志，负责 LSN 分配与连续性校验。
- `src/table.js`：带完整版本历史的键值表，可物化任意 LSN 时刻的快照。
- `src/secondary-index.js`：非唯一二级索引，维护 `fieldValue -> Set<key>`，
  更新时先移除旧 bucket 中的 key，杜绝旧版本文档残留。
- `src/checkpoint-store.js`：checkpoint 存储，提供内存版与文件版
  （临时文件 + rename 原子落盘）。
- `src/online-index-builder.js`：构建状态机（base → catchup → switching → done）。
- `src/index-manager.js`：对外的写入、查询、建索引、切换、回退入口。
- `src/mutex.js`：异步互斥锁，串行化写入与切换。
- `test/`：基于 `node:test` 的 13 个场景测试。

## 快速上手

```js
import {
  WriteLog,
  Table,
  IndexManager,
  MemoryCheckpointStore,
} from './src/index.js';

const writeLog = new WriteLog();
const table = new Table(writeLog);
const manager = new IndexManager({ table, writeLog, field: 'city' });

// 业务持续写入，旧索引在锁内同步维护，查询始终可用。
await manager.put('u1', { city: 'BJ' });
await manager.put('u2', { city: 'SH' });

// 启动一次在线构建，基础快照固定为当前日志头部 LSN。
const builder = manager.startBuild({
  buildId: 'idx-city-v2',
  checkpointStore: new MemoryCheckpointStore(),
  pollIntervalMs: 5,
});
const runPromise = builder.run();

// 构建期间写入不中断，这些变更由构建器从 oplog 追赶。
await manager.put('u3', { city: 'BJ' });
await manager.put('u1', { city: 'GZ' }); // 更新：旧 bucket 的 u1 被移除
await manager.delete('u2');              // 删除：索引项同步移除

// 切换前确认已经追平，无未应用写入。
await builder.waitUntilCaughtUp();
await builder.requestSwitch();
await runPromise;

manager.lookup('BJ'); // ['u3']
manager.lookup('GZ'); // ['u1']
manager.lookup('SH'); // []（u2 已删除）
```

## 追赶流程（catch-up）

构建器从 `baseSnapshotLsn` 开始，保证新索引 = 快照内容 + 快照之后的全部日志。

- **连续性**：每次读取日志前调用 `assertContinuousFrom(appliedLsn)`。
  若日志被截断（压缩）或存在空洞，立即抛出 `LogGapError`，构建中止，
  不会带着不完整数据切换。
- **顺序与幂等**：单条应用时要求条目 LSN 恰好等于 `appliedLsn + 1`，
  否则抛出 `LogGapError`；LSN 小于等于已应用位置的重复条目直接跳过。
  崩溃恢复时同一条日志可能被重放，幂等保证结果不变。
- **无未应用写入**：切换时复检 `appliedLsn === writeLog.headLsn`，
  不满足则抛出 `UnappliedWritesError`。该错误属于可恢复错误，构建器回到
  catchup 继续追赶，调用方重新 `requestSwitch()` 即可。
- **checkpoint**：每处理 `checkpointEvery` 个变更落盘一次，记录阶段、
  base 游标、已应用 LSN、索引快照和文档镜像。崩溃后用相同 `buildId`
  重建构建器并再次 `run()`，自动从最后一个 checkpoint 续建。

```js
// 进程崩溃后的恢复：参数与首次构建保持一致（含相同 buildId 与快照 LSN）。
const resumed = manager.startBuild({
  buildId: 'idx-city-v2',
  baseSnapshotLsn: previousSnapshotLsn,
  checkpointStore,
});
await resumed.run(); // 从 checkpoint 继续，已应用的日志不会重复生效
```

需要主动暂停而不是失败时，可调用 `builder.stop()`，它会先落盘 checkpoint
再让 `run()` 正常结束，后续同样可以续建。

## 切换与取消

- `requestSwitch()` 返回一个 Promise，在追平日志并完成锁内切换后 resolve。
- `cancel()` 用于取消等待中的切换或构建：构建进入 `cancelled` 阶段，
  新索引被丢弃，active 旧索引完全不受影响，继续服务查询与写入。
  已经进入锁内切换（`switching`）或已完成（`done`）的构建无法取消。
- 切换窗口内若出现新写入导致落后，`performSwitch` 在锁内复检失败并抛出
  `UnappliedWritesError`，active 索引保持不变，追赶后重试即可。

## 回退流程（rollback）

切换成功后，旧索引不会立即删除，而是作为 retained 索引继续接收双写
（与新索引应用完全相同的写入）。因此它始终保持完整且最新：

```js
// 新索引上线后发现问题，一键回退到旧索引。
await manager.rollback();
manager.lookup('BJ'); // 走旧索引，内容仍然与表一致

// 问题排除后可再次前滚到新索引（双写期间新索引也在更新）。
await manager.rollback();

// 确认新索引长期稳定后，丢弃 retained 索引释放资源，此后不可再回退。
await manager.commitRetention();
```

回退只是在锁内交换 active 指针，不重建数据；两次 `rollback()` 即可在
新旧索引之间来回切换。

## 错误类型

| 错误 | 含义 | 可恢复方式 |
| --- | --- | --- |
| `LogGapError` | 日志被截断或 LSN 出现空洞 | 需重建基础快照，无法续建 |
| `UnappliedWritesError` | 切换时仍有未应用写入 | 继续 catchup 后重新请求切换 |
| `SwitchCancelledError` | 切换/构建被显式取消 | 旧索引继续服务，需新建构建 |
| `BuildAbortedError` | AbortSignal 中止或模拟崩溃 | 从 checkpoint 续建 |
| `InvalidCheckpointError` | checkpoint 损坏或与构建参数不匹配 | 清理后重新构建 |
| `InvalidStateError` | 在非法阶段调用操作 | 修正调用时机 |

## 测试

```bash
node --test
# 或
npm test
```

覆盖场景：更新覆盖旧 key、构建期间删除、重复写入幂等、base/catchup 两阶段
checkpoint 中断与续建、文件 checkpoint、切换取消、切换前后查询可见性、
日志空洞拒绝切换、切换窗口写入复检与重试、回退/再前滚、AbortSignal 中止。
