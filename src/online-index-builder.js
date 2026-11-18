import { SecondaryIndex } from './secondary-index.js';
import {
  BuildAbortedError,
  InvalidCheckpointError,
  InvalidStateError,
  LogGapError,
  SwitchCancelledError,
  UnappliedWritesError,
} from './errors.js';

const PHASES = new Set(['base', 'catchup', 'switching', 'done', 'cancelled']);

function deferred() {
  const d = {};
  d.promise = new Promise((resolve, reject) => {
    d.resolve = resolve;
    d.reject = reject;
  });
  return d;
}

/**
 * 在线索引构建器。
 *
 * 阶段：base（扫描快照构建基础索引）→ catchup（追赶快照后的写入日志）
 * → switching（锁内最终确认并原子切换）→ done。
 *
 * 每个阶段的进度都通过 checkpointStore 持久化，崩溃后用相同 buildId
 * 重新创建构建器并调用 run() 即可续建；日志条目按 LSN 幂等重放。
 */
export class OnlineIndexBuilder {
  #buildId;
  #table;
  #writeLog;
  #checkpointStore;
  #onSwitch;
  #abortSignal;
  #pollIntervalMs;
  #readBatchSize;
  #checkpointEvery;
  #crashAfter;
  #baseMutationCount = 0;
  #catchupMutationCount = 0;
  #crashInBaseAfter;
  #crashInCatchupAfter;

  #field;
  #baseSnapshotLsn;
  #phase = 'base';
  #baseCursor = null;
  #appliedLsn;
  #index;
  #mirror = new Map();

  #switchDeferred = null;
  #cancellationInFlight = false;
  #cancelReason;
  #stopRequested = false;

  constructor(options) {
    const {
      buildId,
      table,
      writeLog,
      field,
      baseSnapshotLsn,
      checkpointStore,
      onSwitch,
      abortSignal,
      pollIntervalMs = 5,
      readBatchSize = 100,
      checkpointEvery = 20,
      crashAfter = null,
      crashInBaseAfter = null,
      crashInCatchupAfter = null,
    } = options;
    if (buildId == null) throw new TypeError('buildId 不能为空');
    if (!table || !writeLog || !checkpointStore) {
      throw new TypeError('table、writeLog、checkpointStore 均为必选项');
    }
    if (!field || typeof field !== 'string') {
      throw new TypeError('索引字段 field 必须是非空字符串');
    }
    const snapshotLsn = baseSnapshotLsn ?? writeLog.headLsn;
    if (!Number.isInteger(snapshotLsn) || snapshotLsn > writeLog.headLsn) {
      throw new RangeError('baseSnapshotLsn 必须是不晚于日志头部的整数 LSN');
    }
    if (crashAfter != null && (!Number.isInteger(crashAfter) || crashAfter <= 0)) {
      throw new TypeError('crashAfter 必须是正整数');
    }
    for (const value of [crashInBaseAfter, crashInCatchupAfter]) {
      if (value != null && (!Number.isInteger(value) || value <= 0)) {
        throw new TypeError('崩溃注入计数必须是正整数');
      }
    }
    this.#buildId = String(buildId);
    this.#table = table;
    this.#writeLog = writeLog;
    this.#checkpointStore = checkpointStore;
    this.#onSwitch = onSwitch ?? (async () => {});
    this.#abortSignal = abortSignal;
    this.#pollIntervalMs = pollIntervalMs;
    this.#readBatchSize = readBatchSize;
    this.#checkpointEvery = checkpointEvery;
    this.#crashAfter = crashAfter;
    this.#crashInBaseAfter = crashInBaseAfter;
    this.#crashInCatchupAfter = crashInCatchupAfter;
    this.#field = field;
    this.#baseSnapshotLsn = snapshotLsn;
    this.#appliedLsn = snapshotLsn;
    this.#index = new SecondaryIndex(field);
  }

  get buildId() {
    return this.#buildId;
  }

  get field() {
    return this.#field;
  }

  get phase() {
    return this.#phase;
  }

  get baseSnapshotLsn() {
    return this.#baseSnapshotLsn;
  }

  get appliedLsn() {
    return this.#appliedLsn;
  }

  /** 构建中的新索引（切换前查询方不应读取它）。 */
  get index() {
    return this.#index;
  }

  /** 追赶是否已经追到日志头部（没有未应用写入）。 */
  get caughtUp() {
    return this.#appliedLsn === this.#writeLog.headLsn;
  }

  /** 等到追赶阶段追平日志头部（供调用方在请求切换前确认无未应用写入）。 */
  async waitUntilCaughtUp() {
    while (this.#phase === 'base' || !this.caughtUp) {
      this.#throwIfAborted();
      await this.#sleep(this.#pollIntervalMs);
    }
  }

  mirrorSnapshot() {
    return new Map(this.#mirror);
  }

  /**
   运行构建直到切换完成、被取消或抛出异常。
   * 可在崩溃后用相同 buildId 重新调用，从最后一个 checkpoint 续建。
   */
  async run() {
    try {
      await this.#restoreOrInit();
      await this.#runBase();
      await this.#runCatchup();
    } catch (err) {
      if (this.#cancellationInFlight) {
        await this.#finishCancelled();
        throw err;
      }
      if (!err.crashLike) {
        await this.#persist();
      }
      if (this.#switchDeferred) this.#switchDeferred.reject(err);
      throw err;
    }
  }

  /**
   * 请求在追平日志后执行切换，返回切换完成的 Promise。
   */
  requestSwitch() {
    if (this.#phase === 'done') {
      return Promise.resolve({ appliedLsn: this.#appliedLsn });
    }
    if (this.#switchDeferred?.cancelled) {
      return Promise.reject(
        new InvalidStateError('本次切换已被取消，请新建一次构建'),
      );
    }
    if (!this.#switchDeferred) this.#switchDeferred = deferred();
    this.#switchDeferred.requested = true;
    return this.#switchDeferred.promise;
  }

  /**
   * 取消等待中的切换/构建。已经进入锁内切换（phase === 'switching'）
   * 或已完成（done）的构建无法取消。
   */
  cancel(reason = '索引切换已取消') {
    if (this.#phase === 'done') {
      throw new InvalidStateError('构建已完成切换，无法取消');
    }
    if (this.#phase === 'switching') {
      throw new InvalidStateError('切换已在锁内进行，无法取消');
    }
    if (!this.#switchDeferred) this.#switchDeferred = deferred();
    this.#switchDeferred.requested = true;
    this.#switchDeferred.cancelled = true;
    this.#cancellationInFlight = true;
    this.#cancelReason = reason;
  }

  /**
   * 干净地暂停构建：持久化当前进度后让 run() 结束。
   * 与 cancel 不同，checkpoint 保持在 base/catchup，可随时用相同 buildId 续建。
   */
  stop() {
    if (this.#phase === 'done' || this.#phase === 'switching') {
      throw new InvalidStateError(`阶段 ${this.#phase} 无法暂停`);
    }
    this.#stopRequested = true;
  }

  get stopped() {
    return this.#stopRequested;
  }

  async #restoreOrInit() {
    const saved = await this.#checkpointStore.load(this.#buildId);
    if (!saved) {
      this.#appliedLsn = this.#baseSnapshotLsn;
      return;
    }
    if (
      saved.buildId !== this.#buildId ||
      saved.field !== this.#field ||
      saved.baseSnapshotLsn !== this.#baseSnapshotLsn
    ) {
      throw new InvalidCheckpointError('checkpoint 与当前构建参数不匹配');
    }
    if (!PHASES.has(saved.phase) || saved.phase === 'done' || saved.phase === 'cancelled') {
      throw new InvalidCheckpointError(
        `checkpoint 处于不可续建的阶段: ${saved.phase}`,
      );
    }
    this.#phase = saved.phase;
    this.#baseCursor = saved.baseCursor;
    this.#appliedLsn = saved.appliedLsn;
    this.#index = SecondaryIndex.fromSnapshot(saved.index);
    this.#mirror = new Map(saved.mirror);
  }

  async #runBase() {
    if (this.#phase !== 'base') return;
    const entries = this.#table.snapshot(this.#baseSnapshotLsn);
    let startIdx = 0;
    if (this.#baseCursor != null) {
      const cursorIdx = entries.findIndex(([key]) => key === this.#baseCursor);
      if (cursorIdx === -1) {
        throw new InvalidCheckpointError(
          `checkpoint 中的 baseCursor 在快照中不存在: ${this.#baseCursor}`,
        );
      }
      startIdx = cursorIdx + 1;
    }
    for (let idx = startIdx; idx < entries.length; idx += 1) {
      this.#throwIfAborted();
      if (this.#stopRequested) {
        await this.#persist();
        return;
      }
      const [key, doc] = entries[idx];
      this.#index.applyPut(key, doc);
      this.#mirror.set(key, doc);
      this.#baseCursor = key;
      await this.#afterMutation('base');
    }
    this.#phase = 'catchup';
    await this.#persist();
  }

  async #runCatchup() {
    if (this.#phase === 'base') return;
    while (true) {
      this.#throwIfAborted();
      if (this.#stopRequested) {
        await this.#persist();
        return;
      }
      await this.#applyAvailable();
      if (this.#switchDeferred?.requested) {
        if (this.#switchDeferred.cancelled) {
          await this.#finishCancelled();
          throw new SwitchCancelledError(this.#cancelReason ?? '索引切换已取消');
        }
        try {
          await this.#doSwitch();
          return;
        } catch (err) {
          const retryable =
            err instanceof UnappliedWritesError &&
            !this.#cancellationInFlight &&
            !err.crashLike;
          if (!retryable) {
            this.#phase = 'catchup';
            throw err;
          }
          this.#phase = 'catchup';
          await this.#persist();
          this.#switchDeferred.reject(err);
          this.#switchDeferred = null;
        }
      }
      await this.#sleep(this.#pollIntervalMs);
    }
  }

  async #applyAvailable() {
    const batch = this.#writeLog.readAfter(this.#appliedLsn, this.#readBatchSize);
    for (const entry of batch) {
      this.#throwIfAborted();
      await this.applyLogEntry(entry);
    }
    if (batch.length > 0) await this.#persist();
  }

  /**
   * 应用单条日志条目。LSN 必须恰好等于 appliedLsn + 1，
   * 重复或乱序条目按幂等/报错规则处理。
   * 返回 true 表示实际应用，false 表示重复条目被跳过。
   */
  async applyLogEntry(entry) {
    if (!entry || !Number.isInteger(entry.lsn)) {
      throw new TypeError('日志条目必须包含整数 lsn');
    }
    if (entry.lsn <= this.#appliedLsn) return false;
    const expected = this.#appliedLsn + 1;
    if (entry.lsn !== expected) {
      throw new LogGapError(
        `日志位置不连续：期望 LSN ${expected}，实际收到 LSN ${entry.lsn}`,
      );
    }
    if (entry.type === 'put') {
      this.#index.applyPut(entry.key, entry.doc);
      this.#mirror.set(entry.key, entry.doc);
    } else if (entry.type === 'delete') {
      this.#index.applyDelete(entry.key);
      this.#mirror.delete(entry.key);
    } else {
      throw new TypeError(`未知的日志条目类型: ${entry.type}`);
    }
    this.#appliedLsn = entry.lsn;
    await this.#afterMutation('catchup');
    return true;
  }

  async #doSwitch() {
    this.#writeLog.assertContinuousFrom(this.#appliedLsn);
    if (this.#writeLog.headLsn !== this.#appliedLsn) {
      throw new UnappliedWritesError(
        `切换被拒绝：仍有未应用写入（appliedLsn=${this.#appliedLsn}, ` +
          `headLsn=${this.#writeLog.headLsn}）`,
      );
    }
    this.#phase = 'switching';
    await this.#persist();
    await this.#onSwitch({
      buildId: this.#buildId,
      index: this.#index,
      appliedLsn: this.#appliedLsn,
    });
    this.#phase = 'done';
    await this.#persist();
    this.#switchDeferred.resolve({ appliedLsn: this.#appliedLsn });
  }

  async #finishCancelled() {
    if (this.#phase === 'cancelled') return;
    this.#phase = 'cancelled';
    await this.#persist();
    const err = new SwitchCancelledError(this.#cancelReason ?? '索引切换已取消');
    if (this.#switchDeferred) this.#switchDeferred.reject(err);
  }

  async #afterMutation(phase) {
    const count =
      phase === 'base'
        ? (this.#baseMutationCount += 1)
        : (this.#catchupMutationCount += 1);
    const phaseLimit =
      phase === 'base' ? this.#crashInBaseAfter : this.#crashInCatchupAfter;
    const shouldCrash =
      phaseLimit != null
        ? count >= phaseLimit
        : this.#crashAfter != null &&
          this.#baseMutationCount + this.#catchupMutationCount >= this.#crashAfter;
    if (shouldCrash) {
      const err = new BuildAbortedError(
        `模拟崩溃：${phase} 阶段处理完 ${count} 个变更后中断（尚未写入本次 checkpoint）`,
      );
      err.crashLike = true;
      throw err;
    }
    if (count % this.#checkpointEvery === 0) {
      await this.#persist();
    }
    this.#throwIfAborted();
  }

  async #persist() {
    await this.#checkpointStore.save(this.#buildId, {
      version: 1,
      buildId: this.#buildId,
      field: this.#field,
      baseSnapshotLsn: this.#baseSnapshotLsn,
      phase: this.#phase,
      baseCursor: this.#baseCursor,
      appliedLsn: this.#appliedLsn,
      index: this.#index.snapshot(),
      mirror: [...this.#mirror],
    });
  }

  #throwIfAborted() {
    if (this.#abortSignal?.aborted) {
      throw new BuildAbortedError('索引构建被 AbortSignal 中止');
    }
  }

  #sleep(ms) {
    return new Promise((resolve, reject) => {
      if (this.#abortSignal?.aborted) {
        reject(new BuildAbortedError('索引构建被 AbortSignal 中止'));
        return;
      }
      const signal = this.#abortSignal;
      const onAbort = () => {
        clearTimeout(timer);
        signal.removeEventListener('abort', onAbort);
        reject(new BuildAbortedError('索引构建被 AbortSignal 中止'));
      };
      const timer = setTimeout(() => {
        signal?.removeEventListener('abort', onAbort);
        resolve();
      }, ms);
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  }
}
