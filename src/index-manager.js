import { AsyncLock } from './mutex.js';
import { OnlineIndexBuilder } from './online-index-builder.js';
import { SecondaryIndex } from './secondary-index.js';
import { InvalidStateError, UnappliedWritesError } from './errors.js';

/**
 * 索引管理器：对外提供写入、查询、在线建索引、切换和回退。
 *
 * 可见性规则：查询永远只走 activeIndex，构建中的索引不可见，
 * 因此切换前看到的是完整旧索引，切换后看到的是完整新索引。
 *
 * 写入路径在锁内同步更新 active 索引以及所有“在线”索引
 * （正在构建的新索引由构建器通过 oplog 追赶；切换后保留的旧索引
 * 继续接收双写，保证回退时它仍是完整且最新的）。
 */
export class IndexManager {
  #table;
  #writeLog;
  #lock = new AsyncLock();
  #activeIndex;
  #retainedIndex = null;
  #builder = null;

  constructor({ table, writeLog, field }) {
    this.#table = table;
    this.#writeLog = writeLog;
    this.#activeIndex = SecondaryIndex.fromTable(field, table, writeLog.headLsn);
  }

  get field() {
    return this.#activeIndex.field;
  }

  get activeIndex() {
    return this.#activeIndex;
  }

  get retainedIndex() {
    return this.#retainedIndex;
  }

  get buildingIndex() {
    return this.#builder?.index ?? null;
  }

  lookup(value) {
    return this.#activeIndex.lookup(value);
  }

  async put(key, doc) {
    const release = await this.#lock.acquire();
    try {
      const lsn = this.#table.put(key, doc);
      this.#applyToLiveIndexes({ type: 'put', key, doc, lsn });
      return lsn;
    } finally {
      release();
    }
  }

  async delete(key) {
    const release = await this.#lock.acquire();
    try {
      const lsn = this.#table.delete(key);
      this.#applyToLiveIndexes({ type: 'delete', key, lsn });
      return lsn;
    } finally {
      release();
    }
  }

  #applyToLiveIndexes(entry) {
    this.#applyOne(this.#activeIndex, entry);
    if (this.#retainedIndex) this.#applyOne(this.#retainedIndex, entry);
  }

  #applyOne(index, entry) {
    if (entry.type === 'put') index.applyPut(entry.key, entry.doc);
    else index.applyDelete(entry.key);
  }

  /**
   * 启动一次在线索引构建。基础快照固定为调用时刻的日志头部 LSN，
   * 之后的写入都由构建器从 oplog 追赶。
   */
  startBuild(options = {}) {
    if (this.#builder) {
      throw new InvalidStateError('已有正在进行的索引构建');
    }
    const baseSnapshotLsn = options.baseSnapshotLsn ?? this.#writeLog.headLsn;
    const builder = new OnlineIndexBuilder({
      ...options,
      field: options.field ?? this.field,
      table: this.#table,
      writeLog: this.#writeLog,
      baseSnapshotLsn,
      onSwitch: options.onSwitch ?? ((payload) => this.performSwitch(payload)),
    });
    this.#builder = builder;
    return builder;
  }

  /**
   * 锁内执行切换：最终确认日志连续且无未应用写入，然后原子替换 active 索引。
   * 可作为 builder 的 onSwitch 回调，也可在自定义 onSwitch 中显式调用。
   */
  async performSwitch({ index, appliedLsn }) {
    const release = await this.#lock.acquire();
    try {
      this.#writeLog.assertContinuousFrom(appliedLsn);
      if (appliedLsn !== this.#writeLog.headLsn) {
        throw new UnappliedWritesError(
          `切换被拒绝：appliedLsn=${appliedLsn} 落后于 headLsn=${this.#writeLog.headLsn}`,
        );
      }
      this.#retainedIndex = this.#activeIndex;
      this.#activeIndex = index;
    } finally {
      release();
    }
  }

  /** 回退到上一个索引（保留的旧索引一直双写，因此内容完整且最新）。 */
  async rollback() {
    const release = await this.#lock.acquire();
    try {
      if (!this.#retainedIndex) {
        throw new InvalidStateError('没有可回退的旧索引');
      }
      const previous = this.#activeIndex;
      this.#activeIndex = this.#retainedIndex;
      this.#retainedIndex = previous;
      return this.#activeIndex.field;
    } finally {
      release();
    }
  }

  /** 确认新索引无误后丢弃旧索引，释放资源，此后不可再回退。 */
  async commitRetention() {
    const release = await this.#lock.acquire();
    try {
      this.#retainedIndex = null;
    } finally {
      release();
    }
  }

  /** 构建取消/失败后的清理（不影响正在服务的 active 索引）。 */
  async discardBuild() {
    const release = await this.#lock.acquire();
    try {
      this.#builder = null;
    } finally {
      release();
    }
  }
}
