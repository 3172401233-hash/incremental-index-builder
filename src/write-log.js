import { LogGapError } from './errors.js';

/**
 * 仅追加的写入日志（oplog）。每条写入分配单调递增的 LSN，
 * 构建器依靠 LSN 的连续性判断是否存在缺失或未应用的写入。
 */
export class WriteLog {
  #entries = new Map();
  #nextLsn;
  #minLsn;

  constructor({ startLsn = 0 } = {}) {
    if (!Number.isInteger(startLsn) || startLsn < 0) {
      throw new TypeError('startLsn 必须是非负整数');
    }
    this.#nextLsn = startLsn;
    this.#minLsn = startLsn;
  }

  append(entry) {
    if (entry.type !== 'put' && entry.type !== 'delete') {
      throw new TypeError(`未知的日志条目类型: ${entry.type}`);
    }
    const lsn = this.#nextLsn;
    this.#nextLsn += 1;
    this.#entries.set(lsn, { ...entry, lsn });
    return lsn;
  }

  get minLsn() {
    return this.#minLsn;
  }

  /** 最后一条已追加日志的 LSN；日志为空时为 -1。 */
  get headLsn() {
    return this.#nextLsn - 1;
  }

  get nextLsn() {
    return this.#nextLsn;
  }

  get size() {
    return this.#entries.size;
  }

  getEntry(lsn) {
    const entry = this.#entries.get(lsn);
    return entry ? { ...entry } : undefined;
  }

  /**
   * 校验从 lsn+1 到日志头部之间没有空洞（日志被截断或条目缺失即抛错）。
   */
  assertContinuousFrom(lsn) {
    const start = lsn + 1;
    if (start < this.#minLsn) {
      throw new LogGapError(
        `写入日志已截断：需要 LSN ${start}，但现存最旧 LSN 为 ${this.#minLsn}`,
      );
    }
    for (let cur = start; cur < this.#nextLsn; cur += 1) {
      if (!this.#entries.has(cur)) {
        throw new LogGapError(`写入日志在 LSN ${cur} 处存在空洞`);
      }
    }
  }

  /**
   * 读取严格晚于 lsn 的日志条目，最多返回 limit 条。
   * 读取前先做连续性校验。
   */
  readAfter(lsn, limit = Infinity) {
    this.assertContinuousFrom(lsn);
    const out = [];
    for (let cur = lsn + 1; cur < this.#nextLsn && out.length < limit; cur += 1) {
      out.push({ ...this.#entries.get(cur) });
    }
    return out;
  }

  /** 模拟日志压缩/截断：早于 lsn 的条目被清除。 */
  truncateBefore(lsn) {
    for (const cur of this.#entries.keys()) {
      if (cur < lsn) this.#entries.delete(cur);
    }
    if (lsn > this.#minLsn) this.#minLsn = lsn;
  }
}
