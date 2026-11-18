/**
 * 带完整版本历史的键值表。每次 put/delete 都会带 LSN 记录一个版本，
 * 因此可以物化任意 LSN 时刻的一致性快照，供基础索引构建使用。
 */
export class Table {
  #versions = new Map();
  #log;

  constructor(writeLog) {
    this.#log = writeLog;
  }

  put(key, doc) {
    const lsn = this.#log.append({ type: 'put', key, doc });
    this.#pushVersion(key, { lsn, type: 'put', doc });
    return lsn;
  }

  delete(key) {
    const lsn = this.#log.append({ type: 'delete', key });
    this.#pushVersion(key, { lsn, type: 'delete' });
    return lsn;
  }

  #pushVersion(key, version) {
    let history = this.#versions.get(key);
    if (!history) {
      history = [];
      this.#versions.set(key, history);
    }
    history.push(version);
  }

  get(key) {
    return this.getAt(key, this.#log.headLsn);
  }

  /** 返回 key 在指定 LSN 时刻的文档；不存在或已删除时返回 undefined。 */
  getAt(key, lsn) {
    const history = this.#versions.get(key);
    if (!history) return undefined;
    let result;
    for (const version of history) {
      if (version.lsn > lsn) break;
      result = version.type === 'put' ? version.doc : undefined;
    }
    return result;
  }

  /**
   * 物化指定 LSN 时刻的快照，返回按 key 排序的 [key, doc] 数组，
   * 排序保证 checkpoint 中断后可以从 baseCursor 确定性续建。
   */
  snapshot(lsn) {
    const entries = [];
    for (const key of this.#versions.keys()) {
      const doc = this.getAt(key, lsn);
      if (doc !== undefined) entries.push([key, doc]);
    }
    entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return entries;
  }
}
