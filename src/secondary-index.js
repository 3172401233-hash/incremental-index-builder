/**
 * 非唯一二级索引：fieldValue -> Set<key>。
 * 内部维护 key -> 当前 fieldValue 的映射，保证同一 key 更新时
 * 旧 bucket 中的条目被移除，不会残留旧版本文档的索引项。
 */
export class SecondaryIndex {
  #field;
  #entries = new Map();
  #keyField = new Map();

  constructor(field) {
    if (!field || typeof field !== 'string') {
      throw new TypeError('索引字段 field 必须是非空字符串');
    }
    this.#field = field;
  }

  get field() {
    return this.#field;
  }

  get size() {
    return this.#entries.size;
  }

  applyPut(key, doc) {
    this.removeKey(key);
    const value = doc == null ? undefined : doc[this.#field];
    if (value === undefined) return;
    let bucket = this.#entries.get(value);
    if (!bucket) {
      bucket = new Set();
      this.#entries.set(value, bucket);
    }
    bucket.add(key);
    this.#keyField.set(key, value);
  }

  applyDelete(key) {
    this.removeKey(key);
  }

  removeKey(key) {
    const oldValue = this.#keyField.get(key);
    if (oldValue === undefined) return;
    const bucket = this.#entries.get(oldValue);
    if (bucket) {
      bucket.delete(key);
      if (bucket.size === 0) this.#entries.delete(oldValue);
    }
    this.#keyField.delete(key);
  }

  lookup(value) {
    const bucket = this.#entries.get(value);
    return bucket ? [...bucket] : [];
  }

  has(value, key) {
    return this.#entries.get(value)?.has(key) ?? false;
  }

  snapshot() {
    return {
      field: this.#field,
      entries: [...this.#entries].map(([value, keys]) => [value, [...keys]]),
      keyField: [...this.#keyField],
    };
  }

  static fromSnapshot(snap) {
    const index = new SecondaryIndex(snap.field);
    index.#entries = new Map(
      snap.entries.map(([value, keys]) => [value, new Set(keys)]),
    );
    index.#keyField = new Map(snap.keyField);
    return index;
  }

  static fromTable(field, table, lsn) {
    const index = new SecondaryIndex(field);
    for (const [key, doc] of table.snapshot(lsn)) {
      index.applyPut(key, doc);
    }
    return index;
  }
}
