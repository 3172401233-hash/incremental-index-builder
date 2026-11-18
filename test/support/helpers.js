import assert from 'node:assert/strict';
import {
  IndexManager,
  MemoryCheckpointStore,
  Table,
  WriteLog,
} from '../../src/index.js';

export function createHarness(field = 'city') {
  const writeLog = new WriteLog();
  const table = new Table(writeLog);
  const checkpointStore = new MemoryCheckpointStore();
  const manager = new IndexManager({ table, writeLog, field });
  return { field, writeLog, table, checkpointStore, manager };
}

export function expectedIndex(table, writeLog, field, lsn = writeLog.headLsn) {
  const result = new Map();
  for (const [key, doc] of table.snapshot(lsn)) {
    const value = doc[field];
    if (value === undefined) continue;
    if (!result.has(value)) result.set(value, new Set());
    result.get(value).add(key);
  }
  return result;
}

export function assertIndexMatches(manager, table, writeLog, field) {
  const expected = expectedIndex(table, writeLog, field);
  for (const [value, keys] of expected) {
    assert.deepEqual(
      [...manager.lookup(value)].sort(),
      [...keys].sort(),
      `active 索引在 value=${value} 上的 key 集合与表当前状态不一致`,
    );
  }
  return expected;
}

export async function settled(promise) {
  const marker = { settled: false };
  promise.then(
    () => {
      marker.settled = true;
    },
    () => {
      marker.settled = true;
    },
  );
  await new Promise((resolve) => setImmediate(resolve));
  return marker;
}
