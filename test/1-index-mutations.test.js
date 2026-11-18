import test from 'node:test';
import assert from 'node:assert/strict';
import { SwitchCancelledError } from '../src/index.js';
import { assertIndexMatches, createHarness } from './support/helpers.js';

test('更新覆盖旧 key：旧 bucket 不残留，切换后索引与表一致', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  for (const [key, city] of [
    ['u1', 'BJ'],
    ['u2', 'BJ'],
    ['u3', 'SH'],
    ['u4', 'BJ'],
  ]) {
    await manager.put(key, { city });
  }

  const builder = manager.startBuild({
    buildId: 'overwrite',
    checkpointStore,
    pollIntervalMs: 2,
  });
  const runPromise = builder.run();

  await manager.put('u2', { city: 'SH' });
  await manager.put('u4', { city: 'SZ' });
  await manager.put('u5', { city: 'BJ' });
  await manager.put('u2', { city: 'GZ' });

  await builder.waitUntilCaughtUp();
  await builder.requestSwitch();
  await runPromise;

  assert.equal(builder.phase, 'done');
  assertIndexMatches(manager, table, writeLog, field);
  assert.deepEqual(manager.lookup('BJ').sort(), ['u1', 'u5']);
  assert.deepEqual(manager.lookup('SH').sort(), ['u3']);
  assert.deepEqual(manager.lookup('GZ').sort(), ['u2']);
  assert.deepEqual(manager.lookup('SZ'), ['u4']);

  await manager.put('u2', { city: 'HZ' });
  assertIndexMatches(manager, table, writeLog, field);
  assert.deepEqual(manager.lookup('GZ'), []);
  assert.deepEqual(manager.lookup('HZ'), ['u2']);
});

test('构建期间删除：删除穿透到新索引，已删除 key 查不到', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  for (const [key, city] of [
    ['d1', 'BJ'],
    ['d2', 'SH'],
    ['d3', 'BJ'],
    ['d4', 'SZ'],
  ]) {
    await manager.put(key, { city });
  }

  const builder = manager.startBuild({
    buildId: 'delete-during-build',
    checkpointStore,
    pollIntervalMs: 2,
  });
  const runPromise = builder.run();

  await manager.delete('d1');
  await manager.delete('d3');
  await manager.put('d4', { city: 'BJ' });
  await manager.delete('d4');

  await builder.waitUntilCaughtUp();
  await builder.requestSwitch();
  await runPromise;

  assert.equal(builder.phase, 'done');
  assert.deepEqual(manager.lookup('BJ'), []);
  assert.deepEqual(manager.lookup('SZ'), []);
  assert.deepEqual(manager.lookup('SH'), ['d2']);
  assertIndexMatches(manager, table, writeLog, field);
});

test('重复写入：相同 key 的重复/冗余写入幂等，索引无重复条目', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  await manager.put('r1', { city: 'BJ' });
  await manager.put('r2', { city: 'SH' });

  const builder = manager.startBuild({
    buildId: 'duplicate-writes',
    checkpointStore,
    pollIntervalMs: 2,
  });
  const runPromise = builder.run();

  for (let i = 0; i < 5; i += 1) {
    await manager.put('r1', { city: 'BJ' });
  }
  await manager.put('r2', { city: 'SH' });
  await manager.put('r2', { city: 'SH' });

  await builder.waitUntilCaughtUp();
  await builder.requestSwitch();
  await runPromise;

  assert.deepEqual(manager.lookup('BJ'), ['r1']);
  assert.deepEqual(manager.lookup('SH'), ['r2']);
  assertIndexMatches(manager, table, writeLog, field);

  const { OnlineIndexBuilder } = await import('../src/index.js');
  const bare = new OnlineIndexBuilder({
    buildId: 'dup-replay-check',
    table,
    writeLog,
    field,
    baseSnapshotLsn: -1,
    checkpointStore,
  });
  const firstEntry = writeLog.getEntry(0);
  assert.equal(await bare.applyLogEntry(firstEntry), true);
  const headEntry = { ...writeLog.getEntry(writeLog.headLsn), lsn: 0 };
  const reapplied = await bare.applyLogEntry(headEntry);
  assert.equal(reapplied, false);
  assert.equal(bare.appliedLsn, 0);
  assert.deepEqual(bare.index.lookup('BJ'), ['r1']);
});
