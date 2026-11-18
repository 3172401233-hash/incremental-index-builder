import test from 'node:test';
import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  BuildAbortedError,
  FileCheckpointStore,
  OnlineIndexBuilder,
  SwitchCancelledError,
  Table,
  WriteLog,
} from '../src/index.js';
import { assertIndexMatches, createHarness } from './support/helpers.js';

test('checkpoint 中断：base 阶段崩溃后从游标续建，最终索引正确', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  for (let i = 0; i < 12; i += 1) {
    await manager.put(`k${String(i).padStart(2, '0')}`, { city: i % 3 === 0 ? 'BJ' : 'SH' });
  }

  const first = manager.startBuild({
    buildId: 'crash-base',
    checkpointStore,
    checkpointEvery: 1,
    readBatchSize: 2,
    pollIntervalMs: 2,
    crashInBaseAfter: 5,
  });
  await assert.rejects(first.run(), BuildAbortedError);
  assert.equal(first.phase, 'base');
  const saved = await checkpointStore.load('crash-base');
  assert.equal(saved.phase, 'base');
  assert.notEqual(saved.baseCursor, null);
  await manager.discardBuild();

  await manager.put('k12', { city: 'SZ' });
  await manager.delete('k00');

  const resumed = manager.startBuild({
    buildId: 'crash-base',
    checkpointStore,
    baseSnapshotLsn: first.baseSnapshotLsn,
    checkpointEvery: 1,
    readBatchSize: 2,
    pollIntervalMs: 2,
  });
  const runPromise = resumed.run();
  await resumed.waitUntilCaughtUp();
  await resumed.requestSwitch();
  await runPromise;

  assert.equal(resumed.phase, 'done');
  assertIndexMatches(manager, table, writeLog, field);
  assert.deepEqual(manager.lookup('SZ'), ['k12']);
  assert.deepEqual(manager.lookup('BJ').sort(), ['k03', 'k06', 'k09']);
});

test('checkpoint 中断：catchup 阶段崩溃后按 LSN 幂等重放', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  for (let i = 0; i < 6; i += 1) {
    await manager.put(`b${i}`, { city: 'BJ' });
  }

  const first = new OnlineIndexBuilder({
    buildId: 'crash-catchup',
    table,
    writeLog,
    field,
    checkpointStore,
    checkpointEvery: 1,
    readBatchSize: 1,
    pollIntervalMs: 2,
  });
  const snapshotLsn = first.baseSnapshotLsn;
  const firstRun = first.run();

  for (let i = 0; i < 6; i += 1) {
    await manager.put(`c${i}`, { city: i % 2 ? 'SH' : 'GZ' });
  }

  while (first.appliedLsn < snapshotLsn + 2) {
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  first.stop();
  await firstRun;
  assert.equal(first.phase, 'catchup');
  assert.ok(first.appliedLsn < writeLog.headLsn);

  await manager.put('d0', { city: 'SZ' });
  await manager.put('d1', { city: 'SZ' });

  const second = new OnlineIndexBuilder({
    buildId: 'crash-catchup',
    table,
    writeLog,
    field,
    baseSnapshotLsn: snapshotLsn,
    checkpointStore,
    checkpointEvery: 1,
    readBatchSize: 1,
    pollIntervalMs: 2,
    crashInCatchupAfter: 3,
  });
  await assert.rejects(second.run(), BuildAbortedError);
  assert.equal(second.phase, 'catchup');
  const midpoint = await checkpointStore.load('crash-catchup');
  assert.equal(midpoint.phase, 'catchup');
  assert.ok(midpoint.appliedLsn > snapshotLsn);

  const resumed = new OnlineIndexBuilder({
    buildId: 'crash-catchup',
    table,
    writeLog,
    field,
    baseSnapshotLsn: snapshotLsn,
    checkpointStore,
    checkpointEvery: 1,
    readBatchSize: 1,
    pollIntervalMs: 2,
  });
  const done = resumed.run();
  await resumed.waitUntilCaughtUp();
  await resumed.requestSwitch();
  await done;

  assert.equal(resumed.appliedLsn, writeLog.headLsn);
  assertIndexMatches(
    { lookup: (v) => resumed.index.lookup(v) },
    table,
    writeLog,
    field,
  );
});

test('文件 checkpoint：进程重启等价场景下可从落盘状态续建', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'idx-cp-'));
  try {
    const store = new FileCheckpointStore(dir);
    const writeLog = new WriteLog();
    const table = new Table(writeLog);
    for (let i = 0; i < 8; i += 1) table.put(`f${i}`, { city: 'BJ' });

    const snapshotLsn = writeLog.headLsn;
    const first = new OnlineIndexBuilder({
      buildId: 'file-build',
      table,
      writeLog,
      field: 'city',
      baseSnapshotLsn: snapshotLsn,
      checkpointStore: store,
      checkpointEvery: 2,
      readBatchSize: 2,
      pollIntervalMs: 2,
      crashInBaseAfter: 5,
    });
    await assert.rejects(first.run(), BuildAbortedError);

    const files = await fs.readdir(dir);
    assert.ok(files.includes('file-build.checkpoint.json'));
    assert.ok(!files.some((name) => name.endsWith('.tmp') || name.includes('.tmp-')));

    for (let i = 0; i < 4; i += 1) table.put(`g${i}`, { city: 'SH' });

    const resumed = new OnlineIndexBuilder({
      buildId: 'file-build',
      table,
      writeLog,
      field: 'city',
      baseSnapshotLsn: snapshotLsn,
      checkpointStore: store,
      checkpointEvery: 2,
      readBatchSize: 2,
      pollIntervalMs: 2,
    });
    const done = resumed.run();
    await resumed.waitUntilCaughtUp();
    await resumed.requestSwitch();
    await done;

    assert.deepEqual(resumed.index.lookup('BJ').sort(), ['f0', 'f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7']);
    assert.deepEqual(resumed.index.lookup('SH').sort(), ['g0', 'g1', 'g2', 'g3']);
    assert.equal(resumed.appliedLsn, writeLog.headLsn);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
});

test('切换取消：构建停止在 catchup，旧索引继续服务且保持完整', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  for (let i = 0; i < 5; i += 1) {
    await manager.put(`s${i}`, { city: 'BJ' });
  }

  const builder = manager.startBuild({
    buildId: 'cancel-build',
    checkpointStore,
    pollIntervalMs: 2,
  });
  const runPromise = builder.run();

  await manager.put('s5', { city: 'SH' });
  await manager.delete('s0');

  const switchPromise = builder.requestSwitch();
  builder.cancel('发布窗口取消');
  switchPromise.catch(() => {});
  await assert.rejects(runPromise, SwitchCancelledError);
  assert.equal(builder.phase, 'cancelled');

  assert.deepEqual(manager.lookup('BJ').sort(), ['s1', 's2', 's3', 's4']);
  assert.deepEqual(manager.lookup('SH'), ['s5']);

  await manager.put('s6', { city: 'BJ' });
  assertIndexMatches(manager, table, writeLog, field);
  assert.deepEqual(manager.lookup('BJ').sort(), ['s1', 's2', 's3', 's4', 's6']);

  await assert.rejects(builder.requestSwitch(), /已被取消/);
  await manager.discardBuild();
});

test('查询可见性：切换前只有完整旧索引，切换后只有完整新索引', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  for (let i = 0; i < 4; i += 1) {
    await manager.put(`v${i}`, { city: 'BJ' });
  }

  const builder = manager.startBuild({
    buildId: 'visibility',
    checkpointStore,
    pollIntervalMs: 2,
  });
  const runPromise = builder.run();

  await manager.put('v4', { city: 'SH' });
  await manager.put('v0', { city: 'SH' });

  await builder.waitUntilCaughtUp();
  assert.equal(builder.phase, 'catchup');
  assert.deepEqual(manager.lookup('BJ').sort(), ['v1', 'v2', 'v3']);
  assert.deepEqual(manager.lookup('SH').sort(), ['v0', 'v4']);
  assertIndexMatches(manager, table, writeLog, field);

  await builder.requestSwitch();
  await runPromise;

  assert.equal(builder.phase, 'done');
  assertIndexMatches(manager, table, writeLog, field);
  assert.deepEqual(manager.lookup('BJ').sort(), ['v1', 'v2', 'v3']);
  assert.deepEqual(manager.lookup('SH').sort(), ['v0', 'v4']);
});
