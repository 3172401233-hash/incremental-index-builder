import test from 'node:test';
import assert from 'node:assert/strict';
import {
  BuildAbortedError,
  LogGapError,
  OnlineIndexBuilder,
  UnappliedWritesError,
} from '../src/index.js';
import { assertIndexMatches, createHarness } from './support/helpers.js';

test('日志不连续：日志被截断后切换被拒绝，旧索引继续服务', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  for (let i = 0; i < 4; i += 1) {
    await manager.put(`g${i}`, { city: 'BJ' });
  }

  await manager.put('g4', { city: 'SH' });
  await manager.put('g5', { city: 'SZ' });
  const snapshotLsn = 3;

  const builder = new OnlineIndexBuilder({
    buildId: 'log-gap',
    table,
    writeLog,
    field,
    baseSnapshotLsn: snapshotLsn,
    checkpointStore,
    pollIntervalMs: 2,
  });
  const runPromise = builder.run();

  writeLog.truncateBefore(5);
  builder.requestSwitch().catch(() => {});
  await assert.rejects(runPromise, LogGapError);

  assert.notEqual(builder.phase, 'done');
  const saved = await checkpointStore.load('log-gap');
  assert.equal(saved.phase, 'catchup');
  await manager.put('g6', { city: 'HZ' });
  assertIndexMatches(manager, table, writeLog, field);
  assert.deepEqual(manager.lookup('HZ'), ['g6']);
});

test('无未应用写入：切换窗口内出现新写入时切换被拒绝并重试成功', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  for (let i = 0; i < 3; i += 1) {
    await manager.put(`w${i}`, { city: 'BJ' });
  }

  const attempts = { count: 0 };
  const builder = manager.startBuild({
    buildId: 'switch-window',
    checkpointStore,
    pollIntervalMs: 2,
    onSwitch: async (payload) => {
      attempts.count += 1;
      if (attempts.count === 1) {
        await manager.put('w9', { city: 'SH' });
      }
      await manager.performSwitch(payload);
    },
  });
  const runPromise = builder.run();

  await builder.waitUntilCaughtUp();

  const firstSwitch = builder.requestSwitch();
  await assert.rejects(firstSwitch, UnappliedWritesError);
  assert.notEqual(builder.phase, 'done');
  assert.deepEqual(manager.lookup('SH'), ['w9']);

  const retry = builder.requestSwitch();
  await runPromise;
  await retry;
  assert.equal(builder.phase, 'done');
  assert.equal(attempts.count, 2);
  assertIndexMatches(manager, table, writeLog, field);
});

test('回退流程：切换后可回退到旧索引，再重新前滚，两者都保持最新', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  await manager.put('o1', { city: 'BJ' });
  await manager.put('o2', { city: 'SH' });

  const builder = manager.startBuild({
    buildId: 'rollback',
    checkpointStore,
    pollIntervalMs: 2,
  });
  const runPromise = builder.run();
  await manager.put('o3', { city: 'BJ' });
  await manager.put('o1', { city: 'GZ' });
  await builder.requestSwitch();
  await runPromise;

  const newIndex = manager.activeIndex;
  assert.ok(manager.retainedIndex);
  assertIndexMatches(manager, table, writeLog, field);

  await manager.put('o4', { city: 'SZ' });
  await manager.rollback();
  assert.notEqual(manager.activeIndex, newIndex);
  assert.equal(manager.retainedIndex, newIndex);
  assertIndexMatches(manager, table, writeLog, field);
  assert.deepEqual(manager.lookup('SZ'), ['o4']);
  assert.deepEqual(manager.lookup('GZ'), ['o1']);

  await manager.put('o5', { city: 'HZ' });
  await manager.rollback();
  assert.equal(manager.activeIndex, newIndex);
  assertIndexMatches(manager, table, writeLog, field);
  assert.deepEqual(manager.lookup('HZ'), ['o5']);

  await manager.commitRetention();
  assert.equal(manager.retainedIndex, null);
  await assert.rejects(manager.rollback(), /没有可回退/);
});

test('AbortSignal：信号中止构建，checkpoint 保留且旧索引不受影响', async () => {
  const { field, table, writeLog, checkpointStore, manager } = createHarness();
  for (let i = 0; i < 6; i += 1) {
    await manager.put(`a${i}`, { city: 'BJ' });
  }

  const controller = new AbortController();
  const builder = manager.startBuild({
    buildId: 'abort',
    checkpointStore,
    checkpointEvery: 1,
    pollIntervalMs: 2,
    abortSignal: controller.signal,
  });

  const runPromise = builder.run();
  await new Promise((resolve) => setTimeout(resolve, 5));
  controller.abort();
  await assert.rejects(runPromise, BuildAbortedError);

  const saved = await checkpointStore.load('abort');
  assert.ok(['base', 'catchup'].includes(saved.phase));
  assertIndexMatches(manager, table, writeLog, field);
});

test('裸构建器：日志位置乱序到达时抛 LogGapError', async () => {
  const { field, table, writeLog, checkpointStore } = createHarness();
  table.put('x1', { city: 'BJ' });
  table.put('x2', { city: 'SH' });

  const builder = new OnlineIndexBuilder({
    buildId: 'bare-builder',
    table,
    writeLog,
    field,
    baseSnapshotLsn: -1,
    checkpointStore,
  });
  await builder.applyLogEntry({ type: 'put', key: 'x1', doc: { city: 'BJ' }, lsn: 0 });
  await assert.rejects(
    builder.applyLogEntry({ type: 'put', key: 'x2', doc: { city: 'SH' }, lsn: 2 }),
    LogGapError,
  );
  assert.equal(builder.appliedLsn, 0);
});
