import { promises as fs } from 'node:fs';
import * as path from 'node:path';
import { InvalidCheckpointError } from './errors.js';

const CHECKPOINT_VERSION = 1;

function cloneState(state) {
  return JSON.parse(JSON.stringify(state));
}

/**
 * 内存 checkpoint 存储，内部走 JSON 序列化往返，
 * 与文件存储保持相同的可持久化语义（Map 等无法偷偷存活）。
 */
export class MemoryCheckpointStore {
  #data = new Map();

  async save(buildId, state) {
    this.#data.set(buildId, cloneState(state));
  }

  async load(buildId) {
    const state = this.#data.get(buildId);
    return state ? cloneState(state) : null;
  }

  async clear(buildId) {
    this.#data.delete(buildId);
  }
}

/**
 * 文件 checkpoint 存储：临时文件写入 + rename 原子替换，
 * 进程崩溃后仍可从最后一次完整落盘的 checkpoint 续建。
 */
export class FileCheckpointStore {
  #dir;
  #counter = 0;

  constructor(dir) {
    this.#dir = dir;
  }

  #filePath(buildId) {
    const safe = String(buildId).replace(/[^a-zA-Z0-9_.-]/g, '_');
    return path.join(this.#dir, `${safe}.checkpoint.json`);
  }

  async save(buildId, state) {
    await fs.mkdir(this.#dir, { recursive: true });
    const target = this.#filePath(buildId);
    const tmp = `${target}.tmp-${process.pid}-${this.#counter++}`;
    const payload = JSON.stringify({ version: CHECKPOINT_VERSION, buildId, state });
    await fs.writeFile(tmp, payload);
    await fs.rename(tmp, target);
  }

  async load(buildId) {
    let raw;
    try {
      raw = await fs.readFile(this.#filePath(buildId), 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
    let envelope;
    try {
      envelope = JSON.parse(raw);
    } catch (err) {
      throw new InvalidCheckpointError('checkpoint 文件损坏，无法解析', { cause: err });
    }
    if (
      envelope.version !== CHECKPOINT_VERSION ||
      envelope.buildId !== buildId ||
      !envelope.state
    ) {
      throw new InvalidCheckpointError('checkpoint 元数据不匹配');
    }
    return cloneState(envelope.state);
  }

  async clear(buildId) {
    try {
      await fs.rm(this.#filePath(buildId));
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
    }
  }
}
