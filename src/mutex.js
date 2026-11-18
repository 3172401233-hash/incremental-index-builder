export class AsyncLock {
  #queue = [];
  #locked = false;

  acquire() {
    if (!this.#locked) {
      this.#locked = true;
      return Promise.resolve(this.#release.bind(this));
    }
    return new Promise((resolve) => {
      this.#queue.push(() => resolve(this.#release.bind(this)));
    });
  }

  #release() {
    const next = this.#queue.shift();
    if (next) {
      next();
    } else {
      this.#locked = false;
    }
  }
}
