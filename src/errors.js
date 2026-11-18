export class IndexBuildError extends Error {
  constructor(message, options = {}) {
    super(message);
    this.name = new.target.name;
    this.code = options.code;
    if (options.cause !== undefined) this.cause = options.cause;
  }
}

export class LogGapError extends IndexBuildError {
  constructor(message, options = {}) {
    super(message, { ...options, code: options.code ?? 'LOG_GAP' });
  }
}

export class UnappliedWritesError extends IndexBuildError {
  constructor(message, options = {}) {
    super(message, { ...options, code: options.code ?? 'UNAPPLIED_WRITES' });
  }
}

export class SwitchCancelledError extends IndexBuildError {
  constructor(message, options = {}) {
    super(message, { ...options, code: options.code ?? 'SWITCH_CANCELLED' });
  }
}

export class BuildAbortedError extends IndexBuildError {
  constructor(message, options = {}) {
    super(message, { ...options, code: options.code ?? 'BUILD_ABORTED' });
  }
}

export class InvalidCheckpointError extends IndexBuildError {
  constructor(message, options = {}) {
    super(message, { ...options, code: options.code ?? 'INVALID_CHECKPOINT' });
  }
}

export class InvalidStateError extends IndexBuildError {
  constructor(message, options = {}) {
    super(message, { ...options, code: options.code ?? 'INVALID_STATE' });
  }
}
