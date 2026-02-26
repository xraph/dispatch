/**
 * DWP error classes.
 */

export class DispatchError extends Error {
  constructor(
    message: string,
    public readonly code?: number,
  ) {
    super(message);
    this.name = "DispatchError";
  }
}

export class AuthError extends DispatchError {
  constructor(message: string) {
    super(message, 401);
    this.name = "AuthError";
  }
}

export class ConnectionError extends DispatchError {
  constructor(message: string) {
    super(message);
    this.name = "ConnectionError";
  }
}

export class TimeoutError extends DispatchError {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}
