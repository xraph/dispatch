"""Dispatch SDK error hierarchy."""

from __future__ import annotations


class DispatchError(Exception):
    """Base error for all Dispatch SDK errors."""

    def __init__(self, message: str, code: int = 0) -> None:
        super().__init__(message)
        self.code = code


class AuthError(DispatchError):
    """Authentication or authorisation failure."""

    def __init__(self, message: str = "authentication failed") -> None:
        super().__init__(message, code=401)


class ConnectionError(DispatchError):  # noqa: A001 — shadows builtin intentionally
    """WebSocket connection failure."""

    def __init__(self, message: str = "connection failed") -> None:
        super().__init__(message, code=0)


class TimeoutError(DispatchError):  # noqa: A001 — shadows builtin intentionally
    """Request timed out waiting for a response."""

    def __init__(self, message: str = "request timed out") -> None:
        super().__init__(message, code=0)
