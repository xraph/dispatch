"""WebSocket transport with reconnection for DWP."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable

import websockets
import websockets.asyncio.client

from dispatch.errors import AuthError, ConnectionError, TimeoutError
from dispatch.frame import ErrorDetail, Frame, FrameType, Method, _generate_frame_id

logger = logging.getLogger("dispatch")

# Default request timeout in seconds.
_DEFAULT_TIMEOUT = 30.0

# Reconnection backoff.
_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 30.0


class Transport:
    """Low-level WebSocket transport for DWP frames."""

    def __init__(
        self,
        url: str,
        token: str,
        *,
        format: str = "json",
        reconnect: bool = True,
        max_retries: int = 0,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> None:
        self._url = url
        self._token = token
        self._format = format
        self._reconnect = reconnect
        self._max_retries = max_retries
        self._timeout = timeout

        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._session_id: str = ""
        self._connected = False

        # Request-response correlation: id → Future.
        self._pending: dict[str, asyncio.Future[Frame]] = {}

        # Subscription event handlers: channel → callback.
        self._event_handlers: dict[str, Callable[[dict[str, Any]], None]] = {}

        # Background read task.
        self._read_task: asyncio.Task[None] | None = None

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def connected(self) -> bool:
        return self._connected

    # ── Connect / Disconnect ───────────────────

    async def connect(self) -> str:
        """Open WebSocket, authenticate, start read loop. Returns session ID."""
        self._ws = await websockets.asyncio.client.connect(self._url)

        # Send auth frame.
        auth_frame = Frame(
            id=_generate_frame_id(),
            type=FrameType.REQUEST,
            method=Method.AUTH,
            data={"token": self._token, "format": self._format},
        )
        await self._ws.send(json.dumps(auth_frame.to_dict()))

        # Wait for auth response.
        raw = await self._ws.recv()
        resp = Frame.from_dict(json.loads(raw))

        if resp.type == FrameType.ERROR:
            err_msg = resp.error.message if resp.error else "auth failed"
            await self._ws.close()
            raise AuthError(err_msg)

        self._session_id = resp.data.get("session_id", "") if isinstance(resp.data, dict) else ""
        self._connected = True

        # Start background reader.
        self._read_task = asyncio.create_task(self._read_loop())

        return self._session_id

    async def close(self) -> None:
        """Gracefully close the connection."""
        self._connected = False
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
            self._read_task = None

        if self._ws:
            await self._ws.close()
            self._ws = None

        # Reject any pending requests.
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError("connection closed"))
        self._pending.clear()

    # ── Request / Response ─────────────────────

    async def request(self, method: str, data: Any = None) -> Frame:
        """Send a request frame and wait for the correlated response."""
        frame_id = _generate_frame_id()
        frame = Frame(
            id=frame_id,
            type=FrameType.REQUEST,
            method=method,
            data=data,
        )

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Frame] = loop.create_future()
        self._pending[frame_id] = fut

        try:
            await self._send(frame)
            resp = await asyncio.wait_for(fut, timeout=self._timeout)
        except asyncio.TimeoutError:
            self._pending.pop(frame_id, None)
            raise TimeoutError(f"{method} timed out after {self._timeout}s")
        finally:
            self._pending.pop(frame_id, None)

        if resp.type == FrameType.ERROR:
            from dispatch.errors import DispatchError

            err = resp.error or ErrorDetail(code=500, message="unknown error")
            raise DispatchError(err.message, code=err.code)

        return resp

    # ── Subscriptions ──────────────────────────

    def on_event(self, channel: str, handler: Callable[[dict[str, Any]], None]) -> None:
        """Register a handler for events on a channel."""
        self._event_handlers[channel] = handler

    def remove_event(self, channel: str) -> None:
        """Remove the event handler for a channel."""
        self._event_handlers.pop(channel, None)

    # ── Internals ──────────────────────────────

    async def _send(self, frame: Frame) -> None:
        if not self._ws or not self._connected:
            raise ConnectionError("not connected")
        await self._ws.send(json.dumps(frame.to_dict()))

    async def _read_loop(self) -> None:
        """Background task that reads frames and dispatches them."""
        backoff = _INITIAL_BACKOFF
        while self._connected:
            try:
                if not self._ws:
                    break
                raw = await self._ws.recv()
                frame = Frame.from_dict(json.loads(raw))
                backoff = _INITIAL_BACKOFF  # reset on success

                # Route the frame.
                if frame.type in (FrameType.RESPONSE, FrameType.ERROR):
                    fut = self._pending.get(frame.correl_id)
                    if fut and not fut.done():
                        fut.set_result(frame)
                elif frame.type == FrameType.EVENT:
                    handler = self._event_handlers.get(frame.channel)
                    if handler:
                        handler({"type": frame.type.value, "channel": frame.channel, "data": frame.data})
                elif frame.type == FrameType.PING:
                    pong = Frame(
                        id=_generate_frame_id(),
                        type=FrameType.PONG,
                        correl_id=frame.id,
                    )
                    await self._send(pong)

            except asyncio.CancelledError:
                break
            except websockets.exceptions.ConnectionClosed:
                if not self._reconnect or not self._connected:
                    break
                logger.warning("DWP connection lost, reconnecting in %.1fs …", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF)
                await self._try_reconnect()
            except Exception:
                logger.exception("DWP read loop error")
                if not self._reconnect or not self._connected:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF)
                await self._try_reconnect()

    async def _try_reconnect(self) -> None:
        retries = 0
        while self._connected:
            try:
                self._ws = await websockets.asyncio.client.connect(self._url)

                auth_frame = Frame(
                    id=_generate_frame_id(),
                    type=FrameType.REQUEST,
                    method=Method.AUTH,
                    data={"token": self._token, "format": self._format},
                )
                await self._ws.send(json.dumps(auth_frame.to_dict()))
                raw = await self._ws.recv()
                resp = Frame.from_dict(json.loads(raw))

                if resp.type == FrameType.ERROR:
                    logger.error("DWP reconnect auth failed")
                    return

                self._session_id = resp.data.get("session_id", "") if isinstance(resp.data, dict) else ""
                logger.info("DWP reconnected, session=%s", self._session_id)
                return

            except Exception:
                retries += 1
                if self._max_retries > 0 and retries >= self._max_retries:
                    logger.error("DWP max reconnect retries reached")
                    self._connected = False
                    return
                await asyncio.sleep(min(_INITIAL_BACKOFF * (2 ** retries), _MAX_BACKOFF))
