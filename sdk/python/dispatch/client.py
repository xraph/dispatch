"""Dispatch client — main entry point for the Python SDK."""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

from dispatch.frame import Method
from dispatch.transport import Transport


class Dispatch:
    """
    Dispatch is the main client for communicating with a Dispatch server
    via the Dispatch Wire Protocol (DWP).

    Example::

        async with Dispatch(url="wss://localhost:8080/dwp", token="dk_...") as client:
            job = await client.enqueue("send-email", {"to": "user@example.com"})
            run = await client.start_workflow("order-pipeline", {"order_id": "123"})

            async for event in client.watch(run["run_id"]):
                print(event)
    """

    def __init__(
        self,
        url: str,
        token: str,
        *,
        format: str = "json",
        reconnect: bool = True,
        max_retries: int = 0,
        timeout: float = 30.0,
    ) -> None:
        self._transport = Transport(
            url=url,
            token=token,
            format=format,
            reconnect=reconnect,
            max_retries=max_retries,
            timeout=timeout,
        )

    # ── Lifecycle ──────────────────────────────

    async def connect(self) -> str:
        """Connect and authenticate. Returns the session ID."""
        return await self._transport.connect()

    async def close(self) -> None:
        """Close the connection."""
        await self._transport.close()

    async def __aenter__(self) -> Dispatch:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @property
    def session_id(self) -> str:
        return self._transport.session_id

    # ── Jobs ───────────────────────────────────

    async def enqueue(
        self,
        name: str,
        payload: Any,
        *,
        queue: str | None = None,
        priority: int | None = None,
    ) -> dict[str, Any]:
        """Enqueue a job. Returns ``{"job_id", "queue", "state"}``."""
        data: dict[str, Any] = {"name": name, "payload": payload}
        if queue is not None:
            data["queue"] = queue
        if priority is not None:
            data["priority"] = priority
        resp = await self._transport.request(Method.JOB_ENQUEUE, data)
        return resp.data  # type: ignore[return-value]

    async def get_job(self, job_id: str) -> dict[str, Any]:
        """Get a job by ID."""
        resp = await self._transport.request(Method.JOB_GET, {"job_id": job_id})
        return resp.data  # type: ignore[return-value]

    async def cancel_job(self, job_id: str) -> None:
        """Cancel a job by ID."""
        await self._transport.request(Method.JOB_CANCEL, {"job_id": job_id})

    # ── Workflows ──────────────────────────────

    async def start_workflow(
        self,
        name: str,
        input: Any,  # noqa: A002
    ) -> dict[str, Any]:
        """Start a workflow run. Returns ``{"run_id", "name", "state"}``."""
        serialized = input if isinstance(input, str) else json.dumps(input)
        resp = await self._transport.request(Method.WORKFLOW_START, {"name": name, "input": serialized})
        return resp.data  # type: ignore[return-value]

    async def get_workflow(self, run_id: str) -> dict[str, Any]:
        """Get a workflow run by ID."""
        resp = await self._transport.request(Method.WORKFLOW_GET, {"run_id": run_id})
        return resp.data  # type: ignore[return-value]

    async def publish_event(self, name: str, payload: Any) -> None:
        """Publish an event to trigger workflow continuations."""
        serialized = payload if isinstance(payload, str) else json.dumps(payload)
        await self._transport.request(Method.WORKFLOW_EVENT, {"name": name, "payload": serialized})

    async def workflow_timeline(self, run_id: str) -> Any:
        """Get the step timeline for a workflow run."""
        resp = await self._transport.request(Method.WORKFLOW_TIMELINE, {"run_id": run_id})
        return resp.data

    # ── Subscriptions ──────────────────────────

    async def subscribe(self, channel: str) -> AsyncIterator[dict[str, Any]]:
        """
        Subscribe to a stream channel. Returns an async iterator that yields
        events as they arrive.

        Example::

            async for event in client.subscribe("queue:critical"):
                print(event)
        """
        await self._transport.request(Method.SUBSCRIBE, {"channel": channel})
        return _SubscriptionIterator(self._transport, channel)

    async def watch(self, run_id: str) -> AsyncIterator[dict[str, Any]]:
        """Watch a workflow run for real-time events."""
        return await self.subscribe(f"workflow:{run_id}")

    # ── Admin ──────────────────────────────────

    async def stats(self) -> Any:
        """Get broker and connection statistics."""
        resp = await self._transport.request(Method.STATS)
        return resp.data


class _SubscriptionIterator:
    """Async iterator backed by a subscription channel."""

    def __init__(self, transport: Transport, channel: str) -> None:
        self._transport = transport
        self._channel = channel
        self._queue: list[dict[str, Any]] = []
        self._event = __import__("asyncio").Event()
        self._done = False

        transport.on_event(channel, self._on_event)

    def _on_event(self, event: dict[str, Any]) -> None:
        self._queue.append(event)
        self._event.set()

    def __aiter__(self) -> _SubscriptionIterator:
        return self

    async def __anext__(self) -> dict[str, Any]:
        while not self._done:
            if self._queue:
                return self._queue.pop(0)
            self._event.clear()
            await self._event.wait()
        raise StopAsyncIteration

    async def unsubscribe(self) -> None:
        """Stop the subscription."""
        self._done = True
        self._event.set()
        self._transport.remove_event(self._channel)
        try:
            await self._transport.request(Method.UNSUBSCRIBE, {"channel": self._channel})
        except Exception:
            pass
