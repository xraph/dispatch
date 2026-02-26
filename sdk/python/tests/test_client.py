"""Unit tests for the Dispatch client using a mock transport."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from dispatch.client import Dispatch, _SubscriptionIterator
from dispatch.errors import AuthError, ConnectionError, DispatchError, TimeoutError
from dispatch.frame import ErrorDetail, Frame, FrameType, Method


# ─── Mock Transport ─────────────────────────────────────────────


class MockTransport:
    """A mock transport that records calls and returns canned responses."""

    def __init__(self) -> None:
        self.connected = False
        self.session_id = "mock-session-123"
        self._responses: dict[str, Frame] = {}
        self._event_handlers: dict[str, Callable] = {}
        self._requests: list[tuple[str, Any]] = []

    async def connect(self) -> str:
        self.connected = True
        return self.session_id

    async def close(self) -> None:
        self.connected = False

    async def request(self, method: str, data: Any = None) -> Frame:
        self._requests.append((method, data))
        if method in self._responses:
            resp = self._responses[method]
            if resp.type == FrameType.ERROR:
                err = resp.error or ErrorDetail(code=500, message="unknown error")
                raise DispatchError(err.message, code=err.code)
            return resp
        # Default success response with empty data.
        return Frame(type=FrameType.RESPONSE, data={})

    def on_event(self, channel: str, handler: Callable) -> None:
        self._event_handlers[channel] = handler

    def remove_event(self, channel: str) -> None:
        self._event_handlers.pop(channel, None)

    def set_response(self, method: str, data: Any) -> None:
        """Configure a canned response for a method."""
        self._responses[method] = Frame(type=FrameType.RESPONSE, data=data)

    def set_error(self, method: str, code: int, message: str) -> None:
        """Configure an error response for a method."""
        self._responses[method] = Frame(
            type=FrameType.ERROR,
            error=ErrorDetail(code=code, message=message),
        )

    def emit_event(self, channel: str, event: dict[str, Any]) -> None:
        """Simulate an incoming event on a channel."""
        handler = self._event_handlers.get(channel)
        if handler:
            handler(event)


def make_client(transport: MockTransport | None = None) -> Dispatch:
    """Create a Dispatch client with a mock transport."""
    client = Dispatch(url="ws://test:8080/dwp", token="test-token")
    if transport is None:
        transport = MockTransport()
    client._transport = transport  # type: ignore[attr-defined]
    return client


# ─── Connection Tests ───────────────────────────────────────────


class TestConnection:
    async def test_connect_returns_session_id(self) -> None:
        transport = MockTransport()
        client = make_client(transport)
        session_id = await client.connect()
        assert session_id == "mock-session-123"
        assert transport.connected is True

    async def test_close_disconnects(self) -> None:
        transport = MockTransport()
        client = make_client(transport)
        await client.connect()
        await client.close()
        assert transport.connected is False

    async def test_context_manager(self) -> None:
        transport = MockTransport()
        client = make_client(transport)

        async with client:
            assert transport.connected is True
        assert transport.connected is False

    async def test_session_id_property(self) -> None:
        transport = MockTransport()
        client = make_client(transport)
        await client.connect()
        assert client.session_id == "mock-session-123"


# ─── Job Tests ──────────────────────────────────────────────────


class TestJobs:
    async def test_enqueue(self) -> None:
        transport = MockTransport()
        transport.set_response(Method.JOB_ENQUEUE, {
            "job_id": "job-001",
            "queue": "default",
            "state": "pending",
        })
        client = make_client(transport)
        result = await client.enqueue("send-email", {"to": "test@example.com"})

        assert result["job_id"] == "job-001"
        assert result["queue"] == "default"
        assert result["state"] == "pending"

        # Verify request was sent with correct data.
        method, data = transport._requests[-1]
        assert method == Method.JOB_ENQUEUE
        assert data["name"] == "send-email"
        assert data["payload"] == {"to": "test@example.com"}

    async def test_enqueue_with_options(self) -> None:
        transport = MockTransport()
        transport.set_response(Method.JOB_ENQUEUE, {
            "job_id": "job-002",
            "queue": "high",
            "state": "pending",
        })
        client = make_client(transport)
        result = await client.enqueue(
            "process-image",
            {"url": "img.png"},
            queue="high",
            priority=10,
        )

        assert result["queue"] == "high"
        method, data = transport._requests[-1]
        assert data["queue"] == "high"
        assert data["priority"] == 10

    async def test_get_job(self) -> None:
        transport = MockTransport()
        transport.set_response(Method.JOB_GET, {
            "id": "job-001",
            "name": "send-email",
            "state": "completed",
            "queue": "default",
        })
        client = make_client(transport)
        result = await client.get_job("job-001")

        assert result["id"] == "job-001"
        assert result["state"] == "completed"

        method, data = transport._requests[-1]
        assert method == Method.JOB_GET
        assert data["job_id"] == "job-001"

    async def test_cancel_job(self) -> None:
        transport = MockTransport()
        client = make_client(transport)
        await client.cancel_job("job-001")

        method, data = transport._requests[-1]
        assert method == Method.JOB_CANCEL
        assert data["job_id"] == "job-001"

    async def test_enqueue_error(self) -> None:
        transport = MockTransport()
        transport.set_error(Method.JOB_ENQUEUE, 400, "invalid job name")
        client = make_client(transport)

        with pytest.raises(DispatchError, match="invalid job name"):
            await client.enqueue("bad-job", {})


# ─── Workflow Tests ─────────────────────────────────────────────


class TestWorkflows:
    async def test_start_workflow(self) -> None:
        transport = MockTransport()
        transport.set_response(Method.WORKFLOW_START, {
            "run_id": "run-001",
            "name": "order-pipeline",
            "state": "running",
        })
        client = make_client(transport)
        result = await client.start_workflow("order-pipeline", {
            "order_id": "ORD-001",
        })

        assert result["run_id"] == "run-001"
        assert result["name"] == "order-pipeline"
        assert result["state"] == "running"

        method, data = transport._requests[-1]
        assert method == Method.WORKFLOW_START
        assert data["name"] == "order-pipeline"
        # Input is JSON-serialized.
        assert json.loads(data["input"]) == {"order_id": "ORD-001"}

    async def test_get_workflow(self) -> None:
        transport = MockTransport()
        transport.set_response(Method.WORKFLOW_GET, {
            "id": "run-001",
            "name": "order-pipeline",
            "state": "completed",
        })
        client = make_client(transport)
        result = await client.get_workflow("run-001")

        assert result["state"] == "completed"
        method, data = transport._requests[-1]
        assert method == Method.WORKFLOW_GET
        assert data["run_id"] == "run-001"

    async def test_publish_event(self) -> None:
        transport = MockTransport()
        client = make_client(transport)
        await client.publish_event("order-approved", {"order_id": "ORD-001"})

        method, data = transport._requests[-1]
        assert method == Method.WORKFLOW_EVENT
        assert data["name"] == "order-approved"
        # Payload is JSON-serialized.
        assert json.loads(data["payload"]) == {"order_id": "ORD-001"}

    async def test_workflow_timeline(self) -> None:
        transport = MockTransport()
        transport.set_response(Method.WORKFLOW_TIMELINE, [
            {"step_name": "validate", "created_at": "2025-01-01T00:00:00Z"},
            {"step_name": "process", "created_at": "2025-01-01T00:00:01Z"},
        ])
        client = make_client(transport)
        result = await client.workflow_timeline("run-001")

        assert len(result) == 2
        assert result[0]["step_name"] == "validate"

    async def test_start_workflow_error(self) -> None:
        transport = MockTransport()
        transport.set_error(Method.WORKFLOW_START, 404, "unknown workflow")
        client = make_client(transport)

        with pytest.raises(DispatchError, match="unknown workflow"):
            await client.start_workflow("nonexistent", {})


# ─── Subscription Tests ────────────────────────────────────────


class TestSubscriptions:
    async def test_subscribe_receives_events(self) -> None:
        transport = MockTransport()
        client = make_client(transport)

        sub = await client.subscribe("jobs")

        # Simulate events arriving.
        transport.emit_event("jobs", {"type": "event", "channel": "jobs", "data": {"job_id": "j-1"}})
        transport.emit_event("jobs", {"type": "event", "channel": "jobs", "data": {"job_id": "j-2"}})

        events: list[dict] = []
        async for event in sub:
            events.append(event)
            if len(events) >= 2:
                break

        assert len(events) == 2
        assert events[0]["data"]["job_id"] == "j-1"
        assert events[1]["data"]["job_id"] == "j-2"

        # Verify subscribe request was sent.
        method, data = transport._requests[0]
        assert method == Method.SUBSCRIBE
        assert data["channel"] == "jobs"

    async def test_watch_subscribes_to_workflow_channel(self) -> None:
        transport = MockTransport()
        client = make_client(transport)

        sub = await client.watch("run-001")

        # Verify the channel name format.
        method, data = transport._requests[0]
        assert method == Method.SUBSCRIBE
        assert data["channel"] == "workflow:run-001"

        # Simulate an event.
        transport.emit_event("workflow:run-001", {
            "type": "event",
            "channel": "workflow:run-001",
            "data": {"step": "validate", "status": "completed"},
        })

        events = []
        async for event in sub:
            events.append(event)
            if len(events) >= 1:
                break

        assert events[0]["data"]["step"] == "validate"

    async def test_unsubscribe_stops_iteration(self) -> None:
        transport = MockTransport()
        client = make_client(transport)

        sub = await client.subscribe("firehose")
        assert isinstance(sub, _SubscriptionIterator)

        # Unsubscribe should stop the iterator.
        await sub.unsubscribe()

        # Verify unsubscribe request was sent.
        method, data = transport._requests[-1]
        assert method == Method.UNSUBSCRIBE
        assert data["channel"] == "firehose"


# ─── Stats Tests ────────────────────────────────────────────────


class TestStats:
    async def test_stats(self) -> None:
        transport = MockTransport()
        transport.set_response(Method.STATS, {
            "connections": 5,
            "subscriptions": 12,
        })
        client = make_client(transport)
        result = await client.stats()

        assert result["connections"] == 5
        assert result["subscriptions"] == 12

        method, _ = transport._requests[-1]
        assert method == Method.STATS


# ─── Error Handling Tests ───────────────────────────────────────


class TestErrorHandling:
    async def test_dispatch_error_has_code(self) -> None:
        err = DispatchError("bad request", code=400)
        assert str(err) == "bad request"
        assert err.code == 400

    async def test_auth_error(self) -> None:
        err = AuthError("invalid token")
        assert err.code == 401
        assert isinstance(err, DispatchError)

    async def test_connection_error(self) -> None:
        err = ConnectionError("refused")
        assert isinstance(err, DispatchError)

    async def test_timeout_error(self) -> None:
        err = TimeoutError("took too long")
        assert isinstance(err, DispatchError)

    async def test_get_job_error_propagation(self) -> None:
        transport = MockTransport()
        transport.set_error(Method.JOB_GET, 404, "job not found")
        client = make_client(transport)

        with pytest.raises(DispatchError) as exc_info:
            await client.get_job("nonexistent")
        assert exc_info.value.code == 404


# ─── Frame Serialization Tests (for client context) ─────────────


class TestFrameSerialization:
    def test_request_frame_to_dict(self) -> None:
        frame = Frame(
            id="req-1",
            type=FrameType.REQUEST,
            method=Method.JOB_ENQUEUE,
            data={"name": "send-email", "payload": {"to": "user@example.com"}},
        )
        d = frame.to_dict()

        assert d["id"] == "req-1"
        assert d["type"] == "request"
        assert d["method"] == "job.enqueue"
        assert d["data"]["name"] == "send-email"

    def test_response_frame_from_dict(self) -> None:
        d = {
            "id": "resp-1",
            "type": "response",
            "correl_id": "req-1",
            "data": {"job_id": "j-001", "queue": "default", "state": "pending"},
            "ts": "12345",
        }
        frame = Frame.from_dict(d)

        assert frame.type == FrameType.RESPONSE
        assert frame.correl_id == "req-1"
        assert frame.data["job_id"] == "j-001"

    def test_error_frame_from_dict(self) -> None:
        d = {
            "id": "err-1",
            "type": "error",
            "correl_id": "req-1",
            "error": {"code": 404, "message": "not found", "details": ""},
            "ts": "12345",
        }
        frame = Frame.from_dict(d)

        assert frame.type == FrameType.ERROR
        assert frame.error is not None
        assert frame.error.code == 404
        assert frame.error.message == "not found"

    def test_event_frame_from_dict(self) -> None:
        d = {
            "id": "evt-1",
            "type": "event",
            "channel": "jobs",
            "data": {"job_id": "j-001", "state": "completed"},
            "ts": "12345",
        }
        frame = Frame.from_dict(d)

        assert frame.type == FrameType.EVENT
        assert frame.channel == "jobs"
        assert frame.data["state"] == "completed"


# ─── Multiple Operations Test ───────────────────────────────────


class TestMultipleOperations:
    async def test_sequential_operations(self) -> None:
        """Verify that multiple sequential operations work correctly."""
        transport = MockTransport()
        transport.set_response(Method.JOB_ENQUEUE, {
            "job_id": "j-001", "queue": "default", "state": "pending",
        })
        transport.set_response(Method.WORKFLOW_START, {
            "run_id": "r-001", "name": "test-wf", "state": "running",
        })
        transport.set_response(Method.STATS, {
            "connections": 1, "subscriptions": 0,
        })
        client = make_client(transport)

        # Run multiple operations sequentially.
        job = await client.enqueue("test", {})
        wf = await client.start_workflow("test-wf", {})
        stats = await client.stats()

        assert job["job_id"] == "j-001"
        assert wf["run_id"] == "r-001"
        assert stats["connections"] == 1

        # Verify all 3 requests were sent.
        assert len(transport._requests) == 3
        assert transport._requests[0][0] == Method.JOB_ENQUEUE
        assert transport._requests[1][0] == Method.WORKFLOW_START
        assert transport._requests[2][0] == Method.STATS
