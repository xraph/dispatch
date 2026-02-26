"""DWP frame types mirroring dwp/frame.go."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class FrameType(str, Enum):
    REQUEST = "request"
    RESPONSE = "response"
    EVENT = "event"
    ERROR = "error"
    PING = "ping"
    PONG = "pong"


class Method:
    """Well-known DWP method constants."""

    AUTH = "auth"

    JOB_ENQUEUE = "job.enqueue"
    JOB_GET = "job.get"
    JOB_CANCEL = "job.cancel"
    JOB_LIST = "job.list"

    WORKFLOW_START = "workflow.start"
    WORKFLOW_GET = "workflow.get"
    WORKFLOW_EVENT = "workflow.event"
    WORKFLOW_CANCEL = "workflow.cancel"
    WORKFLOW_TIMELINE = "workflow.timeline"
    WORKFLOW_REPLAY = "workflow.replay"

    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"

    CRON_LIST = "cron.list"
    DLQ_LIST = "dlq.list"
    DLQ_REPLAY = "dlq.replay"
    STATS = "stats"


@dataclass
class ErrorDetail:
    code: int = 0
    message: str = ""
    details: str = ""


@dataclass
class Frame:
    id: str = ""
    type: FrameType = FrameType.REQUEST
    method: str = ""
    correl_id: str = ""
    token: str = ""
    app_id: str = ""
    org_id: str = ""
    data: Any = None
    error: ErrorDetail | None = None
    channel: str = ""
    credits: int = 0
    ts: str = field(default_factory=lambda: _generate_frame_id())

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a dict suitable for JSON encoding."""
        d: dict[str, Any] = {"id": self.id, "type": self.type.value, "ts": self.ts}
        if self.method:
            d["method"] = self.method
        if self.correl_id:
            d["correl_id"] = self.correl_id
        if self.token:
            d["token"] = self.token
        if self.app_id:
            d["app_id"] = self.app_id
        if self.org_id:
            d["org_id"] = self.org_id
        if self.data is not None:
            d["data"] = self.data
        if self.error is not None:
            d["error"] = {
                "code": self.error.code,
                "message": self.error.message,
                "details": self.error.details,
            }
        if self.channel:
            d["channel"] = self.channel
        if self.credits:
            d["credits"] = self.credits
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Frame:
        """Deserialise from a dict (parsed JSON)."""
        err = None
        if "error" in d and d["error"]:
            err = ErrorDetail(
                code=d["error"].get("code", 0),
                message=d["error"].get("message", ""),
                details=d["error"].get("details", ""),
            )
        return cls(
            id=d.get("id", ""),
            type=FrameType(d.get("type", "request")),
            method=d.get("method", ""),
            correl_id=d.get("correl_id", ""),
            token=d.get("token", ""),
            app_id=d.get("app_id", ""),
            org_id=d.get("org_id", ""),
            data=d.get("data"),
            error=err,
            channel=d.get("channel", ""),
            credits=d.get("credits", 0),
            ts=d.get("ts", ""),
        )


def _generate_frame_id() -> str:
    """Generate a simple timestamp-based frame ID."""
    return f"{time.time_ns()}"
