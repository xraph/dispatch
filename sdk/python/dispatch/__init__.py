"""
dispatch-sdk — Python client for the Dispatch Wire Protocol (DWP).

Example::

    from dispatch import Dispatch

    async with Dispatch(url="wss://api.example.com/dwp", token="dk_...") as client:
        job = await client.enqueue("send-email", {"to": "user@example.com"})
        run = await client.start_workflow("order-pipeline", {"order_id": "123"})

        async for event in client.watch(run["run_id"]):
            print(f"{event['type']}: {event}")
"""

from dispatch.client import Dispatch
from dispatch.errors import (
    AuthError,
    ConnectionError,
    DispatchError,
    TimeoutError,
)
from dispatch.frame import Frame, FrameType, Method

__all__ = [
    "Dispatch",
    "DispatchError",
    "AuthError",
    "ConnectionError",
    "TimeoutError",
    "Frame",
    "FrameType",
    "Method",
]
