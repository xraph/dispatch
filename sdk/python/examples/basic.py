"""
Basic Dispatch Python SDK example.

Demonstrates connecting to a Dispatch server, enqueueing jobs,
starting workflows, subscribing to events, and publishing events.

Prerequisites:
    1. Start the kitchen-sink example server:
       cd _examples/kitchen-sink && go run .

    2. Run this example:
       python examples/basic.py

The Dispatch server should be running on ws://localhost:8080/dwp.
"""

import asyncio
import json

from dispatch import Dispatch

SERVER_URL = "ws://localhost:8080/dwp"
TOKEN = "demo-token"


async def main() -> None:
    # ── 1. Connect using async context manager ─────────
    async with Dispatch(url=SERVER_URL, token=TOKEN) as client:
        print(f"Connected! Session ID: {client.session_id}")

        # ── 2. Enqueue a job ───────────────────────────
        print("\n--- Enqueueing a job ---")
        job = await client.enqueue("send-email", {
            "to": "alice@example.com",
            "subject": "Hello from Python!",
        })
        print(f"Job enqueued: {json.dumps(job, indent=2)}")

        # Get job details.
        details = await client.get_job(job["job_id"])
        print(f"Job details: {json.dumps(details, indent=2)}")

        # ── 3. Enqueue with options ────────────────────
        print("\n--- Enqueueing with options ---")
        priority_job = await client.enqueue(
            "process-image",
            {"url": "https://example.com/photo.jpg", "width": 800, "height": 600},
            queue="images",
            priority=10,
        )
        print(f"Priority job: {json.dumps(priority_job, indent=2)}")

        # ── 4. Start a workflow ────────────────────────
        print("\n--- Starting a workflow ---")
        run = await client.start_workflow("order-pipeline", {
            "order_id": "PY-001",
            "items": ["laptop", "mouse", "keyboard"],
        })
        print(f"Workflow started: {json.dumps(run, indent=2)}")

        # Get workflow details.
        run_details = await client.get_workflow(run["run_id"])
        print(f"Workflow details: {json.dumps(run_details, indent=2)}")

        # ── 5. Get server stats ────────────────────────
        print("\n--- Server stats ---")
        stats = await client.stats()
        print(f"Stats: {json.dumps(stats, indent=2)}")

        # ── 6. Start a saga workflow ───────────────────
        print("\n--- Starting saga workflow ---")
        saga = await client.start_workflow("booking-saga", {
            "trip_id": "TRIP-PY-001",
        })
        print(f"Saga workflow: {json.dumps(saga, indent=2)}")

        # ── 7. Start a batch workflow ──────────────────
        print("\n--- Starting batch workflow ---")
        batch = await client.start_workflow("batch-process", {
            "item_ids": ["item-a", "item-b", "item-c"],
        })
        print(f"Batch workflow: {json.dumps(batch, indent=2)}")

        # ── 8. Subscribe to events ─────────────────────
        print("\n--- Subscribing to job events ---")
        events = await collect_events(client, "jobs", max_events=3, timeout=5.0)

        print(f"Received {len(events)} event(s):")
        for evt in events:
            print(f"  {json.dumps(evt, indent=2)}")

        print("\nAll operations completed successfully!")


async def collect_events(
    client: Dispatch,
    channel: str,
    max_events: int,
    timeout: float,
) -> list[dict]:
    """Subscribe to a channel and collect up to max_events, with a timeout."""
    events: list[dict] = []

    # Enqueue some jobs to generate events.
    await client.enqueue("send-email", {"to": "bob@example.com", "subject": "Event test 1"})
    await client.enqueue("send-email", {"to": "carol@example.com", "subject": "Event test 2"})
    await client.enqueue("send-email", {"to": "dave@example.com", "subject": "Event test 3"})

    sub = await client.subscribe(channel)

    async def _collect() -> None:
        async for event in sub:
            events.append(event)
            if len(events) >= max_events:
                break

    try:
        await asyncio.wait_for(_collect(), timeout=timeout)
    except asyncio.TimeoutError:
        pass  # Return whatever we collected.

    return events


if __name__ == "__main__":
    asyncio.run(main())
