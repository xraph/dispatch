# Durable execution

This document tracks the work needed to give Dispatch durable workflow execution,
deployment safety, and operational recovery. The existing checkpoint runner stays
available while the new runtime is built. You must select a supported execution
store explicitly; an unsupported store must never fall back to weaker guarantees.

## Execution contract

An execution belongs to a namespace and has a stable workflow ID and a run ID.
Only one open run may use a workflow ID in that namespace. An accepted transition
atomically appends ordered history, updates execution state, finishes its task,
and schedules subsequent work. Every mutation has a request ID and a stored
receipt. Repeating the same request returns its original receipt. Changing its
contents while keeping its ID is an error.

Workers claim tasks with an owner and a monotonically increasing epoch. A worker
whose lease expired cannot renew or commit. Reclaiming a task increments its
epoch, even when the same worker claims it again. The store supplies the clock.
Task deadlines survive worker restarts. Terminal execution transitions cancel
pending execution tasks, and stale results cannot reopen a closed run.

Workflow code produces commands. Activities perform external work. Recovery
replays recorded decisions and results, with command validation and deterministic
time and concurrency APIs. External effects still need idempotency: a service
may accept a payment before its activity completion reaches Dispatch.

The execution store is an additional capability of a backend. Memory is a test
implementation. PostgreSQL is the first persistent implementation. Redis, MongoDB,
and SQLite need the same conformance and failure tests before they can advertise
this capability. Existing job leases, resource admission, artifact storage, and
execution isolation remain shared infrastructure.

## Required work and evidence

Each item stays open until its implementation and verification are recorded.
Store tests alone do not qualify a workflow runtime or a deployment.

| Requirement | Implementation status | Evidence required |
| --- | --- | --- |
| Atomic history, state, tasks, and durable receipts | Memory and PostgreSQL stores integrated with initial Go runtime | Shared memory/PostgreSQL conformance, rollback, concurrent writers, ambiguous-response retry |
| Fenced task claims and durable timer deadlines | Store contract and renewable workers implemented; process-kill qualification open | Expiry, same-owner reclaim, concurrent claims, restart recovery |
| Deterministic Go workflow runtime | Activity, timer and future replay implemented; SDK expansion open | Recorded-history replay with no repeated external effects, changed-command rejection |
| Activity retries and timeout classes | Open | Queue, attempt, overall and heartbeat deadlines; heartbeat progress; asynchronous completion |
| Signals, queries, updates and signal-with-start | Open | Namespace isolation, deduplication, atomic acceptance, update results, read-only queries |
| Child workflows and cancellation | Open | Stable child identity, duplicate creation prevention, parent-close policies, cancellation propagation |
| Compensation, pause, termination and reset | Open | Resumable compensation attempts, audited controls, immutable reset lineage |
| Continue-as-new and run chains | Open | Bounded history, message handoff and version inheritance |
| Schedules | Open | Overlap, catch-up, backfill, timezones and unique scheduled occurrences |
| Deployment versioning | Pinned build polling implemented; rollout and patch markers open | Pinned build routing, gradual rollout, patch markers, replay checks, drainage |
| Distributed scheduling | Open | Partition ownership, long polling, fairness, fleet-wide quotas and backpressure under load |
| Payloads and artifacts | Open | Codec/schema metadata, encryption, accepted-output publication, retention and archival |
| Namespace security and audit | Open | Authorization on every read/write/poll, scoped credentials, denial tests and audit delivery |
| Durable external deliveries | Open | Transactional outbox, retries, deduplication and visible delivery failures |
| Operations and Forge Dashboard | Open | Real contracts and React pages for histories, tasks, messages, deployments, schedules and controls; desktop/narrow verification |
| Recovery and disaster recovery | Open | Process kills, database failover, backup restore, replication, failover fencing, measured RPO/RTO |
| Developer tooling and SDKs | Open | Replay CLI, time skipping, failure injection, protocol/SDK compatibility and language coverage |
| Legacy migration | Open | Explicit execution mode, pinned legacy runs, validated migration boundaries without invented history |

## Delivery order

1. Implement the execution-store contract and shared conformance suite, then the
   PostgreSQL transaction and restart tests.
2. Connect workflow workers, activities and timers to that contract. Implement
   replay and deterministic SDK primitives with recorded-history fixtures.
3. Add messaging, lifecycle controls, children, compensation and schedules.
4. Add deployment routing, partitioning, security and durable deliveries.
5. Extend the Go dashboard contributor and the existing React Dispatch plugin as
   each operator capability becomes available. Use the existing contract transport,
   scope model and shared components. Keep unsupported, denied, empty and failed
   states distinct. Verify rendered desktop and narrow layouts.
6. Qualify restoration, replication, upgrades and sustained load; complete the
   SDK and migration matrix. Verification is part of every preceding step too.

## Decisions

- Work directly on main and preserve other changes. Run make l, make f, then
  make l and relevant tests before focused commits and pushes.
- Keep implementation plans in docs/superpowers and this tracked requirements
  record in docs/architecture so progress is available in a fresh checkout.
- The store boundary is trusted engine infrastructure, not a client command API.
  Authentication and semantic command validation belong to the coordinator.
- PostgreSQL supplies lease timestamps. Memory uses its own clock. Callers supply
  absolute task deadlines but cannot supply lease time or an event sequence.
- Receipts must survive at least as long as the run history. A successful receipt
  remains readable by an identical retry even after a task is reclaimed or the run
  closes. A new mutation must pass the current lease and revision checks.
- Dashboard discovery found the existing packages/plugin-dispatch React package
  and extension/contract Go contributor. Concurrent dashboard edits belong to
  other work and must be preserved.

## Verification record

2026-10-08: implementation started from Dispatch main at 6dfa43e. Docker is
available for PostgreSQL integration tests. No runtime, dashboard, load or
disaster-recovery qualification is claimed by this initial record.

2026-10-08: memory and PostgreSQL pass the shared execution-store suite, including
concurrent claims and completion, same-owner fencing, changed-request rejection,
namespace isolation and rollback after a late task collision. A PostgreSQL test
closes and replaces the connection pool before reading persisted history, receipts
and timer work. The full PostgreSQL integration suite and repository tests pass.

Independent review found a lease renewal that could outlive its grant while
waiting for a database row lock. A regression reproduced the error; renewal now
locks the execution and task before reading database time and checking expiry.
A separate regression proved migration retries failed after schema creation; the
migration now tolerates that retry without deleting existing execution data.
These are store-level checks. Process crash recovery, deterministic execution,
dashboard flows, failover and load qualification remain open.

## Initial Go runtime

Enable the runtime explicitly with `engine.WithDurableWorkflows`. Supply a
namespace, queue, build ID, worker owner and handler maps in `runtime.Options`.
`StartDurableWorkflow` accepts a `durable.StartRequest` containing the same routing
and stable workflow, run and request IDs. It persists work before execution starts.
Unsupported stores fail engine construction. Checkpoint workflows keep their
existing API and their existing guarantees.

Your workflow handler uses `Activity`, `Timer`, `Future.Get` and `Now`. Schedule
multiple futures before calling `Get` to run activities in parallel. An unresolved
future yields a decision. When a worker resumes the run, the handler replays from
its beginning and receives saved results. Changed command inputs, ordering, IDs,
activity types, queues and timer deadlines fail replay before new work is saved.

Activity handlers receive `ActivityInfo.IdempotencyKey()`, a stable key across task
attempts. Pass it to external services that support idempotency. A worker can lose
its lease after an external service accepts an operation, so exactly-once external
effects are not promised. Lease loss and shutdown cancel the activity context;
activity handlers must observe it. Arbitrary blocking Go code cannot be preempted.

Workers renew grants while processing. Result publication and the workflow wakeup
share one transaction. Unknown commit outcomes retry the same request, and an
explicit revision conflict reloads the latest history. Polls filter by pinned build
inside the store. Engine health surfaces a failed durable worker. Polling is bounded
and configurable; partition scheduling and long polling remain separate work.

2026-10-08: evaluator tests cover saved activity results and typed failures,
parallel futures, logical time, fixed timer deadlines, malformed histories,
changed or omitted commands, payload isolation and panics. Worker tests force
concurrent result revision conflicts, recover a lost commit response, check lease
renewal and loss, verify cross-queue wakeups, and stop without closing unfinished
runs. PostgreSQL repeats build-isolation and runtime recovery checks with a new
connection pool and worker. These checks do not qualify database failover,
process-kill recovery, activity timeout/retry policies or the remaining SDK APIs.

2026-10-08: engine integration at ebcb3e7 passes make f, make l, go test ./...,
and race tests across engine, durable runtime and memory. The PostgreSQL durable
suite passed with the race detector after build routing and runtime integration.
An independent review of 48c6a0b through ebcb3e7 found no actionable correctness
issues in this scoped runtime. Its reviewer reran focused runtime and engine race
tests; the PostgreSQL and full-suite evidence came from the implementation checks.

Next: implement persisted activity attempts, retry policies, timeout classes and
heartbeat progress. Durable execution visibility also needs store-level ordered
list/task reads and real Go dashboard contracts before the React plugin can show
these runs. Current dashboard workflow reads describe checkpoint runs only. Add
an execution list and detail view with ordered history, pending work and lease
state, explicit namespace filters, and distinct unsupported/error/empty states.
The rest of the requirements table remains authoritative and open.
