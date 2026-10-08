# Durable execution

This document tracks the work needed to give Dispatch durable workflow execution,
deployment safety, and operational recovery. The existing checkpoint runner stays
available while the new runtime is built. You must select a supported execution
store explicitly; an unsupported store must never fall back to weaker guarantees.

## Execution contract

An execution belongs to a namespace and has a stable workflow ID and a run ID.
Only one open run may use a workflow ID in that namespace. An accepted transition
atomically appends ordered history, updates execution state, updates its task,
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
| Activity retries and timeout classes | Queue, attempt and overall deadlines, retry policies and interrupted-attempt recovery implemented; heartbeats and asynchronous completion open | Queue, attempt, overall and heartbeat deadlines; heartbeat progress; asynchronous completion |
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
- PostgreSQL supplies lease timestamps. Memory uses its own clock. Callers can
  supply absolute availability or relative delays and timeouts. They cannot supply
  lease time or an event sequence.
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

Next: implement heartbeat progress and asynchronous
completion. Durable execution visibility also needs store-level ordered
list/task reads and real Go dashboard contracts before the React plugin can show
these runs. Current dashboard workflow reads describe checkpoint runs only. Add
an execution list and detail view with ordered history, pending work and lease
state, explicit namespace filters, and distinct unsupported/error/empty states.
The rest of the requirements table remains authoritative and open.

## Activity lifecycle foundation

An activity needs a worker lease and separate execution/progress deadlines. A
lease renewal must not extend an activity timeout. Deadline enforcement uses
store time and runs after database locks are acquired, including when a worker
tries to publish a late success before a timeout processor reaches the task.

Task control extends the existing transaction. You can retain a task while
recording attempt state, release it for a durable retry, or finish it. Retained
and retried tasks keep their identity. Retry release invalidates the old token
immediately; the next claim increments its epoch and attempt. Progress is copied
into persisted task state. It is separate from external side effects.

Task versions protect observations. A timeout processor reads a task, then commits
with that version and an expired-deadline condition. If completion, retry or
progress changed the task first, the condition rejects the entire transition.
Cancellation and the timeout outcome then commit together. A normal lease renewal
does not change the task version because it does not extend the activity deadline.

Relative task availability and deadlines are resolved against one store timestamp
per transaction. A newly scheduled task's deadline starts at its availability;
a retained task's new deadline starts at the transition time. Retrying resolves
its next availability first, then its next queue deadline. Clearing a deadline
requires an explicit update. Existing tasks without deadlines retain their current
lease semantics.

These primitives precede activity policies and timeout processors. The next layer
must persist attempt starts before invoking handlers, distinguish queue/attempt/
overall/heartbeat timeouts, retain heartbeat details across attempts, record retry
backoff and failure classification, and support fenced asynchronous completion.
High-frequency heartbeat progress must not force one history event per heartbeat.

2026-10-08: shared memory and PostgreSQL tests cover retained grants, copied
progress, delayed retry, immediate rejection of released tokens, deadline expiry,
conditional cancellation and atomic rollback. PostgreSQL retains retry timing,
progress and deadlines after the connection pool is replaced. A receipt-insertion
failure proves that already-written history, progress, cancellation and new tasks
roll back together. Retrying the same request after that failure succeeds.

Independent review found that a terminal transition could wait for another task
lock after validating its source deadline, then accept an expired completion.
The regression failed before the fix. Terminal transitions now lock every pending
task before sampling database time. Both conditional cancellation and terminal
closure reject a source whose deadline expires during that wait.

After the review fix, make f, make l and go test ./... pass. Race tests pass for
engine, durable runtime and memory, and the full PostgreSQL integration suite
passes with the race detector. The reviewer confirmed the lock-order correction
and reported no further findings in the fix. Process-kill recovery and the
activity-policy layer remain unqualified.

## Activity attempts and retries

Use `ActivityWithOptions` for a recorded retry policy.
Version 1 `Activity` commands retain their existing behavior. New commands store
normalized options, so changing a policy for an existing run fails replay instead
of silently changing its recovery behavior.

Each attempt starts in history before external code runs. Failed attempts record
the failure and chosen delay while releasing the task for a persisted retry.
Exhaustion or a non-retryable failure publishes one final outcome and a workflow
wakeup in the same transaction. The default policy uses a one-second initial
interval, coefficient two, a maximum interval of 100 times the initial interval,
and unlimited attempts. Set maximum attempts to one to disable retries. Both
explicit non-retryable failures and configured error types stop retrying.

Attempt numbers count durable starts, independently from ownership claims. If a
replacement claims a task with an unfinished attempt, it records a worker-lost
failure and applies that attempt's retry policy before invoking external code
again. A claim lost before an attempt starts does not consume an activity attempt.
The idempotency key remains stable across attempts; an interrupted operation may
already have affected an external service.

This layer still needs heartbeat progress and asynchronous
completion. Unlimited retries also require the planned bounded-history and
continue-as-new work for sustained operation. No broad runtime qualification is
claimed by the activity retry implementation alone.

2026-10-08: runtime race tests cover exponential retry delays and caps, final
attempt limits, explicit and type-based non-retryable failures, stable operation
identity, concurrent result conflicts and replacement workers. Lost start,
failure and completion acknowledgements do not duplicate history. A handler does
not run when every start acknowledgement is lost. A claim lost before recording
a start does not consume an activity attempt.

Replay tests reject changed policies, skipped attempts, reused ownership epochs,
early retries, incorrect delays and mismatched final results. PostgreSQL tests
replace the connection pool after a reported failure and after an interrupted
attempt. Both retain retry timing and complete with one final outcome. These are
connection and worker replacement checks; process-kill qualification remains open.

The activity retry change passes make f, make l, go test ./..., engine/runtime/
memory race tests and the PostgreSQL durable integration suite with race detection.
Independent review of 070eead through 6ac3bf4 found no actionable correctness
issues in the retry layer and independently reran the runtime race tests. The
review did not qualify timeout processing, heartbeat progress, asynchronous
completion, dashboard flows, bounded history, process kills, failover or load.

## Activity timeout processing

Timeout ownership is separate from execution ownership. An expired
activity can be claimed for timeout processing within its namespace and pinned
build, regardless of its activity queue. This grant fences earlier workers and
has its own lease. Normal workers cannot publish late success before the timeout
processor arrives. Timeout grants can record a final result or release a retry;
they cannot turn an expired task back into a retained execution grant.

An absolute deadline limit caps relative queue and attempt deadlines. The overall
limit comes from the first schedule event's store timestamp. Backoff cannot move
it later. Starting an attempt atomically replaces its queue deadline and renews
its grant, so a short remaining queue deadline does not prematurely expire a
successfully started attempt.

Queue waiting ends at Dispatch's acknowledged durable attempt start. Queue and
overall expiry are final failures; attempt expiry follows the recorded retry
policy. Timeout processing does not need an activity handler. It records the
outcome and next work in one transaction. Handler cancellation remains
cooperative, while store fencing rejects all results from expired grants.

The coordinator classifies timeouts from recorded schedule, start and retry
history. Replay rejects early timeouts, incorrect timeout classes and results
that do not match the attempt. Zero-valued options preserve existing histories.
Heartbeat timeouts and asynchronous completion remain subsequent required work.

2026-10-08: the store timeout-grant layer passes shared memory/PostgreSQL
conformance, including early and concurrent claim rejection, namespace/build/type
filters, forged-token rejection, independent renewal and reclaim, retry release,
deadlines during backoff and atomic deadline replacement with lease renewal.
Timeout grant identity survives a replaced PostgreSQL connection pool, and schema
migration retries retain data. A captured pre-change request fingerprint remains
unchanged. Regressions also verify that renewal cannot shorten a grant and that a
timeout grant cannot commit against a future deadline after clock rollback.

Store-layer checks pass make f, make l, go test ./..., engine/runtime/memory race
tests and the PostgreSQL durable integration race suite. These checks cover the
store contract. Runtime behavior has separate evidence below.

`ActivityOptions.ScheduleToStartTimeout` bounds each queued attempt,
`StartToCloseTimeout` bounds one durable attempt, and `ScheduleToCloseTimeout`
bounds the entire activity, including queue waits and retry backoff. Zero disables
an individual timeout and preserves older version 2 commands. Set an attempt or
overall timeout for new workflows. All options are recorded and replay checked.

`Worker.Run` polls timeout work alongside workflow, activity and timer tasks.
`RunOnce` accepts `runtime.TaskTimeout` for controlled processing. A coordinator
needs the namespace and pinned build, but no activity handler or matching activity
queue. The activity's saved workflow queue receives its final wakeup. An overall
deadline wins when it equals another deadline. Attempt retries retain the same
operation identity and receive a fresh deadline at their next start.

Handler cancellation is cooperative. The local attempt timer starts before the
attempt-start write, so a slow acknowledgement can conservatively cancel a call.
Renewal polling also observes store deadline expiry. A handler that ignores
cancellation may keep affecting external services, but its late result cannot
advance the run. External idempotency remains required.

2026-10-08: runtime tests cover an offline activity queue, automatic timeout
polling, attempt timeout retries, rejection of late handler success, overall
expiry during execution and backoff, competing coordinators, and replacing a
queue deadline when an attempt starts. Replay rejects changed timeout options,
early or incorrect timeout records, and starts, successes or ordinary failures
after their allowed deadline. PostgreSQL repeats queue, attempt and overall
recovery after replacing the connection pool, using a separate coordinator queue
with no activity handlers. Attempt recovery completes on the next logical attempt.

The combined timeout implementation passes make f, make l, go test ./...,
engine/runtime/memory race tests and the PostgreSQL durable integration race suite.
These checks do not qualify process kills, database failover or sustained load.
Heartbeat progress, asynchronous completion and the rest of the roadmap remain open.

The independent timeout review found a worker-liveness bug: an overall timeout
could commit before a late handler returned and before renewal observed ownership
loss. The worker misclassified the valid completed history as corruption and
stopped all pollers. A regression reproduced that shutdown. The fix reports lease
loss for a superseded result, and the same worker now completes an unrelated
activity after the timeout wins. A second regression reproduced delayed claim
delivery after a replacement worker started; that path now reports lease loss too.

When an attempt or deadline disagrees with a valid history, the runtime checks the
current task grant before reporting corruption. A changed grant is normal
contention. An unchanged grant still reports the history error. This adds a store
read only on disagreement paths; malformed histories and mismatched commands
remain errors.

The review found no other actionable issues. It independently ran runtime and
memory race tests. Heartbeats, asynchronous completion, dashboard flows, bounded
history and other platform features remain outside this timeout review. Process
kills, database failover and sustained load remain unqualified.

After the review fixes, make f, make l (zero issues), go test ./..., the
engine/runtime/memory race suites and the PostgreSQL durable integration race
suite pass. The PostgreSQL suite completed in 27.001 seconds. This closes the
timeout implementation review; the qualification limits above still apply.

## Heartbeat storage contract

Heartbeats update the activity task's progress and progress deadline without
appending a workflow history event or changing its execution revision. Each
accepted request has an immutable receipt in the execution's existing receipt
namespace. Identical retries return that receipt after later progress, reclaim or
closure; changed content is rejected. Receipts still require retention for the
run's lifetime. This avoids history growth per heartbeat, but does not yet bound
receipt storage for a long-running activity.

The attempt-start transition enables heartbeat recording for its execution grant.
A zero heartbeat timeout permits progress recording without a progress deadline.
A positive timeout starts at the transition's store timestamp. The saved hard
limit is the earlier attempt/overall deadline from that same transition. Each
heartbeat can move the progress deadline only up to that limit and renews its
worker lease atomically, still capped by the effective deadline. Lease renewal
alone cannot move the progress deadline.

Heartbeat requests carry a positive consecutive sequence within their enabled
grant, copied progress bytes (at most 1 MiB), a request ID and the fenced task
token. The store validates ownership, expiry and ordering after acquiring locks.
A timeout grant cannot heartbeat. Retry release clears heartbeat timing and
sequence state while retaining the latest progress for the next attempt. A new
claim must be enabled by its own attempt-start transition before it can heartbeat.
Heartbeat mutations increment the task observation version.

The store API is trusted coordinator infrastructure. The following runtime layer
must bind heartbeat calls to the activity context, preserve unknown-outcome
requests, provide progress to replacement attempts and record the final heartbeat
checkpoint when classifying an activity result or heartbeat timeout. Store tests
alone do not establish those runtime behaviors.

2026-10-08: the memory and PostgreSQL implementations pass shared checks for
progress copying, consecutive and concurrent sequence handling, request conflict
rejection, receipt replay after reclaim and closure, unchanged workflow history,
task observation invalidation and heartbeat configuration immutability. Retry
retains progress and requires the replacement grant to enable heartbeat recording.
Tests also cover hard deadline caps, rejection after expiry and timeout grants,
clock reversal and sequence exhaustion.

PostgreSQL tests preserve progress and receipts across pool replacement and schema
migration retry. A lost acknowledgement returns the saved receipt without applying
progress twice. A heartbeat waiting on a task row lock past its deadline is
rejected without partial state. The full durable PostgreSQL race suite passes in
29.647 seconds. An earlier concurrent check run timed out in both existing activity
retry recovery cases, whose store calls have a 50 ms budget; those cases pass
unchanged in isolation. This timing sensitivity remains visible rather than being
treated as load qualification.

The heartbeat store changes pass make f, make l (zero issues), go test ./...
and engine/runtime/memory race tests. Runtime heartbeat APIs and timeout-history
validation remain required before activity heartbeat behavior is complete.
