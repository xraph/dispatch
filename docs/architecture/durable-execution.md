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
| Deterministic Go workflow runtime | Activity, timer, signal, child and saved-winner selection replay implemented; coroutine and SDK expansion open | Recorded-history replay with no repeated external effects, changed-command rejection |
| Activity retries and timeout classes | Queue, attempt, overall and heartbeat deadlines, progress recovery, retry policies and asynchronous Go callbacks implemented; remote authorization and process qualification open | Queue, attempt, overall and heartbeat deadlines; heartbeat progress; asynchronous completion |
| Workflow deadlines and whole-workflow retries | Run/execution deadlines, fencing and atomic timeout closure implemented; runtime polling, replay and retries open | Expiry across lock waits, durable timeout closure, frozen replay, inherited execution deadlines across run chains |
| Signals, queries, updates and signal-with-start | Atomic signals, signal-with-start, Go receive replay and explicit/current/latest queries implemented; tracked updates open | Namespace isolation, deduplication, atomic acceptance, update results, read-only queries |
| Child workflows and cancellation | Individual future, whole-workflow and child cancellation plus child composition implemented; cooperative external-activity completion acknowledgment open | Stable child identity, duplicate creation prevention, parent-close policies, cancellation propagation |
| Compensation, pause, termination and reset | Parent-close termination implemented; operator controls, compensation, pause and reset open | Resumable compensation attempts, audited controls, immutable reset lineage |
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

Next: implement asynchronous completion. Durable execution visibility also needs store-level ordered
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

The activity retry change passes make f, make l, go test ./..., engine, runtime and
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

The independent review of 9ca2865 through 910601a found no actionable issues in
heartbeat storage. Its reviewer independently passed durable/runtime/memory race
tests, PostgreSQL heartbeat recovery and lock-expiry tests (4.665 seconds), and
shared PostgreSQL heartbeat conformance with migration retry (4.618 seconds).

The review does not close runtime heartbeat APIs, request serialization and
in-flight callback draining, progress delivery, final checkpoint replay or
heartbeat timeout classification. Asynchronous completion, receipt retention,
process-kill/failover/load qualification and Dashboard integration remain open.
One immutable receipt per acknowledged heartbeat is an explicit storage cost,
even though heartbeats do not append workflow events.

## Activity heartbeat runtime contract

Version 2 activities expose ActivityInfo.Heartbeat(ctx, details) and
HeartbeatDetails(). Details are copied. Heartbeat is synchronous and confirms
persisted progress; it does not silently buffer or throttle calls. An unknown
outcome retains the exact request until its receipt is resolved. Calls serialize
within the attempt. The callback is bound to the activity context and cannot keep
writing after the handler has returned. Result publication drains in-flight calls
before reading the final persisted checkpoint. It also guards the task version
read with that checkpoint: an uncertain server write may finish after its client
call returns. If progress changed, the result reloads the checkpoint before
committing. A receipt retry confirms the original write; it does not extend that
write's deadline again.

HeartbeatTimeout is recorded in ActivityOptions. Zero permits progress recording
without a progress timeout. A missed heartbeat follows the activity retry policy.
Automatic worker lease renewal cannot postpone it. The attempt and overall limits
still cap the progress deadline, with overall then attempt expiry winning ties.
The initial heartbeat clock begins at the durable attempt start.

New attempt-start events record that heartbeats were enabled and the copied
starting progress. Results and failed-attempt events record a final checkpoint
containing the store timestamp, ownership epoch, heartbeat sequence and details.
Old histories without heartbeat activation retain their existing interpretation.
Replay checks checkpoint presence, timing, shape, inherited progress, matching final failure
metadata and timeout classification. Heartbeats themselves do not add events.
A workflow can inspect a copied checkpoint through ActivityError while errors.As
still exposes its underlying ApplicationError.

The next attempt receives the last persisted progress after failure, timeout or
worker loss. Timeout coordinators need neither the activity handler nor its queue.
PostgreSQL pool replacement must preserve recovery behavior. This contract does
not qualify process kills, failover, sustained load or asynchronous completion.

2026-10-08: runtime tests pass for concurrent calls, lost acknowledgements,
caller cancellation before submission, callback expiry and result publication
while a heartbeat write is in flight. A failed write with an unknown outcome
keeps its request identity until resolution. A call cancelled before submission
cannot leave progress queued for a later call.

Heartbeat expiry retries while the worker still renews its lease. Periodic
heartbeats cannot postpone attempt or overall expiry. Replay rejects changed
heartbeat policy, invalid checkpoint times or ownership epochs, mismatched
inherited progress and contradictory final failure metadata. Older histories
without heartbeat metadata still pass the runtime suite.

PostgreSQL recovery tests replace the connection pool after ordinary failure,
heartbeat timeout and worker loss. The next attempt receives saved progress and
keeps its external idempotency key. A coordinator with no activity handlers can
publish the heartbeat timeout. The durable PostgreSQL race suite passes in
35.587 seconds. Repository tests, make f, make l (zero issues), and races across
engine (18.549 seconds), runtime (3.342 seconds) and memory (2.782 seconds) pass.

Independent review of 02d69a6 through 8f5666b reproduced a late heartbeat commit
between checkpoint capture and result publication. Execution revision alone could
not detect the changed progress. A retry then inherited details missing from its
failure event, making its history fail replay. The regression fails for success,
retry and a lost result acknowledgement before the fix. Final transitions now
guard the source task version and reload after a definite observation conflict.
Ambiguous commits still retry the identical request. The regression and focused
heartbeat race tests pass (1.990 seconds). The reviewer independently passed the
runtime race suite (3.333 seconds), reported no other findings and set no behavior
aside as outside the review.

After that fix, make f, make l (zero issues), go test ./..., and engine/durable/
memory race tests pass. The runtime race suite completes in 3.477 seconds and the
full durable PostgreSQL race suite in 36.246 seconds. These checks close the
scoped heartbeat implementation review. Asynchronous completion, receipt
retention, Dashboard flows and process-kill/failover/load qualification remain open.

## Asynchronous activity ownership contract

An activity can transfer its active attempt to an asynchronous grant in the same
transaction that records the handoff event. The transfer preserves the attempt's
epoch, progress and existing deadlines, increments its observation version, and
immediately fences the worker's execution token. Pollers must not reclaim an
asynchronous grant. It needs no worker renewal; its persisted activity deadline
determines when a timeout coordinator can take ownership. The lease timestamp
mirrors that deadline as a compatibility fence for the previous poller, which
does not inspect grant kind. Asynchronous heartbeats update both timestamps.

The coordinator creates a random 256-bit secret for each handoff. The task stores
only its SHA-256 digest, which is omitted from task JSON. Callback requests carry
the secret with the namespace, run and exact task grant. The store checks the
secret under the same locks as ownership, expiry and the mutation. Request
receipts contain only a fingerprint and result, never the raw secret. An identical
retry can recover its receipt after completion, retry release or timeout fencing.
Changing either the request content or its secret cannot reuse that receipt.

The handoff requires an active heartbeat-capable activity attempt and a finite
persisted activity deadline. Heartbeat-only deadlines are allowed. Asynchronous
heartbeats preserve consecutive request sequencing and can move the progress
deadline within the saved hard limit, without depending on a worker lease. Retry
release clears the asynchronous credential and preserves progress. Timeout
claims fence callbacks before publishing a timeout or scheduling another attempt.

These are trusted store primitives. The runtime must still provide token
generation and a serializable completion handle, coordinate handoff with worker
renewal and heartbeat calls, record and replay the handoff, and expose idempotent
completion/failure and heartbeat calls. The callback API needs namespace
authorization before it can be exposed remotely. Store conformance alone does
not qualify those runtime or transport behaviors.

2026-10-08: memory and PostgreSQL pass shared asynchronous-grant conformance for
handoff/result races, old worker fencing, proof and namespace rejection,
concurrent completion, heartbeat deadline caps, retry progress and credential
cleanup, and receipt recovery after closure, retry and timeout fencing.

PostgreSQL tests replace the connection pool and retry the migration with an
asynchronous grant already saved. Lost handoff, heartbeat and completion responses
recover their original receipts. A receipt-insertion failure rolls back the
handoff, history and new task together. Handoff, heartbeat and completion attempts
that wait on a task lock past the activity deadline are rejected without partial
state. The previous poller's SQL could reclaim a grant with an empty lease
timestamp; the regression passes with the activity-deadline compatibility fence.

The store implementation passes make f, make l (zero issues), go test ./...,
engine/durable/memory race tests and the full durable PostgreSQL race suite
(46.584 seconds). The focused PostgreSQL conformance and asynchronous recovery
suite passes in 15.125 seconds. Runtime handoff/replay and callback APIs remain
required before asynchronous activity completion is usable through the SDK.

Independent review of 7b887b7 through 1e0278d found no actionable issues in the
store contract. The reviewer independently passed durable/runtime/memory race
tests and PostgreSQL shared conformance plus asynchronous recovery tests
(15.188 seconds). Secret generation, completion handles, runtime handoff replay,
callback APIs and remote namespace authorization remain outside this store
qualification and required by the full implementation plan. The review does not
establish asynchronous feature readiness or Temporal parity.

## Stable callback intent receipts

A trusted coordinator may attach an intent digest to a transition receipt. The
digest binds the client's complete request, including its operation, namespace,
run, activity grant, callback secret and result or failure. It excludes the
execution revision and task observation that the coordinator derives while
preparing a transaction. The coordinator must compute this digest itself; a
remote caller cannot supply an authoritative digest.

LookupReceipt accepts the run identity, request ID and exact intent digest. It
returns the original receipt after state advances, closure or ownership changes.
A missing receipt returns found=false; a missing run returns ErrNotFound. A
different digest, or a receipt without an intent digest, returns
ErrRequestConflict. Lookup is read-only and does not validate current ownership
or extend a deadline. A missing receipt cannot prove that an in-flight request
will not commit, so callers must still resolve a concurrent request conflict.

CommitTransition keeps its exact-request fingerprint check. Rebuilding a
transaction under the same client intent never weakens that check: the runtime
uses LookupReceipt to resolve the accepted intent. The intent and receipt are
saved atomically with history, state and task changes. Existing requests omit
the new field and keep their original fingerprints. Older receipts remain
available through exact-request replay only. Intent digests are internal
recovery proofs and must not appear in public projections or logs.

The memory and PostgreSQL stores must exercise state advancement, closure,
namespace/run isolation, changed intents, concurrent requests, failed
transactions and connection replacement. The callback runtime still needs to
construct and resolve intents correctly; this primitive alone does not qualify
the SDK or remote authorization.

Custom durable.Store implementations must implement LookupReceipt. PostgreSQL
adds an intent column that rejects nulls and defaults to empty for older writers.
Downgrade locks the receipt table while checking for saved intents and refuses
to discard any retained proof. Removing those receipts needs an explicit
retention or migration policy.

2026-10-08: shared conformance covers recovery after closure, retry reclamation
and timeout fencing; identity and changed-intent rejection; concurrent requests;
and unchanged exact-request replay. PostgreSQL additionally verifies lost
completion responses across pool replacement, migration retry, legacy receipt
replay, rollback and downgrade protection. Lookup during a blocked completion
does not reveal uncommitted state. It observes the receipt after commit and no
receipt after rollback.

The repository test run exposed an existing heartbeat fixture that expected to
claim a retry scheduled one microsecond in the future immediately. A repeated
test reproduced it, as did the new receipt-retention fixture. Both now use an
already elapsed retry time; production scheduling is unchanged. The corrected
heartbeat case passes 200 runs and the receipt case passes 500 runs.

After that correction, make f, make l (zero issues), go test ./..., and engine,
durable, runtime and memory race tests pass. The full durable PostgreSQL race
suite passes in 52.569 seconds. Callback intent construction and runtime retry
resolution remain required before the SDK can recover callbacks through this API.

Independent review of 78c57ae through 0e2ab44 found one migration defect: an empty
schema before the receipt table's schema in search_path could make the downgrade
guard inspect the wrong schema and report success without removing the column.
The regression reproduces false success both with legacy-only receipts and with
retained intent receipts. The guard now resolves the same table as the migration
lock. Both regressions and the ordinary downgrade tests pass (5.469 seconds).

The reviewer independently passed durable/runtime/memory races and PostgreSQL
shared conformance plus intent tests (11.413 seconds). No other findings or
deferred minors were reported. Runtime intent construction, callback resolution,
credential issuance, remote authorization and the remaining durability roadmap
are still required. They were explicitly outside this store review, not removed
from the full implementation goal.

After the review fix, make f, make l (zero issues), go test ./..., and engine,
durable, runtime and memory race tests pass. The full durable PostgreSQL race
suite passes in 55.585 seconds. This closes the scoped store review and its
single corrective pass.

## Asynchronous activity runtime contract

A running version 2 activity can call ActivityInfo.DeferCompletion to persist a
handoff before dispatching external work. It returns a versioned, serializable
handle containing the namespace/run, pinned build, exact asynchronous grant,
secret and initial heartbeat sequence. The runtime generates the secret with a
cryptographic random source. Human-readable handle formatting redacts it; JSON
serialization deliberately carries it for delivery to a trusted recipient.

Handoff preserves the attempt and all deadlines. It requires a finite overall,
attempt or heartbeat deadline. The worker serializes handoff with renewal and
progress writes, records its checkpoint with a task observation guard, then
stops renewing without cancelling the handler. A result or panic after a
confirmed handoff cannot publish another outcome. Repeated handoff calls during
the handler return the same handle; retained callbacks expire when it returns.
Unknown handoff acknowledgements retain the exact request and handle until
resolved. Before renewing, the worker reconciles an uncertain request that was
already sent. Ordinary heartbeats return ErrHandoffPending without cancelling
delivery. A cancelled preparation that stayed unsent is discarded, and closing
the handler session prevents further background handoff writes. A crash before
handle delivery leaves the grant for deadline recovery.

CompleteAsyncActivity accepts a stable request ID and a result or application
failure. It uses the recorded retry policy and atomically publishes progress,
outcome or retry, and the next workflow task. It computes the complete client
intent before loading current state, resolves an accepted receipt first, and
checks again after a concurrent state or ownership conflict. Different client
intent cannot reuse an accepted request ID. IDs are unique within a run for each
callback operation and are prefixed separately from worker transition IDs.

HeartbeatAsyncActivity accepts the handle, request ID, consecutive sequence and
copied progress. It uses the persisted asynchronous deadline and zero worker
lease duration. Its exact receipt can be recovered after completion, retry or
timeout. The initial sequence in the handle is informational; callers coordinate
subsequent sequences separately. Current grant identity and secret authorize the
store mutation. Application errors retain their existing user-defined types.

Handoff history contains attempt identity and a checkpoint, never credentials.
Replay rejects duplicate, late or mismatched handoffs and final progress that
moves backward from the handoff checkpoint. Old histories remain valid. Store
timeouts still fence callbacks and preserve progress for retries. Worker APIs
check namespace/build routing; remote caller authorization remains required
before adding a transport endpoint.

Qualification requires callback completion before handler return, renewal and
heartbeat races, lost responses, concurrent identical and changed requests,
closure and timeout recovery, engine wrappers, a runnable SDK example, replay
corruption tests and PostgreSQL connection replacement. Process kills, failover,
sustained load and remote authorization retain their separate roadmap gates.

2026-10-08: the Go runtime and engine expose durable handoff, completion, failure
and external heartbeats. Race tests cover completion before handler return,
ignored results and panics after handoff, blocked renewal and progress writes,
late progress at publication, concurrent handoff and completion, terminal failure,
lost responses and exact receipt recovery after closure. Replay tests reject
contradictory handoff and checkpoint histories. PostgreSQL tests replace the
connection pool before callback delivery and again before receipt recovery after
closure; callback failures and heartbeat timeouts preserve progress for retry.

A routing regression showed that changing both a handle's build label and the
callback worker's label could bypass the heartbeat pin. Heartbeats now read the
execution's immutable build before mutation or receipt recovery. The regression
rejects the foreign build both before and after workflow closure. This adds one
execution read per heartbeat call. Completion already checks the recorded build
when publishing and binds it into its stable client intent.

The executable examples/durable-async uses memory and prints completed: paid.
Memory does not persist through process restarts. Handles carry callback secrets
in JSON, heartbeat producers must coordinate sequences, and a crash before handle
delivery relies on the saved deadline to recover. These trusted APIs do not yet
provide an authorized remote callback endpoint.

After the routing fix, make f, make l (zero issues), go test ./..., and engine,
durable, runtime and memory race tests pass. The full durable PostgreSQL race
suite passes in 60.087 seconds with no skipped tests. Independent review of this
runtime layer is the next qualification step; full Temporal parity is not claimed.

Independent review of 0184940 through 8d0cc4e found two Important defects and no
Critical or Minor findings. An accepted handoff with lost responses could leave
old-token renewal and ordinary heartbeats active; their lease-loss cancellation
then blocked recovery of the saved handle. The correction tracks sent requests,
reconciles their exact transaction before renewal and reports a pending handoff
to ordinary heartbeats. The second defect applied the 200-byte command ID limit
to build IDs, even though durable routing accepts 512 bytes. Handles now use the
existing durable build limit.

Regressions reproduce both cancellation paths, cancelled prepared requests and
unusable 201/512-byte build handles. Corrected tests include both accepted and
uncommitted unknown handoffs and preserve explicit retry after cancellation.
The added session state can cause bounded background retry traffic while an
acknowledgement remains unknown. It cannot send a cancelled unsent request.
The reviewer independently passed async race tests (2.738 seconds), reviewed
PostgreSQL evidence and declined to qualify process kills, failover, sustained
load, remote authorization or full parity. Those requirements remain open.

After the single corrective pass, make f, make l (zero issues), go test ./...,
and engine/durable/runtime/memory race tests pass. The full durable PostgreSQL
race suite passes in 61.243 seconds with no skipped tests, including lost handoff
response reconciliation before connection replacement. The example still prints
completed: paid. Both Important findings are fixed with reproducing regressions;
no deferred minor findings remain. There was no second independent review.

## Durable signal contract

Signals acknowledge durable acceptance, not completion of a handler. Queries are
read-only and updates have tracked results; both remain separate requirements.
This distinction follows the [Temporal message model](https://docs.temporal.io/encyclopedia/workflow-message-passing).
The [Go signal channel guidance](https://docs.temporal.io/develop/go/workflows/message-passing)
also makes buffering and explicit draining before closure the workflow author's
responsibility. Dispatch will preserve received messages in history even when
workflow code closes without consuming them.

SignalExecution targets an explicit run or the current open run when RunID is
empty. A new signal requires an open run and its pinned build. It appends a
versioned workflow.signal_received event, advances the execution revision and
inserts a workflow wakeup in one transaction. The wakeup uses the queue retained
on workflow:1; a callback client's queue cannot change workflow routing.
Names contain at most 200 bytes, request IDs and routing identifiers at most
512 bytes, and signal input at most 1 MiB. These limits apply before acceptance.

SignalWithStart atomically chooses the current open run or creates the proposed
run and accepts its first signal. Start.RequestID identifies this whole operation;
there is no separately accepted start request. Start.RunID proposes the identity
only when creation is needed. An existing run retains its original type, input
and queue, and must match the requested build. A closed proposed run cannot be
reused if no open run exists. The receipt returns the actual run and whether this
request created it. A new run begins at revision 1 with two history events and
one initial workflow task. An existing run advances by one revision and event.

Signal request IDs are unique across both signal operations within one namespace
and workflow ID. A workflow-level receipt binds the full request and operation
and records the actual target. An exact retry returns that receipt before current
run selection or lifecycle checks, even if the target has closed and another run
is now open. Changed content is a conflict. Retain these receipts at least as long
as the target run history. PostgreSQL serializes signal requests for one workflow
identity before taking the execution lock; ordinary starts still arbitrate through
the unique open-run constraint. No synthetic task lease is used for acceptance.

Workflow.ReceiveSignal(id, name) returns a deterministic future. Receives use
stable command IDs and names; matching messages are assigned in received order.
A consumed message is recorded with its receive command in the same transaction
as the workflow decision. Repeated Get returns the same copied input. Replay
reserves prior assignments, rejects duplicate or mismatched consumption and uses
the message's recorded receive time for Now, so committing consumption later
cannot move a timer's logical origin. Unconsumed signals remain buffered in
history. The combined new commands, consumptions and final state event must fit
the existing 1000-event transaction bound.

Qualification covers signals before and after a workflow waits, multiple names,
FIFO consumption, repeated Get, replay changes, concurrent acceptance and closure,
wrong namespace/build, lost responses, retries after a later run starts, atomic
signal-with-start, PostgreSQL rollback and connection replacement, engine calls
and a runnable example. Queries, updates, selectors, run-chain routing, remote
authorization, Dashboard messaging views and process/load qualification remain
required work until separately implemented and verified.

Signal store evidence: memory and PostgreSQL conformance cover receipt recovery
across run replacement, queue retention, conflicting requests and closure races.
PostgreSQL fault tests reject the final receipt insertion, reopen the pool after a
lost response, retry migrations and reject downgrade with retained receipts.
Runtime consumption was pending at the store checkpoint ccb9fa9.

You can receive a message with Workflow.ReceiveSignal("approval", "approve").Get().
Use a new command ID for the next receive. Worker.SignalExecution and
Worker.SignalWithStart accept trusted Go calls without a handler registration;
the engine exposes SignalDurableWorkflow and SignalWithStartDurableWorkflow.
The proposed queue and workflow type must fit the Go runtime's existing 200-byte
limit. Build and request IDs retain the store's 512-byte limit.

2026-10-08: signal runtime and engine regressions cover messages before and after
waiting, preserved assignments across activity waits, copied repeated results,
independent names, timer origins, changed commands and corrupt history. The parser
rejects duplicate, missing, mismatched and out-of-order consumption. Per-name queues
keep matching linear across the history. New commands and consumptions share the
999-event decision budget, with one additional event reserved for workflow state.

A forced concurrent signal invalidates a stale workflow commit; replay records
each assignment once. Client tests exhaust lost-response retries, preserve copied
request inputs and recover receipts after closure. PostgreSQL runtime tests reopen
the connection pool after lost acceptance responses, resume two ordered receives,
keep the original workflow queue and recover the receipt after a new run starts.
The development example at examples/durable-signals prints completed: approved.
Queries, updates, selectors, remote authorization, operator pages and process/load
qualification remain open. These checks do not establish full Temporal parity.

2026-10-08: signal commits ccb9fa9 and e539fc0 passed make f, make l with zero
issues, the full Go unit suite, engine/runtime/memory race checks and the full
PostgreSQL durable race suite (74.976 seconds at the runtime checkpoint).
A fresh independent review of both commits found no actionable defects and
independently passed the focused signal race tests. No corrective pass was needed.
The review does not qualify the open roadmap work or remove the current decision
and history limits. PostgreSQL pool recovery remains distinct from process-kill,
failover, load and disaster-recovery qualification.

## Durable query contract

You register a query with Workflow.SetQueryHandler(name, handler) before the
workflow can yield. The handler reads local state reconstructed by replay and
returns bytes or an error. QueryExecution requires a namespace, workflow ID, pinned
build and either an explicit run ID or a current/latest selector. It returns the run key, persisted state, revision,
last history sequence and copied query output. Names contain at most 200 bytes;
input and output each contain at most 1 MiB. Run and build identifiers retain the
store's 512-byte bounds. The Durable query target contract below defines selection
and migration behavior. Run chains remain separate required work.

The query reads one immutable history prefix through the sequence in its first
execution snapshot. Concurrent appends may produce a newer state, but cannot mix
new history with old response metadata. Replay may reconstruct local state from
accepted signals or completed activities before the next workflow decision is
committed. The response state and revision describe the persisted snapshot, not
a claim that those reconstructed decisions have been committed.

Queries never claim or renew tasks, consume persisted messages, append history,
write receipts or publish workflow decisions. Each invocation uses a fresh replay
instance. SDK command scheduling, future consumption and handler registration are
forbidden while the query handler runs, even if the handler recovers an internal
control-flow panic. Reading logical time is allowed. Arbitrary Go side effects
and noncooperative blocking cannot be sandboxed; query handlers must return
promptly and must not perform external work. The client checks cancellation before
reads, before invoking code and after code returns, without leaking background
query goroutines. Use the supplied Workflow and Future objects only within their
current evaluation. Do not copy or reset Workflow values, or retain runtime
objects for another evaluation; those operations are outside the SDK ownership
contract and can bypass instance-local guards.

Completed and failed executions can be queried when their saved terminal result
matches replay. Unknown query names, missing workflow code, wrong scope/build,
invalid history, changed decisions, programming errors and query panics fail
explicitly. Unsupported terminal history formats remain errors until their
lifecycle implementation exists. Application query errors return to the caller
without changing execution state. Query results are observations, so repeated
calls can return different revisions; they have no mutation request receipt.

Qualification requires fresh and waiting workflows, signal and activity state,
terminal success/failure, copied inputs/results, rejected SDK mutations, recovered
mutation attempts, panic/error paths, cancellation, identifier/payload boundaries,
concurrent appends during paged reads, engine integration and PostgreSQL recovery.
Remote authorization, tracked updates and operator query pages remain required
parts of the broader roadmap.

2026-10-08: query evaluator and client tests pass under the race detector. They
cover fresh/waiting state, accepted signals, completed activity results and terminal
success/failure, logical time, rejected query mutations, recovered mutation attempts,
invalid registrations, panics, errors and copied payloads. A read-only adapter
exposes only the real execution/history reads; query tests also compare the saved
projection, history and task state before and after each call.

Tests force a signal or closure between pages of a 1002-event snapshot. The first
query retains revision 3 and sequence 1002; the next sees revision 4 and sequence
1003. Cancellation before reads, during snapshot/replay and during the query returns
no successful result. PostgreSQL tests replace the pool before resuming a workflow
and after closure, query both completed and failed runs, and compare every persisted
task field through a digest. The development example prints pending then approved
without committing a workflow decision to answer either query.


2026-10-08: query commit 02eb107 passed make f, make l with zero issues, the
full Go unit suite, engine/runtime/memory race checks and the full PostgreSQL
durable race suite (76.559 seconds). A fresh independent review found no required
fixes and independently passed focused query, replay and closure race tests, plus
the runnable query example. The reviewer inspected the PostgreSQL tests and saved
results without rerunning that suite.

The review retains the trusted Go and per-evaluation ownership boundaries.
You must not treat the SDK guards as a sandbox for copied runtime objects,
external effects or noncooperative code. Current/latest run selection, tracked
updates, remote authorization, lifecycle controls and operator pages remain open.
Unsupported terminal formats fail explicitly. Pool replacement tests establish
connection recovery; process kills, failover, load and disaster recovery still
need their own qualification.


## Durable selection contract

You can race existing futures with Workflow.Select(id, futures...). The ID is a
unique command ID within the run. Select returns the winning candidate pointer;
call Get to read its copied output or recorded failure. If no candidate is ready,
the workflow yields. Schedule candidates before selecting them. Each call accepts
1 through 1000 distinct, non-nil activity, timer or signal futures belonging to
this evaluation. Reusing a ready future in a later Select is allowed; remove prior
winners yourself when draining a set. Selection never cancels losing work.

A version 1 select command records ordered candidate IDs. It creates no polled
task. The workflow.selected event records a version 1 Selection with CommandID
and FutureID. The winner, any signal consumption, new commands and workflow state
commit atomically under the existing revision, lease and receipt checks.
Replay uses the recorded winner. Changing candidate order, identity or membership
fails command validation even when the old winner remains present.

For a new selection, choose the ready candidate whose availability event has the
lowest history sequence. Signal availability uses message arrival; activity and
timer availability use their final outcome. Candidate order breaks a tie when
two signal futures could consume the same message. Select consumes only its winning
signal and advances logical time to the winning outcome, even before Get. A failed
activity is ready; its error is returned by Get. Losing signals remain buffered,
and losing activities and timers retain their existing lifecycle.

History validation requires each candidate to name a prior non-selection command.
It rejects duplicate candidates, unknown/cyclic references, invalid versions or
fields, repeated winners, winners outside the candidate set and missing outcomes
at the selection event. An unresolved selection cannot precede further recorded
commands. Terminal replay cannot invent an unrecorded winner. New commands,
signal consumptions and selections share the 999-event decision budget; the final
state event uses the remaining slot. The 100000-event history bound stays in force.

Queries may reconstruct a selection privately but must not commit it. Calling
Select from a query handler is prohibited, including after recovered control-flow
panics. Runtime objects retain the existing per-evaluation ownership contract.
This primitive does not supply cancellation, deterministic goroutines, tracked
update handlers or child workflows; those remain required roadmap work.

Qualification requires approval/timeout races in both orders, multiple ready
candidates, later losing results without a changed branch, failed activities,
repeated selection and signal FIFO, logical time, copied results, invalid future
ownership, changed/corrupt history, decision limits and query guards. Real memory
and PostgreSQL workers must preserve selection through response loss, revision
conflict, worker/pool replacement and terminal query replay without selector tasks.


2026-10-08: selector replay and worker race tests pass. They cover both approval
and timeout winners, multiple ready activities ordered by sequence, recorded
failures, stable logical time after later losing results, same-name signal ties,
FIFO consumption and repeated selection. Invalid futures, changed candidate lists,
corrupt histories, query mutation attempts and exact candidate/decision limits
fail explicitly. Terminal replay rejects an unrecorded winner.

The memory worker test forces a stale decision with a concurrent signal, then
loses the successful selection acknowledgement. Its identical retry leaves one
winner and no selector task. A replacement worker processes the losing timer and
finishes with the original branch. PostgreSQL tests pass in both event orders,
replace the pool before selection and after its commit, recover a lost response,
and retain a losing signal for later consumption. Terminal query results agree
with the saved branch and leave the projection, history and tasks unchanged.
The development example prints approved. These checks do not qualify process-kill,
failover, sustained-load or disaster-recovery behavior.


2026-10-08: selector commit 22ea219 passed make f, make l with zero issues,
the full Go unit suite, engine/runtime/memory race tests and the full PostgreSQL
durable race suite (78.801 seconds). A fresh independent review found no defects.
The reviewer independently passed the focused selector/signal/query/replay race
suite and the real PostgreSQL selector recovery tests (4.377 seconds).

Selection retains the existing lifecycle: it leaves losers active while the run
continues, and run closure cancels pending tasks under the store contract.
Individual cancellation, deterministic coroutines, tracked updates, children and
remote authorization remain open. The review does not qualify process-kill,
failover, sustained-load or disaster-recovery behavior.


## Individual future cancellation contract

You request cancellation with Workflow.Cancel(id, target). The ID is a stable,
unique command ID and the target must be an activity, timer or signal-receive
future from this evaluation. The returned future acknowledges the persisted
cancellation decision with nil output and nil error. Wait for it before assuming
the request took effect. A result or signal consumption already recorded in the
same decision wins; cancellation preserves that result. Repeated requests with
different command IDs acknowledge without replacing an earlier result.

Cancellation has a persistence boundary. Scheduling Cancel does not immediately
resolve the target. The workflow decision records the cancel command, a version 1
workflow.future_cancelled event and, when still running, a workflow wakeup in one
transaction. Cancellation records contain CommandID, TargetID, Cancelled, Attempt
and an optional Heartbeat checkpoint. Cancelled distinguishes a newly fenced
future from an already-resolved target. No cancellation task is polled.

For an unfinished stored activity or timer, the coordinator reads its task version
and cancels it through CommitRequest.Conditions and CancelTasks. A changed version
or execution revision causes a new snapshot and replay. Unknown responses retry
the identical request. A task scheduled and canceled in the same decision is never
published. A canceled receive does not consume a buffered signal. Canceling an
already-resolved future is a recorded no-op. Closing the workflow retains the same
atomic cancellation behavior, including the target's recorded outcome.

After acknowledgment, a canceled target's Get returns CancelledError, matching
ErrCancelled through errors.Is. Activity cancellation retains its attempt and last
persisted heartbeat checkpoint; each returned error contains a copy. A pending
retry retains its prior failed-attempt checkpoint. Ordinary activity attempts and
callbacks cannot publish over that outcome, while retries of already-accepted
callback requests still recover their original receipts. Cancel does not by itself
set the whole workflow state to cancelled. You can handle the error and continue;
an unhandled future error follows the existing workflow failure path.

Logical time advances only when you consume recorded cancellation or acknowledgment
outcomes. Their store event time and sequence are authoritative. The acknowledgment
can participate in Select. A target and its cancellation acknowledgment share the
same availability sequence, so candidate order breaks that tie. Query code cannot
call Cancel or consume its future. The existing trusted-Go and per-evaluation
ownership restrictions still apply.

The history parser validates cancel targets against prior cancellable commands,
requires exactly one acknowledgment for every saved cancel command, and rejects
inconsistent dispositions, overwritten outcomes, malformed attempt/checkpoint data
and later activity or timer outcomes after cancellation. Cancel commands reserve
both their command and acknowledgment events within the shared 999-event budget.

The acknowledgment proves storage fencing, not that an external system stopped.
Running workers receive context cancellation when renewal or heartbeat observes
lost ownership. Cooperative completion acknowledgment, whole-workflow cancellation,
child propagation, pause, termination, compensation and reset remain required
lifecycle work. Process interruption, failover and production qualification remain
open too.

Qualification requires same-decision and stored targets; queued, running, retrying
and asynchronous activities; timers and buffered receives; both sides of completion
races; task-version and heartbeat changes; lost responses; preserved callback
receipts; copied progress; cancellation/selector logical time; malformed history;
query guards; memory and real PostgreSQL recovery with replacement workers/pools.

## Individual future cancellation verification

Replay tests cover acknowledgment timing, preserved completions, buffered signals,
repeated requests, changed targets, malformed history, copied active/retry progress,
selector ties, query guards and the two-event cancellation budget. Memory workers
cover same-decision suppression, stored targets, active contexts, retries and
asynchronous activity cancellation. They inject claims, completion, retry activation
and heartbeats around cancellation, then drop an accepted response to verify the
identical retry and a single recorded acknowledgment. A separate snapshot race
restarts an asynchronous attempt between history and task reads; the coordinator
reloads the advanced revision instead of reporting corrupt progress.

Real PostgreSQL tests exercise same-decision activity/timer suppression, a queued
timer, retained signal input, active and asynchronous activity progress, completion
and heartbeat races, lost acknowledgments and connection-pool replacement. A fresh
worker reconstructs the saved outcome through a terminal query. A losing callback
is fenced; retrying a completion accepted before cancellation recovers its receipt.
The active-handler case checks that its stale result never enters history.

The development example at examples/durable-cancellation prints cancelled after a
timer's acknowledgment. The checkpoint runs make f, make l, the repository suite,
engine/runtime/memory races and the full durable PostgreSQL integration suite.
These checks qualify the scoped cancellation paths. They do not establish physical
external interruption, cooperative completion acknowledgment, whole-workflow or
child cancellation, process-kill recovery, fleet-scale behavior or disaster recovery.
Those requirements remain open in the roadmap above.

An independent review of the individual cancellation implementation found no
critical, important or minor issues. Its cancellation, selection, signal and query
race checks passed separately. The reviewer inspected the PostgreSQL tests and
used the executor's passing PostgreSQL evidence; it did not rerun that suite.
Physical interruption and cooperative completion acknowledgment, the remaining
workflow/child lifecycle controls, process kills, fleet-scale behavior and disaster
recovery remain outside this checkpoint's evidence and open in the full roadmap.

## Whole-workflow cancellation contract

RequestCancelExecution accepts a CancelExecutionRequest with an explicit namespace,
workflow ID, pinned build, request ID and optional reason. An empty run ID selects
the current open run. IDs contain at most 512 bytes; a reason contains at most 4096
UTF-8 bytes and no NUL. The store appends workflow.cancellation_requested and a
workflow task atomically with its receipt. Receipts are scoped by namespace and
workflow ID within the cancellation API, survive closure and run replacement, and
bind the entire original request. Exact retries recover the original target;
changed retries fail. A different request ID records another request while the
run is open. The first accepted request controls cancellation and its reason.
A receipt proves acceptance, not completion or physical interruption.

Cancellation runs in three durable phases: acceptance, fencing, and cleanup.
The first workflow decision after acceptance records workflow.cancellation_started,
fences all previously pending tasks, and creates one cleanup workflow task in the
same transaction. CommitRequest.CancelPendingTasks performs this atomic fence on
existing tasks only; newly created tasks in that transaction stay runnable. It
requires a completed source task and uses the execution revision and source grant.
PostgreSQL locks all affected tasks before reading its clock. Rejected or failed
commits leave history, state, task versions and receipts unchanged. A lost response
retries the identical request. Closure still fences all pending work as before.

Normal workflow code is reconstructed from the history prefix before the first
cancellation request. It may replay saved commands and results, but cannot publish
new normal work beyond that prefix. This fixed boundary preserves the captured
state used for cleanup even if an activity finishes between acceptance and fencing.
The main handler must still match every command in that prefix. Go effects remain
trusted: neither cancellation nor replay can preempt arbitrary blocking Go code.

Register an optional SetCancellationHandler before the first workflow command to
handle requests accepted before the initial decision. Registration later in the
normal path is available only if frozen replay reaches it. The handler receives the same Workflow and the first ExecutionCancellation payload. Cleanup
runs only after the saved fencing event, uses ordinary durable activities, timers,
signals and selectors, and resumes after worker or process replacement through
replay. IDs remain unique across the normal and cleanup phases. Logical time starts
from the later of reconstructed normal time and the saved fencing event time.
Unresolved normal activity/timer/receive futures report ErrWorkflowCancelled;
results already recorded before fencing remain readable. The handler can use the
same captured state and query closures. It cannot register another cancellation
handler, and queries cannot register one either.

Without a handler, the run closes as cancelled. A handler returning
ErrWorkflowCancelled closes as cancelled too; returning nil completes normally,
and another error fails the run. A cancelled terminal event references the first
accepted request, has no output, and remains queryable through validated replay.
Individual ErrCancelled errors retain their existing meaning and do not silently
become whole-workflow cancellation. Cleanup must use explicit control flow, not
Go defers that run during the replay engine's internal yielding panic.

The history parser rejects repeated request IDs, malformed request/start/terminal
payloads, start without acceptance, changed fencing boundaries, normal workflow
decisions between acceptance and fencing, later stale outcomes, and cancelled projection
mismatches. Pending normal selectors may be interrupted at the fencing boundary;
cleanup selections retain ordinary saved-winner semantics. Other messages accepted
while cleanup runs remain buffered and can be consumed by cleanup.

Qualification requires shared memory/PostgreSQL request and fencing conformance,
namespace/build and payload validation, concurrent and changed receipts, closure
and replacement races, rollback after receipt failure, migration retry/downgrade
protection, default cancellation, cleanup success/failure/cancellation, frozen
normal replay, selector interruption, buffered signals, active/async fencing,
accepted callback receipt recovery, lost responses, replacement workers/pools,
terminal queries, malformed histories and query guards. This does not finish child
propagation, cooperative activity completion acknowledgment, administrative
termination/reset/pause, remote authorization, or production failure qualification.

The request and fencing store layer has shared memory/PostgreSQL conformance for
receipt identity, distinct requests, concurrent duplicates, target/build isolation,
limits, closure races and retained cleanup work. PostgreSQL cases cover receipt
recovery after pool/run replacement, acceptance and fencing rollback after injected
receipt failures, protected migration retry/downgrade, and a source deadline that
expires while fencing waits on a target-task lock. Runtime and engine APIs connect
those phases to deterministic cleanup. Memory and PostgreSQL worker tests cover
default cancellation and cleanup that succeeds,
fails or closes cancelled, plus query replay, async receipt recovery and late-result
fencing. PostgreSQL tests replace connection pools before fencing, after fencing
and after cleanup results; they also inject a winning callback between the fencing
snapshot and commit, then require revision-conflict recovery. Lost acceptance,
fencing and terminal responses must retry their exact request contents.

Use `go run ./examples/durable-workflow-cancellation` to run the development memory
example. It requests cancellation before the first workflow decision, runs a cleanup
activity and checks the cancelled terminal state. These checks do not establish
process-kill recovery, database failover, physical activity interruption or production
readiness. Those qualification tasks remain open above.

The whole-workflow cancellation checkpoint passed final formatting, lint with zero
issues, the full Go suite, engine/runtime/store race tests and the full durable
PostgreSQL integration race suite (108.783 seconds). A fresh independent review of
bbe74ee..0807fd7 found no critical, important or minor issues. The reviewer reran
focused runtime/engine and memory race checks; PostgreSQL and full-repository
results came from the implementation run.

The review retained the limits above. Child propagation and administrative controls
remain required work. Task fencing does not confirm physical interruption or
cooperative completion, and these trusted Go APIs require caller authorization
before remote exposure. Arbitrary Go effects, blocking and deferred workflow cleanup
remain prohibited. Worker and connection-pool replacement tests do not qualify
process kills, database failover, fleet scale or disaster recovery.

## Durable child workflow contract

A child is a separate execution in its parent's namespace, with its own history,
build pin, queue and run identity. Child creation must atomically commit the parent
command, a child-started event, the immutable parent/child relationship, the child's
initial history/task/receipt and the parent decision receipt. Existing executions
cannot be adopted as children. A conflicting workflow ID rejects the whole decision.
Exact retries recover the original children, including after either execution closes.

A child relationship records the parent run and command ID, the full child start
request, the parent's decision queue and one explicit parent-close policy: terminate,
request_cancel or abandon. The default runtime policy is terminate. The child run ID
is derived deterministically from the parent run and command ID; an optional child
workflow ID lets you choose business identity without allowing an existing run to be
silently reused. Child inputs are limited to 1 MiB. Store identifiers remain bounded
at 512 bytes; the Go command API keeps its existing 200-byte limits.

You must await the child's started acknowledgment before allowing an abandoned child
to outlive its parent. A child command first issued in a terminal decision does not
create an execution. Waiting for a child result yields until a recorded child terminal
outcome arrives. Successful output, application failure, cancellation, termination
and timeout remain distinct outcomes. Selectors can wait on child results. Queries
replay started acknowledgments and results without creating or controlling children.

Child terminal results and parent-close actions use durable delivery records. Record
them in the same transaction as source closure; claim them independently of source
execution state. A delivery lease has a monotonically increasing epoch, store-clock
expiry, retry receipts and namespace/build routing. Applying a delivery changes only
its target execution and the delivery record, so a closed child can notify an open
parent without requiring recursive execution locks. A closed parent is recorded as
an ignored result delivery, not reopened. No accepted lifecycle message is lost when
a worker crashes between source commit and target delivery.

Parent closure enqueues the recorded close policy for each still-open child. Abandon
leaves the child running. Request_cancel accepts the child's normal cleanup request.
Terminate records a terminal event, fences pending work and enqueues that child's own
result and descendant close actions. Cascade delivery is durable and bounded per
transaction; it must not recursively lock or execute a workflow tree. Whole-workflow
cancellation fencing requests cancellation of existing children before cleanup can
create new children. Cleanup may wait for their actual terminal results. A separate
CancelChild command acknowledges durable acceptance at the child and preserves any
winning completion; it does not claim physical interruption.

Relationship reads expose parent identity, child identity, build pins, queues, close
policy and current child state. History exposes creation, cancellation acknowledgments
and outcomes. Operator pages must eventually use those contracts with scoped access
and visible delivery state; the existing Operations and Forge Dashboard requirement
remains open until backend transport and React flows are implemented and verified.

Qualification requires memory/PostgreSQL conformance for atomic multi-child creation,
exact and conflicting retries, same child identity races, namespace/build isolation,
source-grant expiry while waiting for child identity locks, receipt/link insertion
rollback, migration retry and protected downgrade. Delivery qualification must include
expired/reclaimed workers, source/target closure races, lost responses, queue and build
routing, result-versus-cancel races, cascading parent policies and pool replacement.
Runtime qualification must cover deterministic IDs, changed replay, early parent close,
started acknowledgment, failures and every terminal state, cancellation during normal
execution and cleanup, selectors, read-only queries, and actual replacement-worker
recovery. Store creation alone does not qualify the child runtime or lifecycle delivery.

Child creation and relationship storage are implemented in memory and PostgreSQL.
Shared tests cover batch atomicity, existing-identity rejection, original receipts,
parent and reverse lookup, pagination, namespace/build isolation, source grant and
queue checks, input copies and concurrent creators. PostgreSQL tests cover pool/run
replacement, relationship and final receipt rollback, migration retry/protected
downgrade, source expiry after identity-lock waits, and receipt recovery while child
identity locks are held.

Lifecycle deliveries and parent-close policies are implemented in both stores. You
can inspect pending and completed deliveries by source run. Claims route to the
target build, and each application saves a receipt with its original target and
an applied or ignored_closed disposition. Explicit child cancellation uses the
existing cancellation receipt scope, then queues a separate acknowledgment for
the parent. A cancellation fence excludes children created by the same cleanup
decision.

The delivery table references its source execution. It has no target foreign key:
inserting that reference would lock the target while the source is closing, which
can deadlock concurrent parent and child closures. The immutable relationship
retains both identities. Any future retention implementation must preserve pending
delivery targets before removing those relationships.

Shared tests cover expired and reclaimed leases, duplicate applications, payload
copies, routing, cancellation receipt conflicts, winning child completion and
termination cascades. PostgreSQL fault tests reject source outbox writes, target
events, delivery receipts, descendant results and cancellation acknowledgments;
each failed transaction leaves its target unchanged and succeeds on retry. Pool
replacement recovers delivery receipts after both runs close and a replacement run
starts. Lock tests verify expiry after a target-row wait and closure without waiting
on the other execution. Migration tests protect pending messages and saved receipts.

The Go runtime exposes `ChildWorkflow`, `Future.Started` and `CancelChild`. You can
start children in parallel, select their results, or await a saved startup before
returning with an abandon policy:

```go
child := w.ChildWorkflow("shipment", "ship", input, runtime.ChildOptions{
    Queue: "shipping",
    BuildID: "shipping-v1",
    ParentClosePolicy: durable.ParentCloseAbandon,
})
if _, err := child.Started(); err != nil {
    return nil, err
}
return nil, nil // The acknowledged child continues after this parent closes.
```

Empty build and queue inherit the parent. Identity derives from the parent run and
command ID, and replay checks the saved name, input, identity, build, queue and close
policy. A definitive creation conflict records `ChildStartFailure` and wakes the
parent. Other children in the decision can still start. `ChildWorkflowError` retains
the child identity and terminal state, with an application cause you can inspect
through `errors.As`; `errors.Is(err, runtime.ErrChildStart)` identifies a child that
was never created. Cancelling that failed start returns the original creation error.

`Worker.Run` polls lifecycle deliveries alongside workflow, activity, timer and
timeout tasks. It retries the same delivery request after an ambiguous response and
surfaces errors to its supervisor. Engine startup and shutdown own that poller too.
You can run `go run ./examples/durable-children` for a memory-backed example.

Replay tests cover startup acknowledgment, changed commands, selectors, query mutation
guards and distinct child terminal results. Cancellation cleanup can await a child's
real result. Forced termination reconstructs the recorded prefix for queries while
suppressing new decisions, including before cancellation fencing and during cleanup.
Worker tests cover close policies, unacknowledged parent completion, conflicting
creation batches, cancellation of failed starts, separate acceptance and completion,
and exact delivery retries. PostgreSQL replacement tests lose creation and delivery
responses, replace connection pools, and recover the original identity and receipt.
Live PostgreSQL workers and the engine complete parent/child workflows through their
normal pollers.

Child result decoding includes timed_out, but workflow deadline scheduling and
continue-as-new ownership remain separate open work. Operator transport, Dashboard
flows, remote authorization and production failure/load qualification also remain
open.

2026-10-08: the complete child workflow change passed independent review through
7683dbd with no blocking findings. The reviewer independently ran the focused child
suites and checked the PostgreSQL fault and replacement evidence. Final author
checks passed make f, make l, go test ./..., engine/runtime/memory race tests and
the full durable PostgreSQL integration race suite (130.982 seconds). The runnable
example completed both parent and child.

This evidence does not qualify workflow deadlines, timed-out query reconstruction,
run chains, operator transport, remote authorization, Dashboard flows, process kills,
failover, fleet load, disaster recovery or physical interruption of external work.
Future retention must preserve pending delivery targets before changing relationship
retention. These remain required work in the roadmap.

## Durable query target contract

You can select an explicit run, the current running run, or the latest created run.
An empty selector keeps the explicit-run API. Current and latest selectors require
an empty RunID, a namespace, a workflow ID and an explicit build. A build mismatch
must fail before replay; it must not select an older run with compatible code.

Every successful start, including signal-with-start and child creation, must update
a durable latest pointer in the same transaction as its history, tasks and receipts.
Closure retains that pointer. Retrying an old request returns its receipt without
changing latest identity. Store timestamps do not establish run ordering.

Resolution fixes one execution snapshot. History reads use its exact run ID and
last sequence even if that run closes or another starts during the query. The result
identifies the saved run, state, revision and history bound. Queries write nothing.

For databases created before latest pointers, migration can identify a lone run or
the unique running run. Multiple closed runs have no proven order, so latest queries
must return ErrAmbiguousRun. You can query those runs explicitly. A subsequent new
start establishes the latest pointer. Migration retries must preserve known pointers,
and downgrades must not discard populated pointers.

Memory and PostgreSQL now implement ResolveExecution and atomic latest pointers.
PostgreSQL maintains the pointer through an insertion trigger, including for writers
that predate this API. Migration locks execution writes while backfilling and
installing the trigger. Shared tests cover selection, lifecycle states, identity
reuse, copies, namespace isolation and concurrent starts. Database tests cover head
and later-event failures in every creation path, older writers, timestamp reversal,
connection replacement, migration retry, ambiguity and protected downgrade.

2026-10-08: store checks passed make f, make l, go test ./..., engine/runtime/memory
race tests and the full durable PostgreSQL integration race suite (139.127 seconds).
The Go runtime and engine now accept QueryRequest.Selection. You can request a
current or latest snapshot without supplying RunID:

```go
result, err := worker.QueryExecution(ctx, runtime.QueryRequest{
    Key: durable.Key{Namespace: "orders", WorkflowID: "order-42"},
    Selection: durable.RunLatest,
    BuildID: "orders-v1",
    Name: "status",
})
```

Use durable.RunCurrent for an open run, or omit Selection and set RunID for an
explicit run. The pure EvaluateQuery API still requires an explicit identity
because its caller already supplies the snapshot. A wrong build returns an error
before replay. It never falls back to an older compatible run.

Runtime tests expose only resolution and history reads, so any mutation or second
projection lookup fails. They cover scope/build/handler errors, input isolation,
closed runs and replacements after selection. PostgreSQL tests close the selected
run, create its replacement, replace the connection pool and then read history;
the result retains the original key, state, revision and sequence boundary.
Operator transport, remote authorization and Dashboard flows remain required work.

2026-10-08: runtime selection passed make f, make l, go test ./..., engine, runtime and
memory race tests and the full durable PostgreSQL integration race suite (147.767
seconds). Focused query checks also passed after the final lint edits. Independent
review through 4a44a4f found no actionable issues and independently passed runtime,
engine and memory query races plus focused PostgreSQL migration, recovery,
older-writer and query-target races (7.993 seconds).

Separate migration/runtime database roles still need deployment qualification.
Runtime grants must permit selected reads and atomic head writes. Retention and
historical imports also need explicit contracts: protect retained latest identities
and preserve intended creation order when importing historical runs. This checkpoint
does not qualify those operations or the remaining lifecycle and operator roadmap.

## Workflow deadline contract

RunTimeout limits one run. ExecutionTimeout sets the deadline that future retry
and continue-as-new chains must inherit. Chain creation and inheritance remain
separate roadmap work. Zero means unlimited; positive values must be at least one
microsecond. The store resolves both against its creation timestamp and persists
the absolute deadlines. The earlier deadline wins, with execution taking precedence
on a tie. You cannot extend either deadline through a normal mutation.

Once that deadline elapses, claims, lease renewals, heartbeats, signals,
cancellation requests and transitions must reject new progress. An identical
request with an existing receipt still returns its saved outcome. Input acceptance
uses store time after ownership locks. PostgreSQL guards also fence older writers.
A child delivery to an expired running target is acknowledged as ignored_expired;
its timeout processor owns the terminal outcome.

A separate namespace poller will claim expired executions without requiring their
original workflow build or handler. Timeout closure must save its typed terminal
event, projection, pending-task fencing, child lifecycle deliveries and receipt
atomically. Queries will replay the saved prefix without generating more work.
External activities can continue outside Dispatch until they observe cancellation;
a rejected completion does not undo an external side effect.

Implementation is in progress. Deadline persistence and enforcement, durable
closure, and runtime polling have separate validation gates. This contract does
not yet establish a usable workflow timeout service or run-chain support.

2026-10-09: memory and PostgreSQL persist deadlines in ordinary starts,
signal-with-start and child creation. The earliest deadline fences new progress;
exact receipt retries retain their original result. Schema guards reject older
worker updates and prevent deadline changes. Tests reproduce and reject late
completion after history insertion crosses the deadline, and expiry while renewal,
completion, signal and cancellation wait for locks. Tiny timeouts still allow
atomic creation. Migration retries retain deadlines, populated downgrade fails,
and deadlines survive connection-pool replacement. Child deliveries acknowledge
expired targets without advancing their history. Automatic timeout closure and
runtime replay remain the next implementation gates.

This store checkpoint passes make f, make l, go test ./..., engine/runtime/memory
race tests and the full PostgreSQL durable race suite (154.797s). An earlier full
race run hit the existing activity-recovery test's 50 ms store timeout; that test
then passed three isolated runs without changes before the full suite passed.
The deadline plan's independent review follows its closure and runtime tasks.

## Durable workflow timeout closure

ClaimExecutionTimeout polls expired executions within one namespace. It uses an
independent owner, epoch, attempt and lease, so the original workflow build does
not need a worker. Concurrent processors skip owned rows. A replacement can claim
an expired timeout lease, including with the same owner name, but receives a new
epoch. Claims leave execution revision and history unchanged.

ApplyExecutionTimeout accepts that grant and a stable request ID. It locks the
execution and pending tasks, then checks store time. Closure atomically appends a
workflow.timed_out event with its version, timeout kind and absolute deadline;
updates the projection; fences pending tasks; publishes child results and
parent-close deliveries; consumes the timeout grant; and saves a receipt. You can
retry the identical request after losing the response, even after ownership ends.

PostgreSQL rejects a closure whose lease expires during history insertion. Its
schema guard requires explicit grant consumption, so an older worker cannot use
another processor's live timeout grant. Retrying the earlier deadline migration
preserves this exception for valid timeout operations. Downgrade refuses retained
timeout grants.

2026-10-09: focused memory and PostgreSQL tests cover concurrent grants and closure,
same-owner reclaim, child results and all parent-close policies. Fault injection
at history, projection, task, child-delivery and receipt writes verifies rollback.
Memory also rejects task-version overflow before publishing any closure. Deadline
and lease checks run after lock waits, and closure receipts survive pool replacement.
Runtime polling, frozen timeout replay and child SDK options remain in progress;
these store APIs alone do not qualify automatic workflow timeout processing.

The timeout-closure store checkpoint passes make f, make l, go test ./...,
engine/runtime/memory races and the full PostgreSQL durable race suite (198.902s).
Its independent review remains scheduled after runtime integration.
