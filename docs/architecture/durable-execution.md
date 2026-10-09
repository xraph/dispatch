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
| Activity retries and timeout classes | Queue, attempt, overall and heartbeat deadlines, progress recovery, retry policies and asynchronous Go callbacks implemented; remote authorization and process qualification open | Queue, attempt, overall and heartbeat deadlines; heartbeat progress; asynchronous completion |
| Signals, queries, updates and signal-with-start | Atomic signal store acceptance and receipts implemented; runtime, queries and updates open | Namespace isolation, deduplication, atomic acceptance, update results, read-only queries |
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
Runtime consumption remains unimplemented at this store checkpoint.
