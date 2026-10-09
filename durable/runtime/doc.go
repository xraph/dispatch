// Package runtime evaluates durable workflow decisions against recorded history.
//
// A workflow handler schedules named activities, timers and signal receives. Schedule multiple
// futures before calling Get to allow parallel execution. Get returns a saved
// result or yields the decision until an outcome arrives. The handler then runs
// again from its beginning. Now advances from run creation through the outcomes
// consumed by Get or Select, without consulting a worker clock.
//
// You must keep workflow code deterministic. Use activity handlers for network,
// filesystem, database and other external work. Do not use goroutines, channels,
// map iteration order, random values, wall clocks, deferred effects or recovered
// control-flow panics in a workflow handler. Ordinary Go code cannot be sandboxed
// by this API. Command validation detects changed recorded decisions; it cannot
// prevent arbitrary Go code from performing an external effect itself.
//
// Activity execution has at-least-once semantics. An activity may reach an
// external service before its result is recorded. Use its stable execution and
// command identity as an external idempotency key where the service supports it.
// Completing a workflow without waiting for a future abandons that pending work.
//
// A version 2 activity with a finite deadline can call ActivityInfo.DeferCompletion
// before delivering its handle to an external service. A confirmed handoff stops
// worker renewal and suppresses later handler results, including panics. Complete
// it through Worker.CompleteAsyncActivity, or report progress through
// Worker.HeartbeatAsyncActivity with consecutive sequences. Keep each request ID
// and its contents stable when retrying an unknown response. Completion applies
// the recorded retry policy; a retried attempt has a new callback credential.
//
// AsyncActivityHandle is a credential. JSON includes its secret for delivery;
// ordinary formatting redacts it. Keep the handle immutable, coordinate heartbeat
// sequences between producers, and authorize callers before exposing callback
// methods remotely. A crash before handle delivery leaves an orphaned handoff
// which recovers through its persisted deadline. Memory is for development and
// tests; use a qualified persistent execution store for restart recovery.
// An uncertain sent handoff suspends ordinary worker heartbeats with
// ErrHandoffPending. Retry DeferCompletion to recover its original handle.
// Renewal also reconciles that saved request before using the old worker token;
// a cancelled request that stayed unsent is never retried in the background.
//
// ReceiveSignal returns a future for one message of a given name. Give each
// receive a stable command ID. Messages are buffered in history, consumed in
// arrival order within their name, and assigned to a receive in its decision
// transaction. Repeated Get returns a copy of the same input. Now advances using
// the recorded message arrival time, even if consumption commits much later.
//
// SignalExecution targets an explicit run or the current open run with an empty
// RunID. SignalWithStart atomically selects an open run or creates the proposed
// run first. Request IDs are unique across both operations within one namespace
// and workflow ID. Retry the whole original request to recover its original
// receipt, including after a replacement run starts. A receipt confirms durable
// acceptance. Your workflow decides whether to consume all signals before closing.
// Callback clients need the pinned namespace/build, but no registered handlers.
// SignalWithStart proposals must fit the Go runtime's queue/type limits, even
// when those fields are unused because an existing run wins.
//
// Select races 1 through 1000 distinct futures from the current evaluation.
// Give each call a stable command ID. It returns the original winning future,
// consumes its signal if applicable and advances Now. Get then returns its copied
// result or recorded failure. New choices follow availability event sequence,
// with candidate order breaking shared-signal ties. Replay uses the saved winner.
// The ordered candidates and winner commit with the workflow decision. Selection
// does not cancel losing work or create a polled task. For a draining loop, remove
// each winner from your candidate slice; ready futures can otherwise win again.
//
// Cancel requests cancellation of an activity, timer or signal receive. Wait on
// its returned future for durable acknowledgment, then use errors.Is with
// ErrCancelled on the target's Get result. A winning completion remains intact.
// CancelledError carries copied attempt/heartbeat progress when available.
// Cancel fences task results and preserves unused signals. It cannot undo an
// external effect or prove that a running external operation physically stopped.
// A running handler observes lost ownership through renewal or heartbeats.
//
// Register queries with SetQueryHandler before the workflow can yield. A query
// reads local state reconstructed by validated replay, then returns a copied
// result without committing that replay's decisions. QueryExecution requires an
// explicit run and pinned build. Completed and failed runs remain queryable when
// their history matches the registered workflow code. QueryResult carries the
// persisted revision, sequence and lifecycle state for the history prefix used.
// An accepted signal can be visible to a query before a worker commits consumption.
//
// Queries must return promptly and avoid external effects. SDK scheduling,
// Future.Get, Select, Cancel and query registration are prohibited during a query,
// even if it recovers the runtime's control-flow panic. Now remains readable.
// Ordinary Go effects and blocking cannot be sandboxed. QueryExecution checks its
// context between reads and invocation phases, and after code returns; it does
// not launch an evaluator goroutine that could outlive cancellation. Input and
// output are each limited to 1 MiB. Query names contain at most 200 bytes.
//
// Evaluate requires a complete history snapshot with at most 100,000 events and
// accepts at most 999 new command, signal consumption, selection and cancellation
// acknowledgment events per decision. Command IDs, names and queues contain at
// most 200 bytes. History format version 1 is explicit; unknown versions and event
// types fail evaluation so operators can supply compatible
// code. A panic or nondeterministic replay fails the task, not the execution.
package runtime
