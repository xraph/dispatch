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
// Return nil, w.ContinueAsNew(input, options) to close this run and atomically
// start its successor. Return the intent directly and make no later SDK calls.
// Empty routing options inherit the source; a nil RunTimeout inherits its saved
// duration, while a pointer to zero removes the run limit. The chain retains its
// absolute execution deadline. Unread signals follow the chain with their original
// acceptance identities. Pending tasks are fenced and child-close policies apply.
// The successor does not adopt the source's child futures. A child that continues
// remains one invocation to its original parent until the chain finally closes.
// Historical queries of a continued run reconstruct its saved decisions without
// creating another successor.
//
// Set StartRequest.RetryPolicy or ChildOptions.RetryPolicy to opt into whole-workflow
// retries. Nil and entirely zero policies keep retries disabled. A configured policy
// defaults to a 1s initial interval, coefficient 2, a 100x maximum interval and
// unlimited attempts. MaximumAttempts includes the first run. NonRetryableTypes
// and ApplicationError.NonRetryable make failures final. Accepted cancellation,
// termination and execution timeout also prevent retries; run timeout may retry.
//
// Each retry retains the failed/timed-out source and atomically creates a delayed
// successor with the same input, type, build and queue. Unread signals follow it;
// consumed signals and completed activity results stay in the source history.
// Whole-workflow retries can repeat external effects, so choose idempotency keys
// that cover the business operation across runs. Child futures wait for the chain's
// final result. Timeout coordinators need no registered workflow handler.
//
// RunInfo exposes saved lineage and RetryAttempt. ContinueAsNew resets that attempt
// to one; RunNumber includes both retries and continuations. Now starts at saved
// run availability, so retry backoff is excluded from the run timeout but counts
// against the unchanged execution deadline. No retry starts at or after that limit.
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
// RequestCancelExecution accepts whole-workflow cancellation with an explicit
// run or an empty RunID for the current open run. Retry the original request to
// recover its receipt after closure or replacement. Acceptance leaves the run
// open. A later decision atomically fences old pending tasks and schedules cleanup.
//
// Register SetCancellationHandler before your first command if it must handle
// cancellation accepted before the initial decision. Normal code replays from the
// history prefix before first acceptance; cleanup uses the same captured state
// with full history after the saved fence. Already-recorded results remain readable;
// unresolved normal activity, timer and receive futures return ErrWorkflowCancelled.
// Cleanup can use ordinary durable commands with IDs unique across both phases.
// Return ErrWorkflowCancelled to close cancelled, nil to complete, or another error
// to fail. Without a handler, the run closes cancelled. The first request controls
// the reason; later requests cannot replace it or restart cleanup. A handler
// registered beyond the frozen normal boundary cannot handle that request.
//
// Register queries with SetQueryHandler before the workflow can yield. A query
// reads local state reconstructed by validated replay, then returns a copied
// result without committing that replay's decisions. QueryExecution requires an
// explicit run and pinned build. Completed, failed and cancelled runs remain queryable when
// their history matches the registered workflow code. QueryResult carries the
// persisted revision, sequence and lifecycle state for the history prefix used.
// An accepted signal can be visible to a query before a worker commits consumption.
//
// Queries must return promptly and avoid external effects. SDK scheduling,
// Future.Get, Select, Cancel and handler registration are prohibited during a query,
// even if it recovers the runtime's control-flow panic. Now remains readable.
// Ordinary Go effects and blocking cannot be sandboxed. QueryExecution checks its
// context between reads and invocation phases, and after code returns; it does
// not launch an evaluator goroutine that could outlive cancellation. Input and
// output are each limited to 1 MiB. Query names contain at most 200 bytes.
//
// Evaluate requires a complete history snapshot with at most 100,000 events and
// accepts at most 999 new command, signal consumption, selection and cancellation
// acknowledgment events per decision, or 998 when a workflow retry policy is set
// to leave room for a store-owned retry link. Command IDs, names and queues contain at
// most 200 bytes. History format version 1 is explicit; unknown versions and event
// types fail evaluation so operators can supply compatible
// code. A panic or nondeterministic replay fails the task, not the execution.
package runtime
