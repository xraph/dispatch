// Package runtime evaluates durable workflow decisions against recorded history.
//
// A workflow handler schedules named activities and timers. Schedule multiple
// futures before calling Get to allow parallel execution. Get returns a saved
// result or yields the decision until an outcome arrives. The handler then runs
// again from its beginning. Now advances from run creation through the outcomes
// consumed by Get, without consulting a worker clock.
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
// Evaluate requires a complete history snapshot with at most 100,000 events and
// accepts at most 999 new commands per decision. Command IDs, names and queues
// contain at most 200 bytes. History format version 1 is explicit; unknown
// versions and event types fail evaluation so operators can supply compatible
// code. A panic or nondeterministic replay fails the task, not the execution.
package runtime
