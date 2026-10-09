# Durable run chains

Status: the memory and PostgreSQL stores implement atomic continuation and opt-in
workflow retries. The Go runtime supports ContinueAsNew, retry/continuation history
replay, RunInfo and historical queries. Final whole-plan qualification is pending.
The full [durability roadmap](durable-execution.md#required-work-and-evidence)
remains the completion gate.

## Identity and immutable history

A chain keeps one namespace and workflow ID. Each run has its own RunID, history,
revision, tasks and receipts. FirstRunID identifies the original run;
PreviousRunID and NextRunID record adjacent runs, and RunNumber increases by one.
FirstStartedAt records the chain's original creation time. A continued run keeps
its predecessor's absolute ExecutionDeadlineAt. RunTimeout is retained as a
relative duration so each new run can resolve its own run deadline.

Existing runs become independent roots. Do not infer relationships from timestamps
or workflow IDs. Preserve old start fingerprints and receipt results. Older root
writers must receive valid lineage through the schema defaults and insert guard.
New writes retain exact durations; migration can recover only the normalized
microsecond duration from an older run's persisted deadline.

## Atomic handoff

Continue-as-new closes the source and installs exactly one successor in one
transaction. The transaction records the source terminal event and NextRunID,
fences source tasks, applies its children's parent-close policies, creates the
successor projection/history/task, advances current/latest identity and stores the
source receipt. It must leave no externally visible gap in workflow ownership.
An exact retry returns the original receipt without extending either deadline or
creating a second successor. A changed request fails.

The source must own a live ordinary workflow grant and its expected revision.
Identity locks precede execution and task locks. Check authoritative store time
only after all required locks. Expired execution deadlines prohibit successors;
run timeout recovery follows its separately configured retry policy. Overflow,
date bounds, reused run IDs and an already linked source fail before publication.
PostgreSQL faults at every persistence boundary must roll the whole handoff back.

The deterministic SDK captures successor input, workflow type, pinned build,
queue and run timeout. Defaults inherit the source. Changing a captured option
fails replay. Continue-as-new reconstructs old queries without scheduling the
successor again. New history starts with explicit lineage and a fresh sequence.

You can end a handler with `return nil, w.ContinueAsNew(input, options)`. Return
that intent directly, with no output or later SDK calls. A nil RunTimeout inherits
the saved duration; a pointer to zero removes the run limit. Empty type, build and
queue inherit their source values. The runtime resolves the queue from the polled
workflow task, and captures both your options and their resolved values in
workflow.continuation_requested before the store appends workflow.continued_as_new.
Changing an inherited option to an explicit equivalent also fails replay.

Successor IDs derive deterministically from the source namespace, workflow and
run IDs. Exact commit retries reuse that identity and the complete request.
The Go reader requires its captured request when replaying a continuation; raw
store clients own their history format. Existing root starts and version 1 child
results remain readable. Use at most 200 bytes for workflow types and queues so
a Go worker can register the successor. A continuation decision reserves one
event for its captured request and one for the store's terminal record.

Run `go run ./examples/durable-continuation` to see an unread signal cross a
handoff, a replacement worker finish the successor, and a query read the original
run's state. The example uses memory. PostgreSQL runtime tests replace connection
pools between runs and retry lost responses against persisted receipts.

## Messages and children

Carry accepted but unconsumed signals with their original ID, source run and
acceptance sequence. Keep the original workflow-scoped acceptance receipt. A
concurrent signal must either enter the source before its handoff snapshot or
enter the successor afterward; it must not disappear between runs. Consumption
in the handoff decision counts before choosing carried signals. Reject an
oversized carry batch without partial closure so the workflow can drain messages.
The store accepts at most 998 carried signals and 4 MiB of encoded carry records,
and requires a source history of at most 100000 events. Consumption in the same decision
can bring a pending batch within those limits.
Cancellation accepted before handoff prevents normal continuation and enters
cleanup; a cancellation racing after handoff targets the successor under the
same workflow identity lock. Exact older receipts still identify their original
run.

A child chain remains one child invocation to its original parent. Intermediate
handoffs cannot deliver a final child result. Keep the original child identity
and a validated current/final run identity. Parent-close commands resolve the
current child run under the chain's identity lock and remain idempotent across
handoffs. Final child timeout metadata must describe the final run while retaining
the original chain's execution deadline and parent relationship.

CurrentKey resolves from the saved chain. It does not replace Start.Key. Close and
cancel delivery polling follows the current child's build; delivery records retain
their original routing, and receipts identify the run that actually received the
message. A later unrelated root with the same workflow ID cannot inherit it.
Final results from a successor use version 2 child messages with FinalRun metadata.
Version 1 messages remain valid for single-run children and cancellation acknowledgments.

A parent that continues-as-new closes its own run. Apply each existing child's
parent-close policy; the successor does not adopt those old child futures.
Abandoned children keep their original relationship for history and delivery
provenance. Queries and inspection expose both original and current identities.

## Whole-workflow retries

Set `StartRequest.RetryPolicy` or `ChildOptions.RetryPolicy` to opt in. Nil and an
entirely zero policy disable retries. If you configure any field, unspecified
intervals use 1 second initially and 100 times that interval at most, the backoff
coefficient defaults to 2, and zero maximum attempts means unlimited attempts.
`MaximumAttempts` includes the original run. The store saves a normalized copy,
including the sorted, deduplicated non-retryable type list.

Retries are opt-in, with persisted initial/maximum intervals, exponential
coefficient, maximum attempts and non-retryable failure types. Application
non-retryable flags, cancellation, termination and execution timeout prohibit a
retry. A run timeout may retry within that policy. Failure closure and successor
creation use the same atomic handoff machinery, including child relationships
and message provenance. The store must process a run-timeout retry without loading
the retired workflow handler.

Persist the retry attempt and first-task availability. Backoff counts against the
unchanged execution deadline, and no signal wakeup may run workflow code before
the saved retry availability. Resolve the run deadline against that availability;
the earlier run/execution deadline still wins. Continue-as-new starts a fresh retry
attempt while preserving the chain identity and execution deadline.

The failed or timed-out run retains its original terminal event. Immediately
before it, `workflow.retry_scheduled` records the deterministic successor identity,
resolved metadata, delay and failure type. A run timeout uses the failure type
`workflow_run_timeout`; you can put that type in `NonRetryableTypes`. The runtime
checks this record against the saved policy and closure clock during replay.

Use `w.RunInfo()` to read the current `RunNumber` and `RetryAttempt`, both starting
at one. `w.Now()` begins at the saved availability time and advances when you
consume outcomes. The coordinator creates a successor only when its availability
precedes the chain's execution deadline. A delayed run can still expire before a
worker polls it. A retry policy reserves one additional history slot, leaving at
most 998 decision events before the final state and retry records.

Your retry starts with the same input, workflow type, build, queue and timeout
configuration. Consumed signals and activity results remain in the failed run;
only unread signals carry forward. If repeating the business operation can repeat
an external effect, give your activity an idempotency key that covers all relevant
runs. Per-run command identity alone does not deduplicate effects across retries.

Run `go run ./examples/durable-workflow-retry` to inspect a failed run, a replacement
worker completing its successor, signal carry and a historical query. The example
uses memory; PostgreSQL tests exercise the same chain across replaced connections.

## Qualification

Shared memory/PostgreSQL tests must prove atomic creation, receipt recovery,
immutable lineage, current/latest races, message handoff, child-chain results and
parent-close races. Actual PostgreSQL tests must cover row-lock expiry, older
writers, migration retries, populated downgrade, injected write failures and pool
replacement. Runtime tests must prove changed-command rejection, historical
queries, cancellation phases, retry exhaustion and timeout recovery on an unserved
build. Run the repository format/lint/test gates and full durable PostgreSQL races.

Deploy compatible readers before enabling the new history shapes. These tests do
not establish full rolling deployment compatibility, remote authorization,
external-effect reversal, process-kill recovery, failover, disaster recovery or
production capacity. Those remain required roadmap work.

The reference behavior follows Temporal's [execution/run distinction](https://docs.temporal.io/glossary),
[opt-in workflow retry policy](https://docs.temporal.io/encyclopedia/retry-policies)
and [child-chain relationship](https://docs.temporal.io/child-workflows).
