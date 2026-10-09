# Durable run chains

Status: implementation contract. The current runtime supports single runs; the
requirements below are not a claim that retries or continue-as-new already work.
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

## Messages and children

Carry accepted but unconsumed signals with their original ID, source run and
acceptance sequence. Keep the original workflow-scoped acceptance receipt. A
concurrent signal must either enter the source before its handoff snapshot or
enter the successor afterward; it must not disappear between runs. Consumption
in the handoff decision counts before choosing carried signals. Reject an
oversized carry batch without partial closure so the workflow can drain messages.
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

A parent that continues-as-new closes its own run. Apply each existing child's
parent-close policy; the successor does not adopt those old child futures.
Abandoned children keep their original relationship for history and delivery
provenance. Queries and inspection expose both original and current identities.

## Whole-workflow retries

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
