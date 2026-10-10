# Persisted build retirement

Enroll a namespace before using build retirement. Enrollment requires its existing
transactional Chronicle outbox and registers admission state for every historical
build found in executions and child references. It records the inventory count and
digest with the accepted response. It does not establish an artifact identity or
prove that another executable can answer historical queries.

Use the optional `durable.LifecycleStore` capability. The base `durable.Store`
interface and existing execution receipt fingerprints stay unchanged. A custom
store can continue serving ordinary executions without implementing retirement.

## Admission and completion

`RegisterBuild` introduces a new accepting build after enrollment. Build state is
keyed by installation, namespace and build ID. It covers every queue.

`BeginBuildRetirement` advances the build epoch and closes new admission. New
roots, signal-with-start creation and incoming work from another build receive a
`BuildAdmissionError`. An existing run can still receive a signal. Same-build
children, continuations and retries retain their immediate source's admission
epoch and can finish while the build is retiring. An A-to-B-to-A handoff cannot
reuse the first A run's eligibility.

Execution admission epochs are store-owned. Existing runs receive epoch zero
through the expansion migration. Callers cannot select an epoch in StartRequest
or CommitRequest. PostgreSQL also verifies the persisted parent or predecessor
relationship at commit, so a direct insertion cannot invent inherited eligibility.

`InspectBuildLifecycle` reports open executions, unfinished tasks, async callbacks,
delayed runs, pending child deliveries and future child-result obligations. A
sleeping run still counts. A closed parent can still need its build when a child
on another build may send a result later.

The observation contains catalog versions and an observation time. Counts may
change while those versions stay the same. `FinalizeBuildRetirement` takes the
exclusive namespace fence and computes the blockers again in that transaction.
It succeeds only when all blocker counts are zero. Finalization closes admission;
it does not authorize removing a process or deleting historical query code.

`AbortBuildRetirement` explicitly resumes a retiring or retired build at a new
epoch. A crashed controller, timed-out request or expired worker lease cannot
reopen it. Accepted request IDs recover their original responses before current
state/version checks. A changed request with the same identity conflicts.

## Writer compatibility

Enrollment activates retirement schema and writer protocol 1 independently of
audit protocol 1 and publisher protocol 1. Current writers acquire namespace
coordination before execution or task rows and set a transaction-local capability.
It disappears on commit or rollback. Expansion alone does not activate the floor.

The lock domain is shared with existing audit activation. The legacy audit helper
and row-trigger fallback try shared coordination without waiting while holding a
work row. You may see these distinct PostgreSQL codes:

| Code | Meaning |
| --- | --- |
| DL001 | Writer capability is incompatible with the active retirement floor. |
| DL002 | Namespace coordination is busy; the rejected transaction rolls back. |
| DL003 | Build admission or immutable admission metadata was refused. |

Current code exposes typed compatibility, contention and admission errors while
preserving the native SQLSTATE in the error chain. Existing DA and DX codes keep
their meanings. An unchanged old supervisor may stop on DL001 or DL002; the new
schema cannot add retry behavior to that binary.

An old mutation-shaped replay may receive DL001 even when its original receipt
exists. Capable replay still recovers the accepted receipt, and old pure reads
remain available. Keep those two cases separate when qualifying rollback.

## Audit and evidence

Enrollment, build state, accepted response and required Chronicle intent commit
together. Intent preparation failure leaves all state unchanged. Namespace
commands use the existing security envelope, a domain-separated target digest,
and closed `dispatch.retirement.enroll` or `dispatch.build.*` actions. They add no
Relay event or payload-bearing envelope.

The PostgreSQL tests separate deterministic SQL barriers from the independently
pinned `qualification/oldwriter` executable. SQL barriers verify the three-party
lock ordering and fresh floor visibility after an outer statement has started.
The native fixture links an unchanged published old Dispatch module and keeps its
pool alive through expansion and enrollment, then repeats checks in a new process.
It is a newly built old-library fixture, not an archived production executable.

Process drain is described in [Durable worker lifecycle](durable-worker-lifecycle.md).
Local quiescence and persisted retirement are different observations. Keep the
publisher and query runtime available while work drains and while retained
history still needs its original code.
