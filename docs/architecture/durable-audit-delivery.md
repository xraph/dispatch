# Durable audit delivery

Use `durable.NamespaceStore` and `durable.OutboxStore` when your host requires
local durable acceptance of audit or hook delivery. PostgreSQL is the production
backend. Memory implements the same contract for tests and development. Check
both capabilities explicitly; a legacy store does not provide a reliable fallback.

The independent publisher drains accepted intents through an injected
`delivery.Sink.Accept` implementation. Forge composition registers namespaces and
starts that publisher before worker admission. `durable/delivery/ecosystem` supplies Chronicle and Relay
adapters for their reliable acceptance APIs. Local acceptance and memory-engine
tests do not establish delivery to either deployed service.

## Configure ownership before admission

Migrate the store, then call `RegisterNamespace` before admitting workers or
operator commands. Supply installation, namespace, app, tenant, schema version 1
and the required Chronicle audit and Relay hook flags. Registration creates the
record or verifies every configuration field against the existing record.
Conflicts fail. You cannot transfer ownership or disable a required destination.

Namespace names are unique across the physical store because execution keys do
not include installation. Every catalog list, delivery claim and status request
requires an explicit installation. Lists use an exclusive cursor and a limit
from 1 to 100. Empty installation IDs never mean all installations.

Keep a dedicated registered audit namespace for installation-wide reads,
anonymous denials and attempts against unknown or unauthorized resources. It
requires Chronicle delivery and needs no execution or worker queue. Your host
must verify its installation and tenant against the authorization boundary before
admitting protected requests. A requested namespace or identity claim cannot choose
this binding. Successful resource reads can use their authorized catalog namespace
when it requires audit, or an explicit host fallback policy.

Identifiers and trusted metadata fields are bounded to 256 UTF-8 bytes and reject
control characters and surrounding whitespace. Existing command receipt IDs are
hashed with length-prefixed components, preserving support for the core's longer
request IDs without copying them into audit source identity. Actor kinds are
`system`, `worker`, `user`, `service`, `api_key`, `service_acct` or `anonymous`; anonymous actors have no ID.
The host remains responsible for supplying identifiers rather than secrets.

## Acceptance and coverage

For registered namespaces, every newly inserted history event creates one intent
for each required destination. Accepted command receipts create an additional
Chronicle intent when audit is required. Events and receipts have distinct
identity domains. Receipt actions distinguish start, transition, heartbeat, timeout,
signal, cancellation and child delivery. Relay receives history events, not
command receipt telemetry.

| Mutation | Transactional evidence |
| --- | --- |
| Start | Initial history event and execution receipt |
| Signal and signal-with-start | New history and workflow-scoped signal receipt |
| Request cancellation | Cancellation history and workflow-scoped receipt |
| Commit transition | All parent events and execution receipt |
| Async handoff and completion | Transition history and execution receipt |
| Async or worker heartbeat | Execution receipt, with task/progress change in the same transaction |
| Activity timeout | Transition history and execution receipt |
| Execution timeout | Timeout history, receipt, retry successor history and child messages |
| Continue-as-new and workflow retry | Parent closure, successor start and carried history |
| Child start | Parent decision, each child start history and child start receipt |
| Child delivery | Target history when applicable, cancellation receipt when applicable, and source delivery receipt |
| Ignored child delivery | Source delivery receipt with its bounded disposition |

Routine task, timeout and child-message claims or lease renewals retain their
existing fencing. They do not produce security audit telemetry. Required audit
covers accepted commands and history, not every low-level state update.

`CoverageStartedAt` records activation time and `WriterProtocol` records the
minimum audit protocol. Earlier history is not backfilled or represented as
audited. To establish coverage of a specific event, inspect its matching delivery
intent. Event occurrence time is not an acceptance boundary: an event inserted
after activation must have an intent even if its supplied occurrence time is old.

Memory holds one mutation/activation mutex. Each covered operation runs against a
private candidate, deep-copying execution state in the affected namespace and
copying shared durable indexes. It prepares the full intent batch before
publishing any candidate maps. A failure on a later child, successor or second
destination leaves tasks, history, receipts and all indexes unchanged. No
external callbacks run from this candidate. Index copying costs O(store size),
and execution copying costs O(affected namespace state). Use PostgreSQL for load.

PostgreSQL writes history, command receipts and intents in the same transaction.
New writers take a shared namespace advisory transaction lock before execution,
workflow identity or task locks, ownership lookup and authoritative clock reads.
A grant or deadline that expires while activation blocks admission is rejected.
Database triggers also take that shared lock
for older writers. Namespace registration takes the exclusive form in a dedicated
catalog-only transaction. It never reads or locks execution rows, so a writer
holding execution rows cannot form an inverse registration dependency. Children
and their deliveries retain the existing same-namespace invariant.

Every event and command receipt INSERT queues a deferred constraint trigger.
For an active namespace, the trigger requires matching source, installation,
app, tenant, schema and destination in the retained outbox. An old writer cannot
commit covered changes without intents. Forced immediate constraints reject
missing intents as well. Delivery acknowledgement retains this evidence.

Only READ COMMITTED is supported for guarded writes and registration. Database
functions reject REPEATABLE READ and SERIALIZABLE before any catalog absence
check. The VOLATILE guard acquires its lock and reads the catalog in separate
statements, so a writer waiting behind activation sees the committed ownership.
Registration cannot upgrade a transaction's existing shared writer lock.

Quiesce incompatible workers before activating a secured deployment and prevent
their readmission. The guards reject unaudited commits; they do not retrofit old
binaries with a publisher. The audit migration deliberately refuses downgrade,
and ownership and accepted delivery evidence cannot be deleted through normal
SQL mutations. Production migrations do not delete or rewrite existing history.

## Capture trusted facts once

Use `WithAuditMetadata` after authorization to carry verified actor identity,
request/correlation identity and bounded policy decision references into accepted
mutations. It does not authenticate a caller. The catalog supplies installation,
app and tenant; metadata has no scope override. Runtime work without that carrier
records the explicit `system` actor `dispatch`.

`CaptureSecurityAudit` allocates a random server-owned attempt ID and occurrence
time. Retain that value and retry it unchanged after an uncertain acceptance
result. A client request or correlation ID is metadata, not a deduplication key.
Separate decisions in the same incoming request require separate captured audits.
`AppendSecurityAudit` requires a matching registered installation/namespace with
required Chronicle delivery. Changed immutable content under the same attempt ID
fails with a conflict. Failed audit acceptance cannot turn a denial into access.

Envelopes contain typed identity, action, outcome, target and policy-reference
fields. They contain no execution input/output, task progress, callback proof,
raw claims, arbitrary provider metadata, obligations or provider error strings.
The attempted target is descriptive metadata and does not select delivery scope.

Remote audit actions retain the registered REST method and route template,
contract intent or DWP method. These are separate from the installation permission
passed to Warden. Targets identify validated typed resource IDs; bulk commands
capture their effective queue, limit or UTC cutoff. A relative REST purge cutoff
is captured once and passed to the handler. Creation commands record a typed input
selector, such as job name and queue, because an output ID does not exist yet.
Selectors are bounded and exclude payloads, inputs and credentials. Unknown or
invalid targets use an explicit marker. The pre-execution attempt and captured
outcome carry identical action and target fields, so you can find the legacy
resource when an unresolved attempt needs reconciliation.

Delivery identity hashes an unambiguous ordered identity tuple including source
installation, destination and schema version. The fingerprint binds every immutable envelope
field, including occurrence time and actor metadata. Times normalize to UTC
microseconds before hashing, matching PostgreSQL precision. Reloading a persisted
envelope verifies the same fingerprint.

## Claims and sink receipts

Claim one explicit installation and destination at a time. Batches contain at most
100 deliveries, and leases range from one millisecond to five minutes. This lets a
publisher keep a failed Chronicle backlog independent of Relay capacity.

Every grant has an owner, epoch and expiry. Renew, retry and acknowledge require
the current unexpired token. PostgreSQL reads its clock after acquiring row locks,
so waiting for a lock does not preserve an expired grant. A retry records a bounded
error category and delay up to 24 hours. There is no terminal discard or retry
count that silently drops work.

A sink receipt must bind delivery ID, destination, schema and immutable
fingerprint, and include a nonempty stable sink receipt ID. The store validates
and persists it with acknowledgement. A mismatched receipt leaves the delivery
pending. If a reply is lost, inspect status or let another publisher recover the
pending record; never assume delivery from a transport error. A repeated ack with
an already consumed token fails fencing, while status retains the accepted receipt.

Status reads return bounded records, pending and blocked counts, and the oldest pending local
acceptance timestamp. You can derive backlog age from that timestamp. PostgreSQL
count and page reads are separate READ COMMITTED statements, so status is
observational rather than a transactionally consistent monitoring snapshot.


## Compose a secured Forge host

Use `extension.WithDurableDelivery(DeliveryConfig{...})` with PostgreSQL. Supply
an `AuditNamespace`, execution `Namespaces`, a publisher installation and owner,
and explicit sinks for every required destination. The owner identifies this
publisher process. Configure `WithOperatorSecurity` or `WithRemoteSecurity` for
verified identity and authorization. Execution namespace ownership must include
required audit when you enable a durable worker in this mode.

`Register` creates a shared inactive `security.AuditService` before copying the
boundary into REST, DWP and contract handlers. `Start` migrates the store, verifies
configuration, registers or verifies namespace ownership, and starts the publisher
before the engine. It enables the audit handle only after engine startup succeeds.
The audit namespace installation and tenant must match `Boundary.Resource`.
Unresolved and denied targets cannot redirect that binding. Successful
namespace-aware reads require an authorized registered namespace with audit,
unless the host explicitly enables `AllowNamespaceAuditFallback`.

Omitting delivery configuration leaves protected remote operations unavailable,
even with valid identity and permission. A direct trusted engine remains usable.
Tests can deliberately compose real memory acceptance through
`WithMemoryAuditForTesting`; this option does not provide production durability.
Standalone boundaries must explicitly activate their audit service against a
registered catalog. There is no success-only audit bypass.

All protected reads require local audit acceptance before returning data.
Authentication failures, policy denials and policy outages use the host binding;
a failed audit does not grant access or replace a denial with success. The shared
handle counts local acceptance failures without retaining provider error text or
creating recursive audit events. Warden admission and direct contract dispatch
capture separate server-owned decision IDs. Verified `api_key` and `service_acct`
actor kinds survive transport context binding.

## Legacy command outcomes

Legacy REST, DWP and contract mutations accept an attempt through
`durable.LegacyAuditStore` before executing the handler. That write atomically
persists the Chronicle intent and a retained unresolved-attempt marker. After the
handler returns, a separate write atomically accepts its immutable outcome intent
and resolves the marker. `returned_success` means the handler and response reported success;
`returned_error` means a handler or response error, including serialization or
capture overflow. Either result can follow a persisted state change. Neither
write is atomic with the intervening legacy mutation. Durable execution commands retain their existing transactional history
and receipt outbox path.

`UnresolvedLegacyAttempts` returns installation- and audit-namespace-scoped pages
of accepted attempts with no recorded outcome. They may be executing, may have
failed, or may have succeeded before the process lost the result. The marker
survives restart. Do not automatically repeat the mutation or infer an outcome.
Reconcile it using the accepted attempt delivery ID and the actual legacy state.
If you retain a captured `LegacyOutcome`, retry that exact value: identical
acceptance resolves once, while changed content or scope fails.

A post-command outcome write failure returns `OutcomeUnconfirmedError` with the
safe attempt ID. REST captures the response writer until outcome acceptance, so
it returns HTTP 503 rather than an already-written success. Captured mutation
responses have a four MiB limit; larger responses return a reconciliation error
after recording the command outcome. DWP emits an error frame, and contract
handlers return an unavailable error with the same correlation. A process crash
can still lose the in-memory outcome value; the durable marker reports that gap
without claiming exact reconstruction.

## Publisher lifetime and readiness

Each configured destination has its own fixed set of goroutines. Every goroutine
claims one fenced delivery, calls its sink with a deadline, verifies the returned
receipt and acknowledges it through the outbox store. Chronicle backlog cannot
consume Relay goroutines. Store deadlines, lease duration, concurrency and retry
backoff are bounded in `delivery.Config`. Retry categories contain no payload or
provider error strings, and retries have no terminal drop count.

A sink must persist acceptance before returning a receipt and return the same
receipt for identical retries after unknown outcomes. It must reject conflicting
content. The publisher verifies delivery identity, destination, schema and the
immutable fingerprint; transport success alone is insufficient. Sink panic or
outage leaves accepted intents recoverable. A sink that ignores cancellation
occupies its existing goroutine; the publisher never spawns replacements for it.
Go cannot forcibly terminate arbitrary sink code.

Public extension and engine Stop calls serialize with each caller's deadline.
One shutdown task waits for actual durable-worker completion, stops the wake
listener and heartbeat, and deregisters the worker once. A handler that ignores
cancellation can keep this task alive, but callers return at their deadlines and
storage stays open. Retry Stop with a fresh context after the handler exits.
Final close requires confirmed worker and publisher completion and a live caller
context; an already-expired context cannot authorize final close.

The publisher stays alive while workers and legacy shutdown hooks drain.
`Dispatcher.BeforeStoreClose` then drains it before closing storage. If a required
drain reaches the deadline, shutdown returns incomplete and keeps the store open.
A later `Stop` with a fresh context can wait for canceled calls to exit, resume
pending delivery and close storage exactly once. Concurrent callers honor their
own deadlines while waiting for the shutdown gate. A canceled sink reply cannot
acknowledge a delivery. Worker shutdown failure also remains visible and prevents
premature store closure.

`Extension.Health` remains execution readiness. `DeliveryStatus` separately
reports pending and blocked counts, oldest acceptance, calls in flight and the latest bounded
error category for one destination. Sink outage does not fail execution readiness
while required local acceptance works; it degrades delivery and may prevent a
complete shutdown drain. The host must monitor backlog age and volume against its
outage budget. No deployed Chronicle/Relay qualification is claimed here.


## Reliable ecosystem adapters

Inject `ecosystem.Chronicle` and `ecosystem.Relay` through `DeliveryConfig.Sinks`.
Their clients implement Chronicle `RecordOnce` and Relay `SendReliable`. You can
use the actual engines in a composed host or `ecosystem.NewRemote` for a fixed
protected acceptance endpoint. Keep the host's Authsome and Warden composition
in a separate module. Dispatch core imports neither Authsome nor Ctrlplane, and
neither sink imports Dispatch.

Supply an immutable `ecosystem.Binding` for each namespace: producer,
installation, namespace, app, optional organization and tenant. The adapter
rejects a delivery whose persisted ownership differs. If your publisher serves
several namespaces, your host must choose among its configured adapters using
that allowlist; delivery content cannot provide an endpoint or credential.

Mapping version 1 preserves the original occurrence time, actor, scope and source
identity. Chronicle receives the action, outcome, target and typed audit facts.
Relay receives the immutable envelope as `dispatch.delivery.v1`. Neither mapping
copies workflow payloads or callback secrets. Call `RegisterRelaySchema` on the
actual Relay catalog before admitting requests. The schema requires envelope
version 1, the Relay destination and an event source, and rejects additional
payload fields.

Source and sink fingerprints have different meanings. The request binds the
source delivery ID and original fingerprint, while each sink's published
canonicalization computes a separate semantic fingerprint. The adapter recomputes
that fingerprint and verifies receipt scope and identity before returning an
acknowledgement. `SinkReceipt` persists both fingerprints, the mapping version
and the complete verified sink receipt as JSON evidence. Relay acceptance proves
that its event and selected fanout were committed; inspect Relay delivery status
separately to establish webhook completion.

JSON mapping preserves integer sequences above 2^53 and the persisted timestamp's
microsecond precision. Sink canonicalization preserves object-order equivalence
and rejects duplicate keys and invalid numeric data. Remote responses use that
strict canonicalization before decoding, including conflict responses.

Remote clients require HTTPS with certificate verification by default. Configure
one trusted endpoint and bearer credential; redirects are refused, proxies are
not inherited from the environment, responses are capped at 256 KiB, and calls
have a positive timeout no greater than one minute. The explicit local harness
option permits HTTP only for numeric loopback addresses. It does not qualify
production TLS. Client errors contain no URL, bearer value or response body.
The host must use Forge authentication and an explicit destination Warden check
before calling either reliable acceptance API. Disable default unprotected sink
and administration routes.

## Conflicts and publisher compatibility

Only a confirmed reliable-API content conflict becomes `delivery.ErrConfirmedConflict`.
A remote HTTP 409 must carry `ConflictResponse` with the matching destination,
source identity, source fingerprint and recomputed sink fingerprint. A generic
409, timeout, lost response, invalid receipt, unavailable sink or Chronicle stream
head contention stays retryable. Never convert uncertainty into a permanent
conflict.

`BlockDelivery` persists `ErrorCategory=conflict` under the same owner, epoch and
expiry fence as acknowledgement. Blocked rows remain pending, contribute to a
separate blocked count and prevent a complete drain. Claims exclude them. A
publisher restart cannot clear the disposition, change the immutable envelope or
resume retries. Repair needs a separate protected operation; this API supplies
no automatic repair or silent discard.

Migration `20261029120000` installs the independent delivery publisher floor in
`dispatch_delivery_compatibility`. `DeliveryPublisherProtocol=1` means the
publisher understands conflict exclusion and sink receipt verification.
`AuditWriterProtocol=1` still means audited history/receipt intent support and
cannot establish publisher compatibility.

A new PostgreSQL publisher checks the floor at construction. Each claim or
fenced mutation sets its capability marker with transaction-local `set_config`.
The database rejects missing, malformed or incompatible markers with SQLSTATE
`DA003`, and refuses every mutation of a blocked row. Pooled connections cannot
retain the marker after commit. Older artifacts, including Dispatch
`v1.7.1-0.20261009193606-6d86536a4ba5`, cannot acquire durable ownership with their
pre-protocol claim SQL after this migration. Compatible publishers can still
claim unrelated rows. Stop incompatible publishers before migration and exclude
them from rollback admission; a database refusal does not upgrade their code.
The migration refuses downgrade even when no conflicts are present.

The minimum safe artifact must include the conflict-aware publisher, both store
implementations and this migration. Deployment preflight must compare its delivery
publisher capability against the persisted floor, separately from namespace
writer coverage. Installation-wide status is internal publisher telemetry; it is
not an authorized namespace or run projection for an operator API.


`Engine.StopWorkers(ctx)` permanently stops the durable runtime, legacy pool,
scheduler, workflow replay, wake listener and heartbeat producers for that engine.
It waits for the same quiescence task used by `Stop`. A caller deadline can return
before a handler finishes; retry with a fresh context to confirm completion.
This terminal worker stop keeps publisher delivery and storage live, so accepted
commands can still append local intents and the publisher can drain them. It is
not a resumable pause or a build-retirement protocol. Final `Stop` reuses confirmed
worker completion, runs the existing final hooks and required publisher drain,
then closes storage once. `Start` refuses after worker stop has begun.
