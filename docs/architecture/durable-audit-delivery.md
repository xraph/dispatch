# Durable audit delivery

Use `durable.NamespaceStore` and `durable.OutboxStore` when your host requires
local durable acceptance of audit or hook delivery. PostgreSQL is the production
backend. Memory implements the same contract for tests and development. Check
both capabilities explicitly; a legacy store does not provide a reliable fallback.

This slice persists delivery intents and sink receipts. It does not run a
publisher, configure a Forge host, or connect Chronicle or Relay. Those remain
separate composition work. Accepted local intents do not establish sink delivery.

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
mounting protected routes. A requested namespace or identity claim cannot choose
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

Status reads return bounded records, pending count and the oldest pending local
acceptance timestamp. You can derive backlog age from that timestamp. PostgreSQL
count and page reads are separate READ COMMITTED statements, so status is
observational rather than a transactionally consistent monitoring snapshot.
