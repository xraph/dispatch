# Durable operator reads

Enable the read service with `extension.WithDurableOperators(keys, nil)`. You
supply a host-managed `operator.CursorKeys` map containing 32-byte AES keys and
an active version. The default authorizer resolves the installed Warden engine.
The extension supplies its store, installation and shared audit activation.
Requests before successful extension startup remain unavailable.

Provision each namespace through `durable.NamespaceStore`, then call
`operator.RegisterNamespaceSchema` with that persisted record during host policy
configuration. The schema declares the closed actions without granting them.
`operator/warden_test.go` contains a compiling policy example with a named user,
namespace resource, installation/app/tenant conditions and explicit actions.
Unknown actions and every unhandled Warden obligation deny access. Core has no
Authsome or Ctrlplane dependency.

## Contract

Use contributor `dispatch`, envelope `v1`, kind `query`, intent version 1.
`operator/testdata/durable-wire.json` is generated and asserted against the Go
DTOs and Forge success/error envelopes by `TestWireFixtures`. The token in that
file is an illustrative opaque value, not a usable continuation.

| Intent | Input | Permission |
| --- | --- | --- |
| `durable.namespaces` | `limit`, `cursor` | `dispatch.namespace.discover` per candidate |
| `durable.executions` | `namespace`, optional `workflow_id`, `workflow_type`, `build_id`, `state`, `limit`, `cursor` | `dispatch.execution.list` |
| `durable.execution` | `namespace`, `workflow_id`, `run_id` | `dispatch.execution.read` |
| `durable.history` | exact run key, `limit`, `cursor` | `dispatch.history.read` |
| `durable.tasks` | exact run key, optional `kind`, `limit`, `cursor` | `dispatch.task.read` |
| `durable.chain` | exact run key, `limit`, `cursor` | `dispatch.chain.read` plus read grants for linked runs |
| `durable.children` | exact parent key, `limit`, `cursor` | `dispatch.chain.read` plus read grants for children |
| `durable.payload` | exact run key | `dispatch.payload.read` and durable read audit acceptance |
| `durable.audit` | namespace, optional paired workflow/run, `limit`, `cursor` | `dispatch.audit.read` |
| `durable.hooks` | namespace, optional paired workflow/run, `limit`, `cursor` | `dispatch.hook.read` |

The list permission grants the metadata projection across that namespace.
Discovery permission alone does not. There is no installation-global durable
read intent and an empty namespace never acts as a global grant. Legacy intents
retain their installation gate. Both direct dispatch and HTTP call the same
operator service; the durable admission delegate only verifies the known intent,
kind, contributor and identity. The typed decoded target is authoritative.
Browser app, tenant and actor fields cannot establish ownership.

## Paging and freshness

Pages default to 25 entries and cap at 100. Use `as_of` for server observation
time and `cursor` for continuation. `total` is null because this API does not
compute a complete authorized count. Do not turn null into zero.

Namespace discovery evaluates at most 32 catalog candidates per request within
a five-second deadline. Authorization occurs before an item enters the page.
`complete: false` with no visible items means discovery is unfinished. Follow
the opaque cursor to continue; it does not mean you have no namespaces. No
candidate counts or denied keys are returned. Continuations reauthorize their
captured visible scope and every new candidate. A revoked captured grant
rejects the continuation. A newly granted namespace behind the scan position
requires a fresh scan.

Cursor state is encrypted and authenticated with AES-GCM. It binds the verified
principal and kind, installation, operation, explicit scope, normalized filters
and page size, and expires after ten minutes. Preserve older key versions for
that period when rotating. Changing a filter, principal or target rejects the
cursor. Each operation has one fixed documented order; cursors confer no grants.

Trusted execution, task and build reads accept the persisted 512-byte identifier
contract. Catalog ownership and delivery identifiers retain their 256-byte
limit. Cursor budgets account for six-byte JSON escaping, timestamp and binding
fields, base64 expansion, all 32 captured namespace names, the key version and
AES-GCM nonce/tag. Oversized state fails before encoding, and oversized input
fails before decoding. The store cursor constructors return errors so a failed
continuation cannot silently become an empty last page.

Execution pages order by immutable `created_at`, workflow ID and run ID, newest
first, using byte ordering for identity ties. Tasks order by immutable task ID
within one exact run. Tasks have no historical creation timestamp; none is
invented. Each page observes current state, so concurrent starts or filter-state
changes can change which records are visible across pages. This is not a
transactional multi-page snapshot. Execution revisions are per row. Task pages
include the run revision read immediately before their task query.

History captures the run revision and last sequence on its first page. Its
cursor retains that high-water sequence, so later events require a fresh read.
History metadata contains only event type, sequence and time. Run chains start
at the requested run and follow successors. A denied link ends that traversal
with `restricted: true` and `complete: false`; no denied target is disclosed.
Children use a bounded authorized scan and report `restricted` separately from
whether the scan has finished. Detail links are independently authorized.

## Restricted and unavailable data

All 64-bit counters are decimal strings. Keep them as strings in JavaScript.
Metadata has no workflow input/output, event payload, task progress, async secret
or hash, owner/lease proof, intent digest, raw sink envelope or sink receipt.
`payload: restricted` means no payload was read through this response. Explicit
reveal requires its own grant, a durably accepted audit and a combined response
payload no larger than 1 MiB; its bytes use base64 so JSON numbers retain their
original spelling. No durable presigned URL intent is exposed.

Metadata inspection does not require a runtime. The extension reports
`runtime: unavailable` until a host has supplied an exact namespace/build
availability resolver to a directly composed service. Historical builds never execute through active code. Command-capable hosts resolve
the exact namespace/build through `WithDurableOperatorRuntime`; the extension can
also serve its own pinned worker. A missing build remains unavailable.

Delivery counts and pages use installation, destination, namespace and optional
exact run predicates inside storage. `pending` includes `blocked`; a blocked
source requires repair and does not automatically retry. `sink_accepted` means
the source publisher verified reliable sink acceptance. For Relay, this proves
persisted event/fanout acceptance, not endpoint delivery. Remote endpoint
status and Chronicle external anchoring are separate unavailable facts here.
Count and page statements can observe different instants during publication;
they do not claim a shared transaction snapshot.

Authentication, permission, invalid input, not-found and unavailable errors use
Forge's normal envelopes. Provider text is never returned. Denial audit failure
keeps a denial denied. Sensitive-read audit failure returns unavailable before
payload leaves the service. Every durable handler sets `Cache-Control: no-store`.

## Commands and workflow queries

The durable contract accepts `durable.start`, `durable.signal`,
`durable.signalStart` and `durable.cancel` as commands. `durable.query` and
`durable.capabilities` are queries. Every call rechecks current Warden permission;
a capability response is only an observation. Command responses invalidate all
durable run views, delivery observations and protected query intents.

Send an explicit namespace, workflow ID and run ID for start, signal and cancel.
The remote start accepts workflow type, build, queue and base64 input bytes; it
uses the runtime's default retry/timeout policy. Query may select current/latest,
but the service resolves that selector once and authorizes the resulting exact
run. It returns base64 output and decimal-string revision and last sequence.
Query handlers cannot invoke mutating workflow SDK operations. Go code can still
perform external side effects or block; Dispatch cannot sandbox arbitrary Go.

Keep the entire request unchanged after an uncertain response, including its
request ID, target and input bytes. Same identity/content recovers the original
acceptance. Changed content conflicts. Request IDs are bounded to 256 bytes so
trusted audit metadata can retain them. The service checks current permission
before recovery. A cancel response says `cancellation_requested`; the workflow
can still be running while it processes cancellation and cleanup.

Signal-with-start requires `dispatch.workflow.signal_start` and
`dispatch.workflow.signal` on the workflow identity with an empty run/type/build,
plus `dispatch.workflow.start` for the proposed type/build. These workflow-wide
grants authorize either atomic branch. Run-only grants do not substitute. The
optional `durable.SignalStartOutcomeStore` reports fresh versus recovered
acceptance under the store lock/transaction. Recovered receipts additionally
require grants for the original accepted run's persisted type/build, including
when a successor is now current. Custom stores without that capability fail
closed. Unknown-ack runtime retries retain recovery provenance.

`dispatch.workflow.query`, `dispatch.workflow.cancel`, `dispatch.activity.complete`
and `dispatch.activity.heartbeat` are distinct permissions. Persisted namespace
ownership supplies installation, app and tenant. Run policy receives immutable
workflow type and build. Trusted actor and request IDs accompany each mutation's
transactional audit and required hook intents. The policy adapter does not invent
policy decision IDs or versions that Warden has not supplied. Remote sink failure
does not replace the requirement for successful local outbox acceptance.

The Forge API mounts `POST /v1/durable/activities/complete` and `/heartbeat` through
`api.WithDurableCallbacks(service, authenticator)`. The extension adds its base
path, normally `/dispatch`. The authenticator must be a stock
`security.ForgeAuthenticator` using explicit provider-attested Bearer/DPoP
credentials. User sessions cannot call these endpoints. Verified `service`,
`api_key` and `service_acct` identities still require the corresponding Warden
grant and the runtime's saved attempt/secret proof. Human metadata permission
does not confer callback authority. Callback epoch, initial heartbeat sequence
and heartbeat sequence are decimal strings; callback bodies are bounded to
2 MiB and individual byte payloads to 1 MiB. Treat handles as credentials and
never put them in metadata, logs, browser forms or URLs.

Completion and heartbeat acceptance use the existing durable receipt semantics.
Exact authorized retries can recover receipts after expiry or closure; new or
changed requests must satisfy the attempt's current proof and deadlines. All
responses carry `Cache-Control: no-store` and sanitized errors. Build mismatch,
history incompatibility and absent runtime remain explicit states. No operator
reset, force termination, fork or history rewrite is exposed.


Remote proposed start inputs, including `signalStart.start.input`, are limited to
1 MiB of raw bytes by the operator service. This bound also applies to direct
contract and service calls. Base64 expands the HTTP body, so a transport's JSON
envelope limit can reject a smaller payload before it reaches the service.

Durable commands bypass Forge's generic response cache. Persisted runtime receipts
control retry conflicts and accepted-target recovery. Legacy commands retain
Forge deduplication, with current decoded-target authorization before cache access.
Their cache binding includes the full principal and request plus a versioned tuple
of the trusted installation ID and policy tenant. Changed principal facts or a
changed host scope can conflict with an earlier key; retrying must preserve the
original request and current authority. A changed key is not an authorization
workaround.
