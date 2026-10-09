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
availability resolver to a directly composed service. No workflow query or
command is advertised, and historical builds never execute through active code.

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
