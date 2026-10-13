# Dispatch host qualification

Run the credential regression from the repository root with Go 1.26.9:

```sh
make qualification-lint qualification-check
```

This separate Go module consumes published Dispatch, Forge auth, Authsome and
Warden versions. You can run `GOWORK=off GOTOOLCHAIN=go1.26.9 go test -race
-count=1 ./...` from this directory. Keep local replacements out of `go.mod`;
the CI job rejects them and checks the nested module independently.

The fixture starts global Authsome middleware, issues real sessions for two
persisted users, evaluates real Warden policies and exchanges DWP WebSocket
frames. We activate a required Dispatch audit namespace before admission.
The tests inspect admitted identities, Warden subjects and installation targets,
and audit actors. User A can subscribe; user B can also read statistics. A cookie
for A plus an explicit frame for B must retain B's permissions and audit identity.

DWP selects the nonempty `AuthRequest.Token` before `Frame.Token`. A bare token,
`Bearer <token>` or `DPoP <token>` is parsed once at that decode boundary, then
bound to the same canonical scheme and token used for provider verification.
The marker preserves the handshake context and grants no identity. The provider
still verifies the credential. REST and SSE don't produce this marker, so a
cookie-bridged Authorization header remains cookie-sourced there.

For DPoP, send the proof in the WebSocket handshake's `DPoP` header. Proof fields
inside auth-frame data are unsupported. The tests cover bound-bearer refusal,
missing, malformed or mismatched proofs, valid proof admission, replay on a fresh request,
and an equal bound cookie/frame pair sharing the request-local proof cache.
Cookie-only WS, REST and SSE admissions stay denied. A mismatched marker cannot
promote an unchanged bridged cookie, but it does not invalidate an unrelated
ordinary explicit header.

The default suite keeps these memory-store credential regressions fast. It skips
process qualification unless you supply an explicit PostgreSQL fixture and host
binary. A passing default suite alone does not establish sink integration.

Run the separate process gate from the repository root:

```sh
go install golang.org/x/vuln/cmd/govulncheck@latest
make qualification-process-check
```

You need Docker and Go 1.26.9. The gate builds the actual `cmd/sinkhost` executable,
records its compiler/module metadata, scans the program and binary, and starts
one private PostgreSQL 17 container capped at 512 MiB, one CPU and 128 PIDs.
Chronicle, Relay, Dispatch and the webhook receiver run as separate native
processes. Each has a 128 MiB Go memory target and a supervisor that stops it if
sampled RSS exceeds 384 MiB. This is sampled supervision, not a native kernel
memory limit. The scenario has a four-minute deadline and the Go test has a
six-minute timeout. The script removes only its own container, anonymous volume
and temporary files on exit. CI runs this gate separately after the credential
suite and test-graph vulnerability scan. Missing fixture inputs fail the explicit
gate instead of silently skipping it.

Bootstrap uses real Authsome PostgreSQL storage and the public environment-bound
service-account constructor, then issues persisted machine keys. The Forge
acceptance routes verify the real API-key strategy, persisted key/account scope,
trusted installation/app/environment mapping and destination-specific Warden
policies. No default Authsome, Warden, Chronicle or Relay administration routes
are registered. Only acceptance, command, terminal worker-stop and content-free
health routes are exposed by the relevant process. Receiver routes verify Relay
signatures before storing delivery evidence. Token encryption and Chronicle HMAC
use separate random keys held in private temporary configuration files.

The matrix kills each sink independently while PostgreSQL stays available,
checks local source admission/backlog and complete two-endpoint Relay fanout,
and kills Dispatch after sink acceptance but before source acknowledgement.
Restart must recover the same receipt without another sink event. It also stops
the receiver, confirms retry/recovery, stops managed workers while publication
continues, checks denied-command audit without execution, and tests persisted
credential, account, resolver, policy and obligation refusals. Confirmed conflicts
remain immutable, pending and blocked across restart; unrelated work continues,
and final publisher shutdown reports incomplete drain.

Authority phases keep Authsome's default 20-failure/60-second per-IP throttle.
Its fixed-window counters persist in the shared PostgreSQL store, including
across process restarts. The ordinary process topology uses one shared authority
store. Before any HTTP requests, the fixture seals a whole-store baseline of the
engine-issued identities. Independent authority scenarios use disposable clones
of that baseline while retaining the original source, sink and Warden databases.
No identity rows or failure counters are reset.

After a revoked-key publisher attempt, the test stops that publisher, restores
the persisted key and replaces the affected role's authority fixture before
recovering the original source with a verified receipt. This proves recovery
after fixture replacement, not cooldown recovery against the exhausted store.
A new command-created pending source and stopped publisher isolate the denial
matrix. Resolver restoration must pass a valid no-effect probe before policy
checks. A separate pristine fixture tests 19 failures, a valid request, the
20th failure, and rejection of the restored valid key. Restarting against that
same store must still reject it within the same recorded fixed window. A new
fixture then permits the valid request. A bounded wait before the pristine
campaign avoids crossing a window boundary; polluted campaigns are not retried.
All authority clones and the sealed baseline disappear with the task container.

The assertions recompute source/sink bindings from acknowledged rows, compare
stored sink receipt evidence, verify Chronicle HMAC-v5 digests and chain linkage,
and check source-payload and credential exclusion. Set
`DISPATCH_SINK_EVIDENCE_DIR` to retain sanitized logs and receipt JSON outside the
temporary directory. Never retain bootstrap configuration files or upload them
as CI artifacts. Process test fixtures are not production server configuration.
Local numeric-loopback HTTP, one database instance and small fixtures do not
qualify production TLS, high load, failover or external anchoring.

## Durable operator browser fixture

You can run the actual published Dispatch contract locally for the dashboard
inspection work. `internal/operatorhost` uses the shared Authsome bootstrap,
Authsome-issued human sessions, the stock Authsome middleware, Forge dashboard
identity/contract transport and real Warden policies. The injected Dispatch store
can be memory or PostgreSQL. Authsome and Warden fixture stores are memory only.
This is a loopback qualification host, not a production deployment.

Build and start it from this directory:

```sh
operator_dir=$(mktemp -d)
GOWORK=off GOTOOLCHAIN=go1.26.9 go build -o "$operator_dir/operatorhost" ./cmd/operatorhost
"$operator_dir/operatorhost" --state-file "$operator_dir/state.json" &
operator_pid=$!
```

Pass `--run-workers=false` only when you need to inspect an accepted command before
worker execution, such as cancellation requested on a nonterminal run. The default
is true. Both modes register the same exact runtimes, authenticate and authorize
commands, and serve queries. The disabled mode does not start worker loops; it
does not qualify graceful drain, worker readiness or production lifecycle behavior.
Stop either mode with SIGINT or SIGTERM and wait for process exit before removing
its private temporary directory.

The new state file is mode 0600. It contains the numeric-loopback `url` and
`credentials.reader`, `credentials.payload`, `credentials.denied` and
`credentials.commander`, each with an Authsome session `token` and subject.
`credentials.machine` holds the environment-bound service account key. Keep this file private. Do not commit,
print or copy it into screenshots, browser URLs, client bundles or evidence.
Your local dashboard proxy can read the reader token server-side and forward it
as `Authorization: Bearer <token>` to `/api/dashboard/v1`. The host also accepts
the real Authsome session cookie, as covered by its HTTP tests. It provides no
login bypass endpoint, permissive CORS policy or alternative identity provider.
Use the payload credential only for the explicit reveal check.

Set `DISPATCH_OPERATOR_DSN` before starting to use a disposable PostgreSQL
database. You must not point this seeding host at shared or production data.
It persists namespaces, executions, continuation history, task state and outbox
records. Restart keeps that Dispatch state but issues new identities and cursor
keys. The fixture seeds production and foreign tenants, 35 denied discovery
candidates, invoice runs `run-1`/`run-2`, an approval workflow and deliberate
source conflicts. These conflicts test the read projection; no sink acceptance
or external delivery is fabricated. The seeded `historical-v1` build is unavailable.
The running host registers `operator-v1` and `operator-v2` exactly.

The first `durable.namespaces` response has zero visible items and
`complete: false`. Follow its cursor to reach `production`. Read
`production/invoice/run-1` for a continuation link, and `run-2` for its successor.
The reader can inspect production metadata but cannot reveal payloads, read the
foreign namespace or call installation-wide legacy operations. The payload
identity has the additional reveal grant. Denials and payload reveals persist
security audit before returning protected data.

Shut down with `kill -TERM "$operator_pid"` and `wait "$operator_pid"`. The
process removes its state file on normal shutdown. Remove the private directory
after the process has exited. You own any database/container you provided.

From the Dispatch root, `make qualification-operator-check` builds the published
host without module replacements, records its compiler/module versions and runs
the bearer/cookie, policy/session revocation, PostgreSQL and native process
checks. It also scans the test graph, executable source and built binary. Its
disposable PostgreSQL 17 container is capped at 512 MiB, one CPU,
128 PIDs and 40 connections. The exit trap removes only that fixture and its
private temporary directory. `GOMEMLIMIT=128MiB` is a Go runtime target for the
host and test processes, not an OS memory limit.

`internal/operatorhost/testdata/http-errors.json` captures actual status codes
and error envelopes from the pinned Forge HTTP transport. Keep this separate
from `operator/testdata/durable-wire.json` in the Dispatch core, which asserts
DTO serialization. The published Forge transport preserves canonical errors:
401/UNAUTHENTICATED for missing, invalid or revoked sessions;
403/PERMISSION_DENIED for denied scope or reveal grants; 400/BAD_REQUEST for
invalid read input; 404/NOT_FOUND for an absent authorized resource; and
503/UNAVAILABLE when required audit acceptance is unavailable. Tests assert
both status and envelope code. The fixture does not rewrite transport responses.

The commander can start, signal, signal-with-start, request cancellation and query
production runs through the shared contract. Fetch `/api/dashboard/v1/csrf` with
the same session before sending commands. Each action has a separate Warden
grant. Use the `operator` workflow on queue `operator`: its `status` query returns
the original input bytes and its `finish` signal completes the run. The `continue`
workflow produces a successor; `async` and `async-expiry` obtain genuine deferred
activity handles. You cannot obtain those proofs through dashboard metadata.

Send explicit machine tokens to `/v1/durable/activities/complete` and
`/v1/durable/activities/heartbeat`. The registered provider invokes Authsome's
actual API-key strategy and checks persisted ownership before the stock Dispatch
authenticator and Warden command service run. Human sessions are denied on these
routes. This Authsome version issues `service_account`; Dispatch maps it to
`service_acct`. The compatibility aliases `service` and `api_key` are not separate
issued identities here. Agent/workload tests seed fresh account records with
those original kinds, then call the real key issuer and strategy. The unchanged
service-account provider rejects them. These fixtures do not qualify public
agent/workload onboarding or Dispatch's later kind gate for those identities.

The HTTP tests use the default claiming response cache for legacy commands and
check authorization again on cached retries. Durable commands recover persisted
receipts, compare exact content and authorize the accepted target. The suite
also drops an HTTP response after a committed signal or completion, then retries
the same request. Query outputs retain their bytes; the runtime rejects mutation
through its query SDK, but it cannot sandbox arbitrary Go side effects.

Callback coverage includes genuine expiry and rotated attempts, conflicting and
identical completion/heartbeat retries, revoked accepted retries, foreign targets,
malformed string-encoded counters and valid-shaped incorrect proofs. PostgreSQL
fault injection checks that required intent failure leaves no receipt, execution
mutation or new outbox entry. In the process gate, each stopped sink leaves
callback acceptance pending locally; restart must deliver and acknowledge it.
Private callback files are mode 0600 inside a separate mode 0700 test temporary
directory, even when you retain process evidence. Cleanup joins the workers,
collects any unread proofs for recursive evidence checks, then removes that
private directory on success or failure. Never include proofs in retained evidence.

## Lifecycle control host

Use `--lifecycle-instance=<trusted-instance-id>` to enable the deployment lifecycle
contract. Keep that physical identity stable across a restart. The private state
file also includes `runtimes`, with a fresh process RuntimeID for each build and
the captured executable and configuration digests. A restart creates a new
process incarnation. Retrying an old drain never targets it.

You can hold worker startup while the control endpoint accepts enrollment:

```sh
"$operator_dir/operatorhost" --state-file "$operator_dir/state.json" \
  --lifecycle-instance=local-host-a --activation-file="$operator_dir/activate" &
```

Enroll retirement, register the two builds and register each exact runtime through
the authorized contract. Then create the private activation file. Startup checks
for an active binding of that exact runtime, build and physical instance before
polling; an injected `StartupPolicy` can enforce your deployment controller's
physical-instance gate. `RegistrationPolicy` governs new registrations. Accepted
registration replay still checks current authorization and does not issue a new
registration. This local host does not implement a deployment instance registry.

For query-only retention, pass `--run-workers=false`, register the runtime, and
request its drain through `durable.workerDrain`. The prestart handle is real. A
completed drain prevents a later worker start while queries remain available.
`durable.workerStatus` separates process state and retirement compatibility;
`host_qualification` remains `not_observed` there. Query binding verification is
separate evidence.

The default named probe for each build replays `status` on the explicit production
run `lifecycle-history-operator-v1/run-1` or
`lifecycle-history-operator-v2/run-1`. Create that run through `durable.start`, using
workflow type `operator`, queue `operator` and input bytes `history`. Verification
requires closed polling and no local or unknown claims. The evidence digest binds
the exact runtime identity, probe name, sampled run, history revision and sequence,
and output digest. The expected output is configured locally. Callers cannot
supply artifact hashes, verifier URLs or favorable proof fields.

The embedded local removal controller can close its own query route after checking
an exact removal fence. Its abort path revokes future local issuance under the
same lock before capturing fresh query proof. These operations qualify only this
process's routing controller. They do not delete Docker or cloud instances, and
a replacement process cannot settle an older incarnation's removal.

Tests cover the published Dispatch consumer, actual Authsome/Warden authorization,
named persisted-history probes, exact replay, prestart drain, local settlement and
a native PostgreSQL restart. The native restart kills the original process and
checks that its accepted drain stays unknown while replacement admission stays
open. Native fault barriers also cover loss after receipt persistence and after
process invocation. The surviving original reconciles the accepted request; a
replacement reports the original outcome as unknown. The native Chronicle scenario
also qualifies lifecycle audit delivery through a sink outage. Browser verification
remains separate qualification work.
Keep private state files out of evidence and remove them after a killed fixture;
normal termination removes its state file automatically.


### Reproduce active and retained deferrals

You can populate the secured host for a browser or API client without writing
fixture rows. Build `cmd/operatorhost` from this module, then start a fresh host
with a private state directory and delayed worker activation:

```sh
operator_dir=$(mktemp -d)
GOWORK=off GOTOOLCHAIN=go1.26.9 go build -o "$operator_dir/operatorhost" ./cmd/operatorhost
"$operator_dir/operatorhost" --state-file "$operator_dir/state.json" \
  --lifecycle-instance=deferral-demo --activation-file="$operator_dir/activate" &
operator_pid=$!
python3 lifecycle-fixture.py prepare --state-file "$operator_dir/state.json" \
  --activation-file "$operator_dir/activate"
python3 lifecycle-fixture.py inspect --state-file "$operator_dir/state.json"
```

Run these commands from `qualification`. Python 3 uses its standard library only.
By default the host uses memory. Set `DISPATCH_OPERATOR_DSN` to your dedicated
PostgreSQL database before starting it if you need persisted state. The fixture
requires a fresh namespace and uses stable request IDs for each accepted command.
Its private state file contains credentials, so keep it out of logs and evidence.

`prepare` enrolls retirement, registers both builds and their exact runtimes,
retires `operator-v2`, then releases worker startup. It starts `deferred-child`
and `deferred-continue` on `operator-v1` and signals their handoff. You can now read
active deferrals through `durable.tasks` for namespace `production`, workflow IDs
`deferred-child` and `deferred-continue`, run ID `run-1`. The child record has a
command ID. The continuation record has no command ID; its reference kind carries
that distinction. Epochs and counts remain decimal strings.

Keep the host running while you inspect those rows. Then resume the target build:

```sh
python3 lifecycle-fixture.py resume --state-file "$operator_dir/state.json"
python3 lifecycle-fixture.py inspect --state-file "$operator_dir/state.json"
kill "$operator_pid"
wait "$operator_pid"
rm -rf "$operator_dir"
```

You should still see the records with `active: false`. The fixture waits for the
actual secured task response before printing either state. It fetches a real
session-bound CSRF token and uses the command contract for every mutation; it
cannot inject deferral facts. Native PostgreSQL qualification runs this same
script through prepare, inspect, resume and inspect. This establishes the runnable
host fixture, not browser rendering or a Dashboard integration.

Lifecycle mode also registers `sleep` and `retry`. They produce a one-hour timer
and a ten-minute activity retry interval. Together with `async`, the tests verify
that persisted open-run and callback obligations block finalization after a worker
drain, while late signals and valid machine callbacks remain accepted. Resume
preserves the writer protocol floor and leaves local process admission closed.
A separate PostgreSQL scenario completes a real child on the active build after
its parent worker drains. The resulting pending child delivery and open parent
continue to block finalization.

For private crash drills, `--drain-pause-before-file` or
`--drain-pause-after-file` writes a mode 0600 marker at the selected invocation
boundary and holds that first call until its context ends. Choose one flag and a
new path. These local qualification controls do not accept remote configuration.
The receipt already exists at either barrier. Required lifecycle intent failure
is tested separately: a PostgreSQL trigger rejects retirement and drain audit
intents, and neither the receipt, build mutation nor process drain is accepted.


### Native build provenance

Both native runners record the revision, tracked and untracked source state,
file hashes, binary hashes and `go version -m` metadata before qualification.
Generated manifests and binaries stay outside the checkout. CI refuses a dirty
checkout and retains the metadata as an artifact even when a campaign fails.
VCS stamping stays enabled.

For local work, capture the exact uncommitted snapshot you intend to test, then
pass that manifest to the runner. Use a new private path outside the checkout:

```sh
python3 qualification/source-provenance.py capture --repo . --output /private/dispatch-source.json
DISPATCH_QUALIFICATION_SOURCE_MANIFEST=/private/dispatch-source.json make qualification-operator-check
```

The runner refuses any later source change, including an unexpected untracked
file. It labels the resulting build dirty and preserves its exact snapshot hash.
You do not need to publish an unverified commit to run qualification. Set
`DISPATCH_QUALIFICATION_EVIDENCE_DIR` to an existing directory outside the
checkout if you want the retained metadata in a particular location. Each run
prints its evidence directory. Private process files still use separate temporary
directories and are removed after their processes have joined.

### Lifecycle audit delivery with native Chronicle

Pass `--chronicle-routes-file=/private/routes.json` with lifecycle mode to
start the audit publisher without sample executions. You need three native
Chronicle processes: production, audit and foreign. Each process keeps its own
immutable namespace binding, listen address, machine credential and Warden
policy tenant.

Bootstrap production once with `sinkhost.Bootstrap`. Use
`sinkhost.ProvisionChronicleProfile` for audit and foreign with the original app,
environment and signing keys. The helper registers the requested namespace and
issues a separate Chronicle-only credential through Authsome. Service accounts
share the app and environment; the sink's exact configured credential, Warden
policy and complete body mapping enforce the tenant boundary.

Save each sink's configuration privately, then use `sinkhost.ChronicleProfile`
and `sinkhost.SaveChronicleProfiles` to create a version 1 bundle with all three
routes. The bundle contains bindings, numeric loopback addresses and credentials.
It has no database DSNs or signing keys. Keep the directory at mode 0700 and the
file at 0600. Loading rejects symlinks, non-regular files, files above 64 KiB,
unknown fields, multiple JSON documents, duplicate identities and missing routes.
The operator checks the full persisted namespace catalog with a bounded query
before starting publication or HTTP readiness. The old `--chronicle-config` flag
still reads its single-config format, but this host refuses it with
`missing_route`. You cannot combine the two flags.

Stop and join your reader first, then signal and join the operator while all
three Chronicle processes remain available. The operator stops workers and
drains publication before closing storage. An incomplete drain returns a failure,
keeps storage and sink connections open, and lets an in-process caller retry
`Host.Close` with a fresh context. A process that exits with incomplete delivery
leaves its original PostgreSQL rows for a later process to recover. Only stop the
sinks and database after you have retained the operator's sanitized stage records
and outbox counts. The records include fixed error categories, deadline flags and
bounded counts; `counts_before_drain` identifies a snapshot taken before a timed
out drain, and `counts_known=false` means the store observation was unavailable.

`TestNativeAuditProductionOnlyMappingFailure` independently reproduces the old
production-only mapping failure with a real denied read and live sinks. It does
not attribute the historical unexplained operator exit. The other
`TestNativeAudit*` cases check native receipts, all six opposite credentials,
forged binding/body fields, persisted key and policy failures, audit-sink outage,
continued production/foreign delivery, storage retention, fresh-context shutdown
retry and process reopen. The process assertion requires normal exit 1 with
matching incomplete/deadline flags, and rejects signals, panics and other exit
codes. A failure injected immediately after an audit-sink restart checks that
the cleanup supervisor keeps replacement sinks alive until host recovery and
diagnostic retention finish. The provisioning tests verify original app/environment
identity, independent profiles and immutable namespace conflicts.

`make qualification-operator-check` now builds both native executables and runs
`TestNativeLifecycleChronicleOutage` against PostgreSQL. The test first delivers
retirement enrollment, build registration and exact runtime registration intents.
It starts both workers, kills Chronicle, then accepts retirement and worker drain
requests while audit delivery stays pending. An identical retry preserves the
accepted receipt. After Chronicle restarts, publication recovers while both workers
remain drained, and an explicit build resume also reaches the sink.

The test compares each lifecycle source ID, action and actor with its persisted
Chronicle acknowledgement, checks the mapped fingerprint and verifies the live
HMAC chain. It covers enrollment, build registration, runtime registration,
retirement, drain and resume. The isolated profile reaches zero remaining outbox
entries and shuts its publisher down cleanly before closing the store. Sink
outage does not become completed delivery just because a lifecycle command was
accepted locally. The fixture does not establish multi-tenant sink routing or
provider deletion.

### Query proof clocks and private diagnostics

The lifecycle host takes each new proof timestamp from
`InspectQueryRetention(...).ObservedAt` after the retained query succeeds. Abort
first revokes local removal issuance under the existing mutex, takes a settlement
sample from that store, then queries and takes a fresh proof sample. Read failures,
wrong targets and missing samples refuse the operation. Revocation stays in place.
Accepted requests still recover their original receipt before any new probe.

This adds a coordinated store read for each new proof, plus one for abort
settlement. There is no clock tolerance. A store clock rollback can make a proof
future at acceptance or earlier than settlement, and a forward jump can expire it.
Both remain refusals. Local removal and fence deadlines still depend on the host
clock. We have not unified provider clocks or qualified external deletion here.

You can inspect private `dispatch-query-rejection` JSON records on the native
host's standard error. They retain the failing stage, bounded reason and relevant
proof times or digests. They exclude query payloads, credentials and raw database
messages. HTTP responses keep their existing status and public error text. The
host keeps the latest 32 records for an in-process test failure; native tests join
the child before retaining sanitized records from a failed run. Each JSON record
is limited to 4096 bytes. The trusted local writer must return promptly, and
arbitrary Service observers have the same cooperative latency requirement.
Diagnostics never determine audit acceptance.

The clock regression advances only a Go test clock before constructing its host
and pool. It verifies the default sampler against the unchanged PostgreSQL clock,
with explicit cleanup inside that test and an independent test timeout. A separate
rollback fixture checks that settlement ordering remains strict. These tests do
not identify the cause of the earlier retained historical-query HTTP 409.

## Native removal guard compatibility

`querypreguard/` consumes published Dispatch commit `290a77873305`, before removal guards. Its executable exercises the old Begin, Abort, Finish, Resume and proof-refresh methods, plus fixed SQL updates and deletes. You keep this historical pin when updating the qualification host, just as you keep the `oldwriter` and `protocolone` pins.

Run `make qualification-queryguard-check` from the root with `DISPATCH_LIFECYCLE_TEST_DSN` set to a disposable PostgreSQL server. The target builds the three historical binaries in a temporary directory, runs their native compatibility campaigns with required fixture flags, then removes those binaries. Guard tests also cover fresh OS-process recovery, current proof expiry, pinned mutation races, the joint maximum material and audit rollback.

The complete material row is capped at 512 KiB; the exported receipt is capped at 4 KiB. Commit requests remain capped at 16 KiB, while the persisted commit receipt allows 17 KiB for its authenticated wrapper. Close requests and receipts remain capped at 16 KiB. The joint maximum lifecycle keeps all supported identities and proofs together.

The storage tests use synthetic verifier evidence. They qualify native transactions and compatible old-library refusal. Authenticated provider evidence, controller invocation markers, production database grants and server-loss recovery need separate host qualification.

The published operator host accepts an optional `LifecycleOptions.QueryRemovalGuardVerifier` from trusted local Go configuration. Without it, first guard acceptance is unavailable. `TestMemoryNativeGuardPort` and `TestPostgresNativeGuardPort` use local fixture records to exercise the authenticated HTTP commands, exact saved recovery through a host with no verifier, denied replay and rejection of caller-supplied evidence. This qualifies the native port wiring; the fixture does not authenticate provider permissions or cutoff records.
