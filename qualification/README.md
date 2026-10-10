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
