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

The assertions recompute source/sink bindings from acknowledged rows, compare
stored sink receipt evidence, verify Chronicle HMAC-v5 digests and chain linkage,
and check source-payload and credential exclusion. Set
`DISPATCH_SINK_EVIDENCE_DIR` to retain sanitized logs and receipt JSON outside the
temporary directory. Never retain bootstrap configuration files or upload them
as CI artifacts. Process test fixtures are not production server configuration.
Local numeric-loopback HTTP, one database instance and small fixtures do not
qualify production TLS, high load, failover or external anchoring.
