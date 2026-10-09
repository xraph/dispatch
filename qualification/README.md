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

These are memory-store credential regressions. They do not qualify PostgreSQL,
production deployments, service accounts, credential rotation or revocation,
external denial audit sinks, or the final assembled-host authorization matrix.
The fixture is a starting point for those later gates.
