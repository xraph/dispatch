# Writer-floor admission qualification

Keep the admission ledger outside the authority database's restore domain. This module supplies a bounded PostgreSQL ledger and a Forge request boundary using the published Authsome machine provider, Warden policy decisions and a verified private TLS channel. It does not install the isolated host topology or award protocol 1.

You need a trusted setup connection to install `admission.Schema`, a configured admission-service database connection, and four host adapters: the protected floor and restore-incarnation observer, the exact original-permission verifier, the cutover verifier, and the one-attempt executor delegate. All are required. Constructors refuse missing and typed-nil adapters.

Administration installs an empty, closed authority with a frozen record quota. It retains up to four reviewed artifact certificates, immutable process/proxy bindings, revocations, administrative request receipts and delegation facts. The quota covers every immutable row. Each canonical JSON record is at most 4,096 bytes; the campaign can retain at most 4,096 facts. Capacity exhaustion refuses new work. There is no deletion or reuse path for unresolved records.

`begin` closes fresh admission and records an exact pending protected-floor activation. Epochs advance by one, and the minimum protocol cannot decrease. `open` requires the exact marker, executor binding, delegation sequence and frontier, plus affirmative evidence verified by the configured cutover adapter. `close` stops fresh admission. Historical administrative replay returns its original receipt without repeating the transition.

A delegation authenticates the existing executor binding and its certified protocol, reads the protected marker and independent restore incarnation, then verifies the original invocation selection. A single PostgreSQL authority-row lock serializes that acceptance with cutover. The committed fact is the linearization point. That commit enters a nonadmitting `delegating` state. Only the new successful commit path calls the configured delegate, once, with the complete retained executor, floor, incarnation and invocation association. It returns no reusable grant. Historical replay only inspects that fact, including after restart or a lost response. A failed commit acknowledgement never sends. A successful adapter acknowledgement reopens admission only through an exact unchanged-state CAS. A failed acknowledgement or crash leaves fresh work closed until trusted reconciliation. This first slice serializes in-flight forwarding; its acknowledgement records no physical effect completion.

The delegate must target the saved executor incarnation, use its exact typed command, and disable transport retries. It cannot choose the current replacement executor. If admission closes after commitment, that invocation remains possible-effect custody until the exact cutover procedure stops its executor and invalidates its real sessions. Closing admission alone is not transport revocation. This slice records no provider completion, native settlement or physical resource absence.

The Forge handler authenticates an admitted executor service account through real Authsome persisted key/account checks and Warden, then binds it to the certificate from the verified TLS peer chain. The service never accepts a workload header or caller-selected protocol. Bind this handler only to the listener using `host.TLSConfig`. The trusted launcher must provision the private executor proxy and prevent direct or alternate listener access. Client-to-executor authentication and its private client proxies remain a separate required boundary.

Every authority restore, including a restore with the same protected epoch and digest, must replace the independent launcher incarnation before exposure. A mismatch persists quarantine. Reopening requires trusted reconciliation against the complete retained external frontier. Matching the marker is insufficient. Restoring or deleting both databases is outside this trust boundary, as are a compromised launcher, ledger administrator or admitted executor.

The table trigger refuses updates and deletes of immutable facts. Deployment setup must also exclude untrusted SQL sessions, DDL, truncation, trigger disabling, alternate schemas and ledger credentials. Runtime and administration handles are separate application interfaces, not proof of PostgreSQL role isolation. The fixed Linux launcher is documented in [launcher/README.md](launcher/README.md). Role/grant cutover still needs assembled-host qualification. The admission service holds no provider or native credentials; its delegate adapter forwards one exact attempt to the saved executor over the configured trusted channel.

The current snapshot validates the complete finite delegation chain before mutation or state inspection. This is bounded by the frozen campaign quota, not intended as an unbounded production history scan. Indexed pages return at most 64 references. Pages and their cursors provide inspection, never settlement or a proof that all effects are gone. A later insertion with an earlier invocation ID may fall behind a cursor; reconciliation must use the exact sequence and full frontier.

## Checks

Resolve the published module graph with `GOWORK=off GOTOOLCHAIN=go1.26.9 go mod tidy`. There are no replacements. The Ctrlplane pin includes the published protected marker and Docker custody implementation. The pin also includes guarded recovery and independently retained provision receipts. Full terminal recovery qualification remains pending.

Use a disposable PostgreSQL server, then run `make f`, `make l` and `WRITERFLOOR_TEST_DSN=... make check`. Tests create a fresh schema and clean it up. `WRITERFLOOR_REQUIRED=1` turns a missing server into a failure; the Makefile sets it for the test target.

The tests cover concurrent one-shot acceptance, replay through a new database pool, lost dispatch responses, cancellation rollback, drain locking, same-marker restore quarantine, exact reopen evidence, below-floor denial, key/process replacement, capacity, immutable history and canonical corruption. The host tests use real Authsome and Warden with a synthetic already-verified TLS state. They do not qualify a TLS handshake, private proxy isolation, fixed launcher, remote executor transport, actual cutover credentials, process termination or restore orchestration. Those remain required before a protocol certification can be issued.

## Docker custody drill

`cmd/docker-custody-qualify` runs on the isolated Linux qualification VM as its
dedicated provider user. It checks the exact rootless engine identity and requires
a preloaded ARM64 Alpine image selected by its real repository digest. The harness
refuses the launch engine, module replacements, unexpected images and a nonempty
container inventory.

You get eight cases: network creation, container creation, start, container removal,
network removal, a crash after delegation, a crash after retaining the response,
and a lost successful response. Every replay runs in another process. The latter
case must remain unresolved without another send. The test enrollment and custody
attestations are fixtures; they do not qualify the production host's authority.

The campaign allows one container and one network at a time, with a ten-minute
deadline. The container uses 100 millicores, 32 MiB, 16 PIDs and no restart policy.
Cleanup uses only recorded creation IDs. Initial and final inventories must match,
and the private evidence directory is bounded to 64 MiB. Keep that directory when
investigating a failure. Discovery never authorizes removing an unknown resource.

On October 14, 2026, all eight cases passed against Docker 29.9.0 in the isolated
rootless engine using Ctrlplane `db121ca`. The final inventory matched the initial
inventory. This drill covers process crashes and response loss, not VM disk loss
or restoration of the assembled host.

## Fixed launcher and creation custody

You can prepare, enroll, inspect, supervise and stop a finite set of pinned Linux workloads with `cmd/writerfloor-launcher`. The launcher verifies the stopped container's executable and mounted startup bytes before starting it. Its private proxy holds the mTLS key outside the workload. After a retained start attempt, every return path closes that proxy and attempts bounded cleanup of the exact container ID, including a lost Docker response or a failed journal write.

The Linux fixture campaign passes 44 checks with no skips, including private TLS handshakes, startup substitutions, journal restart and cleanup failures. CI reruns these checks with the race detector and requires root ownership tests. Actual Authsome/Warden readiness with the assembled host and live Docker inspection remain separate checks.

`internal/managedhost` retains signed resource creation custody before one SDK send. Its creation admission adapter captures fresh grants, drops stale grants at its finite capacity boundary, and never rebuilds a send grant from historical acceptance. Removal identity comes from the retained creation result. This component still needs the service-worker enrollment, native lifecycle adapter and complete managed provider loop before you can qualify an assembled deployment.
