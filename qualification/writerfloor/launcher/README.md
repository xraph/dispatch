# Fixed Linux qualification launcher

You prepare a finite set of reviewed workloads, enroll their exact machine identities, start the admission hosts, then start each workload. The launcher runs as root on the isolated qualification host. Workload processes run as UID/GID 10001 with no capabilities, a read-only root, fixed mounts and bounded resources.

The launcher uses the published Docker SDK through `/var/run/docker.sock`. This engine must differ from the separately configured provider engine. Client containers have no network. Their only request path is a private Unix socket whose host proxy holds a per-launch mTLS key that the client cannot read. Executors use exactly two preconfigured internal network IDs. The launcher neither creates nor discovers networks.

## Prepare the trusted inputs

The configuration directory and separate state directory must be absolute, root-owned, mode 0700 and free of symlinks. Keep state outside the authority database's restore boundary. Files in the configuration directory are root-owned regular files with mode 0600:

- A canonical `Configuration` JSON file supplies Authsome, Warden and external ledger database connections, the real app/environment/tenant, and the Authsome encryption key.
- `manifest.json` supplies at most four already-certified artifact entries. Each entry binds its OCI repository digest, image ID, executable hash, Go build metadata hash, evidence hash and complete fixed specification.
- `<certificate>-config` supplies the exact role configuration bytes authenticated by the specification. It is valid JSON, at most 4096 bytes. Client configuration contains no writer, provider, ledger or launch credentials.
- `proxy-ca.pem`, `proxy-ca-key.pem` and `upstream-ca.pem` establish the private TLS channel. The proxy CA key stays on the host.

Images must already exist on the launch engine. Use a static Linux Go executable at `/app/workload`, no image environment, volumes or healthcheck, and no runtime libraries. `ExecutableDigest` is SHA-256 of the actual executable bytes. `ModulesDigest` is SHA-256 of the embedded `debug/buildinfo` result's `String()` bytes. The launcher reads and checks both from the stopped container; it never executes a shell inside the image to measure it.

The database schemas, application/environment, exact Warden tenant, certificate facts and bounded internal networks must exist before preparation. The launcher does not migrate databases, self-certify artifacts, install database grants or reopen admission.

## Run in order

For every launch, select one manifest certificate and a new immutable launch ID:

```sh
writerfloor-launcher -config /private/config/launcher.json -command prepare -launch client-one -certificate reviewed-client
writerfloor-launcher -config /private/config/launcher.json -command enroll -launch client-one
```

Preparation retains actual Authsome account/key IDs, the stopped Docker container ID and its immutable ledger binding. Enrollment retains the full Warden rule before its unique insertion and returns `Enrollment` metadata without secrets. Load those records and their `Credential()` pairs into the actual Authsome provider before starting the fixed admission/executor listener. Mount `NewReadiness(...).Handler()` at `/writerfloor/ready` on its direct mTLS listener. The surrounding managedhost composition supplies the actual role handlers; this package does not substitute a permissive executor.

Then start the supervised launch:

```sh
writerfloor-launcher -config /private/config/launcher.json -command start -launch client-one
```

Start verifies the saved image, executable, configuration and stopped container again. It requires a fresh challenge response authenticated by the private TLS channel, actual Authsome account/key, Warden decision and exact unrevoked ledger binding. The response binds the complete enrollment digest. It does not authorize a mutation. The launcher retains `starting` before making one Docker start call and supervises the proxy for at most 20 minutes.

Use `inspect` to read a record and `stop` for bounded teardown. An active supervisor closes the proxy before stopping and removing the recorded Docker ID. It revokes the exact retained machine key and binding. Records and policy/account history remain retained. Stop does not delete by a Docker name or label, treat absence as success, or force-remove a different resource.

## Failure boundaries

A repeated prepare never creates another machine or container. An uncertain `starting` record cannot start again. Lost creation responses can leave an unknown stopped container; without its actual returned ID, this package retains uncertainty and cannot claim cleanup. A lost removal response also remains uncertain. A binding or revocation CAS conflict remains inspectable and needs trusted reconciliation; there is no blind retry with changed authority.

Enrollment retries inspect the retained policy ID and exact rule before succeeding. Readiness can be retried because it is read-only. No readiness response, copied bearer, request header, image label or saved delegation receipt becomes a new transport grant.

The file journal supports one isolated local Linux host with root-owned storage. It is not a distributed filesystem protocol. The trusted root operator, Docker daemon, enrollment administrator and database role separation remain part of the topology. The launcher cannot establish provider effect completion or native guard closure from its process record. Positive writer-floor support remains disabled until the complete core and assembled-host protocol passes qualification.

## Checks still required

The Linux suite includes manifest, journal, private-control, SDK HTTP, proxy mTLS and readiness-probe fixtures. Its 44 checks passed with required root ownership and no skips. They do not establish a live container or real Authsome/Warden readiness pass. Run the module's formatter, lint, normal/race tests and build after integration. Linux tests that need root fail rather than skip when `WRITERFLOOR_LAUNCHER_REQUIRED=1` is set. A disposable Linux host campaign must additionally exercise actual prepare/enroll/host-ready/start/stop, copied credentials, replacement identities, restart, database outages and ambiguous transport responses, with bounded resource and credential cleanup recorded by exact IDs.
