# AGENTS.md

Dispatch is a durable execution engine for Go, shipped as a library. You import it, hand it a store, and register jobs, workflows and cron entries as plain Go functions. Retries, the dead letter queue, distributed workers and durable workflow history are its job. The repo also carries an artifact plane, a REST and WebSocket API, client SDKs, and a Forge extension with an operator contract for the dashboard.

## Modules

| Module | Directory | Tagged by release.yml |
|---|---|---|
| `github.com/xraph/dispatch` | `.` | yes, `vX.Y.Z` |
| `github.com/xraph/dispatch/qualification` | `qualification/` | no |
| `github.com/xraph/dispatch/qualification/oldwriter` | `qualification/oldwriter/` | no |
| `github.com/xraph/dispatch/qualification/protocolone` | `qualification/protocolone/` | no |
| `github.com/xraph/dispatch/qualification/querypreguard` | `qualification/querypreguard/` | no |

Only the root is a library anyone depends on. The four `qualification` modules are test harnesses that consume Dispatch the way an outside host would, from published versions, with no `replace` lines. The root has none either.

`qualification/` pins a published Dispatch commit next to published authsome, warden, chronicle, relay, forge and `forge/extensions/auth`, then runs real authsome sessions, warden policies and DWP WebSocket frames against it (the credential and DPoP regressions). It also builds two loopback hosts. `cmd/operatorhost` serves the durable operator contract for dashboard work, and `cmd/sinkhost` runs Chronicle, Relay, Dispatch and a webhook receiver as separate processes against PostgreSQL. `qualification/README.md` explains each scenario in detail.

`querypreguard` pins published pre-guard commit `290a77873305` and exercises query removal, proof refresh and SQL mutation refusal against the current schema. Keep that pin unchanged when you update the qualification host. Run `make qualification-queryguard-check` with `DISPATCH_LIFECYCLE_TEST_DSN` pointing at a disposable PostgreSQL server. The target builds all three historical executables and requires their named native compatibility tests. It does not start a database.

`oldwriter` and `protocolone` are small executables linked against older Dispatch commits. Tests in `store/postgres` launch them to prove the current store works beside an old writer that never sets the retirement marker, and beside a library from before query retention.

Because `qualification/` pins published code, a change you make in the root isn't visible there until it's pushed to `main` and you bump the require:

```bash
cd qualification && GOWORK=off go get github.com/xraph/dispatch@<commit> && GOWORK=off go mod tidy
```

## Commands

Set `GOWORK=off` on every go command. CI has no workspace, and you want the same module graph it sees.

```bash
GOWORK=off go build ./...
GOWORK=off go test -race ./...     # root module; ./... skips qualification/
make test        # go test -v ./...  (what CI's test job runs)
make test-race   # go test -race -v ./...
make vet
make lint        # golangci-lint run --allow-serial-runners ./...
make lint-fix
make fmt         # gofmt -s -w . then goimports -local github.com/xraph/dispatch
make check       # fmt, vet, lint
make coverage
```

For `qualification/` itself:

```bash
make qualification-lint            # golangci-lint inside qualification/, no issue caps
make qualification-check           # tidy -diff, verify, vet, test -race, build
make qualification-operator-check  # needs Docker and govulncheck on PATH
make qualification-process-check   # needs Docker and govulncheck on PATH
```

These targets pin `GOWORK=off` and a `GOTOOLCHAIN` themselves. `oldwriter`, `protocolone` and `querypreguard` each have a tiny Makefile of their own (`make f`, `make l`, `make test`). The last two qualification targets build the real host binaries, scan them with govulncheck, start a capped `postgres:17-alpine` container, and remove only that container when they exit.

Some targets are broken, so skip them. `make build`, `run`, `install`, `dev` and `all` point at `./cmd/dispatch`, which doesn't exist. `make templ` and `templ-watch` point at `./dashboard/...`, and the old templ dashboard now sits in `_dashboard/`, which the Go tool ignores.

### Tests and what they need

There are four tiers. The shared CI runs the first two. The native query removal guard job also runs its named PostgreSQL tests and all three historical executable campaigns from the fourth tier.

1. Plain unit tests, plus the memory and SQLite stores. They need nothing.
2. Container tests without a build tag: `store/postgres/wake_test.go`, `store/redis/reap_test.go` and `store/mongo/dequeue_test.go` start testcontainers. They skip under `-short` or when no container runtime answers. CI's runner has Docker, so they run there.
3. Files tagged `//go:build integration` in `store/postgres` (most of its tests), `store/redis` and `extension/contract`. They start containers and fail outright without Docker. CI never passes the tag, so you have to run them yourself:

   ```bash
   GOWORK=off go test -race -tags integration ./store/postgres/... ./store/redis/... ./extension/contract/...
   ```

4. Tests that need a PostgreSQL you provide. Without the variables they call `t.Skip` and the package still passes, which is easy to miss. Nothing in CI sets them.

| Variable | What it enables |
|---|---|
| `DISPATCH_READ_TEST_DSN` | Durable reads and signal outcomes in `store/postgres`, the durable HTTP test in `extension/contract` |
| `DISPATCH_LIFECYCLE_TEST_DSN` | Retirement, lifecycle and query diagnostics in `store/postgres` |
| `DISPATCH_OLDWRITER_BINARY` | With the lifecycle DSN, `TestRetirementNativeOldWriter` (build it from `qualification/oldwriter`) |
| `DISPATCH_QUERYPREGUARD_BINARY` | With the lifecycle DSN, the guard-unaware query executable campaign |
| `DISPATCH_PROTOCOL_ONE_BINARY` | With the lifecycle DSN, `TestQueryNativeProtocolOne` (build it from `qualification/protocolone`) |
| `DISPATCH_SINK_TEST_DSN` | `TestSinkConflictRestart`, which also needs `-tags integration` |
| `DISPATCH_READ_REQUIRED=1`, `DISPATCH_LIFECYCLE_REQUIRED=1`, `DISPATCH_QUERY_REQUIRED=1` | Turn the matching skip into a failure |
| `UPDATE_DURABLE_WIRE=1` | Rewrites `operator/testdata/durable-wire.json` from the current DTOs |

Point the DSNs at a throwaway server. These tests create and drop databases, disable triggers, and rewrite rows.

### What CI runs

`.github/workflows/ci.yml` calls the shared `xraph/workflows/.github/workflows/go-ci.yml@v1`. For the root module you get `make test`, a coverage run under `-race`, golangci-lint at the version the shared workflow pins, `gofmt -l .`, `make vet`, a `go mod tidy` diff on the root `go.mod` and `go.sum`, gosec and govulncheck. gosec runs standalone there, so the excludes in `.golangci.yml` don't apply to it (it skips only G115). A nested-modules job checks that the qualification modules require Dispatch at real semver versions.

The same file adds four jobs. Native query removal guard compatibility starts a capped PostgreSQL fixture, verifies the querypreguard module and requires the native guard and historical executable tests. Published host qualification fails if any qualification dependency is replaced, then lints, runs `make qualification-check`, `govulncheck -test ./...`, `make qualification-operator-check` and `make qualification-process-check`. Format runs `goimports -l -local github.com/xraph/dispatch .` over the whole tree. Docs runs `pnpm types:check`, `pnpm lint` and `pnpm build` in `docs/`.

## Layout

| Path | What lives there |
|---|---|
| `options.go`, `config.go`, `errors.go`, `id.go` | `Dispatcher` and `New`, the functional options, `Config`, sentinel errors, the `ID` alias |
| `id/` | TypeID identifiers (`job_`, `wf_`, `wfrun_`, `ckpt_`, `cron_`, `dlq_`, `evt_`, `wkr_`, `art_`) |
| `job/`, `workflow/`, `cron/`, `dlq/`, `event/`, `cluster/`, `queue/` | One subsystem each, with its own store interface |
| `durable/` | The durable workflow plane: store contract, receipts, signals, children, retirement, query retention |
| `durable/runtime/`, `durable/delivery/`, `durable/durabletest/` | Workflow evaluation and workers, the outbox publisher (with chronicle and relay sinks), conformance suites |
| `artifact/` | Tracked references to objects in external storage, staging cache, sweeper, a trove backend |
| `resource/` | Job resource requests and admission against detected worker capacity |
| `worker/`, `engine/`, `exec/` | The runner, the engine that wires subsystems together, the subprocess execution boundary |
| `store/` | Aggregate `Store`, plus `memory`, `sqlite`, `postgres`, `mongo`, `redis` and `storetest` |
| `ext/` | Opt-in lifecycle hook interfaces |
| `audit_hook/`, `relay_hook/` | Hooks that send lifecycle events to an audit recorder and to relay |
| `middleware/`, `observability/`, `backoff/`, `scope/`, `paging/` | Job middleware, OpenTelemetry, retry delays, tenant scope, cursor paging |
| `api/`, `dwp/`, `client/`, `stream/` | REST handlers, the Dispatch Wire Protocol, the Go client, the event broker |
| `security/`, `operator/` | Remote operator authentication and authorization, payload-safe durable reads and commands |
| `extension/` | The Forge extension, with the dashboard contract in `extension/contract/` |
| `examples/` | Runnable `durable-*` programs, built by `go build ./...` |
| `sdk/python`, `sdk/typescript` | Client SDKs |
| `docs/` | Fumadocs site, plus design notes in `docs/architecture/` |
| `_dashboard/`, `_examples/`, `_project_files/` | Ignored by the Go tool: the old templ dashboard, older examples, the original design notes |

`MIGRATION.md` tracks the move from the templ dashboard to the React plugin in forge-dashboard.

## Conventions

`dispatch.New` takes functional options, `type Option func(*Dispatcher) error`, and stops at the first one that fails. The extension has its own `ExtOption func(*Extension)` in `extension/options.go`.

Errors are package-level sentinels such as `errors.New("dispatch: job not found")` in `errors.go`, matched with `errors.Is`. Wrap with `%w`. `ErrPermanent` has a specific meaning: a handler error wrapping it skips the remaining retries and goes straight to the dead letter queue. Only use it when a retry is certain to fail the same way.

IDs come from `id/`, as TypeIDs with the prefixes above. The root re-exports `id.ID` as `dispatch.ID`.

`store.Store` composes the job, workflow, cron, DLQ, event, cluster and artifact stores, and `store/store_test.go` asserts that all five backends implement it. Postgres, SQLite and Mongo take a `*grove.DB`. Redis takes a grove `*kv.Store`. If you change a store method, you'll need it working in all five, and the `store/storetest` suites are how you find out. The durable plane is narrower: `durable.Store` and its companion interfaces exist only in `store/memory` and `store/postgres`, checked with `var _ durable.Store = (*Store)(nil)` lines and exercised by `durable/durabletest`.

Logging goes through `github.com/xraph/go-utils/log` (`log.Logger`). The dispatcher defaults to `log.NewNoopLogger()`. Don't reach for `log/slog`: no package here imports it outside tests.

The Forge extension embeds `*forge.BaseExtension` and reads config from `extensions.dispatch` or `dispatch`. For a store it uses a named `*grove.DB` or grove KV store when you configure one, and otherwise auto-discovers the default `*grove.DB` from the vessel container. It provides `*engine.Engine` back to the container.

### Lint rules that bite

- `govet` enables every analyzer but `fieldalignment`, so `shadow` catches a redeclared `err`.
- `errcheck` runs with `check-blank: true`. An error you drop on purpose needs `//nolint:errcheck // reason`.
- `revive`'s `exported` rule wants doc comments on every exported name. `var-naming` is off.
- `nolintlint` flags stale directives.
- gosec, errcheck and gocritic are off in `_test.go`.
- Imports group as stdlib, third party, then `github.com/xraph/dispatch/...`.

Commit subjects use conventional prefixes, often scoped: `feat(durable):`, `fix(sqlite):`, `test(qualification):`.

## Dependencies

The root requires chronicle, relay, the trove root module, warden, grove (mongodriver, pgdriver, sqlitedriver, `grove/kv` and its redisdriver), forge, `forge/extensions/auth`, vessel and go-utils. Exact versions are in `go.mod`. Consumers are `trove/extension` and authsome.

Watch the loop with trove. Dispatch needs trove's root module, while `trove/extension` needs Dispatch. The order is trove root (released first), then Dispatch, then a trove release that re-pins `trove/extension`. `qualification/` depends on authsome, and authsome depends on Dispatch. The release leaves it out, and it re-pins whenever you want it to qualify a newer commit.

To bump an xraph dependency, run `go get` once per module, then tidy each module that requires it. There is no `updatemod.sh` here.

```bash
GOWORK=off go get github.com/xraph/<mod>@vX.Y.Z
GOWORK=off go mod tidy
(cd qualification && GOWORK=off go get github.com/xraph/<mod>@vX.Y.Z && GOWORK=off go mod tidy)
```

`oldwriter` and `protocolone` pin old commits on purpose. Don't bump their Dispatch require along with everything else, or they stop testing anything old.

Never commit a `go.work` or a `replace`. CI's qualification job rejects any replacement in the qualification graph.

## Releasing

Dispatch the workflow. Never push a tag by hand.

```bash
gh workflow run release.yml --ref main -f tag=vX.Y.Z
```

Wait for main's CI to go green first. Check `go.mod` too. A pseudo-version (`vX.Y.Z-0.<timestamp>-<hash>`) on an xraph module means you're depending on an unreleased commit, so release that module, pin its tag here, and let CI pass before you cut Dispatch. Afterwards, trove and authsome re-pin.

The workflow creates `vX.Y.Z` on the dispatched commit and pushes it, then runs `go mod download`, `go build ./...` and `go test -race -count=1 ./...` in the root, and publishes a GitHub release. It tags the root module only. The qualification modules are never tagged.

The tag is pushed before the build and tests run. A red release leaves the tag on the remote, and a re-run fails at `git tag` because the tag already exists. Fix main and release the next patch.

## Branch rules

The ruleset on `main` blocks deletion and force-pushes. It requires no checks or reviews, so a direct push to `main` works.
