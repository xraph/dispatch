# Dispatch

**Composable durable execution engine for Go.**

Dispatch is a library — not a service. Import it, configure a store, and register jobs or workflows as ordinary Go functions. It handles background processing, workflow orchestration, cron scheduling, distributed coordination, and observability.

## Features

- **Background jobs** — Define typed handlers, enqueue with priority, retry with configurable backoff
- **Durable workflows** — Multi-step functions with checkpointing, parallel execution, and event waiting
- **Distributed cron** — Leader-elected cron scheduling with per-tenant support
- **Dead letter queue** — Automatic promotion after exhausted retries; inspect, replay, and purge
- **Distributed workers** — Worker registration, heartbeats, leader election, and work stealing
- **Middleware** — Composable chain for logging, tracing, metrics, panic recovery, and scope injection
- **Extension hooks** — Opt-in lifecycle interfaces for every job, workflow, cron, and shutdown event
- **OpenTelemetry** — Built-in metrics and tracing via the `observability` and `middleware` packages
- **Relay integration** — Emit typed webhook events at every lifecycle point via `relay_hook`
- **Artifact plane** — Track gigabyte-scale job inputs and outputs in object storage, staged to a content-addressed local cache
- **Resource model** — Size jobs by what they actually need and admit them against detected worker capacity, instead of counting identical slots
- **Pluggable storage** — Memory, PostgreSQL (pgx/v5), Grove ORM, SQLite, Redis

## Quick Start

```go
package main

import (
    "context"
    "log"
    "log/slog"
    "os"
    "os/signal"

    "github.com/xraph/dispatch"
    "github.com/xraph/dispatch/engine"
    "github.com/xraph/dispatch/job"
    "github.com/xraph/dispatch/store/memory"
)

type EmailInput struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

var SendEmail = job.NewDefinition("send_email",
    func(ctx context.Context, input EmailInput) error {
        log.Printf("sending email to %s: %s", input.To, input.Subject)
        return nil
    },
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    d, err := dispatch.New(
        dispatch.WithStore(memory.New()),
        dispatch.WithLogger(slog.Default()),
        dispatch.WithConcurrency(10),
    )
    if err != nil {
        log.Fatal(err)
    }

    eng := engine.Build(d)
    engine.Register(eng, SendEmail)

    if err := d.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer d.Stop(ctx)

    engine.Enqueue(ctx, eng, SendEmail, EmailInput{
        To:      "user@example.com",
        Subject: "Welcome!",
    })

    <-ctx.Done()
}
```

## Resource-Aware Scheduling

A worker slot is a promise that one job fits. That works while every job is the same size, and stops working the moment one definition serves both a 2 MB input and a 2 GB one: `concurrency: 4` says the worker may run four jobs, never how big they are, so a slot-counting pool starts four of the large ones on a box sized for the small ones and the kernel decides which of them dies. Adding a dedicated queue per size is the usual workaround, and it trades an OOM for a fleet that is idle in one queue and backed up in another.

The `resource` package replaces the slot with a vector. A job declares what it needs — or computes it at enqueue from its input size — the worker detects what it has (cgroup-first, so a container reports its quota rather than the host's cores), and admission is a comparison. A job that does not fit stays pending instead of being started next to work already using the memory.

Minimal wiring, in one process:

```go
capacity := resource.Detect(resource.CapacityConfig{DiskBytes: 20 << 30})
resources := resource.NewManager(capacity)

// The SAME manager goes to both. The staging cache holds a lease per
// cached entry and reclaims disk on demand; the pool offers disk at
// dequeue as free plus what the cache could evict. Give the cache its
// own manager and that second half is always zero.
staging, _ := cache.New(cacheDir, backend, cache.WithManager(resources))

eng, _ := engine.Build(d,
    engine.WithArtifacts(artifacts, staging),
    engine.WithResourceManager(resources),
)
```

Sizing one definition from its input:

```go
job.NewDefinition("render", handler,
    job.WithArtifactInputs(artifact.Input("scene", artifact.Required)),
    job.WithResourceFunc(func(_ context.Context, r resource.Request) (resource.Set, error) {
        return resource.MemoryBytes(32<<20 + r.InputBytes*16), nil
    }),
)
```

The function runs once, in the enqueuing process, and the result is written to the job row — nothing evaluates user code on the scheduling path, and two workers can never disagree about how big a job is.

Under Forge, the same thing is configuration:

```yaml
extensions:
  dispatch:
    resources:
      enabled: true
      cpu_overcommit: 1.0      # CPU is compressible; there is no memory equivalent
      memory_fraction: 0.8     # leave the rest for the runtime and the page cache
      explicit:                # overrides detection, and the only way to declare
        gpu: 4000              # a custom resource — nothing detects an FPGA
        fpga: 2
```

Leave it out and nothing changes: no ledger is built, the pool dequeues unbounded, every store backend skips its fit predicate, and the staging cache keeps the private disk budget it has always had.

Runnable end to end in [`_examples/resources`](./_examples/resources).

## Package Index

| Package | Description |
|---------|-------------|
| `dispatch` | Root — `Dispatcher`, `Config`, options, errors, `Entity` base type |
| `engine` | Wires all subsystems; `Build`, `Register`, `Enqueue`, `RegisterWorkflow`, `RegisterCron` |
| `job` | `Job` entity, `State` machine, `Definition[T]`, `Registry` |
| `artifact` | Tracked object storage — declared inputs, imperative outputs, staging cache, lifecycle sweeping |
| `resource` | Resource vectors, capacity detection, the shared admission ledger and its reclaimers |
| `workflow` | `Definition[T]`, `Run`, `State`, step checkpointing |
| `cron` | `Entry`, `Scheduler`, distributed leader-elected cron |
| `dlq` | `Entry`, `Service` — list, replay, purge |
| `event` | `Event` entity and store interface |
| `cluster` | `Worker`, distributed coordination, heartbeats, work stealing |
| `queue` | `Config`, `Manager` — per-queue rate limiting and concurrency |
| `middleware` | `Middleware`, `Chain`, built-ins (Logging, Recover, Timeout, Tracing, Metrics, Scope) |
| `ext` | Extension interface, lifecycle hook interfaces, `Registry` |
| `backoff` | Retry backoff strategies |
| `observability` | OpenTelemetry `MetricsExtension` for system-wide counters |
| `id` | TypeID-based identifiers (`JobID`, `RunID`, `CronID`, etc.) |
| `api` | Forge-style HTTP admin API handlers |
| `scope` | Forge scope helpers — tenant ID extraction from context |
| `relay_hook` | Relay webhook delivery extension |
| `extension` | Forge framework integration adapter |
| `store` | Composite `Store` interface |
| `store/memory` | In-memory backend (testing) |
| `store/postgres` | PostgreSQL backend (pgx/v5) |
| `store/grovestore` | Grove ORM backend |
| `store/sqlite` | SQLite backend |
| `store/redis` | Redis backend |
| `cluster/k8s` | Kubernetes consensus for leader election |

## Store Backends

| Package | Driver | Use Case |
|---------|--------|----------|
| `store/memory` | — | Development and testing |
| `store/postgres` | pgx/v5 | Production (recommended) |
| `store/grovestore` | Grove ORM | Production (Grove-based projects) |
| `store/sqlite` | modernc/sqlite | Embedded / single-node |
| `store/redis` | go-redis | Redis-backed queue state |

## Install

```bash
go get github.com/xraph/dispatch
```

Requires Go 1.25+.

## Documentation

Full documentation is available at the docs portal: [`dispatch/docs`](./docs).

Run locally:

```bash
cd docs
pnpm install
pnpm dev
```
