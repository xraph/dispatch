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
- **Pluggable storage** — Memory, PostgreSQL (pgx/v5), Bun ORM, SQLite, Redis

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

## Package Index

| Package | Description |
|---------|-------------|
| `dispatch` | Root — `Dispatcher`, `Config`, options, errors, `Entity` base type |
| `engine` | Wires all subsystems; `Build`, `Register`, `Enqueue`, `RegisterWorkflow`, `RegisterCron` |
| `job` | `Job` entity, `State` machine, `Definition[T]`, `Registry` |
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
| `store/bun` | Bun ORM backend |
| `store/sqlite` | SQLite backend |
| `store/redis` | Redis backend |
| `cluster/k8s` | Kubernetes consensus for leader election |

## Store Backends

| Package | Driver | Use Case |
|---------|--------|----------|
| `store/memory` | — | Development and testing |
| `store/postgres` | pgx/v5 | Production (recommended) |
| `store/bun` | Bun ORM | Production (Bun-based projects) |
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
