# Dispatch — Project Design & Development Phases (v3)

> Composable, extensible durable execution engine for Go. Library-first background jobs, workflow orchestration, lifecycle hooks, and distributed workers.
>
> `github.com/xraph/dispatch`

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Core Design Principles](#2-core-design-principles)
3. [TypeID Identity System](#3-typeid-identity-system)
4. [Store Architecture (ControlPlane Pattern)](#4-store-architecture)
5. [Module Layout & Package Design](#5-module-layout)
6. [Core Types & Interfaces](#6-core-types)
7. [Extension System](#7-extension-system)
8. [Hooks & Relay Integration](#8-hooks--relay-integration)
9. [Distributed Workers & Kubernetes Consensus](#9-distributed-workers)
10. [Integration Platform Pattern (Zapier-Style)](#10-integration-platform-pattern)
11. [Forge Scope Integration](#11-forge-scope-integration)
12. [Linting & Code Quality (golangci-lint v2)](#12-linting)
13. [Development Phases](#13-development-phases)
14. [Claude Code Development Guide](#14-claude-code-guide)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Consumer Application                         │
│           (standalone binary, forge app, or K8s deployment)         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                      dispatch.Dispatcher                            │
│  ┌─────────────┐ ┌──────────┐ ┌───────────┐ ┌───────────────────┐  │
│  │  Job Svc    │ │Workflow  │ │ Cron Svc  │ │  DLQ Service      │  │
│  │             │ │ Svc      │ │           │ │                   │  │
│  └──────┬──────┘ └────┬─────┘ └─────┬─────┘ └────────┬──────────┘  │
│         │             │             │                 │              │
│  ┌──────▼─────────────▼─────────────▼─────────────────▼──────────┐  │
│  │                    Extension Registry                          │  │
│  │  [Custom Ext] [Custom Ext] [Relay Hook Ext] [Metrics Ext]     │  │
│  └──────────────────────────────┬────────────────────────────────┘  │
│                                 │                                    │
│  ┌──────────────────────────────▼────────────────────────────────┐  │
│  │                    Hooks (Lifecycle Events)                    │  │
│  │  OnEnqueue → OnStart → OnComplete/OnFail → OnRetry → OnDLQ   │  │
│  │                        ↓ (optional)                           │  │
│  │               ┌─────────────────────┐                         │  │
│  │               │  Relay Webhook      │ → Customer endpoint     │  │
│  │               │  delivery via       │ → Monitoring system     │  │
│  │               │  relay.Send()       │ → Slack/PagerDuty       │  │
│  │               └─────────────────────┘                         │  │
│  └──────────────────────────────┬────────────────────────────────┘  │
│                                 │                                    │
│  ┌──────────────────────────────▼────────────────────────────────┐  │
│  │                    Worker Pool + Executor                      │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │  │
│  │  │ Local    │  │ K8s Pod  │  │ K8s Pod  │  │ K8s Pod      │  │  │
│  │  │ Workers  │  │ Worker A │  │ Worker B │  │ Worker C     │  │  │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────┘  │  │
│  └──────────────────────────────┬────────────────────────────────┘  │
│                                 │                                    │
│  ┌──────────────────────────────▼────────────────────────────────┐  │
│  │                    Middleware Chain                            │  │
│  │  [Tracing] → [Metrics] → [Logging] → [Timeout] → [Scope]    │  │
│  └──────────────────────────────┬────────────────────────────────┘  │
│                                 │                                    │
│  ┌──────────────────────────────▼────────────────────────────────┐  │
│  │                    Queue Abstraction                           │  │
│  │  ┌──────────┐ ┌───────────┐ ┌────────────┐                   │  │
│  │  │ Priority │ │Rate Limit │ │ Per-Tenant │                   │  │
│  │  └──────────┘ └───────────┘ └────────────┘                   │  │
│  └──────────────────────────────┬────────────────────────────────┘  │
│                                 │                                    │
│  ┌──────────────────────────────▼────────────────────────────────┐  │
│  │                   Cluster / Consensus Layer                    │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐   │  │
│  │  │ Leader       │  │ Worker       │  │ Work Stealing     │   │  │
│  │  │ Election     │  │ Registry     │  │ / Rebalancing     │   │  │
│  │  └──────────────┘  └──────────────┘  └───────────────────┘   │  │
│  └──────────────────────────────┬────────────────────────────────┘  │
│                                 │                                    │
│  ┌──────────────────────────────▼────────────────────────────────┐  │
│  │                    Store Interface                             │  │
│  │  ┌──────────┐ ┌───────┐ ┌───────┐ ┌────────┐ ┌──────┐       │  │
│  │  │ Postgres │ │  Bun  │ │SQLite │ │ Redis  │ │Memory│       │  │
│  │  │  (pgx)   │ │ (ORM) │ │       │ │        │ │      │       │  │
│  │  └──────────┘ └───────┘ └───────┘ └────────┘ └──────┘       │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌───────────────────┐  ┌──────────────────────────────────────┐   │
│  │  Event Bus        │  │  forge.Extension (optional mount)    │   │
│  │  (WaitForEvent)   │  │                                      │   │
│  └───────────────────┘  └──────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core Design Principles

**Library, not service.** Import it. No cluster to run, no separate process. Just Go.

**Workflows as Go functions.** No DSL, no YAML, no protobuf. Define workflows with ordinary Go functions and get durable execution with per-step checkpointing.

**Composable store pattern (ControlPlane-style).** Each subsystem defines its own store interface. The aggregate `store.Store` composes them all. Five backends: Postgres (pgx), Bun (ORM), SQLite, Redis, and memory — matching and extending ControlPlane's store lineup.

**TypeID everywhere (ControlPlane pattern).** All entity IDs use `go.jetify.com/typeid` — type-prefixed, K-sortable, UUIDv7-based, compile-time safe. `job_01h...`, `wfrun_01h...`, `cron_01h...`. Same pattern Nexus and ControlPlane use.

**Distributed-ready from day one.** Single-process by default. Add Kubernetes consensus, worker registration, and work stealing without changing your job code. Scale from one goroutine to a fleet of pods.

**Forge-native, standalone-capable.** Reads `forge.Scope` from context when available. Falls back gracefully for standalone usage. Mounts as a Forge extension via `dispatch_ext.New()`.

**Middleware-driven.** Every cross-cutting concern (tracing, metrics, logging, timeout, tenant isolation) is a composable middleware wrapping job execution. Same mental model as HTTP middleware.

**Extensible by design.** A first-class extension registry lets users hook into every lifecycle event — enqueue, start, complete, fail, retry, DLQ. Extensions are the building block for integrations. Relay ships as a built-in extension for webhook delivery.

**Relay-native hooks.** When Relay is available, Dispatch automatically emits typed webhook events at every lifecycle point. Customers subscribe to `dispatch.job.completed`, `dispatch.workflow.failed`, etc. via Relay's endpoint management. No custom webhook code needed.

---

## 3. TypeID Identity System

Dispatch uses the same TypeID pattern as ControlPlane and Nexus — every entity gets a compile-time safe, Stripe-style prefixed ID.

### `id/id.go`

```go
package id

import "go.jetify.com/typeid"

// ──────────────────────────────────────────────────
// Prefix types — each entity has its own prefix
// ──────────────────────────────────────────────────

type JobPrefix struct{}
func (JobPrefix) Prefix() string { return "job" }

type WorkflowPrefix struct{}
func (WorkflowPrefix) Prefix() string { return "wf" }

type RunPrefix struct{}
func (RunPrefix) Prefix() string { return "wfrun" }

type CheckpointPrefix struct{}
func (CheckpointPrefix) Prefix() string { return "ckpt" }

type CronPrefix struct{}
func (CronPrefix) Prefix() string { return "cron" }

type DLQPrefix struct{}
func (DLQPrefix) Prefix() string { return "dlq" }

type EventPrefix struct{}
func (EventPrefix) Prefix() string { return "evt" }

type WorkerPrefix struct{}
func (WorkerPrefix) Prefix() string { return "wkr" }

// ──────────────────────────────────────────────────
// Typed ID aliases — compile-time safe
// ──────────────────────────────────────────────────

type JobID        = typeid.TypeID[JobPrefix]        // job_01h2xcejqtf2nbrexx3vqjhp41
type WorkflowID   = typeid.TypeID[WorkflowPrefix]   // wf_01h2xcejqtf2nbrexx3vqjhp41
type RunID        = typeid.TypeID[RunPrefix]         // wfrun_01h2xcejqtf2nbrexx3vqjhp41
type CheckpointID = typeid.TypeID[CheckpointPrefix]  // ckpt_01h2xcejqtf2nbrexx3vqjhp41
type CronID       = typeid.TypeID[CronPrefix]        // cron_01h455vb4pex5vsknk084sn02q
type DLQID        = typeid.TypeID[DLQPrefix]         // dlq_01h6rz1g6p2m3q9xvz1t2b7c4d
type EventID      = typeid.TypeID[EventPrefix]       // evt_01h8f3k2n7p4r6s9t1v3x5z7b9
type WorkerID     = typeid.TypeID[WorkerPrefix]      // wkr_01h9a1b2c3d4e5f6g7h8j9k0m1

// AnyID for cases where the prefix is dynamic.
type AnyID = typeid.AnyID

// ──────────────────────────────────────────────────
// Constructors
// ──────────────────────────────────────────────────

func NewJobID() JobID               { return must(typeid.New[JobID]()) }
func NewRunID() RunID               { return must(typeid.New[RunID]()) }
func NewCheckpointID() CheckpointID { return must(typeid.New[CheckpointID]()) }
func NewCronID() CronID             { return must(typeid.New[CronID]()) }
func NewDLQID() DLQID               { return must(typeid.New[DLQID]()) }
func NewEventID() EventID           { return must(typeid.New[EventID]()) }
func NewWorkerID() WorkerID         { return must(typeid.New[WorkerID]()) }

// ──────────────────────────────────────────────────
// Parsing (type-safe: ParseJobID("cron_01h...") fails)
// ──────────────────────────────────────────────────

func ParseJobID(s string) (JobID, error)     { return typeid.Parse[JobID](s) }
func ParseRunID(s string) (RunID, error)     { return typeid.Parse[RunID](s) }
func ParseCronID(s string) (CronID, error)   { return typeid.Parse[CronID](s) }
func ParseDLQID(s string) (DLQID, error)     { return typeid.Parse[DLQID](s) }
func ParseEventID(s string) (EventID, error) { return typeid.Parse[EventID](s) }
func ParseWorkerID(s string) (WorkerID, error) { return typeid.Parse[WorkerID](s) }
func ParseAny(s string) (AnyID, error)       { return typeid.FromString(s) }

// ──────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────

func must[T any](v T, err error) T {
    if err != nil {
        panic(err)
    }
    return v
}
```

### Base Entity

All Dispatch entities embed a common base, same as ControlPlane:

```go
// entity.go
package dispatch

import (
    "time"
)

// Entity is the base type embedded by all dispatch domain objects.
type Entity struct {
    CreatedAt time.Time `json:"created_at" bun:"created_at,notnull,default:current_timestamp"`
    UpdatedAt time.Time `json:"updated_at" bun:"updated_at,notnull,default:current_timestamp"`
}

// NewEntity returns an Entity with timestamps set to now.
func NewEntity() Entity {
    now := time.Now().UTC()
    return Entity{CreatedAt: now, UpdatedAt: now}
}
```

### Why TypeID

| Property | TypeID | UUID v4 | XID |
|----------|--------|---------|-----|
| Type-safe prefix | ✅ `job_...` | ❌ | ❌ |
| K-Sortable | ✅ (UUIDv7) | ❌ | ✅ |
| Compile-time safety | ✅ (generics) | ❌ | ❌ |
| Human-debuggable | ✅ prefix tells type | ❌ | ❌ |
| DB compatible | ✅ stores as TEXT | ✅ | ✅ |
| Stripe-style | ✅ | ❌ | ❌ |

---

## 4. Store Architecture (ControlPlane Pattern)

Dispatch follows the exact same composite store pattern established in ControlPlane. Each subsystem defines its own store interface in its own package. The top-level `store.Store` composes all of them. **Five backends** — matching ControlPlane's lineup plus Bun:

| Backend | Driver | Use Case |
|---------|--------|----------|
| **Postgres** | `pgx/v5` | Production. Raw SQL, SKIP LOCKED, LISTEN/NOTIFY, advisory locks |
| **Bun** | `uptrace/bun` | Production. ORM-based, Bun model tags, migration integration. For teams already using Bun in their Forge/ControlPlane stack |
| **SQLite** | `modernc.org/sqlite` | Embedded/edge. Single-file DB for CLI tools, dev, and standalone apps |
| **Redis** | `redis/go-redis/v9` | High-throughput ephemeral workloads. Speed over durability |
| **Memory** | In-process maps | Unit tests. Zero dependencies |

### Subsystem Store Interfaces

Each domain package defines a focused store contract:

```go
// job/store.go
package job

import "github.com/xraph/dispatch/id"

type Store interface {
    Enqueue(ctx context.Context, j *Job) error
    Dequeue(ctx context.Context, queues []string, limit int) ([]*Job, error)
    Get(ctx context.Context, jobID id.JobID) (*Job, error)
    Update(ctx context.Context, j *Job) error
    Delete(ctx context.Context, jobID id.JobID) error
    ListByState(ctx context.Context, state State, opts ListOpts) ([]*Job, error)
    Heartbeat(ctx context.Context, jobID id.JobID, workerID id.WorkerID) error
    ReapStale(ctx context.Context, threshold time.Duration) ([]*Job, error)
    Count(ctx context.Context, opts CountOpts) (int64, error)
}

// workflow/store.go
package workflow

import "github.com/xraph/dispatch/id"

type Store interface {
    CreateRun(ctx context.Context, run *Run) error
    GetRun(ctx context.Context, runID id.RunID) (*Run, error)
    UpdateRun(ctx context.Context, run *Run) error
    ListRuns(ctx context.Context, opts ListOpts) ([]*Run, error)
    SaveCheckpoint(ctx context.Context, runID id.RunID, stepName string, data []byte) error
    GetCheckpoint(ctx context.Context, runID id.RunID, stepName string) ([]byte, error)
    ListCheckpoints(ctx context.Context, runID id.RunID) ([]*Checkpoint, error)
}

// cron/store.go
package cron

import "github.com/xraph/dispatch/id"

type Store interface {
    Register(ctx context.Context, entry *Entry) error
    Get(ctx context.Context, entryID id.CronID) (*Entry, error)
    List(ctx context.Context) ([]*Entry, error)
    AcquireLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error)
    ReleaseLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID) error
    UpdateLastRun(ctx context.Context, entryID id.CronID, at time.Time) error
    Delete(ctx context.Context, entryID id.CronID) error
}

// dlq/store.go
package dlq

import "github.com/xraph/dispatch/id"

type Store interface {
    Push(ctx context.Context, entry *Entry) error
    List(ctx context.Context, opts ListOpts) ([]*Entry, error)
    Get(ctx context.Context, entryID id.DLQID) (*Entry, error)
    Replay(ctx context.Context, entryID id.DLQID) error
    Purge(ctx context.Context, before time.Time) (int64, error)
    Count(ctx context.Context) (int64, error)
}

// event/store.go
package event

import "github.com/xraph/dispatch/id"

type Store interface {
    Publish(ctx context.Context, evt *Event) error
    Subscribe(ctx context.Context, name string, timeout time.Duration) (*Event, error)
    Ack(ctx context.Context, eventID id.EventID) error
}

// cluster/store.go
package cluster

import "github.com/xraph/dispatch/id"

type Store interface {
    RegisterWorker(ctx context.Context, w *Worker) error
    DeregisterWorker(ctx context.Context, workerID id.WorkerID) error
    Heartbeat(ctx context.Context, workerID id.WorkerID) error
    ListWorkers(ctx context.Context) ([]*Worker, error)
    ReapDead(ctx context.Context, threshold time.Duration) ([]*Worker, error)
    AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error)
    RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error)
    GetLeader(ctx context.Context) (*Worker, error)
}
```

### Composite Store

```go
// store/store.go
package store

import (
    "context"

    "github.com/xraph/dispatch/cluster"
    "github.com/xraph/dispatch/cron"
    "github.com/xraph/dispatch/dlq"
    "github.com/xraph/dispatch/event"
    "github.com/xraph/dispatch/job"
    "github.com/xraph/dispatch/workflow"
)

// Store is the aggregate persistence interface.
// Each subsystem store is a composable interface — same pattern as ControlPlane.
// A single backend (postgres, bun, sqlite, etc.) implements all of them.
type Store interface {
    job.Store
    workflow.Store
    cron.Store
    dlq.Store
    event.Store
    cluster.Store

    // Migrate runs all schema migrations.
    Migrate(ctx context.Context) error

    // Ping checks database connectivity.
    Ping(ctx context.Context) error

    // Close closes the store connection.
    Close() error
}
```

### Backend Directory Structure

```
store/
├── store.go                   # Composite Store interface
├── postgres/
│   ├── store.go               # *PGStore: pgx/v5, pgxpool, embed.FS migrations
│   ├── job.go                 # job.Store — SELECT FOR UPDATE SKIP LOCKED
│   ├── workflow.go            # workflow.Store
│   ├── cron.go                # cron.Store — pg_advisory_lock for leader election
│   ├── dlq.go                 # dlq.Store
│   ├── event.go               # event.Store — LISTEN/NOTIFY for WaitForEvent
│   ├── cluster.go             # cluster.Store — advisory locks for leadership
│   └── migrations/
│       ├── 001_jobs.sql
│       ├── 002_workflows.sql
│       ├── 003_cron.sql
│       ├── 004_dlq.sql
│       ├── 005_events.sql
│       └── 006_cluster.sql
├── bun/
│   ├── store.go               # *BunStore: uptrace/bun, model-driven, Bun migrations
│   ├── models.go              # Bun model structs with bun:"" tags
│   ├── job.go                 # job.Store via Bun query builder
│   ├── workflow.go            # workflow.Store via Bun
│   ├── cron.go                # cron.Store via Bun
│   ├── dlq.go                 # dlq.Store via Bun
│   ├── event.go               # event.Store via Bun
│   ├── cluster.go             # cluster.Store via Bun
│   └── migrations/
│       └── 001_initial.go     # Bun Go-based migrations
├── sqlite/
│   ├── store.go               # Embeds migrations, single-file DB
│   └── migrations/
│       └── 001_initial.sql
├── redis/
│   ├── store.go               # Redis Streams + Sorted Sets
│   ├── job.go                 # XREADGROUP for dequeue
│   ├── scripts/               # Lua scripts for atomic operations
│   └── cluster.go             # Redis-based leader election (Redlock)
└── memory/
    └── store.go               # sync.Map + channels, for unit tests
```

### Bun Store — Models

The Bun store uses Bun ORM model structs that map directly to Dispatch entities. This is the store to use when your app already uses Bun (as Forge and ControlPlane do):

```go
// store/bun/models.go
package bun

import (
    "time"

    "github.com/uptrace/bun"
    "github.com/xraph/dispatch/id"
)

// JobModel is the Bun model for the dispatch_jobs table.
type JobModel struct {
    bun.BaseModel `bun:"table:dispatch_jobs,alias:j"`

    ID          string     `bun:"id,pk"`
    Name        string     `bun:"name,notnull"`
    Queue       string     `bun:"queue,notnull,default:'default'"`
    Payload     []byte     `bun:"payload,notnull,type:bytea"`
    State       string     `bun:"state,notnull,default:'pending'"`
    Priority    int        `bun:"priority,notnull,default:0"`
    MaxRetries  int        `bun:"max_retries,notnull,default:3"`
    RetryCount  int        `bun:"retry_count,notnull,default:0"`
    LastError   string     `bun:"last_error"`
    ScopeAppID  string     `bun:"scope_app_id"`
    ScopeOrgID  string     `bun:"scope_org_id"`
    WorkerID    string     `bun:"worker_id"`
    RunAt       time.Time  `bun:"run_at,notnull,default:current_timestamp"`
    StartedAt   *time.Time `bun:"started_at"`
    CompletedAt *time.Time `bun:"completed_at"`
    HeartbeatAt *time.Time `bun:"heartbeat_at"`
    CreatedAt   time.Time  `bun:"created_at,notnull,default:current_timestamp"`
    UpdatedAt   time.Time  `bun:"updated_at,notnull,default:current_timestamp"`
}

// WorkflowRunModel is the Bun model for the dispatch_workflow_runs table.
type WorkflowRunModel struct {
    bun.BaseModel `bun:"table:dispatch_workflow_runs,alias:wr"`

    ID          string     `bun:"id,pk"`
    Name        string     `bun:"name,notnull"`
    State       string     `bun:"state,notnull,default:'running'"`
    Input       []byte     `bun:"input,type:bytea"`
    Output      []byte     `bun:"output,type:bytea"`
    Error       string     `bun:"error"`
    ScopeAppID  string     `bun:"scope_app_id"`
    ScopeOrgID  string     `bun:"scope_org_id"`
    StartedAt   time.Time  `bun:"started_at,notnull,default:current_timestamp"`
    CompletedAt *time.Time `bun:"completed_at"`
    CreatedAt   time.Time  `bun:"created_at,notnull,default:current_timestamp"`
    UpdatedAt   time.Time  `bun:"updated_at,notnull,default:current_timestamp"`
}

// WorkerModel is the Bun model for the dispatch_workers table.
type WorkerModel struct {
    bun.BaseModel `bun:"table:dispatch_workers,alias:w"`

    ID          string    `bun:"id,pk"`
    Hostname    string    `bun:"hostname,notnull"`
    Queues      []string  `bun:"queues,array"`
    Concurrency int       `bun:"concurrency,notnull,default:10"`
    State       string    `bun:"state,notnull,default:'active'"`
    IsLeader    bool      `bun:"is_leader,notnull,default:false"`
    LeaderUntil *time.Time `bun:"leader_until"`
    LastSeen    time.Time `bun:"last_seen,notnull,default:current_timestamp"`
    Metadata    map[string]string `bun:"metadata,type:jsonb,default:'{}'"`
    CreatedAt   time.Time `bun:"created_at,notnull,default:current_timestamp"`
}
```

### Bun Store — Job Implementation Example

```go
// store/bun/job.go
package bun

import (
    "context"
    "database/sql"

    "github.com/uptrace/bun"
    "github.com/xraph/dispatch/job"
)

func (s *BunStore) Dequeue(ctx context.Context, queues []string, limit int) ([]*job.Job, error) {
    var models []JobModel

    // Bun's raw query for SKIP LOCKED (same strategy as raw postgres store)
    err := s.db.NewRaw(`
        UPDATE dispatch_jobs
        SET state = 'running', started_at = NOW(), updated_at = NOW(), worker_id = ?
        WHERE id IN (
            SELECT id FROM dispatch_jobs
            WHERE state = 'pending'
              AND queue IN (?)
              AND run_at <= NOW()
            ORDER BY priority DESC, run_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ?
        )
        RETURNING *
    `, s.workerID, bun.In(queues), limit).Scan(ctx, &models)

    if err != nil {
        return nil, err
    }
    return modelsToJobs(models), nil
}
```

### PostgreSQL Migrations

```sql
-- store/postgres/migrations/001_jobs.sql
CREATE TABLE IF NOT EXISTS dispatch_jobs (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    queue           TEXT NOT NULL DEFAULT 'default',
    payload         BYTEA NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending',
    priority        INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    scope_app_id    TEXT,
    scope_org_id    TEXT,
    worker_id       TEXT,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    heartbeat_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dispatch_jobs_dequeue
    ON dispatch_jobs (queue, priority DESC, run_at ASC)
    WHERE state = 'pending';
CREATE INDEX idx_dispatch_jobs_state ON dispatch_jobs (state);
CREATE INDEX idx_dispatch_jobs_scope ON dispatch_jobs (scope_app_id, scope_org_id);
CREATE INDEX idx_dispatch_jobs_heartbeat ON dispatch_jobs (heartbeat_at)
    WHERE state = 'running';
```

```sql
-- store/postgres/migrations/002_workflows.sql
CREATE TABLE IF NOT EXISTS dispatch_workflow_runs (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'running',
    input           BYTEA,
    output          BYTEA,
    error           TEXT,
    scope_app_id    TEXT,
    scope_org_id    TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dispatch_checkpoints (
    id              TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES dispatch_workflow_runs(id) ON DELETE CASCADE,
    step_name       TEXT NOT NULL,
    data            BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, step_name)
);

CREATE INDEX idx_dispatch_workflow_runs_state ON dispatch_workflow_runs (state);
CREATE INDEX idx_dispatch_checkpoints_run ON dispatch_checkpoints (run_id);
```

```sql
-- store/postgres/migrations/003_cron.sql
CREATE TABLE IF NOT EXISTS dispatch_cron_entries (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    schedule        TEXT NOT NULL,
    job_name        TEXT NOT NULL,
    payload         BYTEA,
    scope_app_id    TEXT,
    scope_org_id    TEXT,
    last_run_at     TIMESTAMPTZ,
    next_run_at     TIMESTAMPTZ,
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dispatch_cron_next ON dispatch_cron_entries (next_run_at)
    WHERE enabled = TRUE;
```

```sql
-- store/postgres/migrations/004_dlq.sql
CREATE TABLE IF NOT EXISTS dispatch_dlq (
    id              TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL,
    job_name        TEXT NOT NULL,
    queue           TEXT NOT NULL,
    payload         BYTEA NOT NULL,
    error           TEXT NOT NULL,
    retry_count     INTEGER NOT NULL,
    scope_app_id    TEXT,
    scope_org_id    TEXT,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dispatch_dlq_queue ON dispatch_dlq (queue, failed_at DESC);
```

```sql
-- store/postgres/migrations/005_events.sql
CREATE TABLE IF NOT EXISTS dispatch_events (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    payload         BYTEA,
    scope_app_id    TEXT,
    scope_org_id    TEXT,
    acked           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dispatch_events_pending ON dispatch_events (name, created_at)
    WHERE acked = FALSE;
```

```sql
-- store/postgres/migrations/006_cluster.sql
CREATE TABLE IF NOT EXISTS dispatch_workers (
    id              TEXT PRIMARY KEY,
    hostname        TEXT NOT NULL,
    queues          TEXT[] DEFAULT '{}',
    concurrency     INTEGER NOT NULL DEFAULT 10,
    state           TEXT NOT NULL DEFAULT 'active',
    is_leader       BOOLEAN NOT NULL DEFAULT FALSE,
    leader_until    TIMESTAMPTZ,
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dispatch_workers_state ON dispatch_workers (state);
CREATE INDEX idx_dispatch_workers_leader ON dispatch_workers (is_leader)
    WHERE is_leader = TRUE;
CREATE INDEX idx_dispatch_workers_stale ON dispatch_workers (last_seen)
    WHERE state = 'active';
```

---

## 5. Module Layout & Package Design

```
github.com/xraph/dispatch/
├── dispatch.go                # Dispatcher struct, New(), Start(), Stop()
├── entity.go                  # Base Entity type (CreatedAt, UpdatedAt)
├── options.go                 # Functional options (WithStore, WithConcurrency, etc.)
├── config.go                  # Configuration struct, defaults
├── errors.go                  # Sentinel errors
├── backoff.go                 # Backoff strategies (Constant, Exponential, Jitter)
├── doc.go                     # Package documentation
│
├── id/
│   └── id.go                  # TypeID prefixes, typed IDs, constructors, parsers
│
├── job/
│   ├── job.go                 # Job entity (uses id.JobID), State enum
│   ├── definition.go          # Definition[T] generic type, NewJob()
│   ├── registry.go            # Job type registry (name → handler mapping)
│   ├── store.go               # job.Store interface
│   └── options.go             # Per-job options (retries, timeout, queue, priority)
│
├── workflow/
│   ├── workflow.go            # Workflow definition, NewWorkflow()
│   ├── run.go                 # Run entity (uses id.RunID)
│   ├── step.go                # Step(), Parallel(), WaitForEvent()
│   ├── checkpoint.go          # Checkpoint entity (uses id.CheckpointID)
│   ├── context.go             # Workflow context (carries run state)
│   └── store.go               # workflow.Store interface
│
├── cron/
│   ├── cron.go                # Cron scheduler, parser
│   ├── entry.go               # CronEntry entity (uses id.CronID)
│   ├── leader.go              # Leader election (distributed lock)
│   └── store.go               # cron.Store interface
│
├── queue/
│   ├── queue.go               # Queue interface
│   ├── priority.go            # Priority queue implementation
│   └── rate_limiter.go        # Per-queue / per-tenant rate limiting
│
├── worker/
│   ├── pool.go                # Worker pool (goroutine management)
│   ├── executor.go            # Job/workflow executor
│   └── lifecycle.go           # Graceful shutdown, heartbeat, drain
│
├── cluster/
│   ├── cluster.go             # Cluster manager (coordinates distributed workers)
│   ├── worker.go              # Worker entity (uses id.WorkerID)
│   ├── consensus.go           # Consensus interface + Postgres/Redis implementations
│   ├── rebalancer.go          # Work stealing / queue rebalancing
│   ├── store.go               # cluster.Store interface
│   └── k8s/
│       ├── discovery.go       # K8s pod discovery via endpoints API
│       ├── leader.go          # K8s Lease-based leader election
│       └── labels.go          # Pod label management for queue assignment
│
├── middleware/
│   ├── middleware.go           # Middleware type definition
│   ├── logging.go             # Structured logging per job
│   ├── tracing.go             # OpenTelemetry trace per job execution
│   ├── metrics.go             # Prometheus metrics
│   ├── timeout.go             # Per-job timeout enforcement
│   ├── recover.go             # Panic recovery
│   └── scope.go               # Forge scope restoration from job metadata
│
├── ext/
│   ├── ext.go                 # Extension interface + all lifecycle hook interfaces
│   ├── registry.go            # Extension registry, type-cached event dispatch
│   └── options.go             # Extension config options
│
├── relay_hook/
│   ├── extension.go           # relay_hook.Extension: bridges lifecycle → Relay
│   ├── events.go              # Event type constants (dispatch.job.completed, etc.)
│   └── options.go             # WithEvents(), WithPayload(), WithEnricher()
│
├── dlq/
│   ├── dlq.go                 # Dead letter queue service
│   ├── entry.go               # DLQ entry entity (uses id.DLQID)
│   ├── replay.go              # Replay failed jobs
│   └── store.go               # dlq.Store interface
│
├── event/
│   ├── event.go               # Event entity (uses id.EventID)
│   ├── bus.go                 # Event bus (in-memory + store-backed)
│   └── store.go               # event.Store interface
│
├── scope/
│   └── scope.go               # Forge scope helpers (capture/restore)
│
├── store/
│   ├── store.go               # Composite Store interface
│   ├── postgres/              # pgx/v5 raw SQL implementation
│   │   ├── store.go
│   │   ├── job.go
│   │   ├── workflow.go
│   │   ├── cron.go
│   │   ├── dlq.go
│   │   ├── event.go
│   │   ├── cluster.go
│   │   └── migrations/
│   ├── bun/                   # Bun ORM implementation
│   │   ├── store.go
│   │   ├── models.go
│   │   ├── job.go
│   │   ├── workflow.go
│   │   ├── cron.go
│   │   ├── dlq.go
│   │   ├── event.go
│   │   ├── cluster.go
│   │   └── migrations/
│   ├── sqlite/
│   │   ├── store.go
│   │   └── migrations/
│   ├── redis/
│   │   ├── store.go
│   │   ├── job.go
│   │   ├── cluster.go
│   │   └── scripts/
│   └── memory/
│       └── store.go
│
├── api/
│   ├── handler.go             # Admin API handlers
│   └── routes.go              # Route mounting
│
├── extension/
│   ├── extension.go           # forge.Extension implementation
│   └── options.go
│
├── _examples/
│   ├── basic/
│   ├── workflow/
│   ├── cron/
│   ├── extensions/            # Custom extension (Slack notifier + audit logger)
│   ├── relay-hooks/           # Dispatch + Relay webhook delivery
│   ├── distributed/           # K8s multi-pod example
│   ├── integration-platform/  # Zapier-style pattern example
│   └── forge/
│
├── .golangci.yml
├── Makefile
├── go.mod
├── go.sum
└── README.md
```

---

## 6. Core Types & Interfaces

### Dispatcher (Root)

```go
// dispatch.go
package dispatch

type Dispatcher struct {
    config     Config
    store      store.Store
    registry   *job.Registry
    pool       *worker.Pool
    cluster    *cluster.Manager
    cron       *cron.Scheduler
    dlq        *dlq.Service
    eventBus   *event.Bus
    extensions *ext.Registry
    middleware []middleware.Middleware
    logger     *slog.Logger
}

func New(opts ...Option) (*Dispatcher, error) {
    d := &Dispatcher{
        config:   DefaultConfig(),
        registry: job.NewRegistry(),
    }
    for _, opt := range opts {
        if err := opt(d); err != nil {
            return nil, err
        }
    }
    if d.store == nil {
        return nil, ErrNoStore
    }
    d.wireServices()
    return d, nil
}

func (d *Dispatcher) Register(definitions ...any)       { /* register jobs/workflows */ }
func (d *Dispatcher) Start(ctx context.Context) error   { /* start pool + cluster + cron */ }
func (d *Dispatcher) Stop(ctx context.Context) error    { /* graceful drain + deregister */ }
func (d *Dispatcher) Cron(name, schedule string, j any, opts ...CronOption) { /* ... */ }
func (d *Dispatcher) Routes() http.Handler              { /* admin API */ }
```

### Job Entity

```go
// job/job.go
package job

import "github.com/xraph/dispatch/id"

type State string

const (
    StatePending   State = "pending"
    StateRunning   State = "running"
    StateCompleted State = "completed"
    StateFailed    State = "failed"
    StateRetrying  State = "retrying"
    StateCancelled State = "cancelled"
)

type Job struct {
    dispatch.Entity

    ID          id.JobID       `json:"id"`
    Name        string         `json:"name"`
    Queue       string         `json:"queue"`
    Payload     []byte         `json:"payload"`
    State       State          `json:"state"`
    Priority    int            `json:"priority"`
    MaxRetries  int            `json:"max_retries"`
    RetryCount  int            `json:"retry_count"`
    LastError   string         `json:"last_error,omitempty"`
    ScopeAppID  string         `json:"scope_app_id,omitempty"`
    ScopeOrgID  string         `json:"scope_org_id,omitempty"`
    WorkerID    id.WorkerID    `json:"worker_id,omitempty"`
    RunAt       time.Time      `json:"run_at"`
    StartedAt   *time.Time     `json:"started_at,omitempty"`
    CompletedAt *time.Time     `json:"completed_at,omitempty"`
    HeartbeatAt *time.Time     `json:"heartbeat_at,omitempty"`
}
```

---

## 7. Extension System

Dispatch is extensible via a first-class extension registry. Extensions hook into the job and workflow lifecycle at well-defined points, enabling custom behaviors without modifying Dispatch internals. This is how Relay integration, custom metrics, audit logging, and notification systems plug in.

### The Extension Interface

```go
// ext/ext.go
package ext

import (
    "context"

    "github.com/xraph/dispatch/job"
    "github.com/xraph/dispatch/workflow"
)

// Extension is the primary extensibility point for Dispatch.
// Implement any subset of the lifecycle interfaces below.
// Dispatch discovers which interfaces an extension implements
// and calls only those hooks.
type Extension interface {
    // Name returns a unique identifier for the extension.
    Name() string
}

// ──────────────────────────────────────────────────
// Lifecycle interfaces — implement any subset
// ──────────────────────────────────────────────────

// OnInit is called when the extension is registered with the Dispatcher.
// Use for setup: register event types, validate config, acquire resources.
type OnInit interface {
    OnInit(ctx context.Context, d *dispatch.Dispatcher) error
}

// OnShutdown is called during graceful shutdown.
type OnShutdown interface {
    OnShutdown(ctx context.Context) error
}

// ──────────────────────────────────────────────────
// Job lifecycle hooks
// ──────────────────────────────────────────────────

// JobEnqueued is called after a job is persisted to the store.
type JobEnqueued interface {
    OnJobEnqueued(ctx context.Context, j *job.Job) error
}

// JobStarted is called when a worker begins executing a job.
type JobStarted interface {
    OnJobStarted(ctx context.Context, j *job.Job) error
}

// JobCompleted is called after a job finishes successfully.
type JobCompleted interface {
    OnJobCompleted(ctx context.Context, j *job.Job, duration time.Duration) error
}

// JobFailed is called when a job fails (before retry or DLQ).
type JobFailed interface {
    OnJobFailed(ctx context.Context, j *job.Job, err error) error
}

// JobRetrying is called when a job is about to be retried.
type JobRetrying interface {
    OnJobRetrying(ctx context.Context, j *job.Job, attempt int, nextRunAt time.Time) error
}

// JobDLQ is called when a job exhausts retries and enters the dead letter queue.
type JobDLQ interface {
    OnJobDLQ(ctx context.Context, j *job.Job, err error) error
}

// ──────────────────────────────────────────────────
// Workflow lifecycle hooks
// ──────────────────────────────────────────────────

// WorkflowStarted is called when a workflow run begins.
type WorkflowStarted interface {
    OnWorkflowStarted(ctx context.Context, run *workflow.Run) error
}

// WorkflowStepCompleted is called after each workflow step succeeds.
type WorkflowStepCompleted interface {
    OnWorkflowStepCompleted(ctx context.Context, run *workflow.Run, stepName string, duration time.Duration) error
}

// WorkflowStepFailed is called when a workflow step fails.
type WorkflowStepFailed interface {
    OnWorkflowStepFailed(ctx context.Context, run *workflow.Run, stepName string, err error) error
}

// WorkflowCompleted is called when an entire workflow finishes.
type WorkflowCompleted interface {
    OnWorkflowCompleted(ctx context.Context, run *workflow.Run, duration time.Duration) error
}

// WorkflowFailed is called when a workflow fails terminally.
type WorkflowFailed interface {
    OnWorkflowFailed(ctx context.Context, run *workflow.Run, err error) error
}

// ──────────────────────────────────────────────────
// Cron lifecycle hooks
// ──────────────────────────────────────────────────

// CronFired is called when a cron entry triggers.
type CronFired interface {
    OnCronFired(ctx context.Context, entryName string, jobID id.JobID) error
}
```

### Extension Registry

```go
// ext/registry.go
package ext

// Registry manages registered extensions and dispatches lifecycle events.
type Registry struct {
    extensions []Extension
    logger     *slog.Logger

    // Typed caches — built at registration time, not on every event
    jobEnqueued       []JobEnqueued
    jobStarted        []JobStarted
    jobCompleted      []JobCompleted
    jobFailed         []JobFailed
    jobRetrying       []JobRetrying
    jobDLQ            []JobDLQ
    workflowStarted   []WorkflowStarted
    workflowCompleted []WorkflowCompleted
    workflowFailed    []WorkflowFailed
    cronFired         []CronFired
    // ... etc
}

// Register adds an extension and discovers which lifecycle interfaces it implements.
func (r *Registry) Register(e Extension) {
    r.extensions = append(r.extensions, e)

    // Type-switch discovery — O(1) per event dispatch, not O(n) type assertions
    if h, ok := e.(JobEnqueued); ok {
        r.jobEnqueued = append(r.jobEnqueued, h)
    }
    if h, ok := e.(JobStarted); ok {
        r.jobStarted = append(r.jobStarted, h)
    }
    if h, ok := e.(JobCompleted); ok {
        r.jobCompleted = append(r.jobCompleted, h)
    }
    if h, ok := e.(JobFailed); ok {
        r.jobFailed = append(r.jobFailed, h)
    }
    if h, ok := e.(JobRetrying); ok {
        r.jobRetrying = append(r.jobRetrying, h)
    }
    if h, ok := e.(JobDLQ); ok {
        r.jobDLQ = append(r.jobDLQ, h)
    }
    // ... discover all lifecycle interfaces
}

// EmitJobCompleted fires OnJobCompleted on all extensions that implement it.
// Errors are logged but don't block — extensions must not break job execution.
func (r *Registry) EmitJobCompleted(ctx context.Context, j *job.Job, duration time.Duration) {
    for _, h := range r.jobCompleted {
        if err := h.OnJobCompleted(ctx, j, duration); err != nil {
            r.logger.Error("extension hook failed",
                "extension", h.(Extension).Name(),
                "hook", "OnJobCompleted",
                "job_id", j.ID.String(),
                "error", err,
            )
        }
    }
}
```

### Registering Extensions

```go
d := dispatch.New(
    dispatch.WithStore(pgStore),
    dispatch.WithConcurrency(20),

    // Built-in Relay hook extension (see Section 8)
    dispatch.WithExtension(relay_hook.New(relayInstance)),

    // Custom audit logging extension
    dispatch.WithExtension(&AuditExtension{logger: auditLogger}),

    // Custom Slack notification extension
    dispatch.WithExtension(&SlackNotifier{webhook: slackURL}),

    // Custom metrics extension
    dispatch.WithExtension(&DatadogExtension{client: ddClient}),
)
```

### Extension vs Middleware

Extensions and middleware serve different purposes:

| Concern | Middleware | Extension |
|---------|-----------|-----------|
| **When** | Wraps job execution (before + after) | Fires at discrete lifecycle points |
| **Can block execution?** | Yes (can reject/cancel jobs) | No (errors are logged, never block) |
| **Has access to** | Job payload, context, next handler | Job entity, metadata, duration, error |
| **Use for** | Tracing, timeout, auth, scope restore | Webhooks, notifications, audit, metrics |
| **Ordering** | Strict chain order (first registered → outermost) | All fire, no ordering guarantees |
| **Error behavior** | Error stops execution | Error logged, other hooks still fire |

**Rule of thumb:** Use middleware when you need to wrap or gate execution. Use extensions when you need to react to lifecycle events without interfering with job processing.

### Writing a Custom Extension

```go
// Example: Slack notifier that pings on workflow failures
type SlackNotifier struct {
    webhook string
    client  *http.Client
}

func (s *SlackNotifier) Name() string { return "slack-notifier" }

// Only implement the interfaces you care about
func (s *SlackNotifier) OnWorkflowFailed(ctx context.Context, run *workflow.Run, err error) error {
    scope := forge.ScopeFrom(ctx)
    msg := fmt.Sprintf("🚨 Workflow `%s` failed for org %s: %s", run.Name, scope.OrgID(), err)
    return s.postSlack(ctx, msg)
}

func (s *SlackNotifier) OnJobDLQ(ctx context.Context, j *job.Job, err error) error {
    msg := fmt.Sprintf("💀 Job `%s` entered DLQ: %s", j.Name, err)
    return s.postSlack(ctx, msg)
}

func (s *SlackNotifier) postSlack(ctx context.Context, text string) error {
    body, _ := json.Marshal(map[string]string{"text": text})
    req, _ := http.NewRequestWithContext(ctx, "POST", s.webhook, bytes.NewReader(body))
    req.Header.Set("Content-Type", "application/json")
    resp, err := s.client.Do(req)
    if err != nil {
        return err
    }
    return resp.Body.Close()
}
```

---

## 8. Hooks & Relay Integration

Dispatch integrates with Relay to deliver lifecycle webhook events to external systems. When a customer subscribes to `dispatch.job.completed` via Relay's endpoint management, they automatically receive signed webhook deliveries every time a job completes — with Relay handling retries, signatures, fan-out, and delivery logs.

### How It Works

```
Job completes in Dispatch
       │
       ▼
Extension Registry fires OnJobCompleted
       │
       ├─→ relay_hook extension → relay.Send(ctx, event)
       │         │
       │         ▼
       │   Relay persists event, resolves matching endpoints,
       │   signs payload, delivers with retries, logs delivery
       │         │
       │         ▼
       │   Customer receives webhook:
       │     POST /webhooks
       │     X-Relay-Event-Type: dispatch.job.completed
       │     X-Relay-Signature: v1=...
       │     {"job_id": "job_01h...", "name": "send-email", "status": "completed"}
       │
       ├─→ Slack notifier extension → posts to Slack
       ├─→ Audit extension → writes audit log
       └─→ Datadog extension → emits custom metric
```

### Dispatch Event Types (Relay Catalog)

When the Relay hook extension initializes, it registers all Dispatch event types with Relay's schema registry:

```go
// relay_hook/events.go
package relay_hook

// All Dispatch lifecycle events available for webhook subscription.
const (
    // Job events
    EventJobEnqueued  = "dispatch.job.enqueued"
    EventJobStarted   = "dispatch.job.started"
    EventJobCompleted = "dispatch.job.completed"
    EventJobFailed    = "dispatch.job.failed"
    EventJobRetrying  = "dispatch.job.retrying"
    EventJobDLQ       = "dispatch.job.dlq"

    // Workflow events
    EventWorkflowStarted       = "dispatch.workflow.started"
    EventWorkflowStepCompleted = "dispatch.workflow.step.completed"
    EventWorkflowStepFailed    = "dispatch.workflow.step.failed"
    EventWorkflowCompleted     = "dispatch.workflow.completed"
    EventWorkflowFailed        = "dispatch.workflow.failed"

    // Cron events
    EventCronFired = "dispatch.cron.fired"

    // Cluster events
    EventWorkerJoined = "dispatch.worker.joined"
    EventWorkerLeft   = "dispatch.worker.left"
    EventLeaderElected = "dispatch.leader.elected"
)
```

### Relay Hook Extension

This is a built-in extension that ships with Dispatch. It bridges the extension lifecycle to Relay's webhook delivery:

```go
// relay_hook/extension.go
package relay_hook

import (
    "context"

    "github.com/xraph/dispatch"
    "github.com/xraph/dispatch/ext"
    "github.com/xraph/dispatch/job"
    "github.com/xraph/dispatch/workflow"
    "github.com/xraph/relay"
)

// Extension bridges Dispatch lifecycle events to Relay for webhook delivery.
// Customers subscribe to dispatch.* events via Relay's endpoint management.
type Extension struct {
    relay   *relay.Relay
    config  Config
}

type Config struct {
    // Which events to emit. Default: all events.
    EnabledEvents []string

    // Whether to include job payload in webhook body. Default: false.
    // Payloads may contain sensitive data — opt-in only.
    IncludePayload bool

    // Custom data enricher — add extra fields to webhook payloads.
    Enricher func(ctx context.Context, event map[string]any) map[string]any
}

func New(r *relay.Relay, opts ...Option) ext.Extension {
    e := &Extension{
        relay:  r,
        config: DefaultConfig(),
    }
    for _, opt := range opts {
        opt(&e.config)
    }
    return e
}

func (e *Extension) Name() string { return "relay-hooks" }

// OnInit registers all Dispatch event types with Relay's schema registry.
func (e *Extension) OnInit(ctx context.Context, d *dispatch.Dispatcher) error {
    schemas := []struct {
        Type        string
        Description string
    }{
        {EventJobEnqueued, "Fired when a job is enqueued for processing"},
        {EventJobStarted, "Fired when a worker begins executing a job"},
        {EventJobCompleted, "Fired when a job completes successfully"},
        {EventJobFailed, "Fired when a job fails (may retry)"},
        {EventJobRetrying, "Fired when a job is scheduled for retry"},
        {EventJobDLQ, "Fired when a job exhausts retries and enters the dead letter queue"},
        {EventWorkflowStarted, "Fired when a workflow run begins"},
        {EventWorkflowStepCompleted, "Fired when a workflow step completes"},
        {EventWorkflowStepFailed, "Fired when a workflow step fails"},
        {EventWorkflowCompleted, "Fired when an entire workflow completes successfully"},
        {EventWorkflowFailed, "Fired when a workflow fails terminally"},
        {EventCronFired, "Fired when a cron entry triggers"},
    }

    for _, s := range schemas {
        if e.isEnabled(s.Type) {
            e.relay.RegisterEventType(s.Type, relay.EventSchema{
                Description: s.Description,
                Version:     "2025-01-01",
            })
        }
    }
    return nil
}

// ──────────────────────────────────────────────────
// Job lifecycle → Relay events
// ──────────────────────────────────────────────────

func (e *Extension) OnJobCompleted(ctx context.Context, j *job.Job, duration time.Duration) error {
    if !e.isEnabled(EventJobCompleted) {
        return nil
    }
    return relay.Send(ctx, &relay.Event{
        Type: EventJobCompleted,
        Data: e.enrichJobPayload(ctx, map[string]any{
            "job_id":      j.ID.String(),
            "name":        j.Name,
            "queue":       j.Queue,
            "status":      "completed",
            "duration_ms": duration.Milliseconds(),
            "retry_count": j.RetryCount,
            "started_at":  j.StartedAt,
            "completed_at": j.CompletedAt,
        }),
    })
}

func (e *Extension) OnJobFailed(ctx context.Context, j *job.Job, err error) error {
    if !e.isEnabled(EventJobFailed) {
        return nil
    }
    return relay.Send(ctx, &relay.Event{
        Type: EventJobFailed,
        Data: e.enrichJobPayload(ctx, map[string]any{
            "job_id":      j.ID.String(),
            "name":        j.Name,
            "queue":       j.Queue,
            "status":      "failed",
            "error":       err.Error(),
            "retry_count": j.RetryCount,
            "max_retries": j.MaxRetries,
            "will_retry":  j.RetryCount < j.MaxRetries,
        }),
    })
}

func (e *Extension) OnJobDLQ(ctx context.Context, j *job.Job, err error) error {
    if !e.isEnabled(EventJobDLQ) {
        return nil
    }
    return relay.Send(ctx, &relay.Event{
        Type: EventJobDLQ,
        Data: e.enrichJobPayload(ctx, map[string]any{
            "job_id":      j.ID.String(),
            "name":        j.Name,
            "queue":       j.Queue,
            "status":      "dead_letter",
            "error":       err.Error(),
            "retry_count": j.RetryCount,
        }),
    })
}

// ──────────────────────────────────────────────────
// Workflow lifecycle → Relay events
// ──────────────────────────────────────────────────

func (e *Extension) OnWorkflowCompleted(ctx context.Context, run *workflow.Run, duration time.Duration) error {
    if !e.isEnabled(EventWorkflowCompleted) {
        return nil
    }
    return relay.Send(ctx, &relay.Event{
        Type: EventWorkflowCompleted,
        Data: map[string]any{
            "run_id":      run.ID.String(),
            "name":        run.Name,
            "status":      "completed",
            "duration_ms": duration.Milliseconds(),
        },
    })
}

func (e *Extension) OnWorkflowFailed(ctx context.Context, run *workflow.Run, err error) error {
    if !e.isEnabled(EventWorkflowFailed) {
        return nil
    }
    return relay.Send(ctx, &relay.Event{
        Type: EventWorkflowFailed,
        Data: map[string]any{
            "run_id": run.ID.String(),
            "name":   run.Name,
            "status": "failed",
            "error":  err.Error(),
        },
    })
}

// ──────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────

func (e *Extension) isEnabled(eventType string) bool {
    if len(e.config.EnabledEvents) == 0 {
        return true // all enabled by default
    }
    for _, et := range e.config.EnabledEvents {
        if et == eventType {
            return true
        }
    }
    return false
}

func (e *Extension) enrichJobPayload(ctx context.Context, data map[string]any) map[string]any {
    if e.config.Enricher != nil {
        data = e.config.Enricher(ctx, data)
    }
    return data
}
```

### Usage: Wiring Relay Hooks

```go
// Create Relay instance
r := relay.New(
    relay.WithDatabase(db),
    relay.WithSigningSecret("whsec_..."),
    relay.WithMaxRetries(5),
)

// Create Dispatch with Relay hooks
d := dispatch.New(
    dispatch.WithStore(pgStore),
    dispatch.WithConcurrency(20),

    // Wire Relay as a lifecycle hook extension
    dispatch.WithExtension(relay_hook.New(r,
        // Optional: only emit certain events
        relay_hook.WithEvents(
            relay_hook.EventJobCompleted,
            relay_hook.EventJobFailed,
            relay_hook.EventJobDLQ,
            relay_hook.EventWorkflowCompleted,
            relay_hook.EventWorkflowFailed,
        ),
        // Optional: include job payload in webhooks
        relay_hook.WithPayload(true),
    )),
)

// Start both
r.Start(ctx)
d.Start(ctx)
```

### What the Customer Receives

When a customer registers a webhook endpoint via Relay's API and subscribes to `dispatch.job.*`:

```http
POST /webhooks HTTP/1.1
Host: customer.example.com
Content-Type: application/json
X-Relay-Event-Type: dispatch.job.completed
X-Relay-Signature: v1=a2b3c4d5...
X-Relay-ID: evt_01h9a1b2c3...
X-Relay-Delivery-Attempt: 1

{
  "id": "evt_01h9a1b2c3...",
  "type": "dispatch.job.completed",
  "timestamp": "2026-02-17T12:00:00Z",
  "data": {
    "job_id": "job_01h2xcejqtf2nbrexx3vqjhp41",
    "name": "send-email",
    "queue": "default",
    "status": "completed",
    "duration_ms": 342,
    "retry_count": 0,
    "started_at": "2026-02-17T11:59:59Z",
    "completed_at": "2026-02-17T12:00:00Z"
  }
}
```

### Relay Integration Flow Between Libraries

```
Dispatch emits "dispatch.job.completed"
    │
    ├─→ relay_hook extension → relay.Send()
    │       │
    │       ├─→ Customer's monitoring endpoint (signed webhook)
    │       ├─→ Customer's Slack bot endpoint (signed webhook)
    │       └─→ Customer's audit log endpoint (signed webhook)
    │
    ├─→ Ledger extension (if present) → meter job execution for billing
    │
    └─→ Custom extension → whatever the developer needs

Dispatch emits "dispatch.workflow.failed"
    │
    ├─→ relay_hook extension → relay.Send()
    │       │
    │       ├─→ Customer's PagerDuty endpoint (urgent webhook)
    │       └─→ Customer's logging endpoint
    │
    └─→ SlackNotifier extension → internal team notification
```

### Standalone (Without Relay)

Extensions work without Relay. If you don't need webhook delivery, skip the relay_hook extension. The extension system is the foundation; Relay is one extension that plugs into it.

```go
// No Relay — just custom extensions
d := dispatch.New(
    dispatch.WithStore(pgStore),
    dispatch.WithExtension(&AuditLogger{store: auditStore}),
    dispatch.WithExtension(&SlackNotifier{webhook: url}),
)
```

---

## 9. Distributed Workers & Kubernetes Consensus

Dispatch is single-process by default. You scale by running multiple Dispatcher instances pointing at the same store — Postgres SKIP LOCKED naturally distributes work across consumers. But for **production Kubernetes deployments**, you want more: worker registration, leader election, health-aware rebalancing, and work stealing.

### The Problem

When you scale to N pods in K8s, several things need coordination:

- **Who runs cron?** Only one pod should fire each cron tick (leader election).
- **What if a pod dies mid-job?** Stale heartbeats need reaping, and those jobs need re-assignment.
- **How do you rebalance?** If pod A is overloaded and pod B is idle, work should shift.
- **How do pods discover each other?** Pod IPs change. Deployments scale up/down.

### Design: Consensus Interface

```go
// cluster/consensus.go
package cluster

import "github.com/xraph/dispatch/id"

// Consensus defines the contract for distributed coordination.
// Multiple implementations: Postgres advisory locks, Redis Redlock,
// Kubernetes Lease objects.
type Consensus interface {
    // AcquireLeadership attempts to become the cluster leader.
    // Returns true if this worker is now leader. TTL ensures leader
    // failover if the holder crashes.
    AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error)

    // RenewLeadership extends the leader's hold. Must be called
    // before TTL expires.
    RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error)

    // ReleaseLeadership explicitly gives up leadership (graceful shutdown).
    ReleaseLeadership(ctx context.Context, workerID id.WorkerID) error

    // IsLeader checks if the given worker is currently the leader.
    IsLeader(ctx context.Context, workerID id.WorkerID) (bool, error)
}
```

### Consensus Implementations

| Implementation | Mechanism | Best For |
|---------------|-----------|----------|
| `PostgresConsensus` | `pg_advisory_lock` | Single-DB deployments (default) |
| `RedisConsensus` | Redlock (multi-instance) | Redis-backed deployments |
| `K8sLeaseConsensus` | K8s Lease objects in `coordination.k8s.io` | Native K8s deployments |

### Kubernetes-Specific: Pod Discovery + Lease Election

```go
// cluster/k8s/discovery.go
package k8s

// Discovery watches Kubernetes Endpoints or Pod resources to find
// other Dispatch worker pods in the same deployment/statefulset.
type Discovery struct {
    clientset  kubernetes.Interface
    namespace  string
    labelSelector string  // e.g. "app=my-service,dispatch=worker"
    onChange   func(peers []Peer)
}

// Peer represents another Dispatch worker pod.
type Peer struct {
    WorkerID  id.WorkerID
    PodName   string
    PodIP     string
    Ready     bool
    Queues    []string
}
```

```go
// cluster/k8s/leader.go
package k8s

import (
    coordinationv1 "k8s.io/api/coordination/v1"
    "k8s.io/client-go/tools/leaderelection"
)

// LeaseConsensus implements cluster.Consensus using Kubernetes Lease objects.
// This is the K8s-native way to do leader election — no external dependencies.
type LeaseConsensus struct {
    clientset kubernetes.Interface
    namespace string
    leaseName string  // e.g. "dispatch-leader"
}

func (lc *LeaseConsensus) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
    // Uses K8s Lease API — same mechanism used by kube-controller-manager
    // and kube-scheduler for their own leader election.
    // ...
}
```

### Cluster Manager

```go
// cluster/cluster.go
package cluster

// Manager coordinates distributed Dispatch workers.
type Manager struct {
    workerID   id.WorkerID
    store      Store
    consensus  Consensus
    rebalancer *Rebalancer
    config     ManagerConfig
    logger     *slog.Logger
}

type ManagerConfig struct {
    HeartbeatInterval time.Duration  // How often to heartbeat (default: 10s)
    StaleThreshold    time.Duration  // When to consider a worker dead (default: 30s)
    RebalanceInterval time.Duration  // How often to check balance (default: 60s)
    LeaderTTL         time.Duration  // Leadership lock TTL (default: 15s)
}

func (m *Manager) Start(ctx context.Context) error {
    // 1. Register this worker in the store
    // 2. Start heartbeat goroutine (reports alive + queue depth)
    // 3. Start leader election loop
    // 4. If leader: start cron scheduler + reaper + rebalancer
    // 5. Watch for peer changes
}

func (m *Manager) Stop(ctx context.Context) error {
    // 1. Drain in-flight jobs
    // 2. Release leadership if held
    // 3. Deregister worker
}
```

### Work Stealing / Rebalancing

```go
// cluster/rebalancer.go
package cluster

// Rebalancer redistributes queued work when workers are unevenly loaded.
// Only the leader runs the rebalancer.
type Rebalancer struct {
    store Store
}

// Rebalance checks for:
// 1. Dead workers (no heartbeat) → reassign their running jobs
// 2. Overloaded workers → steal pending jobs from their queues
// 3. Idle workers → no action needed, they'll pick up naturally
func (r *Rebalancer) Rebalance(ctx context.Context) error { /* ... */ }
```

### Usage: Single Process (Default)

```go
// No cluster config — just works with SKIP LOCKED
d := dispatch.New(
    dispatch.WithStore(pgStore),
    dispatch.WithConcurrency(20),
)
```

### Usage: Kubernetes Deployment

```go
d := dispatch.New(
    dispatch.WithStore(pgStore),
    dispatch.WithConcurrency(20),
    dispatch.WithCluster(
        cluster.WithConsensus(k8s.NewLeaseConsensus(clientset, "default", "dispatch-leader")),
        cluster.WithDiscovery(k8s.NewDiscovery(clientset, "default", "app=my-service")),
        cluster.WithRebalanceInterval(60 * time.Second),
    ),
)
```

### Usage: Multi-Process (Postgres Only)

```go
// Multiple processes, same DB — Postgres advisory locks for consensus
d := dispatch.New(
    dispatch.WithStore(pgStore),
    dispatch.WithConcurrency(20),
    dispatch.WithCluster(
        cluster.WithPostgresConsensus(pgPool),
    ),
)
```

---

## 10. Integration Platform Pattern (Zapier-Style)

**Yes, Dispatch can absolutely power a Zapier-style integration platform.** The workflow engine, step functions, and event bus provide all the primitives needed. Here's how it maps:

### Concept Mapping

| Zapier Concept | Dispatch Primitive | How It Works |
|---------------|-------------------|--------------|
| **Trigger** | `event.Bus` + `WaitForEvent` | External webhook/poll → publishes event → wakes workflow |
| **Action** | `workflow.Step()` | Each integration action is a step with retries and checkpointing |
| **Zap (flow)** | `workflow.NewWorkflow()` | The entire trigger → action chain is a durable workflow |
| **Multi-step Zap** | `wf.Step()` chained | Each step checkpointed. Crash-safe. Independent retries |
| **Fan-out** | `wf.Parallel()` | "When trigger fires, do A AND B AND C simultaneously" |
| **Delay** | `wf.Sleep()` | "Wait 30 minutes, then send the follow-up" |
| **Filter/condition** | Go `if` in workflow | "Only continue if amount > $100" — just Go code |
| **Error handling** | DLQ + per-step retry | Failed steps retry independently. Exhausted → DLQ for inspection |
| **Rate limiting** | `queue.RateLimiter` | Per-tenant rate limits to respect API quotas per integration |
| **Execution log** | Job/Run state + OTel | Full trace of every step, every retry, every failure |

### Architecture: How an Integration Platform Uses Dispatch

```
┌──────────────────────────────────────────────────────────────────────┐
│                     Integration Platform Layer                        │
│                     (YOUR code, not Dispatch)                         │
│                                                                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │ Connector SDK   │  │ Flow Builder    │  │ Execution Tracker   │  │
│  │ (Slack, Gmail,  │  │ (UI defines     │  │ (Shows run history, │  │
│  │  Sheets, etc.)  │  │  trigger→action │  │  step status, logs) │  │
│  │                 │  │  chains)        │  │                     │  │
│  └────────┬────────┘  └───────┬─────────┘  └──────────┬──────────┘  │
│           │                   │                        │              │
│  ┌────────▼───────────────────▼────────────────────────▼──────────┐  │
│  │              Dispatch (the engine underneath)                   │  │
│  │                                                                 │  │
│  │  • Workflows = user-defined automation flows                    │  │
│  │  • Steps = individual connector actions (with retries)          │  │
│  │  • Events = triggers from webhooks/polls                        │  │
│  │  • Cron = scheduled triggers ("every hour", "daily at 9am")     │  │
│  │  • DLQ = failed automations for user inspection                 │  │
│  │  • Per-tenant rate limits = respect each API's quotas           │  │
│  │  • Scope = org-level isolation (each customer's flows)          │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

### Example: "When Stripe payment received → create Jira ticket → notify Slack"

```go
// This is what your integration platform generates when a user
// configures a flow. Dispatch powers the execution; your platform
// owns the connector SDK and flow builder UI.

var StripeToJiraToSlack = dispatch.NewWorkflow("stripe-jira-slack",
    func(wf dispatch.Workflow, trigger StripePaymentEvent) error {

        // Step 1: Transform Stripe data into Jira ticket fields
        ticket, err := wf.StepWithResult("create-jira-ticket",
            func(ctx dispatch.Context) (*JiraTicket, error) {
                return jiraConnector.CreateTicket(ctx, jira.CreateInput{
                    Project: trigger.Metadata["jira_project"],
                    Summary: fmt.Sprintf("Payment received: $%.2f from %s",
                        float64(trigger.Amount)/100, trigger.CustomerEmail),
                    Type: "Task",
                })
            },
        )
        if err != nil {
            return err
        }

        // Step 2: Notify Slack (retries independently of step 1)
        _, err = wf.Step("notify-slack", func(ctx dispatch.Context) error {
            return slackConnector.SendMessage(ctx, slack.MessageInput{
                Channel: trigger.Metadata["slack_channel"],
                Text: fmt.Sprintf("💰 Payment $%.2f → Jira ticket %s created",
                    float64(trigger.Amount)/100, ticket.Key),
            })
        })

        return err
    },
)
```

### Trigger Pattern: Webhook → Event → Workflow

```go
// Your platform's webhook receiver — not part of Dispatch itself
func handleStripeWebhook(w http.ResponseWriter, r *http.Request) {
    event := parseStripeEvent(r)

    // Publish into Dispatch's event bus
    // This wakes up any WaitForEvent listeners AND
    // can be used to trigger registered flows
    dispatch.PublishEvent(r.Context(), &dispatch.Event{
        Name:    "stripe.payment.received",
        Payload: marshal(event),
    })

    // OR directly run the workflow for this trigger
    dispatch.RunWorkflow(r.Context(), StripeToJiraToSlack, event)
}
```

### Trigger Pattern: Cron (Polling-Based Triggers)

```go
// For integrations that don't support webhooks — poll on a schedule
d.Cron("poll-gmail-inbox", "*/5 * * * *", PollGmailInbox,
    dispatch.CronTenant("*"), // runs per-tenant
)

var PollGmailInbox = dispatch.NewJob("poll-gmail-inbox",
    func(ctx dispatch.Context, _ struct{}) error {
        scope := forge.MustScope(ctx)
        // 1. Get this tenant's Gmail credentials
        // 2. Check for new emails since last poll
        // 3. For each new email, run the user's configured flow
        newEmails := gmailConnector.Poll(ctx, scope.OrgID())
        for _, email := range newEmails {
            dispatch.RunWorkflow(ctx, userFlow, email)
        }
        return nil
    },
)
```

### What Dispatch Handles vs What You Build

| Concern | Owned By | Notes |
|---------|----------|-------|
| **Durable execution** | Dispatch | Crash-safe, checkpointed workflows |
| **Retries + backoff** | Dispatch | Per-step, configurable |
| **Rate limiting** | Dispatch | Per-tenant, per-queue (respect API quotas) |
| **Scheduling** | Dispatch | Cron for polling triggers |
| **Dead letter queue** | Dispatch | Failed flows surfaced to users |
| **Scope isolation** | Dispatch | Each customer's flows isolated |
| **Execution history** | Dispatch | Workflow runs, step status, timings |
| **Observability** | Dispatch | OTel traces per flow execution |
| **Connector SDK** | You | The Slack/Gmail/Jira/Stripe adapters |
| **Flow builder UI** | You | Visual editor for connecting triggers → actions |
| **Credential vault** | You | OAuth tokens, API keys per tenant |
| **Flow registry** | You | Which workflows each tenant has configured |
| **Marketplace** | You | Published connectors, templates |

### Key Insight

Dispatch doesn't know about Slack or Jira. It knows about **jobs, workflows, steps, events, and cron**. Your integration platform maps user-configured flows onto these primitives. Dispatch handles all the hard stuff — durable execution, retries, scheduling, rate limiting, tenant isolation — so your platform code focuses purely on the connector logic and user experience.

---

## 11. Forge Scope Integration

Dispatch reads `forge.Scope` from context when available. When a job is enqueued, the current scope is captured into the job's metadata. When the worker executes the job, the scope is restored onto the execution context.

```go
// scope/scope.go
package scope

import (
    "context"
    "github.com/xraph/forge"
)

// Capture extracts forge.Scope from context and returns app/org IDs.
// Returns empty strings if no scope is present (standalone mode).
func Capture(ctx context.Context) (appID, orgID string) {
    s := forge.ScopeFrom(ctx)
    if s.IsZero() {
        return "", ""
    }
    return s.AppID(), s.OrgID()
}

// Restore creates a context with forge.Scope from stored app/org IDs.
// No-op if both are empty (standalone mode).
func Restore(ctx context.Context, appID, orgID string) context.Context {
    if appID == "" {
        return ctx
    }
    if orgID == "" {
        return forge.WithScope(ctx, forge.NewAppScope(appID))
    }
    return forge.WithScope(ctx, forge.NewOrgScope(appID, orgID))
}
```

### Scope Reference for Dispatch

| Operation | Scope Level | Rationale |
|-----------|-------------|-----------|
| Job execution | Inherited | Jobs carry the scope they were enqueued with |
| Cron (global) | App | Platform-wide scheduled tasks |
| Cron (per-tenant) | Org | Per-customer scheduled tasks |
| Queues | Both | App-level queues + org-level isolation |
| DLQ | Inherited | Failed jobs retain original scope |
| Rate limiting | Org | Per-customer job rate limits |
| Worker registration | App | Workers are platform-level resources |
| Leader election | App | One leader per app deployment |

---

## 12. Linting & Code Quality (golangci-lint v2)

### Configuration

```yaml
# .golangci.yml — golangci-lint v2 configuration for Dispatch
version: "2"

linters:
  default: none
  enable:
    # Core correctness
    - errcheck
    - govet
    - staticcheck
    - unused
    - ineffassign
    - typecheck

    # Code quality
    - gofmt
    - goimports
    - gocritic
    - revive
    - misspell
    - unconvert
    - unparam
    - prealloc

    # Bug prevention
    - bodyclose
    - noctx
    - rowserrcheck
    - sqlclosecheck
    - exportloopref
    - gosec
    - errname
    - errorlint

    # Style
    - nolintlint
    - whitespace
    - predeclared
    - tenv

  settings:
    gocritic:
      enabled-tags:
        - diagnostic
        - style
        - performance
      disabled-checks:
        - hugeParam
        - rangeValCopy

    revive:
      rules:
        - name: blank-imports
        - name: context-as-argument
        - name: context-keys-type
        - name: dot-imports
        - name: error-return
        - name: error-strings
        - name: error-naming
        - name: exported
          arguments: [checkPrivateReceivers]
        - name: if-return
        - name: increment-decrement
        - name: var-naming
        - name: var-declaration
        - name: range
        - name: receiver-naming
        - name: time-naming
        - name: unexported-return
        - name: indent-error-flow
        - name: errorf
        - name: empty-block
        - name: superfluous-else
        - name: unused-parameter
        - name: unreachable-code

    gosec:
      excludes:
        - G104
        - G304

    errcheck:
      check-type-assertions: true
      check-blank: true
      exclude-functions:
        - (io.Closer).Close
        - (*database/sql.Rows).Close

    govet:
      enable-all: true
      disable:
        - fieldalignment

  exclusions:
    presets:
      - comments
      - std-error-handling
    rules:
      - path: _test\.go
        linters: [gosec, errcheck, gocritic]
      - path: _examples/
        linters: [errcheck, gosec]
      - path: ".*_gen\\.go"
        linters: [all]

formatters:
  enable:
    - gofmt
    - goimports
  settings:
    goimports:
      local-prefixes:
        - github.com/xraph/dispatch

output:
  sort-order:
    - linter
    - file
```

### Makefile

```makefile
.PHONY: build test lint lint-fix lint-install migrate

build:
	@go build ./...

test:
	@go test -race -count=1 ./...

test-integration:
	@go test -race -tags=integration ./store/postgres/... ./store/bun/...

lint-install:
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

lint:
	@golangci-lint run ./...

lint-fix:
	@golangci-lint run --fix ./...

migrate:
	@go run ./cmd/migrate/main.go up
```

### Claude Code Enforcement

Every phase must pass `golangci-lint run ./...` with zero errors before proceeding. Never use `//nolint` without a justifying comment.

---

## 13. Development Phases

### Phase 0 — Project Scaffold & Tooling

**Goal:** Repo skeleton, go.mod, TypeID package, lint config, Makefile.

**Claude Code instructions:**
```
Initialize github.com/xraph/dispatch with Go 1.23+.
Create the full directory structure from the Module Layout section.
Create id/id.go with all TypeID prefix types, typed IDs, constructors, and parsers.
Create entity.go with the base Entity type.
Create .golangci.yml with the v2 config.
Create Makefile. Create errors.go with sentinel errors.
Verify: go build ./... and golangci-lint run ./... both pass.
```

**Exit criteria:** `go build ./...` and `golangci-lint run ./...` pass. TypeID package compiles.

---

### Phase 1 — Core Types & Store Interface

**Goal:** All entity types with TypeID, store interfaces per subsystem, composite store, memory backend.

**Claude Code instructions:**
```
Implement all entity types using TypeID:
- job/job.go: Job struct with id.JobID, State enum
- job/definition.go: Definition[T] generic type
- workflow/run.go: Run with id.RunID, Checkpoint with id.CheckpointID
- cron/entry.go: Entry with id.CronID
- dlq/entry.go: Entry with id.DLQID
- event/event.go: Event with id.EventID
- cluster/worker.go: Worker with id.WorkerID

Implement store interfaces following ControlPlane composite pattern:
- Each subsystem defines Store in its package (job.Store, workflow.Store, etc.)
- store/store.go composes all into single Store interface (including cluster.Store)
- store/memory/store.go implements the full composite for testing

Write table-driven unit tests for the memory store.
Verify: go test ./... and golangci-lint run ./... pass.
```

**Exit criteria:** Full memory store with passing tests. All entities use TypeID. Lint clean.

---

### Phase 2 — Job Registry, Enqueue, Worker Pool & Extension System

**Goal:** Register typed jobs, enqueue them, process from worker pool with scope capture/restore. Create the extension system so lifecycle hooks fire from day one.

**Claude Code instructions:**
```
Implement the extension system FIRST:
- ext/ext.go: Extension interface + all lifecycle hook interfaces
  (JobEnqueued, JobStarted, JobCompleted, JobFailed, JobRetrying, JobDLQ,
   WorkflowStarted, WorkflowCompleted, WorkflowFailed, etc.)
- ext/registry.go: Registry struct with Register(), type-cached emit methods
  (EmitJobCompleted, EmitJobFailed, etc.). Errors logged, never block.

Then implement job/registry.go, middleware system (middleware.go, recover.go, logging.go),
worker pool (pool.go, executor.go, lifecycle.go), scope helpers (scope/scope.go,
middleware/scope.go).

CRITICAL: The executor must call ext.Registry.Emit*() at every lifecycle point:
- After enqueue → EmitJobEnqueued
- When worker picks up job → EmitJobStarted
- After success → EmitJobCompleted
- After failure → EmitJobFailed
- Before retry → EmitJobRetrying
Wire WithExtension(e) into Dispatcher options.

Integration tests: enqueue→execute with hooks firing, scope flows through,
custom test extension receives all lifecycle events, graceful shutdown drains.
Verify: go test ./... and golangci-lint run ./... pass.
```

**Files:**
- `ext/ext.go`, `ext/registry.go`, `ext/options.go`
- `job/registry.go`
- `middleware/middleware.go`, `middleware/recover.go`, `middleware/logging.go`
- `worker/pool.go`, `worker/executor.go`, `worker/lifecycle.go`
- `scope/scope.go`, `middleware/scope.go`
- Updated `dispatch.go`, `options.go` (WithExtension)
- Tests: `ext/registry_test.go`, `dispatch_test.go`, `worker/pool_test.go`

**Exit criteria:** End-to-end job processing with memory store. Extensions fire at all lifecycle points. Scope flows. Clean shutdown.

---

### Phase 3 — Retry, Backoff & Dead Letter Queue

**Goal:** Automatic retries with backoff. Failed jobs land in DLQ. Extensions fire at retry and DLQ points.

**Claude Code instructions:**
```
Implement backoff.go (Constant, Linear, Exponential, ExponentialWithJitter).
Implement dlq/dlq.go service and dlq/replay.go.
Update executor to handle retries + push to DLQ on exhaustion.
Wire extension hooks: EmitJobRetrying before retry, EmitJobDLQ when entering DLQ.
Tests: retry-then-succeed, exhaust-retries-to-DLQ, DLQ replay,
       test extension receives OnJobRetrying and OnJobDLQ callbacks.
Verify: go test ./... and golangci-lint run ./... pass.
```

**Exit criteria:** Retries work. DLQ captures failures. Replay re-enqueues. Extension hooks fire at retry and DLQ.

---

### Phase 4 — Workflows & Step Functions

**Goal:** Multi-step workflows with checkpointing, fan-out/fan-in, durable waits. Extension hooks fire at workflow and step lifecycle points.

**Claude Code instructions:**
```
Implement workflow execution: Workflow struct with Step(), StepWithResult(),
Parallel() (errgroup), WaitForEvent(), Sleep(). Checkpoint serialization via gob.
event/bus.go for in-memory + store-backed event bus.
Wire extension hooks: EmitWorkflowStarted, EmitWorkflowStepCompleted,
EmitWorkflowStepFailed, EmitWorkflowCompleted, EmitWorkflowFailed.
Tests: multi-step, crash-resume from checkpoint, parallel, WaitForEvent + timeout,
       test extension receives all workflow lifecycle callbacks.
Verify: go test ./... and golangci-lint run ./... pass.
```

**Exit criteria:** Full workflow lifecycle. Checkpoint survives crashes. WaitForEvent works. Workflow extension hooks fire.

---

### Phase 5 — Cron Scheduling & Leader Election

**Goal:** Distributed cron with leader election. Per-tenant cron support.

**Claude Code instructions:**
```
Implement cron/cron.go (parser via robfig/cron/v3), cron/leader.go (distributed lock).
Per-tenant cron: CronTenant("*") iterates orgs, enqueues per-org with correct scope.
Tests: correct firing, two dispatchers with leader election, per-tenant cron.
Verify: go test ./... and golangci-lint run ./... pass.
```

**Exit criteria:** Cron fires correctly. Only leader executes. Per-tenant works.

---

### Phase 6 — Queue Features (Priority, Rate Limiting, Per-Tenant)

**Goal:** Priority queues, per-queue and per-tenant rate limiting.

**Exit criteria:** Priority ordering. Rate limiting throttles. Tenant isolation verified.

---

### Phase 7 — Observability (OpenTelemetry + Prometheus)

**Goal:** OTel traces per job, Prometheus metrics, timeout middleware.

**Exit criteria:** Spans created. Metrics exposed. Timeouts cancel jobs.

---

### Phase 8 — PostgreSQL Store (pgx)

**Goal:** Production Postgres backend with pgx/v5. SKIP LOCKED dequeue, advisory lock leader election, LISTEN/NOTIFY events.

**Claude Code instructions:**
```
Implement store/postgres/ — all subsystem implementations including cluster.Store.
Embed SQL migrations. Use pgxpool for connection pooling.
Key patterns: SELECT FOR UPDATE SKIP LOCKED for dequeue,
pg_advisory_lock for leader election, LISTEN/NOTIFY for events.
Integration tests with testcontainers-go.
Verify: go test -tags=integration ./store/postgres/... and golangci-lint run ./... pass.
```

**Exit criteria:** Full Postgres store with integration tests passing.

---

### Phase 9 — Bun Store

**Goal:** Bun ORM backend for teams using Bun in their Forge/ControlPlane stack.

**Claude Code instructions:**
```
Implement store/bun/ — Bun model structs in models.go with bun:"" tags.
Each subsystem implementation uses Bun query builder.
For SKIP LOCKED dequeue, use bun.NewRaw() (Bun supports raw queries).
Bun Go-based migrations in migrations/001_initial.go.
Share the same SQL schemas as Postgres (same tables, Bun just uses ORM for access).
Integration tests with testcontainers-go.
Verify: go test -tags=integration ./store/bun/... and golangci-lint run ./... pass.
```

**Exit criteria:** Full Bun store. Passes same integration test suite as Postgres store.

---

### Phase 10 — SQLite & Redis Stores

**Goal:** SQLite for embedded/edge. Redis for high-throughput ephemeral workloads.

**Claude Code instructions:**
```
SQLite: store/sqlite/ — embedded migrations, adapted SQL (no SKIP LOCKED, use
BEGIN IMMEDIATE + rowid ordering instead). modernc.org/sqlite for pure-Go driver.

Redis: store/redis/ — Redis Streams for job queues (XREADGROUP for consumer groups),
Sorted Sets for priority/scheduling, Lua scripts for atomic dequeue,
Redlock for leader election.

Tests for both backends.
Verify: go test ./store/sqlite/... ./store/redis/... and golangci-lint run ./... pass.
```

**Exit criteria:** SQLite and Redis stores pass tests.

---

### Phase 11 — Distributed Workers & Cluster

**Goal:** Worker registration, consensus interface, Postgres + K8s leader election, rebalancing.

**Claude Code instructions:**
```
Implement cluster/ package:
- cluster.go: Manager with Start/Stop lifecycle
- consensus.go: Consensus interface
- worker.go: Worker entity
- rebalancer.go: Work stealing logic (leader-only)

Implement consensus backends:
- Store-based (Postgres advisory locks) — default
- cluster/k8s/leader.go: K8s Lease-based (optional, behind build tag)
- cluster/k8s/discovery.go: K8s pod discovery

Wire into Dispatcher: WithCluster() option. Manager starts alongside pool.
Tests: worker registration, leader election, stale reaping, rebalance.
Verify: go test ./... and golangci-lint run ./... pass.
```

**Exit criteria:** Cluster manager works. Leader election prevents duplicate cron. Dead workers reaped.

---

### Phase 12 — Forge Extension

**Goal:** Mount Dispatch into Forge as an extension.

**Exit criteria:** Dispatch mounts into Forge. Auto-discovers store from DI.

---

### Phase 13 — Relay Hook Extension

**Goal:** Built-in extension that bridges Dispatch lifecycle events to Relay for webhook delivery.

**Claude Code instructions:**
```
Implement relay_hook/ package:
- relay_hook/events.go: All event type constants (dispatch.job.completed, etc.)
- relay_hook/extension.go: Extension struct implementing ext.Extension +
  all job/workflow lifecycle interfaces. Each hook calls relay.Send() with
  typed event payload.
- relay_hook/options.go: WithEvents() to filter events, WithPayload() to include
  job payload in webhook, WithEnricher() for custom data enrichment.

OnInit registers all Dispatch event types with Relay's schema registry.
Relay is a soft dependency — relay_hook imports the relay package,
but Dispatch core does NOT import relay.

Tests: mock Relay, register extension, process job, verify relay.Send() called
with correct event type and payload structure.
Integration test: real Relay + Dispatch, verify webhook delivered for job.completed.
Verify: go test ./... and golangci-lint run ./... pass.
```

**Files:**
- `relay_hook/extension.go`, `relay_hook/events.go`, `relay_hook/options.go`
- Tests: `relay_hook/extension_test.go`

**Exit criteria:** Relay hook extension fires for all lifecycle events. Customers receive signed webhooks via Relay when subscribed to dispatch.* events.

---

### Phase 14 — Admin API, Examples & Documentation

**Goal:** HTTP admin API, examples (including extensions, relay hooks, integration platform, and distributed), README.

**Claude Code instructions:**
```
Implement admin API (Dispatcher.Routes()).
Write examples:
- _examples/basic/: Simple job
- _examples/workflow/: Multi-step with fan-out
- _examples/cron/: Scheduled jobs
- _examples/extensions/: Custom extension (Slack notifier + audit logger)
- _examples/relay-hooks/: Dispatch + Relay webhook delivery
- _examples/distributed/: K8s multi-pod with cluster config
- _examples/integration-platform/: Zapier-style trigger→action flow
- _examples/forge/: Forge extension
Write README.md with extension system + Relay integration docs.
Verify: go test ./... and golangci-lint run ./... pass.
```

**Exit criteria:** Admin API working. All examples run. README complete.

---

## 14. Claude Code Development Guide

### Starting Each Phase

```
Read the Dispatch design document at docs/DESIGN.md.
We are implementing Phase N: [Phase Name].
Follow the files, exit criteria, and instructions for this phase.
Before writing code, read existing codebase.
After writing code, run:
  1. go build ./...
  2. go test ./...
  3. golangci-lint run ./...
All three must pass before the phase is complete.
```

### Code Style Rules

- **IDs:** Use TypeID via `id.NewJobID()`, `id.ParseJobID(s)`, etc. Never raw strings for IDs.
- **Errors:** Always wrap with `fmt.Errorf("dispatch: %w", err)`. Use sentinel errors.
- **Context:** Always first parameter. Check `ctx.Done()` in loops.
- **Imports:** Group as `stdlib → external → internal`. `goimports` handles this.
- **No globals.** All state on structs. No `init()` with side effects.
- **No panics in library code.** Return errors. Only `id.must()` panics (internal, infallible).
- **Table-driven tests.** All tests use `tests := []struct{ ... }` pattern.

### Dependency Graph

```
dispatch (root)
├── id              (TypeID definitions — zero deps)
├── job             (entities + store interface) → id
├── workflow        (entities + store interface) → id
├── cron            (entities + store interface) → id
├── dlq             (entities + store interface) → id
├── event           (entities + store interface) → id
├── cluster         (entities + store + consensus interface) → id
│   └── k8s         (K8s-specific consensus + discovery) → cluster
├── ext             (extension interfaces + registry) → job, workflow, id
├── relay_hook      (Relay bridge extension) → ext, relay (soft dep)
├── queue           (queue abstraction) → job
├── worker          (pool + executor) → job, middleware, store, ext
├── middleware      → job, scope
├── scope           → forge (soft dependency)
├── store           → job, workflow, cron, dlq, event, cluster (composes interfaces)
│   ├── memory      → store
│   ├── postgres    → store (pgx/v5)
│   ├── bun         → store (uptrace/bun)
│   ├── sqlite      → store (modernc.org/sqlite)
│   └── redis       → store (go-redis/v9)
├── api             → dispatch, job, workflow, cron, dlq, cluster
└── extension       → dispatch, forge
```

**Rules:**
- Subsystem packages NEVER import each other.
- `ext/` defines interfaces only — no implementation imports.
- `relay_hook/` imports `ext/` and `relay` but relay is a soft dependency (interface-based).
- `worker/` calls `ext.Registry.Emit*()` after job execution.
- `store/` composes interfaces but never imports implementations.
- `cluster/k8s/` is behind a build tag (`//go:build k8s`).
- `extension/` imports `dispatch` root and `forge` but nothing else.

### Key Decisions Log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| IDs | TypeID (`go.jetify.com/typeid`) | Consistent with ControlPlane/Nexus. Type-safe, prefixed, K-sortable |
| Store pattern | ControlPlane composite | Subsystem interfaces composed into Store. Proven pattern |
| Store backends | Postgres, Bun, SQLite, Redis, Memory | Matches ControlPlane + adds Bun for ORM users |
| Bun store | `uptrace/bun` | Teams using Forge/ControlPlane already have Bun. Natural fit |
| Postgres driver | pgx/v5 | Best Go Postgres driver |
| Dequeue strategy | SKIP LOCKED | Postgres-native concurrent polling |
| Leader election | Pluggable Consensus interface | Postgres advisory locks (default), K8s Lease, Redis Redlock |
| K8s support | Optional build tag | `cluster/k8s/` behind `//go:build k8s`. No K8s deps for non-K8s users |
| Event delivery | LISTEN/NOTIFY (Postgres) | Real-time without polling. Fallback to polling for other stores |
| Serialization | gob for checkpoints, JSON for payloads | gob fast for internal state. JSON for user payloads |
| Extension system | Interface-based discovery | Extensions implement any subset of lifecycle interfaces. Registry type-asserts at registration time for O(1) dispatch |
| Extension error policy | Log and continue | Extension errors never block job execution. Logged via slog |
| Relay integration | Extension, not core | Relay is one extension (`relay_hook/`). Dispatch core does not import Relay. Soft dependency |
| Webhook delivery | Relay-native | Relay handles signing, retries, fan-out, delivery logs. Dispatch just calls relay.Send() |
| Middleware vs Extension | Both, different purpose | Middleware wraps execution (can block). Extensions react to lifecycle (can't block) |
| Linting | golangci-lint v2 | Modern, fast, comprehensive |

---

## Phase Summary

| Phase | Name | Effort | Depends On |
|-------|------|--------|------------|
| 0 | Project Scaffold & Tooling | 0.5 day | — |
| 1 | Core Types & Store Interface | 1 day | Phase 0 |
| 2 | Job Registry, Worker Pool & Extension System | 2.5 days | Phase 1 |
| 3 | Retry, Backoff & Dead Letter Queue | 1 day | Phase 2 |
| 4 | Workflows & Step Functions | 2 days | Phase 2 |
| 5 | Cron Scheduling & Leader Election | 1.5 days | Phase 2 |
| 6 | Queue Features | 1 day | Phase 2 |
| 7 | Observability | 1 day | Phase 2 |
| 8 | PostgreSQL Store (pgx) | 2 days | Phase 1 |
| 9 | Bun Store | 1.5 days | Phase 1 |
| 10 | SQLite & Redis Stores | 2 days | Phase 1 |
| 11 | Distributed Workers & Cluster | 2.5 days | Phase 2, 8 |
| 12 | Forge Extension | 1 day | Phase 2 |
| 13 | Relay Hook Extension | 1.5 days | Phase 2 |
| 14 | Admin API, Examples & Docs | 2.5 days | All |

**Total estimated effort: ~23 days (6–8 weeks at part-time pace)**

### Parallelizable Phases

```
Phase 0 → Phase 1 → Phase 2 ─┬─→ Phase 3  (Retry + DLQ)
       (ext/ created here) ────├─→ Phase 4  (Workflows + workflow hooks)
                               ├─→ Phase 5  (Cron)
                               ├─→ Phase 6  (Queue features)
                               ├─→ Phase 7  (Observability)
                               ├─→ Phase 11 (Distributed workers) *needs Phase 8
                               ├─→ Phase 12 (Forge extension)
                               └─→ Phase 13 (Relay hook extension)
                    Phase 1 ───┬─→ Phase 8  (Postgres store)
                               ├─→ Phase 9  (Bun store)
                               └─→ Phase 10 (SQLite + Redis)
                                         │
                                         ▼
                                    Phase 14 (API + Examples + Docs)
```

---

## v0.1.0 MVP Target

- Jobs + enqueue + worker pool + extension system (Phase 2)
- Basic retries + DLQ (Phase 3)
- Basic workflows (Phase 4, without WaitForEvent)
- Cron (Phase 5)
- Postgres store (Phase 8)
- Memory store for testing (Phase 1)
- Extension hooks fire at all lifecycle points (but no Relay yet)

## v0.2.0

- Bun store (Phase 9)
- Priority queues + rate limiting (Phase 6)
- Full workflows with WaitForEvent + Sleep (Phase 4 complete)
- Observability (Phase 7)
- **Relay hook extension** (Phase 13) — webhook delivery for all lifecycle events
- Forge extension (Phase 12)

## v0.3.0

- Distributed workers + K8s consensus (Phase 11)
- SQLite + Redis stores (Phase 10)
- Admin API (Phase 14)
- Extension + relay-hook + integration-platform examples (Phase 14)
