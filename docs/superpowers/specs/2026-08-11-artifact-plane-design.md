# Artifact Plane — Design

**Date:** 2026-08-11
**Status:** Approved for planning
**Scope:** Sub-project A of the Dispatch heavy-workload track

---

## 1. Problem

Dispatch today assumes small payloads and short jobs. `job.Payload` is a `[]byte`
stored inline as `BYTEA` (`store/postgres/migrations.go:26`), the default timeout is
five minutes (`job/options.go:29`), retry re-runs a handler from the top
(`worker/executor.go:115`), and there is no concept of CPU, memory, disk, or GPU
anywhere in the tree.

TwinOS is the opposite workload: multi-gigabyte IFC, glTF, and point-cloud models, and
PDF documents in the gigabyte range. Nobody will put those bytes in a `BYTEA` column, so
users pass an object-store URL as an opaque string inside the payload. Because the
payload is opaque to the engine, Dispatch then knows nothing about the data a job
touches. That blindness blocks every downstream capability:

- No input size, so no resource estimation and no pod sizing.
- No content identity, so no dedupe and no locality-aware scheduling.
- No ownership record, so intermediates accumulate with no lifecycle.
- No lineage, so the dashboard cannot show what a run consumed or produced.
- No staging boundary, so a sandboxed executor has nothing to mount.

The artifact plane makes data a first-class concept in Dispatch. It is the foundation
for the four sub-projects that follow it.

### Position in the larger track

| | Sub-project | Depends on |
|---|---|---|
| **A** | **Artifact plane** (this document) | — |
| B | Resource model and resource-aware scheduling | A (input-size signal) |
| C | Execution isolation (sandbox, pod-per-job) | A (staging boundary), B (resource requests) |
| D | Long-run durability (progress checkpoints, resume) | independent |
| E | Resource prediction | B (measurement data) |

### Non-goals

This document does not cover sandboxing, resource declaration or scheduling, job-level
progress checkpointing, or resource prediction. It defines only the data plane those
tracks build on. Where a decision here creates a seam for a later track, that seam is
noted explicitly.

---

## 2. Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Artifact model | First-class entity across all five stores | Tracks B, C, and E all require the engine to know size, identity, and ownership. A payload-embedded ref would strand them. |
| Binding | Declared inputs on the definition; imperative outputs | Declaration lets the engine know total input size before scheduling and stage automatically. Outputs stay imperative so dynamic fan-out works. |
| Ownership | Two-tier: own ephemeral, track durable | Dispatch never deletes bytes the application uploaded. GC operates only on artifacts Dispatch itself created. |
| Scratch | Shared content-addressed cache with a disk budget | Restaging is free, concurrent stages dedupe, and the budget prevents disk exhaustion. Also the first instance of admission control (track B). |
| Backend | Small `artifact.Backend` interface; Trove is the reference implementation | Dispatch is a library and users choose their storage. No hard Trove dependency in core. |

---

## 3. Package layout

`artifact` must be a leaf package. `job.Options` carries input declarations, so `job`
imports `artifact`; therefore `artifact` may depend only on `id` and the root `dispatch`
package, never on `job`.

```
artifact/            leaf: Ref, Artifact, InputSpec, Lifecycle, Role,
                           Accessor, Backend interface, Store interface
artifact/cache/      worker-local CAS cache: staging, LRU eviction,
                           disk budget, single-flight download
artifact/staging/    the execution middleware — imports job + middleware
artifact/trove/      Trove-backed Backend adapter
```

The staging middleware lives in `artifact/staging`, not in `artifact`. Its signature is
`func(ctx, *job.Job, next) error`, which requires importing `job` — and `job` imports
`artifact` for `Options.Inputs`. Keeping the middleware in a sub-package breaks that
cycle. `artifact` itself stays free of any `job` dependency.

`artifact.Store` joins the composite `store.Store` (`store/store.go:33`) alongside
`job.Store`, `workflow.Store`, `cron.Store`, `dlq.Store`, `event.Store`, and
`cluster.Store` — the same composable idiom, implemented by all five backends.

### Backend interface

```go
type Backend interface {
    Name() string
    Open(ctx context.Context, ref Ref) (io.ReadCloser, error)
    Create(ctx context.Context, key string) (Writer, error)
    Stat(ctx context.Context, ref Ref) (ObjectInfo, error)
    Delete(ctx context.Context, ref Ref) error
}

// Opt-in capabilities, matching Trove's capability idiom.
type RangeReader interface {
    OpenRange(ctx context.Context, ref Ref, off, n int64) (io.ReadCloser, error)
}
type Presigner interface {
    PresignGet(ctx context.Context, ref Ref, ttl time.Duration) (string, error)
}
```

`Writer.Commit` returns the **logical** size and hash of the bytes the handler wrote.
Trove middleware (compress, encrypt) means stored bytes differ from written bytes, and
the artifact row records what the handler produced, not what landed on disk.

`Presigner` is what lets a DWP remote worker (`dwp/server.go`) fetch a multi-gigabyte
model directly from object storage instead of streaming it through the coordinator over
a WebSocket. Without it, the coordinator is a bandwidth bottleneck and the
untrusted-tenant-worker model in track C is not viable.

---

## 4. Data model

```sql
CREATE TABLE dispatch_artifacts (
  id             TEXT PRIMARY KEY,        -- art_01h...
  backend        TEXT NOT NULL,           -- Trove store name
  bucket         TEXT NOT NULL,
  key            TEXT NOT NULL,
  size           BIGINT NOT NULL,
  content_hash   TEXT,                    -- 'blake3:9f2a...', NULL until known
  content_type   TEXT,
  lifecycle      TEXT NOT NULL,           -- 'durable' | 'ephemeral'
  scope_app_id   TEXT,
  scope_org_id   TEXT,
  expires_at     TIMESTAMPTZ,
  created_at     TIMESTAMPTZ NOT NULL,
  deleted_at     TIMESTAMPTZ,
  UNIQUE (backend, bucket, key)
);

CREATE TABLE dispatch_artifact_links (
  artifact_id    TEXT NOT NULL REFERENCES dispatch_artifacts(id),
  owner_kind     TEXT NOT NULL,           -- 'job' | 'run' | 'step'
  owner_id       TEXT NOT NULL,
  role           TEXT NOT NULL,           -- 'input' | 'output' | 'intermediate'
  name           TEXT NOT NULL,           -- declared slot name, or created filename
  attempt        INT  NOT NULL DEFAULT 0,
  created_at     TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (artifact_id, owner_kind, owner_id, name, attempt)
);

CREATE INDEX ON dispatch_artifact_links (owner_kind, owner_id);
CREATE INDEX ON dispatch_artifacts (lifecycle, deleted_at) WHERE deleted_at IS NULL;
CREATE INDEX ON dispatch_artifacts (content_hash) WHERE content_hash IS NOT NULL;
```

IDs use the existing TypeID system with prefix `art`.

**Refcount is derived, not stored.** GC counts live links rather than maintaining a
counter column. A counter is faster and drifts; the join is correct and this table will
not be hot. Materialize only if it becomes so.

**`content_hash` is nullable and filled opportunistically.** Hashing a 2 GB file costs a
full read pass, so `Register` does not do it — enqueue stays a cheap row insert. The
hash is computed during the first staging, when the cache is already streaming every
byte to disk. Until then the artifact is identified by `(backend, bucket, key)` and does
not participate in dedupe. Dedupe is a property an artifact earns after first use rather
than a tax charged at ingest.

**`scope_app_id` / `scope_org_id` mirror the job columns** so tenant isolation follows
the existing `scope` package pattern.

**`expires_at` is the per-artifact retention override.** When `NULL`, eligibility is
computed from owner terminal time plus the configured retention (§8). When set — by
`artifact.Retain(d)` — it takes precedence and the artifact is eligible once
`expires_at` has passed and all owners are terminal. Owners being terminal is required
in both cases; `expires_at` shortens or lengthens the window, it never bypasses
liveness.

**Ephemeral object keys embed the attempt.** Because `Commit` is attempt-scoped (§6) but
`(backend, bucket, key)` is unique, a retry creating `mesh.glb` a second time would
otherwise collide. Ephemeral keys are therefore:

```
<ephemeral_prefix>/<owner_kind>/<owner_id>/<attempt>/<name>
  e.g. ephemeral/job/job_01h.../2/mesh.glb
```

`attempt` is taken from `job.RetryCount` at execution time. `IfAbsent` resolves across
attempts by querying links on `(owner_kind, owner_id, name)` and ignoring `attempt`,
which is why `attempt` is part of the link primary key rather than a bare column.

---

## 5. Trove extension integration

Trove's Forge extension registers `*trove.Trove` in the DI container both unnamed and
named per store (`trove/extension/extension.go:630`, `:639`). Dispatch therefore
supports Trove multi-store without importing `trove/extension` as a module — named
lookup is sufficient.

Resolution mirrors the existing `grove.DB` auto-discovery at
`extension/extension.go:141`:

```go
func (e *Extension) resolveArtifactBackend(fapp forge.App) (artifact.Backend, error) {
    if e.artifactBackend != nil {                       // 1. programmatic
        return e.artifactBackend, nil
    }
    if name := e.config.Artifacts.TroveStore; name != "" {
        t, err := vessel.InjectNamed[*trove.Trove](fapp.Container(), name)
        if err != nil {
            return nil, fmt.Errorf("trove store %q not found in container: %w", name, err)
        }
        return trovebackend.New(t, e.config.Artifacts), nil
    }
    if t, err := vessel.Inject[*trove.Trove](fapp.Container()); err == nil {
        e.Logger().Info("dispatch: auto-discovered trove from container")
        return trovebackend.New(t, e.config.Artifacts), nil
    }
    return nil, nil   // artifacts disabled; Dispatch behaves exactly as today
}
```

Mounting both extensions is the entire wiring:

```go
app := forge.New(
    troveext.New(),      // provides *trove.Trove into DI
    dispatchext.New(),   // discovers it, enables the artifact plane
)
```

```yaml
extensions:
  dispatch:
    artifacts:
      enabled: true
      trove_store: "models"        # "" → default *trove.Trove from DI
      bucket: dispatch-artifacts
      ephemeral_prefix: ephemeral/
      retention: 168h
      purge_grace: 24h
      cache:
        dir: /var/lib/dispatch/cache
        budget: 200GB
```

Two consequences of building on Trove that this design deliberately does not duplicate:

**Trove's multi-store names are the `backend` column.** Routing heavy meshes to S3 and
thumbnails to local disk is Trove configuration. Dispatch records which store an
artifact lives in and adds no parallel routing system.

**Trove CAS and Dispatch links refcount different things.** Trove CAS dedupes *bytes* —
two logically distinct artifacts with identical content share one object. Dispatch links
track *logical* references — which runs still need this artifact. Dispatch decides when
an artifact is logically dead and calls `Delete`; Trove decides whether the underlying
bytes are still shared. They compose. Refcounting inside Dispatch's storage layer would
have conflicted with Trove's.

Two capabilities obtained by configuration rather than code: Trove's `encrypt`
middleware gives artifacts AES-256-GCM at rest, and its `scan` (ClamAV) middleware sits
on the write path, so a malicious IFC or PDF can be rejected at registration before any
memory-unsafe parser opens it. That is a real layer of the track-C defense with no
Dispatch code.

---

## 6. Handler-facing API

```go
var Tessellate = job.NewDefinition("tessellate.model",
    func(ctx context.Context, in TessellateInput) error {
        art := artifact.From(ctx)

        // Declared input — already on local disk before the handler was called.
        src := art.Path("model")   // /var/lib/dispatch/cache/blake3/9f/9f2a...

        mesh, err := occt.Tessellate(src, in.Detail)
        if err != nil {
            return err
        }

        w, err := art.Create(ctx, "mesh.glb",
            artifact.ContentType("model/gltf-binary"))
        if err != nil {
            return err
        }
        defer w.Abort()               // no-op after a successful Commit
        if _, err := io.Copy(w, mesh); err != nil {
            return err
        }
        _, err = w.Commit(ctx)        // uploads, inserts row, links role=output
        return err
    },
    artifact.Input("model",
        artifact.Required,
        artifact.MaxSize(8<<30),
        artifact.StageAsPath),
    job.WithTimeout(6*time.Hour),
)

engine.Enqueue(ctx, eng, Tessellate, in, artifact.Bind("model", ref))
```

### Staging is a middleware

`middleware.Middleware` is `func(ctx, *job.Job, next Handler) error`
(`middleware/middleware.go:19`), which fits staging exactly. `artifact.Middleware(cache,
store)` stages declared inputs, injects the accessor into the context, calls `next`, and
finalizes. Nothing in `worker/executor.go` changes.

This is also the correct layering for track C: when the executor becomes a sandbox, the
staging middleware runs *outside* the boundary, and the sandbox receives a directory
rather than storage credentials.

### Staging modes

`StageAsPath` pre-downloads to local disk for native libraries that seek and memory-map
— OpenCASCADE, Assimp, PDFium. `StageLazy` skips the download and `art.Open(name)`
streams on demand, which is right for data read once and wrong for an IFC.

### Commit is immediate and attempt-scoped

A six-hour job splitting a 400-page PDF cannot buffer commits until it returns, so
`Commit` uploads and inserts immediately and the link carries an `attempt` column.
Outputs from a failed attempt become orphaned-ephemeral and are swept.

```go
w, err := art.Create(ctx, "page-317.png", artifact.IfAbsent())
// a prior attempt already committed this → returns the existing ref with
// artifact.ErrExists
```

`IfAbsent` is the seam for track D: a retried job skipping the 316 pages it already
rendered is resumption built from the artifact plane rather than a separate checkpoint
mechanism.

### Workflow steps carry refs, not bytes

`dispatch_checkpoints.data` is `BYTEA` (`store/postgres/migrations.go:109`), so a step
returning a 4 GB mesh has nowhere sane to put it today. A step now returns an
`artifact.Ref` — a few hundred bytes of JSON in the checkpoint — while the bytes live in
Trove, linked to the run.

---

## 7. Staging cache

```
<cache_dir>/
  tmp/<uuid>                      in-flight downloads, wiped at startup
  blake3/9f/9f2a3c...             content-addressed, shared across jobs
  index.db                        sqlite: hash, size, last_used, backend/bucket/key
```

Downloads stream through a hasher into `tmp/` and are then renamed into the hash path,
so the hash is computed during a read that was happening anyway. This is what fills in
the nullable `content_hash` from §4 at no cost.

- **Single-flight** via `golang.org/x/sync/singleflight` (already a dependency): eight
  jobs staging the same model trigger one download and eight cache hits.
- **Leases** — `Stage` returns a `release func()`; a leased entry cannot be evicted.
- **Budget** — `Acquire(n)` blocks until `n` bytes are reclaimable, evicting unleased
  entries by LRU.

The failure mode requiring explicit design: if every cached entry is leased and the
budget is exhausted, a waiting job would block forever. Two guards —

1. `Acquire` is bounded by the job's remaining context deadline and returns
   `ErrCacheBudgetExceeded` rather than hanging.
2. A definition whose declared `MaxSize` total exceeds the entire cache budget is
   rejected at `engine.Register`. A job that can never be staged fails on a developer's
   machine, not in production.

Crash recovery is deliberately dumb: wipe `tmp/`, rebuild the index by walking the hash
directories. The cache is a cache; a corrupt index costs a re-download, never
correctness.

### Seams for later tracks

`Acquire` is admission control. A job needing 8 GB of staging waits rather than running.
Extending the same mechanism to memory and CPU is track B's shape, and the `size` column
is the resource estimator's first feature.

Content addressing makes the cache a scheduling signal: a worker can advertise held
hashes in `cluster.Worker.Metadata` (`cluster/worker.go:33`), and the fetch loop can
prefer jobs whose inputs are already local. Re-tessellating one building at five detail
levels then pulls 2 GB from S3 once instead of five times.

---

## 8. Lifecycle sweeping

Two mechanisms that must not be conflated. **Cache eviction** is worker-local, LRU, and
loses nothing (§7). **Artifact sweeping** deletes bytes from object storage.

Eligibility, shown illustratively — `owner_is_terminal` and `owner_terminal_at` stand
for joins against `dispatch_jobs` and `dispatch_workflow_runs`, resolved per `owner_kind`.
The real implementation is one statement per owner kind rather than a polymorphic join,
and each backend expresses it in its own dialect:

```sql
UPDATE dispatch_artifacts SET deleted_at = now()
WHERE lifecycle = 'ephemeral'            -- literal, never a parameter
  AND deleted_at IS NULL
  AND id IN (
    SELECT a.id FROM dispatch_artifacts a
    JOIN dispatch_artifact_links l ON l.artifact_id = a.id
    GROUP BY a.id
    HAVING bool_and(owner_is_terminal(l))
       AND max(owner_terminal_at(l)) + $retention < now()
  );
```

An artifact with **zero** links is not matched by this statement at all — the join
eliminates it. Orphans are handled by a separate pass keyed on `created_at` (below), so
the two cases never share logic.

`lifecycle = 'ephemeral'` appears as a literal in every sweep statement and is never
bound from a variable. Durable artifacts — every customer upload — are unreachable from
this code path even if the eligibility logic above it is wrong.

**Two-phase deletion.** The sweeper sets `deleted_at` and stops serving the artifact; a
separate purge pass removes bytes after `purge_grace` (default 24h). A GC bug is
observable and recoverable for a day rather than instantly destructive, and both phases
are idempotent under retry.

**Leader-only.** Sweeping runs on the elected leader (`cluster/`), batched and
rate-limited, with a dry-run mode and a kill switch. Metrics:
`dispatch_artifacts_swept_total`, `dispatch_artifacts_bytes_reclaimed`.

**Orphans** are rare by construction — `Commit` inserts the artifact row and its link in
one transaction, so a zero-link artifact results only from partial failure. Those get a
longer, independent grace window.

Sweeps emit through the existing extension registry (`EmitArtifactSwept`), so
`audit_hook` and `relay_hook` observe them with no new plumbing.

Retention is overridable per definition and per artifact via `artifact.Retain(d)`.

---

## 9. Error handling

Transient versus permanent, mirroring `isTransientStoreErr` (`worker/pool.go:24`):

| Failure | Handling |
|---|---|
| Input artifact deleted (`ErrNotFound`) | Fail fast to DLQ. Retrying a fetch of something that no longer exists wastes three attempts. |
| Backend timeout or 5xx during staging | Transient. Normal retry with backoff. |
| Declared input exceeds `MaxSize` | Rejected at enqueue, returned to the caller. Never becomes a failed job. |
| Declared total exceeds cache budget | Rejected at `engine.Register`. |
| Cache budget exhausted, all entries leased | `ErrCacheBudgetExceeded`, bounded by the job's context deadline. Retried, never hangs. |
| Hash mismatch on a staged file | Evict, re-download once, then fail permanently. |
| `Commit` fails after upload | Orphaned object; handled by the orphan pass. |
| Worker killed mid-job | Leases are in-memory, so process death releases them. The stale-job reaper (`worker/pool.go:562`) handles the job. |
| `Register` on a nonexistent object | `Stat` fails; error returned synchronously to the caller. |

---

## 10. Testing

- **`artifacttest`** — in-memory `Backend` plus a fake clock, mirroring Trove's
  `trovetest`. Everything below builds on it.
- **Store conformance suite** — one shared table-driven suite run against all five
  backends, following the existing `store_test.go` and testcontainers setup already used
  for Postgres, Mongo, and Redis.
- **Cache** — single-flight proven with N goroutines against a download-counting backend
  asserting exactly one fetch; eviction under budget; leases blocking eviction; index
  corruption recovering by re-download; hash mismatch handling.
- **GC invariant test** — property-style over arbitrary sequences of register, create,
  commit, fail, retry, and sweep, asserting that no durable artifact is ever deleted and
  no artifact with a live non-terminal owner is ever swept. Table-driven tests cover the
  known eligibility cases; the property test covers the unknown ones.
- **Integration** — a full job staging a generated multi-hundred-megabyte file through
  the memory backend, asserting artifact rows, links, attempt numbering, and `IfAbsent`
  resumption end to end. CI generates the bytes rather than storing them.
- **Benchmarks** — staging throughput and the cache-hit path, in the existing `bench`
  style.

---

## 11. Backward compatibility

The artifact plane is entirely opt-in. With no backend resolved,
`resolveArtifactBackend` returns `nil` and Dispatch behaves exactly as it does today.
Definitions without `artifact.Input` declarations never invoke the staging middleware.
The two new tables are additive; no existing table or column changes.

---

## 12. Suggested phasing

The design is one coherent feature but large enough to land incrementally. Each phase is
independently useful and independently testable:

1. **Entity and stores** — `artifact` leaf package, `artifact.Store` in the composite,
   migrations and implementations across all five backends, `artifacttest`, conformance
   suite. No execution changes.
2. **Backend and Trove adapter** — `Backend` interface, `artifact/trove`, `Register`,
   capability interfaces. Artifacts can be registered and read; nothing stages yet.
3. **Cache** — `artifact/cache` with single-flight, leases, budget, eviction, crash
   recovery. Standalone and heavily unit-tested before anything depends on it.
4. **Staging middleware and handler API** — `artifact/staging`, `artifact.Input`
   declarations, `From`/`Path`/`Open`/`Create`/`Commit`, attempt scoping, `IfAbsent`.
   This is the phase that changes job execution.
5. **Extension wiring** — DI resolution, YAML config, dashboard surfacing of artifacts
   and lineage.
6. **Sweeper** — two-phase deletion, orphan pass, leader-only scheduling, metrics,
   dry-run, kill switch. Last, because it is the only destructive component and should
   run against a system already producing real artifacts.

Workflow-step integration (refs in checkpoints) can follow phase 4 or ship with it.
