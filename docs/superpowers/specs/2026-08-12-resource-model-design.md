# Resource Model and Resource-Aware Scheduling — Design

**Date:** 2026-08-12
**Status:** Approved for planning
**Scope:** Sub-project B of the Dispatch heavy-workload track
**Depends on:** [Artifact plane](2026-08-11-artifact-plane-design.md) (track A) — input-size signal, disk budget

---

## 1. Problem

Dispatch has no concept of CPU, memory, disk, or GPU. Concurrency is `N` identical
worker slots (`worker/pool.go:99`), narrowed by per-queue max-concurrency and a
token-bucket rate limit (`queue/queue.go:17`) and by per-tenant limits
(`queue/tenant.go:11`). Every job costs exactly one slot whether it sends an email or
tessellates a 4 GB building model.

TwinOS runs both, and their footprints differ by four orders of magnitude. With identical
slots there are only two ways to size the pool, and both are wrong:

- **Size for the heavy jobs.** Concurrency drops to two or three, and the box sits idle
  whenever the queue is notifications.
- **Size for the light jobs.** Concurrency is thirty, two tessellations land on the same
  worker, and the OOM killer takes down twenty-eight unrelated jobs with them.

The second failure is the expensive one. A slot model cannot express "these two jobs must
not be co-resident" because it has no vocabulary for why. This document gives Dispatch
that vocabulary, and a scheduler that uses it.

### Position in the larger track

| | Sub-project | Depends on |
|---|---|---|
| A | Artifact plane | — |
| **B** | **Resource model and resource-aware scheduling** (this document) | A (input-size signal) |
| C | Execution isolation (sandbox, pod-per-job) | A (staging boundary), B (resource requests) |
| D | Long-run durability (progress checkpoints, resume) | independent |
| E | Resource prediction | B (measurement data) |

### Non-goals

**Track E is explicitly out of scope.** This document defines the `Estimator` interface a
predictor implements and the measurement schema it trains on, and it ships a non-ML
default estimator (§6). It does not design a model. A p95 quantile per
`(job_name, input_bucket)` captures most of the achievable accuracy, and a model is worth
revisiting only after months of real measurement data exist.

Also out of scope: sandboxing and pod construction (track C — this document defines only
the contract C consumes, §9), and job-level progress checkpointing (track D).

### Constraints

Dispatch is a library. Users choose their deployment. No hard Kubernetes dependency may
enter the core, and every mechanism here degrades to single-process operation with no
configuration (§12).

---

## 2. Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Quantity model | `map[string]int64` in canonical units | The core operations are `Add`/`Sub`/`Fits`/`Max`. A map makes each one loop; a typed struct plus a custom map makes each one two code paths and two storage representations. |
| Resolution time | At enqueue, written to the job row | Scheduling reads columns. It never calls user code, so the dequeue predicate stays a numeric comparison expressible in all five backends. |
| CPU vs memory | Same arithmetic, different overcommit policy | Overrunning CPU makes a job slow. Overrunning memory makes it dead. That asymmetry belongs in capacity config, not in a second mechanism. |
| Admission | One `resource.Manager`, generalizing `artifact/cache/budget.go` | Track A already proved the shape. Memory and CPU get the same cond-var-and-context-bounded-wait, with per-key `Reclaimer` for the one dimension that can be reclaimed. |
| Dequeue | Widen `job.Store.DequeueJobs` to take `DequeueOpts` | `DequeueJobs` claims atomically, so a worker cannot inspect requirements before owning a job. The fit predicate must live in the query or heavy jobs thrash. |
| Custom resources | Key-set matched at dequeue, quantity enforced locally | Exact quantity matching needs a document comparison or a join table in five backends, to serve a rare case. Key containment is portable and catches the case that matters: "this worker has no GPU at all". |
| Starvation | Reservation with backfill bounded by `job.Timeout` | `Timeout` is enforced, so it is an upper bound rather than a guess. Backfill is sound today and does not wait on track E. |
| Measurement | One row per terminal run plus a bounded rollup | Raw rows are the training set; the rollup is the estimator. Cardinality is `job_name × ~40 buckets`, fixed. |
| Locality | In this track, last phase, advisory | The dequeue query is being redesigned here. Deferring means editing every backend's dequeue twice. |

---

## 3. Package layout

`resource` must be a leaf package, for the same reason `artifact` is one. `job.Options`
will carry the resolved spec, so `job` imports `resource`; therefore `resource` may depend
only on `id` and the root `dispatch` package — never on `job`, and never on `artifact`.

```
resource/            leaf: Set, keys, Spec, Request, InputSize, Estimator,
                           Usage, Sampler, Manager, Lease, Reclaimer, Store
resource/cgroup/     cgroup v2 sampler (linux build tag)
worker/admission.go  the scheduler: capacity, reservation, backfill
```

The `resource` → `artifact` prohibition is load-bearing rather than cosmetic. It forces
the estimator's input to be plain data:

```go
type InputSize struct {
    Name  string   // declared slot name
    Bytes int64
    Hash  string   // may be empty; track A fills content_hash opportunistically
}
```

`engine` translates `artifact.Ref` bindings into `[]InputSize` at enqueue. The consequence
is that the estimator — the component track E replaces — is testable with a struct
literal and no storage backend at all.

`resource.Store` joins the composite `store.Store` (`store/store.go:34`) alongside
`job.Store`, `artifact.Store`, `workflow.Store`, `cron.Store`, `dlq.Store`, `event.Store`,
and `cluster.Store`, implemented by all five backends. The scheduler lives in
`worker/admission.go` rather than in `resource` because reservation logic needs `*job.Job`,
and rather than in `worker/pool.go` because that file is already 630 lines.

---

## 4. The resource model

```go
package resource

const (
    CPU    = "cpu"     // millicores:      1 core  = 1000
    Memory = "memory"  // bytes
    Disk   = "disk"    // bytes
    GPU    = "gpu"     // milli-devices:   1 device = 1000
)

// Set is a resource vector. Absent keys are zero.
type Set map[string]int64

func CPUs(n float64) Set          // CPUs(2.5)      → {"cpu": 2500}
func MemoryBytes(n int64) Set
func MemoryGB(n int64) Set
func DiskBytes(n int64) Set
func GPUs(n float64) Set
func Custom(key string, n int64) Set

func (s Set) Add(o Set) Set
func (s Set) Sub(o Set) Set
func (s Set) Max(o Set) Set
func (s Set) Scale(f float64) Set
func (s Set) Fits(capacity Set) bool   // ∀k: s[k] ≤ capacity[k]
func (s Set) Keys() []string           // sorted; the custom-key set for dequeue
func (s Set) IsZero() bool
```

**`int64`, not `float64`.** Budget accounting adds and subtracts the same quantities
thousands of times over a worker's lifetime. Integers do not drift. Millicores give three
decimal places, which is more precision than any real declaration needs, and map directly
onto Kubernetes' `resource.NewMilliQuantity`.

**Milli-devices for GPU** so fractional-GPU declarations are expressible in the same way
Ray expresses them. Kubernetes accepts only whole devices, so track C rounds up at
translation and the spec says so out loud (§9).

**Custom resources** are any other key: `"license"`, `"fpga"`, `"nvme-scratch"`. Integer
units with user-defined semantics, exactly Ray's resource dict. They participate fully in
local admission and partially in dequeue filtering (§7).

### CPU is compressible, memory is not

Both use the same arithmetic. They differ in how worker capacity is derived:

```yaml
capacity:
  cpu_overcommit: 1.0     # configurable; 2.0 means 8 cores advertise 16000 millicores
  memory_fraction: 0.8    # of detected limit; the remainder is runtime + OS headroom
```

There is no `memory_overcommit`. Overcommitting memory is how you get the OOM cascade this
track exists to prevent, and a knob that only ever causes incidents should not exist.

### Capacity detection

Autodetected by default, overridable per key:

| Key | Detection |
|---|---|
| `cpu` | cgroup v2 `cpu.max` quota when present, else `runtime.NumCPU()`, × `cpu_overcommit` × 1000 |
| `memory` | cgroup v2 `memory.max` when present, else `MemTotal`, × `memory_fraction` |
| `disk` | the artifact cache budget (§7 of track A) |
| `gpu` | zero unless declared |
| custom | always explicit |

Reading the cgroup limit before falling back to `runtime.NumCPU()` matters: in a container
with a 2-core quota, `NumCPU()` reports the host's 64 and every capacity derived from it
is wrong by a factor of 32.

---

## 5. Declaration

```go
var Tessellate = job.NewDefinition("tessellate.model", handler,
    artifact.Input("model", artifact.Required, artifact.MaxSize(8<<30)),
    job.WithResources(resource.CPUs(4), resource.MemoryGB(16)),
    job.WithTimeout(6*time.Hour),
)
```

Static declaration is the floor. It is not enough on its own: a 4 GB model and a 40 MB
model are the same job definition and need wildly different memory. So requirements may
also be a function of the input.

```go
job.WithResourceFunc(func(ctx context.Context, r resource.Request) (resource.Set, error) {
    // Tessellation peaks at roughly 3× the source geometry, floored at 2 GB.
    return resource.MemoryBytes(max(2<<30, r.InputBytes*3)).
        Add(resource.CPUs(4)), nil
})
```

```go
type Request struct {
    JobName    string
    Queue      string
    Payload    []byte
    Inputs     []InputSize
    InputBytes int64   // sum over Inputs
    Declared   Set     // the definition's static declaration, if any
    Attempt    int
    ScopeOrgID string
}
```

`InputBytes` is available at enqueue because track A validates artifact bindings there and
the artifact row already carries `size`. That is the track A seam paying off: the engine
knows a job's input is 4 GB before it is ever scheduled.

### Resolution happens once, at enqueue, and is written to the row

This is the most consequential decision in this document. `engine.Enqueue` resolves the
requirement to a concrete `Set` and persists it. The scheduler then reads columns.

The alternative — evaluating a user function at dequeue time — would put arbitrary user
code inside the scheduling hot path, make the dequeue predicate inexpressible in SQL, and
give a job different requirements on different workers. Resolving once at enqueue avoids
all three, and the cost is that a requirement cannot depend on anything discovered later.
The escape hatch for that case is `Lease.Extend` (§6) and retry escalation (below).

**Resolution is a per-key merge, explicit beating inferred:**

```
global default  →  queue default  →  static declaration  →  estimator  →  enqueue override
```

Per-key rather than first-non-empty-wins, so an estimator that predicts only memory leaves
a declared CPU value intact. The estimator sits above the static declaration but receives
`Declared` in the `Request` and may return it unchanged; installing an estimator is an
explicit opt-in to letting it override. The per-call override is last:

```go
engine.Enqueue(ctx, eng, Tessellate, in,
    artifact.Bind("model", ref),
    job.WithResources(resource.MemoryGB(48)),   // this caller knows better
)
```

**Requests and limits.** The declaration produces `Requests`. `Limits` default to
`Requests` for memory and the incompressible keys, and are left unset for CPU — the
guaranteed-memory, burstable-CPU shape, which is the correct default for the compressible
split in §4. Both are overridable via `job.WithResourceLimits(...)`.

### Retry escalation

A job that OOMs at 16 GB must not retry three times at 16 GB. When a failure is classified
as resource-related, the retry re-resolves with the memory request scaled by
`oom_backoff_factor` (default 1.5), capped at the largest known worker capacity, and
increments `resource_escalations` on the row. Classification is deliberately narrow:
`ErrOOMKilled` reported by a track C sampler, or a cgroup `memory.events` `oom_kill` delta.
An in-process Go OOM takes the whole worker down and is handled by the stale-job reaper,
not here.

---

## 6. Admission

### The manager generalizes track A's budget

`artifact/cache/budget.go:28` is a single-key budget: a mutex and cond var, an evictor
callback, a context-bounded wait, and `Acquire`/`Release`/`Adjust`. That is exactly the
right structure. `resource.Manager` is the same structure widened to N keys.

```go
type Manager interface {
    // Acquire blocks until want fits, reclaiming where a Reclaimer is
    // registered. Bounded by ctx — a blocked job cannot outlive its deadline.
    Acquire(ctx context.Context, owner string, want Set) (Lease, error)
    TryAcquire(owner string, want Set) (Lease, bool)

    Free() Set          // immediately available
    Reclaimable() Set   // what a Reclaimer could free
    Capacity() Set
    Leases() []LeaseInfo

    RegisterReclaimer(key string, r Reclaimer)
}

type Lease interface {
    Held() Set
    Extend(ctx context.Context, extra Set) error   // advanced; see below
    Release()
}

// Reclaimer frees capacity for one key on the manager's behalf.
type Reclaimer interface {
    Reclaim(ctx context.Context, key string, need int64) (freed int64, err error)
    Available(key string) int64
}
```

`ErrCapacityExceeded` mirrors `cache.ErrBudgetExceeded` and carries the same two cases: a
request larger than total capacity fails immediately rather than blocking on something no
eviction can satisfy, and a request that merely does not fit yet fails when the caller's
context ends.

### The cache becomes the `disk` reclaimer

`artifact/cache` registers itself as the `Reclaimer` for `disk`. Its LRU eviction of
unleased entries is a disk-specific *reclaim policy*, not a competing budget system —
memory has no reclaimer, so blocking is its only option, and that difference is the whole
reason the hook exists.

Concretely, `cache.budget` becomes a `disk`-scoped view of the shared manager. When no
manager is injected the cache constructs a private single-key one, so a Dispatch instance
with artifacts but no resource configuration behaves exactly as it does today.

This distinction matters at the dequeue boundary: **the disk ceiling is
`Free()+Reclaimable()`, the memory ceiling is `Free()` alone.** Cached-but-unleased bytes
are available to a new job; leased memory is not.

### Slots stay

The `slots` channel (`worker/pool.go:99`) is not replaced. It remains a valid cap on
goroutines, store connections, and heartbeat traffic. A job needs a slot **and** a lease;
whichever binds first, binds. With 32 slots and memory for two tessellations, a worker
holds two leases and 30 idle slots, and its next dequeue asks only for jobs that fit in the
remaining memory. The two limits compose with no special-casing.

### Handler-facing API

```go
resource.Report(ctx, resource.MemoryBytes(n))  // measurement only; never blocks
lease := resource.LeaseFrom(ctx)
err := lease.Extend(ctx, resource.MemoryGB(8)) // accounting; may block
```

`Report` is the primary API and is the highest-value measurement source outside a sandbox
(§8): a tessellator knows exactly how large the buffer it just allocated is, and no
sampler can infer that from a shared Go heap.

`Extend` is an escape hatch with a documented hazard: a handler holding a lease and
blocking for more can deadlock against another doing the same. It is context-bounded so
the deadlock resolves at the job deadline rather than never, and the documentation says
plainly that the correct pattern is to declare the peak up front.

---

## 7. Scheduling

### The dequeue contract

```go
type DequeueOpts struct {
    Queues []string
    Limit  int

    // Budget is the per-key ceiling. A job is eligible only if every
    // requirement fits. Absent keys are unconstrained, so a store called
    // with a zero Budget behaves exactly as DequeueJobs does today.
    Budget resource.Set

    // CustomKeys are the custom resource keys this worker has at all.
    // Eligibility requires req_custom_keys ⊆ CustomKeys.
    CustomKeys []string

    // PreferHashes is advisory: matching jobs sort first. Never a filter.
    PreferHashes []string

    // ReservedFor, when set, restricts the result to that job. Used by a
    // worker holding a reservation.
    ReservedFor *id.JobID
}

DequeueJobs(ctx context.Context, opts DequeueOpts) ([]*job.Job, error)
```

Widening the signature is a breaking change to `job.Store`, implemented across all five
backends. It is the right one: `DequeueJobs` claims and marks running atomically, so a
worker cannot inspect requirements before owning a job. Claim-then-requeue would leave a
32 GB job bouncing between small workers, burning a dequeue write each time and delaying
precisely the job that is already hardest to place.

### Schema

`dispatch_jobs` gains:

```sql
req_cpu_milli        BIGINT NOT NULL DEFAULT 0,
req_memory_bytes     BIGINT NOT NULL DEFAULT 0,
req_disk_bytes       BIGINT NOT NULL DEFAULT 0,
req_gpu_milli        BIGINT NOT NULL DEFAULT 0,
req_custom_keys      TEXT,          -- sorted, comma-delimited; empty for most jobs
resource_requests    JSONB,         -- full fidelity, including custom quantities
resource_limits      JSONB,
resource_escalations INT NOT NULL DEFAULT 0,
input_bytes          BIGINT NOT NULL DEFAULT 0,
primary_input_hash   TEXT,
reserved_by          TEXT,
reserved_until       TIMESTAMPTZ,
unschedulable_since  TIMESTAMPTZ
```

```sql
CREATE INDEX idx_dispatch_jobs_dequeue_res
  ON dispatch_jobs (queue, priority DESC, run_at ASC)
  INCLUDE (req_cpu_milli, req_memory_bytes, req_disk_bytes, req_gpu_milli)
  WHERE state IN ('pending', 'retrying');
```

Four scalar columns *and* a JSON column is deliberate duplication. The scalars are what
the predicate compares and must be indexable and portable; JSON comparison semantics differ
across Postgres, SQLite, Mongo, and Redis, and a scheduler that behaves differently per
backend is not a scheduler. The JSON column carries custom quantities, which the predicate
does not compare.

Every column defaults to zero, so **every row written before this migration remains
dequeueable by every worker**. That is what makes the change safe to deploy against a live
queue.

`cluster.Worker` gains typed `Capacity` and `Available` fields next to the existing
`Concurrency int`, published on heartbeat. `Metadata` (`cluster/worker.go:33`) stays free
for locality hashes.

### Custom resources: keys at dequeue, quantities locally

Eligibility tests `req_custom_keys ⊆ CustomKeys` — a set-containment check each backend
expresses natively (Postgres array overlap, SQLite/Bun `LIKE` over the delimited string,
Mongo `$nin`, Redis set intersection, memory trivially). The *quantity* is enforced by
`Manager.TryAcquire` after the claim; if two jobs each want the worker's one FPGA, the
second requeues with backoff.

This accepts occasional requeue churn for custom resources in exchange for not building
document-comparison predicates in five backends. It is the right trade because the case it
handles badly — many jobs contending for a scarce custom resource on one worker — is rare,
while the case it handles exactly — a worker that lacks the key entirely — is the common
one.

### Starvation: reservation with sound backfill

A job pending longer than `reservation_threshold` (default 60s) becomes *reserving*. A
worker attempts to claim it only when both conditions hold: the job **does not fit its free
capacity now** — otherwise an ordinary dequeue would already have taken it, and reserving
would be pure loss — and it **fits the worker's total capacity**, so draining can eventually
satisfy it. The claim itself:

```sql
UPDATE dispatch_jobs
   SET reserved_by = $worker, reserved_until = now() + $ttl
 WHERE id = $job
   AND state IN ('pending','retrying')
   AND (reserved_by IS NULL OR reserved_until < now())
```

First writer wins; other workers move on. No leader is required, and `reserved_until`
expiry releases a crashed or wedged holder. One reservation per worker.

The holder then computes the **satisfiability time** `T` exactly: sort its in-flight leases
by deadline (`started_at + timeout`), accumulate the resources each release would free, and
take the earliest point at which the reserved job fits. It admits a backfill candidate
if and only if:

```
now + candidate.Timeout ≤ T
```

**This is sound without any prediction.** `job.Timeout` is enforced by the executor, so it
is a hard upper bound on when a job releases its resources, not an estimate. This is the
same principle Slurm's backfill scheduler rests on — it uses the job's declared walltime
limit, not a predicted runtime — and Dispatch already has the field.

The default shape fits TwinOS directly: notification jobs at the five-minute default
timeout backfill freely against a six-hour tessellation drain, so the reserving worker
stays busy while it waits. Track E can later substitute p95 durations to backfill more
aggressively, but that would be an optimization layered on a correct algorithm, never a
correctness dependency.

If `T` cannot be computed — an in-flight job with no timeout — the worker falls back to
strict drain: no backfill until the reservation is satisfied.

**`reserved_until` is a liveness lease, not a deadline for the work.** The holder renews it
on the existing worker heartbeat cadence for as long as it is draining, so a reservation
behind a six-hour tessellation survives the six hours; it expires only when the holder stops
heartbeating, which means the holder crashed. A fixed TTL would be the bug this section
exists to prevent — releasing the reservation just before it becomes satisfiable is exactly
how a large job starves. Renewal stops, and the reservation is released, if the holder's own
`T` recedes past `reservation_max_hold` (default 24h), which catches the pathological case
of a drain that never converges. `dispatch_reservations_active` and the reservations
endpoint (§10) make an active hold visible while it is happening rather than after.

### Unschedulable jobs

A job whose requirements exceed the largest known worker capacity will never run. Following
track A's treatment of a definition whose declared `MaxSize` exceeds the cache budget, it is
**rejected at enqueue** and the error is returned to the caller synchronously — so it fails
on a developer's machine rather than accumulating silently in production.

The fleet can also shrink after enqueue. For that case, a leader sweep stamps
`unschedulable_since` on jobs no registered worker can fit, exposes them via the API (§10),
and sends them to the DLQ after `unschedulable_timeout` (default 1h) with a message naming
the dimension that does not fit. Silently pending forever is the one outcome this must not
produce.

### Locality

Ships in this track, in the last phase, advisory and off by default.

Workers advertise the content hashes they hold in `cluster.Worker.Metadata`; the fetch loop
passes them as `PreferHashes`, and the dequeue adds one `ORDER BY` term ahead of priority's
tiebreak:

```sql
ORDER BY (primary_input_hash = ANY($prefer)) DESC, priority DESC, run_at ASC
```

It belongs here because the dequeue query is already being redesigned; deferring it means
editing five backends' dequeue twice. It stays advisory — a preference, never a filter — so
it can never itself cause starvation.

The honest limitation: track A fills `content_hash` opportunistically during first staging,
so it is usually `NULL` at enqueue and locality does nothing on an artifact's first use. It
helps from the second use onward — which is exactly the motivating case, re-tessellating one
building at five detail levels pulling 2 GB from S3 once instead of five times.

---

## 8. Measurement

Measurement exists to check estimates against reality. It is the training data for track E,
and before track E exists it drives the default estimator (§6 below) and the
over-provisioning view that pays for this whole track (§10).

### Sampling

```go
type Sampler interface {
    // Start captures a baseline. Stop returns usage for the interval.
    Start(ctx context.Context, jobID id.JobID) (Session, error)
}
type Session interface {
    Sample(ctx context.Context) (Usage, error)  // live, for the dashboard
    Stop(ctx context.Context) (Usage, error)
}
```

| Implementation | Source | Accuracy |
|---|---|---|
| `resource/cgroup` | cgroup v2 `memory.peak`, `cpu.stat`, `memory.events` | exact when the job owns the cgroup — track C's pod-per-job case |
| in-process | `runtime.ReadMemStats` delta, sole-tenant only | heuristic |
| handler reports | `resource.Report` | exact for what the handler measured |
| cache | bytes leased for staging | exact, free — track A already accounts them |

cgroup v2 needs no polling for the numbers that matter: `memory.peak` is a high-water mark
read once at the end, and `cpu.stat` is cumulative, read at start and end. Polling
(default 10s) exists only for the live dashboard and for the in-process fallback.

Per-job memory attribution inside a single Go process is not solvable — one heap, no
per-goroutine accounting. This document does not pretend otherwise. It records how a number
was obtained and lets the consumer decide whether to trust it.

### Schema

```sql
CREATE TABLE dispatch_resource_usage (
  id                  TEXT PRIMARY KEY,          -- rusage_01h...
  job_id              TEXT NOT NULL,
  job_name            TEXT NOT NULL,             -- denormalized, see below
  queue               TEXT NOT NULL,
  attempt             INT  NOT NULL DEFAULT 0,
  input_bytes         BIGINT NOT NULL DEFAULT 0,
  input_bucket        INT  NOT NULL,             -- 0 = no inputs; else floor(log2(bytes))+1
  requested           JSONB NOT NULL,
  limits              JSONB,
  peak_memory_bytes   BIGINT,
  cpu_seconds         DOUBLE PRECISION,
  max_disk_bytes      BIGINT,
  gpu_seconds         DOUBLE PRECISION,
  wall_seconds        DOUBLE PRECISION NOT NULL,
  outcome             TEXT NOT NULL,   -- 'completed'|'failed'|'oom'|'timeout'|'cancelled'
  quality             TEXT NOT NULL,   -- 'exact'|'reported'|'attributed'|'estimated'
  censored            BOOLEAN NOT NULL DEFAULT FALSE,
  worker_id           TEXT,
  scope_org_id        TEXT,
  created_at          TIMESTAMPTZ NOT NULL
);

CREATE INDEX ON dispatch_resource_usage (job_name, input_bucket, created_at DESC);
CREATE INDEX ON dispatch_resource_usage (created_at);
```

One row per terminal run, written once at completion. Not a time series — per-sample rows
would be three orders of magnitude larger and answer no question the summary does not.

**`job_name` and `input_bytes` are denormalized deliberately.** The rollup query must not
join `dispatch_jobs`, because jobs get pruned and archived on a schedule that has nothing to
do with how long a predictor wants its training data.

**`quality` is not decoration.** It is what stops the estimator training on garbage. The
rollup consumes `exact` and `reported` by default; `attributed` and `estimated` are recorded
for debugging and excluded from the aggregate unless `trust_attributed` is set.

**`censored` marks a lower bound.** An OOM-killed run's peak RSS says only "at least this
much". The observation used for such a run is its *limit*, the bucket is flagged
under-provisioned, and the flag is surfaced (§10) rather than silently averaged in.

### Bounding the table

Two mechanisms, and only the first is a delete:

1. **Raw retention.** `resource_usage_retention` (default 14d), swept by the leader in the
   same batched, rate-limited, kill-switched pass as the artifact sweeper (§8 of track A).
2. **Rollup.** `dispatch_resource_stats`, keyed `(job_name, input_bucket)`, holding
   `count`, `p50`/`p95`/`max` per dimension, `p95_wall_seconds`, `oom_count`, and
   `updated_at`. Cardinality is `job_name × ~40 log2 buckets` — bounded and small.

The leader recomputes the rollup from the raw window every `rollup_interval` (default 15m)
with a plain `GROUP BY`, EWMA-blending into the previous values so history survives raw rows
aging out. No streaming sketch and no new dependency: the raw window is always small enough
to aggregate directly, and the blend is what carries knowledge past the window.

Power-of-two bucketing on `input_bytes` gives roughly 40 buckets across the full range from
kilobytes to terabytes, which is fine granularity where the interesting variation is and
coarse granularity where it is not. Bucket 0 is reserved for jobs with no declared inputs so
that a no-input job and a one-byte input never share a bucket; every other bucket is
`floor(log2(input_bytes)) + 1`.

### The default estimator ships in this track

```go
type Estimator interface {
    Estimate(ctx context.Context, r Request) (Set, error)
}
```

`resource.RollupEstimator` reads `dispatch_resource_stats` for `(job_name, input_bucket)`
and returns `p95 × safety_factor` (default 1.2) when `count ≥ min_samples` (default 20),
otherwise returns `r.Declared` unchanged. Output is clamped to
`[declared_floor, max_known_worker_capacity]`, so an estimator can never produce a job that
§7 would then have to reject as unschedulable.

This is the p95-per-`(job_name, input_bucket)` that captures most of the achievable
accuracy, built from a `GROUP BY`. It is also the seam track E slots into: same one-method
interface, a better implementation behind it, and nothing else in the system moves.

---

## 9. The track C contract

The contract is bidirectional, and stating both directions is the clearest way to show the
tracks compose.

**B → C: the spec.**

```go
type Spec struct {
    Requests Set
    Limits   Set
    Class    string   // optional; C maps to priorityClass / nodeSelector / runtimeClass
}

func SpecFrom(ctx context.Context) (Spec, bool)
```

Resolved, immutable, attached to the job at enqueue and readable from the execution context.
Core guarantees canonical units so translation is mechanical:

| Key | Kubernetes |
|---|---|
| `cpu` (millicores) | `resource.NewMilliQuantity(v, DecimalSI)` |
| `memory` (bytes) | `resource.NewQuantity(v, BinarySI)` |
| `disk` (bytes) | `ephemeral-storage` |
| `gpu` (milli-devices) | `nvidia.com/gpu`, **rounded up to whole devices** |
| custom | extended-resource name via a C-side mapping table |

The `corev1` import lives in track C. Nothing in core knows Kubernetes exists, which is the
constraint that makes single-process operation the default rather than a degraded mode.

**C → B: the sampler.** Track C supplies the `resource.Sampler` implementation. Pod-per-job
is precisely what makes `quality = 'exact'` achievable, and it is also what lets an OOM be
attributed to the job that caused it instead of taking the worker down. The loop closes: C
sizes the pod from B's spec, and the pod's cgroup produces the measurement that makes the
next spec better.

---

## 10. API and dashboard

| Endpoint | Purpose |
|---|---|
| `GET /resources/capacity` | Per-worker capacity, free, reclaimable, and active leases; plus a summed cluster view |
| `GET /jobs/{id}/usage` | Requested vs. actual vs. quality for each attempt |
| `GET /resources/stats?job=&bucket=` | The rollup: p50/p95/max per dimension, sample count, OOM count |
| `GET /jobs?unschedulable=true` | Jobs stamped `unschedulable_since`, with the offending dimension |
| `GET /resources/reservations` | Active reservations, holder, satisfiability time, backfill admitted |

Handlers follow the existing `api/stats_handler.go` shape, reading through the composite
store.

**The dashboard view that justifies the track is estimate error**: `requested / actual` per
`(job_name, input_bucket)`, sorted descending, with sample count and quality mix. "This job
asks for 24 GB and has never exceeded 4 GB across 340 runs" is the sentence that turns
measurement into reclaimed capacity, and it is available the moment measurement lands —
before any estimator or predictor exists.

Metrics, through the existing `observability` package:

```
dispatch_resource_capacity{key}
dispatch_resource_free{key}
dispatch_resource_leased{key}
dispatch_admission_wait_seconds          histogram
dispatch_reservations_active
dispatch_backfill_admitted_total
dispatch_jobs_unschedulable
dispatch_resource_estimate_error_ratio{job_name}
dispatch_resource_oom_total{job_name}
```

Lifecycle events go through the existing extension registry, so `audit_hook` and
`relay_hook` observe them with no new plumbing.

---

## 11. Error handling

Mirroring track A's table and `isTransientStoreErr` (`worker/pool.go:24`):

| Failure | Handling |
|---|---|
| Requirements exceed largest known worker capacity | Rejected at enqueue, returned to the caller. Never becomes a pending job. |
| Fleet shrank; job now unschedulable | `unschedulable_since` stamped by the leader sweep; DLQ after `unschedulable_timeout`. |
| `Acquire` cannot fit within the job's deadline | `ErrCapacityExceeded`. Job requeued with backoff, never hangs. |
| Custom-resource quantity does not fit after claim | Requeue with backoff. Bounded by `MaxRetries` like any other failure. |
| Job OOM-killed (cgroup-detected) | Usage row with `outcome='oom'`, `censored=true`; retry re-resolves with `oom_backoff_factor`. |
| Worker killed mid-job | Leases are in-memory, so process death releases them. The stale-job reaper (`worker/pool.go:546`) handles the job. |
| Reservation holder crashes | `reserved_until` expires; another worker may reserve. |
| Reservation cannot be satisfied within `reservation_ttl` | Released; the job re-reserves later, possibly elsewhere. Logged and counted. |
| Sampler unavailable or fails | Usage row written with `quality='estimated'` and null measurements. Never fails the job. |
| Estimator returns an error | Logged; falls back to the static declaration. An estimator must never block enqueue. |
| Rollup query fails | Previous rollup values are retained. The estimator degrades to declarations. |

The consistent principle: **no resource mechanism may ever fail a job that would otherwise
have succeeded.** Measurement is best-effort, estimation falls back, and admission failures
requeue.

---

## 12. Backward compatibility and degradation

With no resource configuration, capacity is autodetected, no definition declares anything,
every requirement column is zero, `DequeueOpts.Budget` is empty, and the predicate matches
everything. Behaviour is identical to today.

Each layer is independently switchable:

- Declaration without measurement — admission works, estimates are never checked.
- Measurement without declaration — usage is recorded for jobs costing zero, which is
  exactly how you gather the data needed to write the first declaration.
- Both without reservation — starvation is possible, everything else works.
- All of it in a single process — no cluster, no leader, no Kubernetes. The manager is a
  mutex and a cond var.

The schema changes are additive with zero defaults, so existing rows remain dequeueable by
every worker during a rolling deploy. The one breaking change is the `job.Store` interface
(§7), which affects in-tree backends and any third-party implementation; it is called out in
the changelog rather than softened with a shim, because a store that silently ignores the
budget would produce exactly the OOM cascade this track exists to prevent.

---

## 13. Testing

- **`resourcetest`** — fake `Sampler`, fake clock, in-memory `Manager`, mirroring
  `artifacttest`.
- **`Set` arithmetic** — table-driven over `Add`/`Sub`/`Max`/`Scale`/`Fits`, including
  absent keys, negative results clamped, and custom keys.
- **Resolution precedence** — table-driven over every combination of global, queue,
  declaration, estimator, and override, asserting per-key merge rather than
  whole-set replacement.
- **`Manager` invariant, property-style** — N goroutines acquiring and releasing random
  sets against random capacity; assert leased never exceeds capacity, no goroutine blocks
  past its context, and released capacity is always reusable.
- **Reclaimer** — assert `disk` acquisition triggers cache eviction and that memory
  acquisition never calls a reclaimer.
- **Starvation** — the named test for this track: a stream of small jobs plus one job
  requiring most of capacity; assert the large job starts within a bounded time, and that
  backfilled jobs never delay it past `T`.
- **Backfill soundness** — table-driven over lease deadline sets and candidate timeouts,
  asserting a candidate is admitted only when `now + Timeout ≤ T`.
- **Dequeue conformance** — one shared table-driven suite over `DequeueOpts` run against
  all five backends via the existing testcontainers setup: budget filtering per key,
  custom-key containment, `PreferHashes` ordering, `ReservedFor`, and zero-budget
  equivalence with today's behaviour.
- **cgroup sampler** — against a fixture directory tree of `memory.peak` / `cpu.stat` /
  `memory.events` files, not a live cgroup, so it runs in CI on any platform.
- **Rollup** — quantile correctness against a known distribution; EWMA blending across a
  window boundary; `quality` filtering; censored-observation handling.
- **Integration** — a worker with a small fixed capacity and a mixed job stream, asserting
  no admission ever exceeds capacity, usage rows are written with the expected quality, and
  the rollup converges on the true p95.
- **Benchmarks** — `Set` arithmetic and the `TryAcquire` hot path, in the existing `bench`
  style.

---

## 14. Suggested phasing

Each phase is independently useful and independently testable.

1. **`resource` leaf package** — `Set`, keys, arithmetic, `Manager`, `Lease`, `Reclaimer`,
   capacity detection, `resourcetest`. Standalone; nothing depends on it yet.
2. **Cache integration** — `artifact/cache` registers as the `disk` reclaimer; `budget`
   becomes a disk-scoped view. Behaviour-preserving refactor with the existing cache tests
   as the guard.
3. **Declaration and resolution** — `job.WithResources`, `WithResourceFunc`,
   `WithResourceLimits`, enqueue-time resolution, job-row columns and migrations across all
   five backends. Requirements are recorded but nothing schedules on them.
4. **`DequeueOpts` and local admission** — the store contract, the conformance suite, the
   fetcher passing its budget, leases held across execution. **This is the phase that
   changes scheduling.**
5. **Measurement** — `Sampler`, `resource/cgroup`, `resource.Report`, the usage table and
   store, retention sweep.
6. **Rollup and estimation** — `dispatch_resource_stats`, leader recompute,
   `RollupEstimator`, retry escalation.
7. **Reservation and backfill** — `worker/admission.go`, reservation columns, satisfiability
   computation, unschedulable detection and sweep.
8. **Locality** — hash advertisement in worker metadata, `PreferHashes` in every backend's
   dequeue. Off by default.
9. **Surface** — API handlers, dashboard views (capacity, usage, estimate error), metrics,
   extension events.

Phases 1–4 deliver the capability that stops the OOM cascade. Phases 5–6 make it accurate.
Phase 7 makes it fair. Phases 8–9 make it fast and legible.
