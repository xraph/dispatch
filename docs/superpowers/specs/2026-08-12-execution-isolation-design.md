# Execution Isolation — Design

**Date:** 2026-08-12
**Status:** Approved for planning
**Scope:** Sub-project C of the Dispatch heavy-workload track
**Depends on:** A (artifact plane, staging boundary), B (resource requests)

---

## 1. Problem

A Dispatch handler is an ordinary Go function invoked in-process through a middleware
chain (`worker/executor.go:66`). It runs with the host process's memory, file
descriptors, network access, database credentials, and every other tenant's in-flight
payload. There is no isolation of any kind.

TwinOS processes untrusted customer uploads — multi-gigabyte IFC, glTF, and point-cloud
models, and gigabyte-scale PDFs — using memory-unsafe native libraries: OpenCASCADE,
Assimp, Draco, PDFium. Malicious IFC and PDF files are a well-established remote-code-
execution vector. Today that parser runs in the same address space as the database
credentials.

`job.WithTimeout` does not help. It cancels a context (`middleware/timeout.go`), and a
native library that has been exploited, or has merely stopped honoring cancellation, will
ignore it. The timeout is advisory. A wedged OpenCASCADE call keeps a worker slot and
keeps heartbeating for as long as the process lives.

Two distinct attacks, which the rest of this document treats separately because they need
different answers:

- **Credential theft.** A parser exploit reads the host's memory, environment, and
  filesystem. Defeated by putting the parser in a different address space.
- **Cross-tenant exposure.** A parser exploit reaches the other jobs on the same worker,
  or the network the worker sits on. Defeated only by a per-task boundary the kernel or
  hypervisor enforces.

### Position in the larger track

| | Sub-project | Depends on |
|---|---|---|
| A | Artifact plane | — |
| B | Resource model and resource-aware scheduling | A |
| **C** | **Execution isolation** (this document) | A, B |
| D | Long-run durability (progress checkpoints, resume) | independent |
| E | Resource prediction | B |

### Non-goals

This document does not cover resource *estimation* or scheduling policy (track B), the
prediction model that chooses a larger memory request after an OOM (track E), or
progress checkpointing (track D). It defines the execution boundary those tracks act
across, and names each seam where it creates one.

It also does not build the untrusted-third-party-handler case. The trust model is mixed,
first-party first: handlers are first-party TwinOS Go code today, with tenant-supplied
handlers on the roadmap. The threat being defended against now is **malicious file
content, not malicious handler code.** §16 states plainly what that leaves undefended,
and §5 names the two seams the third-party case will use.

---

## 2. Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Insertion point | Replace the `terminal` closure in `worker/executor.go:66` | Everything cross-cutting — recover, tracing, metrics, logging, scope, timeout, and track A's staging middleware — already sits outside it. Staging keeps running in the host process, so the sandbox receives a directory and never a credential. |
| Abstraction | `exec.Executor` with `Run(ctx, *Request) (*Result, error)` | Generalizes today's in-process call. Four implementations form an escalating ladder. `Result` carries a typed status, because out-of-process a handler saying no and a handler dying are no longer the same event. |
| Handler entrypoint | Re-exec self, same image, explicit `shim.Main` | An in-process Go closure cannot be shipped to a pod. The same binary re-invoked as `argv[1] == "dispatch-exec"` has the same registry by construction. No second build artifact, no image registry, no code serialization, no drift. |
| Registration seam | `job.Registrable` — a method on a generic type | Go forbids generic methods but permits methods *on* generic types, so `(*Definition[T]).Register(*Registry)` compiles and heterogeneous definitions fit in one slice. That slice is what a credential-free entrypoint can consume. |
| Handler credentials | Never, at any rung | Track A's invariant: the process touching storage credentials is never the process parsing the file. In K8s this means a three-container pod, not a scoped token. Works with any `artifact.Backend`, requiring no presigning or credential-scoping capability. |
| K8s retry | `backoffLimit: 0` | Dispatch owns `RetryCount`, backoff, and the DLQ. Two retry loops racing is a production-only bug. |
| K8s launch identity | Deterministic Job name `dispatch-<jobid>-<attempt>` | The name is the fence. A reaped job or a crashed-then-restarted worker gets `AlreadyExists` and adopts the running Job instead of starting a second one against the same attempt-scoped key prefix. |
| Downgrade | Rejected at `engine.Register` unless explicit | A definition that requires isolation must never silently run unisolated because it was deployed to a cluster that cannot provide it. |
| Dependencies | Zero new ones in core | K8s reuses `client-go`, already a direct dependency (`go.mod:146`). OCI drives a `runc`/`crun` binary rather than linking a runtime client. |

---

## 3. Package layout

`exec` must be a leaf. It may depend on `id`, `scope`, and the root `dispatch` package,
never on `job` — so that `job.Options` can later carry execution options without a cycle,
exactly as `artifact` is positioned in track A.

```
exec/               leaf: Executor, Request, Result, Status, Usage,
                          Resources, ResourceResolver, Isolation, options
exec/wire/          the boundary codec: frames, msgpack encoding, fd transport
exec/shim/          the child side: Main, local artifact accessor, signal handling
exec/inproc/        rung 1 — today's behavior
exec/subprocess/    rung 2 — fork/exec, rlimits, cgroup v2, process groups
exec/oci/           rung 3 — drives a runc/crun binary
exec/k8s/           rung 4 — Job-per-task, informers, three-container pod
exec/exectest/      the conformance suite every rung must pass
```

`exec/k8s` is deliberately separate from `cluster/k8s`. The latter is a `cluster.Store`
implementation — Lease election and pod-annotation worker discovery. Executing jobs is a
different concern with a different RBAC surface. They share a client and nothing else.

The rung packages are separate from `exec` so that a user who never leaves the default
never compiles `client-go` paths into their worker's reachable set, and so each rung's
platform-specific code (`syscall.SysProcAttr`, cgroup writes) stays behind its own build
constraints.

---

## 4. The Executor abstraction

```go
type Executor interface {
    // Name identifies this executor in configuration and metrics.
    Name() string

    // Run executes one attempt. The returned error is reserved for
    // failures to *launch*; a handler that ran and failed is reported
    // through Result.Status.
    Run(ctx context.Context, req *Request) (*Result, error)

    // Reclaim releases sandboxes this worker leaked across a restart.
    // Called once on pool start, and by the leader for dead workers.
    Reclaim(ctx context.Context, workerID id.WorkerID) error

    Close() error
}
```

### Request

```go
type Request struct {
    JobID       id.JobID
    Name        string            // handler name — the registry key
    Payload     []byte
    Attempt     int               // job.RetryCount, matching track A's key scheme
    Deadline    time.Time
    Fingerprint string            // registry fingerprint; see §5

    InputDir    string            // staged, read-only (track A)
    OutputDir   string            // handler writes here
    Inputs      []InputSlot       // declared name → relative path within InputDir

    Resources   Resources         // track B
    ScopeAppID  string            // for labels and logs; never a credential
    ScopeOrgID  string
    Env         map[string]string // non-secret only; see §6
}
```

### Result

```go
type Status string

const (
    StatusOK           Status = "ok"
    StatusHandlerError Status = "handler_error"  // the handler returned an error
    StatusTimeout      Status = "timeout"        // deadline hit; process killed
    StatusOOMKilled    Status = "oom_killed"     // cgroup or rlimit, not the handler's fault
    StatusKilled       Status = "killed"         // signal: SIGSEGV from OpenCASCADE, seccomp trap
    StatusLaunchFailed Status = "launch_failed"  // image pull, quota, runtime error
)

type Result struct {
    Status     Status
    HandlerErr string          // the handler's error string, verbatim
    ExitCode   int
    Signal     syscall.Signal
    Usage      Usage
    Outputs    []OutputFile    // name, size, hash, content type
}

type Usage struct {
    WallTime    time.Duration
    CPUTime     time.Duration
    PeakRSS     int64
    DiskWritten int64
}

// Err converts a Result into the error worker.Runner propagates.
// Returns nil for StatusOK; otherwise an *exec.Error carrying Status.
func (r *Result) Err() error
```

Returning a status rather than a bare `error` is the load-bearing change. Today a handler
returning `err` and a handler *dying* are the same value, so retry policy cannot
distinguish them. Out-of-process it must: "your IFC file was malformed" and "your IFC
file segfaulted the parser" are different events with different handling (§13) and only
one of them is worth an audit record.

`Usage` is track B's measurement feed and track E's training data, obtained at no cost
because every rung above the first already accounts it — `wait4`/`rusage` for subprocess,
`memory.peak` for cgroups, pod metrics for K8s.

### Wiring

`worker.Executor` is renamed `worker.Runner`. It orchestrates an *attempt* — middleware,
retry, DLQ, state transitions, lifecycle events — and was never the thing that invokes
the handler. `type Executor = Runner` and a deprecated `NewExecutor` wrapper keep v1.6
source-compatible; a type alias costs nothing and this is a v1 module.

The only line of execution logic that changes is the terminal closure at
`worker/executor.go:66`:

```go
terminal := func(ctx context.Context) error {
    res, err := r.exec.Run(ctx, r.request(ctx, j))
    if err != nil {
        return err          // launch failure — never reached the handler
    }
    return res.Err()        // nil, or *exec.Error carrying Status
}
```

Nothing above it moves. Staging, timeout, tracing, metrics, scope, and recover all
continue to run in the host process, which is precisely what keeps storage credentials
out of the sandbox.

`exec/inproc` is a registry lookup and a call:

```go
func (e *InProcess) Run(ctx context.Context, req *Request) (*Result, error) {
    h, ok := e.registry.Get(req.Name)
    if !ok {
        return nil, fmt.Errorf("exec: no handler registered for job %q", req.Name)
    }
    start := time.Now()
    err := h(ctx, req.Payload)
    return &Result{
        Status:     statusOf(err),
        HandlerErr: errString(err),
        Usage:      Usage{WallTime: time.Since(start)},
    }, nil
}
```

Byte-for-byte today's behavior, the default, requiring no configuration.

### Selection, and the no-silent-downgrade rule

Isolation is a property of the handler — this one parses IFC, that one sends an email —
so it is declared on the definition:

```go
var Tessellate = job.NewDefinition("tessellate.model", tessellate,
    exec.WithIsolation(exec.Sandboxed),   // minimum rung
    exec.WithGracePeriod(60*time.Second),
    artifact.Input("model", artifact.Required, artifact.StageAsPath),
    job.WithTimeout(6*time.Hour),
)
```

```go
type Isolation int

const (
    IsolationNone      Isolation = iota // in-process
    IsolationProcess                    // separate address space
    IsolationSandboxed                  // + namespaces, seccomp, no network
    IsolationVM                         // + independent kernel (gVisor, Kata)
)
```

The definition declares a **minimum**. Engine configuration maps rungs to configured
executors. If a definition demands a rung the deployment cannot provide, `engine.Register`
fails at startup with a message naming the definition, the required rung, and the
configured executors. Downgrade requires `exec.AllowDowngrade()` on the definition or
`allow_downgrade: true` in config, and logs a warning naming the definition every time.

Failing at `Register` rather than at execution is deliberate, and matches track A's
rejection of definitions whose declared `MaxSize` exceeds the cache budget: a
misconfiguration that can never work should fail on a developer's machine, not on the
first malicious upload in production.

---

## 5. Registration and the shim

An in-process Go closure cannot be shipped to a pod. Three mechanisms were considered:
a handler-to-image mapping, a re-exec-self pattern, and a DWP-based remote worker pool.

**Re-exec self is the answer for the first-party case**, because the sandbox runs the
same binary and therefore has the same registry by construction. There is no second build
artifact to keep in sync, no image registry to maintain, and no possibility of a pod
running a stale handler.

The other two are not discarded, they are relegated:

- **Handler-to-image mapping** survives as `job.WithImage("...")`, an override rather
  than the default. It is the seam the third-party case will use, and the K8s rung
  defaults its image to the worker's own, read from the downward API.
- **The DWP remote worker pool** (`dwp/` already implements a WebSocket/SSE frame
  protocol with auth, codec negotiation, and a connection manager) is the right shape for
  *tenant-supplied workers* later. It is explicitly wrong for pod-per-task: a long-lived
  worker processes many jobs, so a compromise from tenant A's IFC file persists into
  tenant B's. Reusing it here would trade the isolation property the track exists to
  provide for a protocol we would have to write anyway.

### The `job.Registrable` seam

Go forbids generic methods but permits methods on generic types:

```go
// job/registry.go
type Registrable interface {
    Register(*Registry)
    JobName() string
}

func (d *Definition[T]) Register(r *Registry) { RegisterDefinition(r, d) }
func (d *Definition[T]) JobName() string      { return d.Name }
```

That single method lets heterogeneous definitions live in one `[]job.Registrable`, which
is the thing a credential-free entrypoint can consume. `engine.Register` is reimplemented
in terms of it and `engine.RegisterAll(eng, defs...)` is added. Without this seam, every
out-of-process design collapses into code generation or reflection.

### One handler list, two consumers

```go
// handlers/handlers.go
var All = []job.Registrable{Tessellate, ExtractPDF, DecimateMesh}

// cmd/worker/main.go
func main() {
    if len(os.Args) > 1 && os.Args[1] == "dispatch-exec" {
        shim.Main(handlers.All...)   // no store, no DI, no config, no credentials
    }

    app := forge.New(troveext.New(), dispatchext.New())
    engine.RegisterAll(eng, handlers.All...)
    // ...
}
```

`shim.Main` is deliberately not auto-detected inside the Forge extension. By the time an
extension's boot hook runs, sibling extensions may already have dialled the database, so
detection there would make the credential-free guarantee a hope about boot ordering rather
than a property. Three lines at the top of `main` buy a guarantee.

`shim.Main` never returns. It:

1. builds a bare `job.Registry` and registers the definitions
2. reads the `Request` from fd 3, or from `$DISPATCH_REQUEST_FILE` in the K8s rung
3. verifies the registry fingerprint
4. installs a **local** `artifact.Accessor` (§6)
5. applies its own deadline from `Request.Deadline`, as defense in depth against a parent
   that dies without killing it
6. traps SIGTERM into cancellation of the handler context
7. runs the handler, writes the `Result`, and exits

### Registry fingerprint

`Request.Fingerprint` is a hash over the sorted registered job names plus the build's VCS
revision from `debug.ReadBuildInfo`. The shim rejects a request whose fingerprint does not
match its own, with `StatusLaunchFailed`.

In the re-exec-self case this is always satisfied and costs one comparison. Its purpose is
the `WithImage` override: it converts the silent-stale-handler failure mode — the specific
weakness that made an image mapping unattractive as the default — into a loud, immediate,
correctly-classified launch failure.

---

## 6. Crossing the boundary

Three things cross: the payload in, the staged inputs in, the result and outputs back.

### Inputs

Track A's staging middleware runs outside the boundary and produces a directory of local
files in the content-addressed cache. How that directory reaches the handler differs by
rung:

| Rung | Mechanism |
|---|---|
| in-process | not applicable; the accessor reads the cache directly |
| subprocess | the child inherits the path; the CAS entry stays leased for the attempt |
| OCI | read-only bind mount of the leased CAS entries at `/dispatch/in` |
| K8s | an **init container** stages into a shared `emptyDir`; it holds the read credential, the handler container does not |

The K8s row is the one that preserves track A's invariant across a node boundary. Staging
still happens outside the sandbox — outside the *handler container* rather than outside
the pod — so the process that touches storage credentials is still never the process that
parses the file.

One consequence: `StageLazy` is promoted to `StageAsPath` at the K8s rung, because lazy
streaming would require a credential inside the handler container. The promotion is logged
once per definition at `Register`, not silently.

### The request

The payload crosses as part of the `Request` frame, not as an argument or an environment
variable. A 200 KB payload does not belong in `ps` output, and `Env` carries only
non-secret values — the executor strips anything matching the configured secret-key
patterns and, at rungs above in-process, does not inherit the parent environment at all.
The child's environment is constructed, not inherited.

| Rung | Request transport | Result transport |
|---|---|---|
| subprocess, OCI | fd 3 | fd 4 |
| K8s | `/dispatch/in/request.msgpack`, written by the init container | exit code + `/dev/termination-log` |

fd 3 and fd 4 rather than stdin and stdout, so that stdout and stderr stay free for the
handler's logging and for whatever OpenCASCADE writes to them. Both are streamed to the
worker's logger tagged with `job_id` and `job_name`, line-buffered and rate-limited.

In K8s there is no inherited descriptor, so the result crosses as the process exit code
plus `terminationMessagePath` — a file the kubelet lifts into pod status, capped at 4 KB.
That yields a structured result with **zero egress** from the handler container. Anything
larger is an artifact by track A's design and does not belong in a result.

### Exit-code discipline

A handler that returns an error exits **0** with `Result{Status: handler_error}`. Non-zero
exits and signals are reserved for the shim and the kernel.

This is what lets the parent distinguish a business failure from a sandbox failure without
parsing error strings, and it is why `Result.Status` can be trusted for `handler_error`
while `oom_killed` and `killed` are derived from the parent's own observation
(`wait4` status, cgroup `memory.events`, pod status) rather than from anything the
possibly-compromised child reported.

### Outputs

Track A keeps outputs imperative — `art.Create(ctx, "page-317.png")` — so dynamic fan-out
works. Out-of-process, the accessor the shim installs is a **local** implementation:

- `art.Path(name)` resolves a declared input inside `InputDir`
- `art.Open(name)` opens that file
- `art.Create(ctx, name, opts...)` creates a file in `OutputDir` and returns a writer
- `Commit` closes the file, hashes it, and appends an entry to a local manifest

No backend, no network, no credentials. The handler code from track A §6 is unchanged and
unaware of which side of the boundary it is on.

Committing those files to the artifact plane happens outside:

| Rung | Who uploads |
|---|---|
| subprocess, OCI | the worker, after `Run` returns, reading `OutputDir` |
| K8s | a **native sidecar** container (`restartPolicy: Always` init container), which the kubelet SIGTERMs *after* the handler container exits |

The sidecar mechanism matters because it removes the piece of this design that would
otherwise be ugly. Kubernetes gives sibling containers no completion notification, so the
usual workaround is a marker file and a polling loop, which a compromised handler can lie
about. A native sidecar is terminated by the kubelet on handler exit, which the handler
cannot influence. Its `terminationGracePeriodSeconds` must exceed the expected upload
time, and `activeDeadlineSeconds` bounds it.

**The worker is the authority on what was produced**, in all rungs. It reads the manifest
but verifies against the actual directory listing or the actual object-store prefix, and
inserts artifact rows and links itself. A compromised handler can write garbage into its
own attempt-scoped ephemeral prefix — which track A already sweeps when the attempt fails
— but it cannot fabricate an artifact row, cannot link one to another job, and cannot
write outside its prefix.

Where the backend supports credential scoping, the sidecar's credential should be scoped
to `<ephemeral_prefix>/<owner_kind>/<owner_id>/<attempt>/`. That is defense in depth, not
a requirement: the design works with any `artifact.Backend` because it needs neither
presigning nor scoped credentials.

---

## 7. Rung 1 — in-process

`exec/inproc`. Today's behavior, the default, zero configuration. Present in the ladder so
the abstraction has a trivial implementation to validate against, and so the conformance
suite (§17) has a baseline every other rung must match on the cases that do not involve
containment.

It defends against nothing (§16). It remains the right choice for handlers that do not
touch untrusted bytes — sending an email, updating a row, calling an internal API — where
a process launch per job would be pure overhead.

---

## 8. Rung 2 — subprocess

`exec/subprocess`. Re-exec of `/proc/self/exe` with `argv[1] = "dispatch-exec"`.

**Address space.** The parser no longer shares memory with the database credentials, the
object-store client, or any other tenant's payload. This is the rung that answers the
first of the two attacks in §1, and it is available in every deployment, including a
laptop.

**Process group.** `SysProcAttr{Setpgid: true}` so that children a native library forks
die with the shim. Killing the process rather than the group is a common and silent bug:
OpenCASCADE spawning a helper leaves it running after the timeout appears to have worked.

**rlimits**, set in the child before `exec` via `SysProcAttr` and applied by the shim on
entry: `RLIMIT_AS` (address space), `RLIMIT_NOFILE`, `RLIMIT_NPROC`, `RLIMIT_CORE` set to
zero so a segfaulting parser does not write a multi-gigabyte core dump containing the
input file, and `RLIMIT_FSIZE`.

**cgroup v2** where available (Linux, delegated cgroup namespace): `memory.max`,
`memory.swap.max`, `cpu.max`, `pids.max`, written into a per-job sub-cgroup created under
the worker's own. This gives a genuine OOM kill with `memory.events` to read afterwards,
rather than an `RLIMIT_AS` failure surfacing as a confusing allocation error inside a
native library. When cgroup v2 is unavailable the rung degrades to rlimits only, and says
so at startup.

**Identity.** The child runs as a dedicated low-privilege UID configured by
`exec.WithUser(uid, gid)`. This is not optional advice. Running the child as the same UID
as the worker leaves it able to read the Dispatch config file, `~/.aws`, and
`/var/run/secrets`, which removes most of the value of the rung. The executor refuses to
start if configured for a UID equal to the worker's own, unless
`allow_same_user: true` is set.

---

## 9. Rung 3 — OCI

`exec/oci`. Drives an OCI runtime **binary** — `runc` or `crun`, configurable — through
its command-line and JSON state protocol, rather than linking a container-runtime client.
That keeps the core module's dependency set unchanged and makes the rung work identically
under Docker, Podman, and bare containerd, since all of them sit on the same runtime.

The bundle is generated per job: a config.json with the handler's own image rootfs mounted
read-only, `/dispatch/in` bind-mounted read-only from the leased CAS entries,
`/dispatch/out` and `/tmp` as writable tmpfs or scratch mounts, and the fd 3/4 pair
inherited through the runtime.

What this adds over rung 2: a **mount namespace**, so the filesystem the handler can see
is exactly the staged directories and nothing else — no config file, no cloud credential
file, no `/var/run/secrets`; a **network namespace** with no interfaces, so exfiltration
has nowhere to go and the database is unreachable even if credentials were somehow
obtained; **PID, IPC, and UTS namespaces**; a **user namespace** with UID remapping so
root inside is unprivileged outside; a **seccomp** filter; dropped capabilities; and a
read-only root filesystem.

Cancellation escalates through `runc kill` and then `runc kill --all`, which targets the
container's cgroup, so nothing escapes.

`Reclaim` lists containers labelled with the worker ID and kills them. This rung must run
the runtime attached, or record container IDs durably before starting them, or a worker
crash leaves containers running with no owner.

---

## 10. Rung 4 — Kubernetes Job-per-task

`exec/k8s`. One `batch/v1` Job per attempt.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: dispatch-<jobid-suffix>-<attempt>      # deterministic — see below
  namespace: dispatch-sandbox
  labels:
    dispatch.xraph.io/job-id: job_01h...
    dispatch.xraph.io/job-name: tessellate.model
    dispatch.xraph.io/attempt: "2"
    dispatch.xraph.io/worker-id: wkr_01h...
    dispatch.xraph.io/app-id: app_01h...
    dispatch.xraph.io/org-id: org_01h...
spec:
  backoffLimit: 0                              # Dispatch owns retry
  completions: 1
  parallelism: 1
  activeDeadlineSeconds: <timeout + grace>     # backstop if the worker dies
  ttlSecondsAfterFinished: 900                 # backstop GC, not primary
  template:
    spec:
      restartPolicy: Never
      runtimeClassName: gvisor                 # or kata-containers; configurable
      automountServiceAccountToken: false      # pod level
      serviceAccountName: dispatch-sandbox
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        seccompProfile: { type: RuntimeDefault }
      volumes:
        - name: in     ; emptyDir: {}
        - name: out    ; emptyDir: {}
        - name: tmp    ; emptyDir: {}
        - name: sa     ; projected: { sources: [ serviceAccountToken ] }

      initContainers:
        - name: stage                          # holds the READ credential
          volumeMounts: [ in(rw) ]
        - name: upload                         # native sidecar
          restartPolicy: Always                # kubelet SIGTERMs after handler exits
          volumeMounts: [ out(ro), sa(ro) ]    # token mounted HERE only

      containers:
        - name: handler                        # no credentials, no token, no network
          image: <worker's own image, downward API>
          args: ["dispatch-exec"]
          volumeMounts: [ in(ro), out(rw), tmp(rw) ]
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: { drop: ["ALL"] }
          resources: <from track B>
```

**`backoffLimit: 0`.** Dispatch owns `RetryCount`, the backoff strategy, and the DLQ.
Letting Kubernetes retry as well produces two loops racing, each unaware of the other's
count — a bug that only appears under load in production.

**The deterministic name is the fence.** A worker that crashes after creating the Job but
before recording it, or a second worker that picks the job up after the reaper has reset
it, gets `AlreadyExists` on create. The executor treats that as *adopt and watch*, not as
an error. Without it, a reaped job means two pods writing the same attempt-scoped key
prefix with no way to tell which output won.

**`automountServiceAccountToken: false` is pod-level, but the projected token volume is
mounted into the sidecar only.** That combination — deny by default at the pod, grant
explicitly to one container — is what gives the uploader an identity while leaving the
handler with none.

**Both deadlines are needed.** `activeDeadlineSeconds` covers the case where the worker
dies mid-job: without it, a wedged pod runs until something else notices. The worker's own
kill ladder covers the normal case and is faster.

**NetworkPolicy.** A default-deny ingress and egress policy in the sandbox namespace, with
explicit egress to the object-store endpoint and to kube-dns when that endpoint is a
hostname. §16 states the limitation this cannot overcome.

**ResourceQuota** on the sandbox namespace. This is what stops a job storm from starving
the cluster that Dispatch itself runs in, and it is the reason a dedicated namespace is
recommended over same-namespace execution.

### Resources are track B's input

```go
type Resources struct {
    CPUMillis      int64
    MemoryBytes    int64
    EphemeralBytes int64
    GPUCount       int64
    GPUClass       string
}

type ResourceResolver interface {
    Resolve(ctx context.Context, j *job.Job) (Resources, error)
}
```

Track C ships `exec.StaticResolver`, reading per-definition options and falling back to
configured defaults. Track B replaces the implementation; nothing in `exec/k8s` changes.
Requests and limits are derived by a configurable ratio, defaulting to requests == limits
for memory (Guaranteed QoS, so the sandbox is not the first thing evicted under node
pressure) and a burstable ratio for CPU.

---

## 11. Cancellation and timeouts

`middleware/timeout.go` cancels a context that a wedged native library is free to ignore.
From rung 2 upward the deadline is enforced by killing, and `job.WithTimeout` stops being
advisory. This is the single most visible behavioral change in the track.

| Rung | Cancel | Escalation |
|---|---|---|
| in-process | ctx cancel | none — this *is* the status quo limitation |
| subprocess | ctx cancel → SIGTERM to the shim → grace → SIGKILL to the **process group** | `setpgid`, so forked children die too |
| OCI | `runc kill TERM` → grace → `runc kill --all KILL` | targets the cgroup |
| K8s | delete Job, `propagationPolicy: Background`, `gracePeriodSeconds` | kubelet SIGTERM → SIGKILL; `activeDeadlineSeconds` if the worker is gone |

The grace period is `exec.WithGracePeriod(d)`, defaulting to 30 seconds, and must be long
enough for the sidecar to finish uploading partial outputs when the operator wants them
kept. On expiry the result is `StatusTimeout` with whatever `Usage` was observed.

---

## 12. Heartbeats, the reaper, and reclamation

**Heartbeats need no code change.** The worker goroutine stays alive, blocked inside
`Run`, so `sendHeartbeats` (`worker/pool.go:519`) continues to work against
`p.activeJobs` exactly as written. What changes is its meaning: it now attests to a
supervised sandbox's liveness rather than to a goroutine's. That is strictly more honest
than today, where a heartbeat continues happily for a handler that has been spinning
inside native code for six hours.

**The reaper** (`worker/pool.go:562`) resets stale jobs to pending and clears the worker
assignment. Out-of-process that creates two hazards, each with an answer already in the
design:

1. *The zombie sandbox.* A worker dies; its pod keeps running and keeps writing outputs.
   The reaper resets the job; another worker picks it up. Because the reaper does not
   increment `RetryCount`, the second launch targets the same attempt and therefore the
   same ephemeral key prefix. The deterministic Job name turns the second create into
   `AlreadyExists`, and the executor adopts the running Job rather than starting a rival.
   For subprocess and OCI the zombie dies with its parent's process group or cgroup.

2. *The leaked sandbox.* A worker restarts and has forgotten what it left behind.
   `Reclaim(ctx, workerID)` runs on pool `Start`: a no-op for subprocess, a
   kill-by-label for OCI, and for K8s a list of Jobs labelled
   `dispatch.xraph.io/worker-id=<id>` which are adopted when the corresponding job row is
   still running and assigned to this worker, and deleted otherwise. The elected leader
   runs the same sweep for workers that `cluster.ReapDeadWorkers` has declared dead,
   alongside the artifact sweeper from track A.

**No `ownerReference` from the sandbox Job to the worker pod.** It is the tempting way to
get Kubernetes garbage collection for free, and it would delete every in-flight sandbox
each time a worker restarts. Labels plus explicit reclaim plus `ttlSecondsAfterFinished`
as a backstop is the correct combination.

---

## 13. Failure taxonomy and retry policy

| Status | Policy |
|---|---|
| `handler_error` | Existing retry, backoff, and DLQ path, unchanged. |
| `timeout` | Retry; counts against `MaxRetries`. |
| `killed` | Retry; counts against `MaxRetries`. A SIGSEGV, SIGILL, SIGBUS, or seccomp trap from a memory-unsafe parser is also a security-relevant event: it emits a sandbox-violation through the existing extension registry, so `audit_hook` and `relay_hook` observe it with no new plumbing. |
| `oom_killed` | Retry at the same size by default. `exec.WithEscalation()` opts into a larger request on retry; *choosing* the size is track E's job, and track C provides the hook plus the recorded `Usage`. |
| `launch_failed` | **Requeue without incrementing `RetryCount`**, with backoff, capped by a separate `MaxLaunchAttempts`. |

The last row is a correctness requirement, not a nicety. An `ImagePullBackOff`, a
`FailedScheduling` against an exhausted quota, or a runtime that is momentarily missing is
infrastructure, not a property of the work. Letting it consume the job's three retries
means one bad node sends real customer work to the DLQ. The launch-attempt counter is
tracked separately and surfaced in the dashboard, so an infrastructure problem looks like
an infrastructure problem.

Diagnosis matters here: `exec/k8s` watches pod events as well as status, so
`FailedScheduling` and `ImagePullBackOff` reach the operator as themselves rather than as
a mysterious timeout twenty minutes later.

---

## 14. What `cluster/k8s` grows

Today it is a `cluster.Store` implementation — Lease-based leader election and
Pod-annotation worker discovery (`cluster/k8s/provider.go`). None of that changes. What
the package must grow:

**RBAC.** `batch/jobs`: create, get, list, watch, delete. `pods` and `pods/log`: get,
list, watch. `events`: list, watch. Scoped to the sandbox namespace, in a Role rather than
a ClusterRole. The existing Lease and Pod-annotation permissions stay in the worker's own
namespace. Two ServiceAccounts, not one: the worker's, and the sandbox's (which the
handler container never receives a token for).

**A shared informer factory.** Two hundred concurrent jobs must not open four hundred
watches. One Job informer and one Pod informer, filtered by the
`dispatch.xraph.io/worker-id` label selector, with per-job channels fanned out from the
event handlers. Without this, the K8s rung's failure mode under load is API-server
throttling that looks like random job timeouts.

**Namespace and quota management.** A `dispatch-sandbox` namespace with its own
ResourceQuota, LimitRange, and default-deny NetworkPolicy. Dispatch does not create these
— it validates their presence at startup and refuses to run the rung if the NetworkPolicy
is absent unless `require_network_policy: false` is set explicitly. Manifests ship as
documentation; a library does not apply cluster policy on its own.

**Client sharing.** `exec/k8s` and `cluster/k8s` accept a `kubernetes.Interface` rather
than constructing one, so a deployment has one client, one rate limiter, and one set of
connection pools.

---

## 15. Configuration

```yaml
extensions:
  dispatch:
    execution:
      default: inprocess              # inprocess | subprocess | oci | k8s
      allow_downgrade: false

      subprocess:
        user: 65532
        group: 65532
        allow_same_user: false
        grace_period: 30s
        rlimits:
          address_space: 16GB
          nofile: 1024
          nproc: 256
          core: 0
        cgroup:
          enabled: true
          parent: /dispatch.slice

      oci:
        runtime: crun                 # or runc
        bundle_dir: /var/lib/dispatch/bundles
        rootfs: /var/lib/dispatch/rootfs
        network: none

      k8s:
        namespace: dispatch-sandbox
        service_account: dispatch-sandbox
        runtime_class: gvisor
        image: ""                     # "" → the worker's own image, downward API
        require_network_policy: true
        ttl_after_finished: 900s
        upload_grace_period: 300s
        default_resources:
          cpu_millis: 2000
          memory_bytes: 4GB
          ephemeral_bytes: 32GB
```

---

## 16. Threat model

What each rung defends against, and what it does not. The second column is the one that
matters; a security design that only lists its wins is marketing.

### Rung 1 — in-process

**Defends against:** nothing.

**Does not defend against:** everything. A malicious IFC achieving RCE inside OpenCASCADE
owns the worker process: the database credentials in memory, every other tenant's
in-flight payload, the object-store client and its credentials, the Kubernetes service
account token, the filesystem, and the network. This is the current state of the system
and the reason the track exists.

### Rung 2 — subprocess

**Defends against:** memory-safety exploitation confined to a child address space, so the
database credentials and co-tenant payloads are not readable by the exploited parser;
resource exhaustion, bounded by rlimits and cgroup v2; runaway execution, since the
deadline is now enforced by SIGKILL to the process group rather than by a context the
handler can ignore; core dumps that would otherwise write the malicious input and process
memory to disk.

**Does not defend against:** a shared kernel — a kernel LPE escapes; a shared filesystem —
the child can read anything its UID can, so `~/.aws`, `/var/run/secrets`, and the Dispatch
config file are reachable unless the child runs as a dedicated low-privilege UID, which
this design requires by default and enforces at startup; a shared network namespace — the
child can dial the database and can exfiltrate anything it obtains; shared PID and IPC
namespaces.

### Rung 3 — OCI

**Defends against:** everything rung 2 does, plus filesystem exposure, since a mount
namespace limits the visible filesystem to the staged directories; network exfiltration
and lateral movement, since an empty network namespace has nowhere to send anything and
cannot reach the database even with stolen credentials; privilege escalation, via user
namespaces with UID remapping, dropped capabilities, and no-new-privileges; large classes
of kernel attack surface, via seccomp.

**Does not defend against:** a shared kernel — a Linux LPE still escapes; container-escape
CVEs of the `runc` CVE-2019-5736 class; anything for a handler that legitimately requires
network access, since the isolation is all-or-nothing at this rung; co-tenancy on the
host, since containers from different tenants share a kernel.

### Rung 4 — Kubernetes with gVisor or Kata

**Defends against:** everything rung 3 does, plus cross-tenant persistence, since a pod
per task means an exploit cannot survive into the next job; credential exposure entirely,
since the handler container holds no storage credential and no service account token; a
Linux kernel LPE, since a RuntimeClass interposes either a user-space kernel (gVisor) or a
real VM (Kata), so a kernel exploit must first defeat that; cluster-wide resource
exhaustion, bounded by ResourceQuota; scheduling-level tenant separation, if node
selectors and taints are configured to keep tenants apart.

**Does not defend against:** the pod-scoped nature of NetworkPolicy. This is the honest
limitation of the design and deserves a paragraph rather than a clause. NetworkPolicy
selects pods, not containers, and every container in a pod shares one network namespace.
The handler container therefore *can reach* the object-store endpoint at the network
level, because the uploader sidecar in the same pod must. It holds no credential to use it
with, and where the backend supports scoping, the sidecar's own credential is confined to
this job's ephemeral prefix — but the network path exists. Eliminating it requires putting
the uploader in a separate pod, which requires a shared volume, which requires a
ReadWriteMany PVC or node affinity. That trade is available as a documented option for
deployments that need it; it is not the default because the cost is high and the residual
risk is low.

Also undefended: a gVisor sentry escape or a Kata hypervisor escape; the Kubernetes
control plane itself; and the object storage the pod legitimately writes to.

### What no rung defends against

**A malicious handler author.** The trust model is first-party handlers, and every rung
assumes the handler code is trying to do its job. A handler that deliberately exfiltrates
its own tenant's data through its own declared outputs succeeds at every rung. Closing
this requires the third-party track: `job.WithImage` for the handler artifact, per-tenant
credential scoping, and the DWP remote-worker path for tenant-operated workers.

**Supply-chain compromise of the image.** The handler container runs the worker's own
image; if that image is compromised, isolation is irrelevant because the worker is
compromised too.

**Cross-pod side channels.** Spectre-class attacks between co-tenant pods on one node are
addressed only by node-level tenant separation, which is a scheduling decision made
outside Dispatch.

**Denial of service by legitimate means.** A handler that consumes its full resource
allocation for its full timeout is indistinguishable from one doing real work. Track B's
admission control bounds the aggregate; it does not bound the individual.

### Where this leaves TwinOS

The subprocess rung is what stops a malicious IFC from reading the database password. The
pod rung is what stops it from reading another tenant's model. These are different
attacks, and the ladder is worth climbing for both.

---

## 17. Testing

**`exectest` — the conformance suite.** One table-driven suite, run against all four
implementations, following the existing `store_test.go` pattern. Cases: success; handler
error; handler panic; deadline exceeded with a cooperative handler; deadline exceeded with
a handler that ignores SIGTERM; OOM; signal death; cancellation mid-flight; a payload
large enough to exercise framing; an output large enough to exercise upload; unknown
handler name; fingerprint mismatch; empty output directory; a handler that writes outputs
then fails.

This is the highest-value artifact in the track. It is what makes each rung landable
independently without redesign, and what keeps the four implementations behaviorally
identical everywhere they should be.

**Kill-ladder tests.** A fixture handler that traps SIGTERM and then spins, asserting
SIGKILL after the grace period, that the process group is gone, and that no orphan
survives. A fixture that forks a child before spinning, asserting the child dies too —
this is the bug that silently does not work if `Setpgid` is forgotten.

**Wire tests.** Round-trip encoding; truncated frames; a shim that exits without writing a
result; a shim that writes a result larger than the K8s termination-message cap; garbage
on fd 4.

**K8s golden-file test.** The generated Job spec, asserted against a checked-in golden
file using the `client-go` fake clientset. A refactor that silently drops
`readOnlyRootFilesystem`, `automountServiceAccountToken: false`, or `backoffLimit: 0`
fails CI rather than shipping. This is the single most valuable test in the rung, because
the security properties of §16 are all spec fields and all of them are one careless edit
from disappearing.

**Idempotent-launch test.** Create the same job twice; assert adoption rather than
duplication, and that only one pod exists.

**Reclaim test.** Jobs labelled with a dead worker are deleted; jobs labelled with a live
worker whose job row is still running are adopted.

**The hostile-handler fixture.** A deliberately malicious handler that allocates without
bound, forks aggressively, opens `/var/run/secrets` and `~/.aws`, attempts a TCP
connection to the store, and writes outside its output directory. It is asserted to
succeed or fail *differently at each rung*, exactly per the table in §16. This turns the
threat model from prose into an executable specification, and any future change that
weakens a rung fails a named test rather than quietly eroding the guarantee.

**Integration.** A `kind`-based test behind a build tag, running a real Job through a real
kubelet with a real gVisor RuntimeClass where CI supports it.

**Benchmarks.** Launch overhead per rung, in the existing `bench` style, so the cost of
climbing the ladder is a measured number in the docs rather than an assumption.

---

## 18. Backward compatibility

The default executor is in-process, so a deployment that configures nothing behaves
exactly as it does today. `worker.Executor` survives as a type alias for `worker.Runner`,
and `worker.NewExecutor` as a deprecated wrapper, so no import breaks. `job.Registrable`
is additive; `engine.Register` keeps its signature and is reimplemented over it.
Definitions without `exec.WithIsolation` never leave the process.

One additive migration: a nullable `launch_attempts INT` column on `dispatch_jobs`,
required by §13 so that infrastructure failures survive a worker restart without
consuming the job's retry budget. It defaults to NULL and is ignored by every existing
query, so the migration is additive across all five backends and needs no backfill. That
is the only persistent state execution isolation introduces.

---

## 19. Phasing

Each phase is independently useful and independently testable.

1. **Abstraction.** `exec` leaf package, `exec/inproc`, the `worker.Runner` rename with
   its alias, `job.Registrable`, `engine.RegisterAll`, and `exectest` with the cases that
   apply to in-process. A pure refactor: no behavior change, no new dependency, and every
   later rung now has a suite to satisfy.
2. **Subprocess.** `exec/wire`, `exec/shim`, `exec/subprocess` with rlimits, process
   groups, the kill ladder, constructed environments, and stdio streaming. The first real
   containment, and the first time `job.WithTimeout` actually stops work.
3. **cgroups and usage.** cgroup v2 limits and `Usage` reporting on Linux, degrading to
   rlimits elsewhere. Track B's measurement feed begins here.
4. **OCI.** `exec/oci` driving `runc`/`crun`, bundle generation, namespaces, seccomp.
5. **Kubernetes.** `exec/k8s` — Job-per-task, shared informers, the three-container pod,
   adoption, reclaim, event-based diagnosis.
6. **Cluster and operations.** `cluster/k8s` RBAC, namespace/quota/NetworkPolicy
   validation and shipped manifests, dashboard surfacing of sandbox status, usage, and
   launch attempts, and the benchmark numbers in the docs.

Phase 1 is worth landing on its own: it makes the boundary explicit, gives the ladder a
test suite, and changes nothing for existing users.
