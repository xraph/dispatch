# Execution Isolation Phase 1 — The Abstraction — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce an `exec.Executor` abstraction that generalises today's in-process handler call, with an in-process implementation that preserves current behaviour exactly, plus the conformance suite every later rung must pass.

**Architecture:** A new leaf package `exec` defines `Executor`, `Request`, `Result`, and a `Policy` declared per job definition. `worker.Executor` is renamed `worker.Runner` (keeping a type alias) and its terminal closure delegates to an `exec.Executor` instead of calling the handler directly. `job.Registrable` — a method on the generic `Definition[T]` — lets heterogeneous definitions be registered from a slice, which is the seam a credential-free entrypoint will consume in Phase 2.

**Tech Stack:** Go 1.25.7, standard library only. No new module dependencies.

## Global Constraints

- Module is `github.com/xraph/dispatch`, Go 1.25.7. **No new dependencies may be added to `go.mod` in this phase.**
- `exec` must be a **leaf package**. It may import only `id`, `scope`, and the root `dispatch` package. It must **never** import `job`, `worker`, `engine`, or `artifact`. Enforced by a test in Task 4.
- Linting is golangci-lint v2 per `.golangci.yml`. `revive`'s `exported` rule runs with `checkPrivateReceivers`, so **every exported symbol needs a doc comment starting with its own name**. `errcheck`, `gosec`, `errorlint`, and `prealloc` are enabled.
- Errors are wrapped with `%w` and package-prefixed: `fmt.Errorf("dispatch/exec: ...: %w", err)`.
- Tests are table-driven where there is more than one case, live in `package <pkg>_test` (external test package, as `job/registry_test.go` does), and use `t.Fatalf`/`t.Errorf` with `got`/`want` phrasing. No third-party assertion library.
- IDs use the existing TypeID system in `id/`. No new prefixes in this phase.
- Commit messages: conventional-commit prefixes (`feat:`, `refactor:`, `test:`, `docs:`). **Never add `Co-Authored-By` trailers.**
- Run `make test` and `make lint` before each commit.

### Deliberate deviations from the spec, with reasons

1. **`Result.Signal` is `int`, not `syscall.Signal`.** `exec` is a leaf that must compile everywhere; storing the raw signal number keeps `syscall` out of it. The subprocess rung converts.
2. **`Request` omits the `Resources` field in this phase.** The spec types it as `resource.Spec`, and track B's `resource` package does not exist yet. It is added in Phase 4, where the Kubernetes rung is the first consumer. Nothing in Phase 1 reads it.
3. **`Request.PriorOutputs` is defined but always empty in this phase.** In-process execution reaches the real `artifact.Service` directly, so resumption already works. The worker populates it in Phase 2, when the shim's in-memory store first needs seeding.

---

## File Structure

| File | Responsibility |
|---|---|
| `exec/doc.go` | Package documentation |
| `exec/policy.go` | `Level`, `Policy`, `PolicyOption`, and its options |
| `exec/status.go` | `Status` constants and classification helpers |
| `exec/result.go` | `Result`, `Usage`, `Error`, status sentinels |
| `exec/request.go` | `Request`, `InputSlot`, `PriorOutput` |
| `exec/fingerprint.go` | Registry fingerprint derivation |
| `exec/executor.go` | The `Executor` interface |
| `exec/registry.go` | Name→`Executor` map, default, and `Select` with the downgrade rule |
| `exec/inproc/inproc.go` | The in-process rung |
| `exec/exectest/suite.go` | The conformance suite all rungs must pass |
| `exec/exectest/handlers.go` | Shared fixture handlers the suite installs |
| `job/registrable.go` | `Registrable`, `(*Definition[T]).Register`, `JobName` |
| `job/options.go` (modify) | `Options.Execution`, `WithExecution` |
| `job/registry.go` (modify) | Store and expose per-name `exec.Policy` |
| `worker/runner.go` (rename from `executor.go`) | `Runner`, delegating to `exec.Executor` |
| `engine/engine.go` (modify) | Build the `exec.Registry`, wire it, `RegisterAll`, validate at `Register` |

---

## Task 1: `exec` policy types

**Files:**
- Create: `exec/doc.go`, `exec/policy.go`
- Test: `exec/policy_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `exec.Level` (int enum: `LevelNone`, `LevelProcess`, `LevelSandboxed`, `LevelVM`), `Level.String() string`, `exec.Policy{Level Level; GracePeriod time.Duration; AllowDowngrade bool; Image string}`, `exec.PolicyOption func(*Policy)`, `exec.NewPolicy(opts ...PolicyOption) Policy`, and options `Isolate(Level)`, `GracePeriod(time.Duration)`, `AllowDowngrade()`, `Image(string)`.

- [ ] **Step 1: Write the failing test**

Create `exec/policy_test.go`:

```go
package exec_test

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
)

func TestNewPolicy_Defaults(t *testing.T) {
	p := exec.NewPolicy()

	if p.Level != exec.LevelNone {
		t.Errorf("Level = %v, want %v", p.Level, exec.LevelNone)
	}
	if p.GracePeriod != 30*time.Second {
		t.Errorf("GracePeriod = %v, want %v", p.GracePeriod, 30*time.Second)
	}
	if p.AllowDowngrade {
		t.Error("AllowDowngrade = true, want false")
	}
	if p.Image != "" {
		t.Errorf("Image = %q, want empty", p.Image)
	}
}

func TestNewPolicy_Options(t *testing.T) {
	p := exec.NewPolicy(
		exec.Isolate(exec.LevelSandboxed),
		exec.GracePeriod(90*time.Second),
		exec.AllowDowngrade(),
		exec.Image("twinos/worker:v3"),
	)

	if p.Level != exec.LevelSandboxed {
		t.Errorf("Level = %v, want %v", p.Level, exec.LevelSandboxed)
	}
	if p.GracePeriod != 90*time.Second {
		t.Errorf("GracePeriod = %v, want %v", p.GracePeriod, 90*time.Second)
	}
	if !p.AllowDowngrade {
		t.Error("AllowDowngrade = false, want true")
	}
	if p.Image != "twinos/worker:v3" {
		t.Errorf("Image = %q, want %q", p.Image, "twinos/worker:v3")
	}
}

func TestNewPolicy_NonPositiveGracePeriodKeepsDefault(t *testing.T) {
	// A zero or negative grace period would make the kill ladder in later
	// rungs degenerate into an immediate SIGKILL, losing every chance of a
	// clean shutdown. Reject it at construction rather than at kill time.
	for _, d := range []time.Duration{0, -1 * time.Second} {
		p := exec.NewPolicy(exec.GracePeriod(d))
		if p.GracePeriod != 30*time.Second {
			t.Errorf("GracePeriod(%v) = %v, want default %v", d, p.GracePeriod, 30*time.Second)
		}
	}
}

func TestLevel_String(t *testing.T) {
	tests := []struct {
		level exec.Level
		want  string
	}{
		{exec.LevelNone, "none"},
		{exec.LevelProcess, "process"},
		{exec.LevelSandboxed, "sandboxed"},
		{exec.LevelVM, "vm"},
		{exec.Level(99), "Level(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.level.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./exec/...`
Expected: FAIL — no Go files in `exec`, package does not exist.

- [ ] **Step 3: Write the package documentation**

Create `exec/doc.go`:

```go
// Package exec defines the execution boundary between the Dispatch worker
// and a job handler.
//
// Today a handler is an ordinary Go function called in-process, sharing the
// worker's memory, credentials, and network. Handlers that parse untrusted
// bytes with memory-unsafe native libraries need more than that, so exec
// generalises the call into an [Executor] with implementations forming an
// escalating ladder: in-process, subprocess, OCI container, and Kubernetes
// Job-per-task.
//
// exec is a leaf package. It imports only id, scope, and the root dispatch
// package — never job, worker, or engine — so that job.Options can carry an
// execution [Policy] without an import cycle. This mirrors how artifact is
// positioned for input declarations.
package exec
```

- [ ] **Step 4: Write the policy implementation**

Create `exec/policy.go`:

```go
package exec

import (
	"fmt"
	"time"
)

// DefaultGracePeriod is how long a sandbox is given to exit after being
// asked politely, before it is killed outright.
const DefaultGracePeriod = 30 * time.Second

// Level is the minimum isolation a job definition requires. The levels are
// ordered, so a deployment offering a stronger level satisfies a definition
// asking for a weaker one.
type Level int

const (
	// LevelNone runs the handler in the worker process. This is the
	// default and it provides no isolation of any kind.
	LevelNone Level = iota

	// LevelProcess runs the handler in a separate address space, so an
	// exploited parser cannot read the worker's credentials.
	LevelProcess

	// LevelSandboxed adds mount, network, PID, and user namespaces, a
	// seccomp filter, and dropped capabilities.
	LevelSandboxed

	// LevelVM adds an independent kernel — gVisor or Kata — so a Linux
	// privilege escalation is not by itself an escape.
	LevelVM
)

// String renders the level for configuration, logs, and errors.
func (l Level) String() string {
	switch l {
	case LevelNone:
		return "none"
	case LevelProcess:
		return "process"
	case LevelSandboxed:
		return "sandboxed"
	case LevelVM:
		return "vm"
	default:
		return fmt.Sprintf("Level(%d)", int(l))
	}
}

// Policy is a job definition's execution declaration. It states the minimum
// isolation the handler requires, not the executor it runs on: which rung
// satisfies the requirement is a deployment decision.
type Policy struct {
	// Level is the minimum isolation required.
	Level Level

	// GracePeriod is how long the sandbox has to exit after SIGTERM
	// before it is killed.
	GracePeriod time.Duration

	// AllowDowngrade permits running at a weaker level than Level when
	// the deployment cannot provide it. Without it, a deployment that
	// cannot satisfy the policy fails at registration rather than
	// silently running the handler unisolated.
	AllowDowngrade bool

	// Image overrides the container image for out-of-process rungs.
	// Empty means the worker's own image, which is the correct default
	// because the sandbox re-execs the same binary.
	Image string
}

// PolicyOption configures a Policy.
type PolicyOption func(*Policy)

// NewPolicy builds a Policy from options, starting from the defaults:
// no isolation and a 30-second grace period.
func NewPolicy(opts ...PolicyOption) Policy {
	p := Policy{
		Level:       LevelNone,
		GracePeriod: DefaultGracePeriod,
	}
	for _, opt := range opts {
		opt(&p)
	}

	return p
}

// Isolate sets the minimum isolation level the handler requires.
func Isolate(l Level) PolicyOption {
	return func(p *Policy) { p.Level = l }
}

// GracePeriod sets how long the sandbox has to exit cleanly after being
// signalled. Non-positive durations are ignored, because a zero grace
// period reduces the kill ladder to an immediate SIGKILL and loses any
// chance of a clean shutdown.
func GracePeriod(d time.Duration) PolicyOption {
	return func(p *Policy) {
		if d > 0 {
			p.GracePeriod = d
		}
	}
}

// AllowDowngrade permits running below the declared level when the
// deployment cannot satisfy it.
func AllowDowngrade() PolicyOption {
	return func(p *Policy) { p.AllowDowngrade = true }
}

// Image overrides the container image used by out-of-process rungs.
func Image(ref string) PolicyOption {
	return func(p *Policy) { p.Image = ref }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./exec/...`
Expected: PASS, 4 tests.

- [ ] **Step 6: Lint**

Run: `golangci-lint run ./exec/...`
Expected: no issues.

- [ ] **Step 7: Commit**

```bash
git add exec/doc.go exec/policy.go exec/policy_test.go
git commit -m "feat(exec): add the execution policy type

Policy is a definition's declaration of the minimum isolation its handler
requires. Levels are ordered so a stronger deployment satisfies a weaker
requirement, and AllowDowngrade is opt-in so a definition that must be
isolated cannot silently run unisolated."
```

---

## Task 2: Status, Usage, Result, and Error

**Files:**
- Create: `exec/status.go`, `exec/result.go`
- Test: `exec/result_test.go`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `exec.Status` (string enum: `StatusOK`, `StatusHandlerError`, `StatusTimeout`, `StatusOOMKilled`, `StatusKilled`, `StatusLaunchFailed`), `Status.IsFailure() bool`, `Status.CountsAgainstRetries() bool`, `exec.Usage{WallTime, CPUTime time.Duration; PeakRSS, DiskWritten int64}`, `exec.OutputFile{Name string; Size int64; Hash, ContentType string}`, `exec.Result{Status, HandlerErr, ExitCode, Signal, Usage, Outputs}`, `(*Result).Err() error`, `exec.Error{Status Status; Msg string; ExitCode, Signal int}` with `Error()`, `Unwrap()`, and sentinels `ErrHandler`, `ErrTimeout`, `ErrOOMKilled`, `ErrKilled`, `ErrLaunchFailed`.

- [ ] **Step 1: Write the failing test**

Create `exec/result_test.go`:

```go
package exec_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
)

func TestResult_Err(t *testing.T) {
	tests := []struct {
		name     string
		result   exec.Result
		wantNil  bool
		wantIs   error
		wantText string
	}{
		{
			name:    "ok returns nil",
			result:  exec.Result{Status: exec.StatusOK},
			wantNil: true,
		},
		{
			name:     "handler error carries the handler message",
			result:   exec.Result{Status: exec.StatusHandlerError, HandlerErr: "bad IFC header"},
			wantIs:   exec.ErrHandler,
			wantText: "bad IFC header",
		},
		{
			name:     "timeout",
			result:   exec.Result{Status: exec.StatusTimeout},
			wantIs:   exec.ErrTimeout,
			wantText: "timeout",
		},
		{
			name:     "oom killed",
			result:   exec.Result{Status: exec.StatusOOMKilled},
			wantIs:   exec.ErrOOMKilled,
			wantText: "oom_killed",
		},
		{
			name:     "killed by signal",
			result:   exec.Result{Status: exec.StatusKilled, Signal: 11},
			wantIs:   exec.ErrKilled,
			wantText: "signal 11",
		},
		{
			name:     "launch failed",
			result:   exec.Result{Status: exec.StatusLaunchFailed, HandlerErr: "image pull backoff"},
			wantIs:   exec.ErrLaunchFailed,
			wantText: "image pull backoff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.result.Err()

			if tt.wantNil {
				if err != nil {
					t.Fatalf("Err() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Err() = nil, want error")
			}
			if !errors.Is(err, tt.wantIs) {
				t.Errorf("errors.Is(%v, %v) = false, want true", err, tt.wantIs)
			}
			if !contains(err.Error(), tt.wantText) {
				t.Errorf("Err() = %q, want it to contain %q", err.Error(), tt.wantText)
			}
		})
	}
}

func TestStatus_CountsAgainstRetries(t *testing.T) {
	// A launch failure is infrastructure, not a property of the work.
	// Letting it consume the retry budget means one bad node sends real
	// customer work to the DLQ.
	tests := []struct {
		status exec.Status
		want   bool
	}{
		{exec.StatusOK, false},
		{exec.StatusHandlerError, true},
		{exec.StatusTimeout, true},
		{exec.StatusOOMKilled, true},
		{exec.StatusKilled, true},
		{exec.StatusLaunchFailed, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if got := tt.status.CountsAgainstRetries(); got != tt.want {
				t.Errorf("CountsAgainstRetries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatus_IsFailure(t *testing.T) {
	if exec.StatusOK.IsFailure() {
		t.Error("StatusOK.IsFailure() = true, want false")
	}
	for _, s := range []exec.Status{
		exec.StatusHandlerError, exec.StatusTimeout,
		exec.StatusOOMKilled, exec.StatusKilled, exec.StatusLaunchFailed,
	} {
		if !s.IsFailure() {
			t.Errorf("%s.IsFailure() = false, want true", s)
		}
	}
}

func TestUsage_ZeroValueIsUsable(t *testing.T) {
	var u exec.Usage
	if u.WallTime != 0 || u.CPUTime != 0 || u.PeakRSS != 0 || u.DiskWritten != 0 {
		t.Errorf("zero Usage = %+v, want all zero", u)
	}
	u.WallTime = time.Second
	if u.WallTime != time.Second {
		t.Errorf("WallTime = %v, want %v", u.WallTime, time.Second)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./exec/...`
Expected: FAIL — undefined: `exec.StatusOK`, `exec.Result`, etc.

- [ ] **Step 3: Write the status implementation**

Create `exec/status.go`:

```go
package exec

// Status classifies how an execution attempt ended.
//
// A bare error cannot express this. In-process, a handler returning an
// error and a handler dying are the same value; out-of-process they are
// different events needing different handling, and only some of them are
// the handler's fault.
type Status string

const (
	// StatusOK means the handler ran and returned nil.
	StatusOK Status = "ok"

	// StatusHandlerError means the handler ran and returned an error.
	// This is a business failure and follows the normal retry path.
	StatusHandlerError Status = "handler_error"

	// StatusTimeout means the deadline expired and the sandbox was
	// killed. Unlike a cancelled context, this is enforced.
	StatusTimeout Status = "timeout"

	// StatusOOMKilled means a memory limit was hit. The handler did not
	// choose this and may succeed with a larger allocation.
	StatusOOMKilled Status = "oom_killed"

	// StatusKilled means the process died on a signal — a SIGSEGV from a
	// memory-unsafe parser, or a seccomp trap. It is security-relevant.
	StatusKilled Status = "killed"

	// StatusLaunchFailed means the sandbox never started: an image pull
	// failure, an exhausted quota, a missing runtime. The handler never
	// ran, so this is infrastructure rather than work.
	StatusLaunchFailed Status = "launch_failed"
)

// IsFailure reports whether the status represents anything other than
// success.
func (s Status) IsFailure() bool { return s != StatusOK }

// CountsAgainstRetries reports whether an attempt ending in this status
// should consume the job's retry budget.
//
// Launch failures do not. An ImagePullBackOff or a FailedScheduling says
// nothing about the work, and burning three retries on one bad node would
// send healthy jobs to the DLQ.
func (s Status) CountsAgainstRetries() bool {
	switch s {
	case StatusHandlerError, StatusTimeout, StatusOOMKilled, StatusKilled:
		return true
	case StatusOK, StatusLaunchFailed:
		return false
	default:
		return true
	}
}
```

- [ ] **Step 4: Write the result implementation**

Create `exec/result.go`:

```go
package exec

import (
	"errors"
	"fmt"
	"time"
)

// Status sentinels, so callers can classify a failure with errors.Is
// rather than by comparing strings.
var (
	// ErrHandler marks an error the handler itself returned.
	ErrHandler = errors.New("handler error")
	// ErrTimeout marks an attempt killed for exceeding its deadline.
	ErrTimeout = errors.New("execution timeout")
	// ErrOOMKilled marks an attempt killed for exceeding a memory limit.
	ErrOOMKilled = errors.New("out of memory")
	// ErrKilled marks an attempt whose process died on a signal.
	ErrKilled = errors.New("killed by signal")
	// ErrLaunchFailed marks a sandbox that never started.
	ErrLaunchFailed = errors.New("launch failed")
)

// Usage records what an attempt consumed. Every rung above in-process
// accounts these anyway, so collecting them costs nothing and gives the
// resource model its measurements.
type Usage struct {
	WallTime    time.Duration
	CPUTime     time.Duration
	PeakRSS     int64
	DiskWritten int64
}

// OutputFile describes one artifact the handler produced, as claimed by
// the sandbox. The worker verifies the claim against what is actually on
// disk before recording anything.
type OutputFile struct {
	Name        string
	Size        int64
	Hash        string
	ContentType string
}

// Result reports how one execution attempt ended.
type Result struct {
	// Status classifies the outcome.
	Status Status

	// HandlerErr is the handler's error string, or a diagnostic for a
	// launch failure. Empty on success.
	HandlerErr string

	// ExitCode is the sandbox process's exit status, where one applies.
	ExitCode int

	// Signal is the signal number that killed the process, or zero.
	// Stored as an int rather than a syscall.Signal so this leaf package
	// stays free of syscall.
	Signal int

	// Usage records what the attempt consumed.
	Usage Usage

	// Outputs lists the artifacts the sandbox claims to have written.
	Outputs []OutputFile
}

// Err converts a Result into the error the worker propagates. It returns
// nil for StatusOK and an *Error otherwise.
func (r *Result) Err() error {
	if r == nil || r.Status == StatusOK {
		return nil
	}

	return &Error{
		Status:   r.Status,
		Msg:      r.HandlerErr,
		ExitCode: r.ExitCode,
		Signal:   r.Signal,
	}
}

// Error is a failed execution attempt. It carries the Status so retry
// policy can branch on how the attempt failed rather than parsing text.
type Error struct {
	Status   Status
	Msg      string
	ExitCode int
	Signal   int
}

// Error implements the error interface.
func (e *Error) Error() string {
	switch {
	case e.Msg != "":
		return fmt.Sprintf("dispatch/exec: %s: %s", e.Status, e.Msg)
	case e.Signal != 0:
		return fmt.Sprintf("dispatch/exec: %s: signal %d", e.Status, e.Signal)
	case e.ExitCode != 0:
		return fmt.Sprintf("dispatch/exec: %s: exit %d", e.Status, e.ExitCode)
	default:
		return fmt.Sprintf("dispatch/exec: %s", e.Status)
	}
}

// Unwrap returns the sentinel for this error's status, so errors.Is works.
func (e *Error) Unwrap() error {
	switch e.Status {
	case StatusHandlerError:
		return ErrHandler
	case StatusTimeout:
		return ErrTimeout
	case StatusOOMKilled:
		return ErrOOMKilled
	case StatusKilled:
		return ErrKilled
	case StatusLaunchFailed:
		return ErrLaunchFailed
	case StatusOK:
		return nil
	default:
		return nil
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./exec/...`
Expected: PASS.

Note: the `killed by signal` case asserts the message contains `signal 11`; `Error()` reaches that branch because `Msg` is empty. The `handler error` case asserts the message contains `bad IFC header` via the `Msg` branch.

- [ ] **Step 6: Lint and commit**

```bash
golangci-lint run ./exec/...
git add exec/status.go exec/result.go exec/result_test.go
git commit -m "feat(exec): add execution status, result, and error types

Run returns a typed Status rather than a bare error, because
out-of-process a handler returning an error and a handler being killed by
the kernel are different events. Launch failures are classified as not
counting against the retry budget: an ImagePullBackOff says nothing about
the work, and burning retries on one bad node would DLQ healthy jobs."
```

---

## Task 3: Request, and the registry fingerprint

**Files:**
- Create: `exec/request.go`, `exec/fingerprint.go`
- Test: `exec/request_test.go`, `exec/fingerprint_test.go`

**Interfaces:**
- Consumes: `artifact.Ref` from the already-implemented track A.
- Produces: `exec.InputSlot{Name, Path string}`, `exec.PriorOutput{Name string; Ref artifact.Ref}`, `exec.Request{JobID id.JobID; Name string; Payload []byte; Attempt int; Deadline time.Time; Fingerprint string; InputDir, OutputDir string; Inputs []InputSlot; PriorOutputs []PriorOutput; Policy Policy; ScopeAppID, ScopeOrgID string; Env map[string]string}`, `(*Request).Validate() error`, `exec.FingerprintOf(names []string, revision string) string`, `exec.Fingerprint(names []string) string`.

**Note on the leaf constraint:** `artifact` is itself a leaf that does not import `job`, so `exec` importing `artifact.Ref` does not create a cycle. Task 4's dependency test allows `artifact` explicitly.

- [ ] **Step 1: Write the failing tests**

Create `exec/fingerprint_test.go`:

```go
package exec_test

import (
	"testing"

	"github.com/xraph/dispatch/exec"
)

func TestFingerprintOf_StableAcrossOrder(t *testing.T) {
	a := exec.FingerprintOf([]string{"b.job", "a.job", "c.job"}, "abc123")
	b := exec.FingerprintOf([]string{"a.job", "b.job", "c.job"}, "abc123")

	if a != b {
		t.Errorf("fingerprint depends on order: %q != %q", a, b)
	}
}

func TestFingerprintOf_ChangesWithNames(t *testing.T) {
	a := exec.FingerprintOf([]string{"a.job"}, "abc123")
	b := exec.FingerprintOf([]string{"a.job", "b.job"}, "abc123")

	if a == b {
		t.Error("fingerprint did not change when a handler was added")
	}
}

func TestFingerprintOf_ChangesWithRevision(t *testing.T) {
	a := exec.FingerprintOf([]string{"a.job"}, "abc123")
	b := exec.FingerprintOf([]string{"a.job"}, "def456")

	if a == b {
		t.Error("fingerprint did not change with the build revision")
	}
}

func TestFingerprintOf_DoesNotCollideOnSeparatorAmbiguity(t *testing.T) {
	// {"a", "b"} and {"a\nb"} must not hash the same, or a handler named
	// with an embedded separator could impersonate a two-handler set.
	a := exec.FingerprintOf([]string{"a", "b"}, "r")
	b := exec.FingerprintOf([]string{"a\nb"}, "r")

	if a == b {
		t.Error("separator ambiguity produced a collision")
	}
}

func TestFingerprintOf_Empty(t *testing.T) {
	if got := exec.FingerprintOf(nil, "r"); got == "" {
		t.Error("FingerprintOf(nil) = empty, want a hash")
	}
}
```

Create `exec/request_test.go`:

```go
package exec_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
)

func validRequest() *exec.Request {
	return &exec.Request{
		JobID:    id.NewJobID(),
		Name:     "tessellate.model",
		Payload:  []byte(`{"detail":3}`),
		Attempt:  0,
		Deadline: time.Now().Add(time.Hour),
	}
}

func TestRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*exec.Request)
		wantErr error
	}{
		{
			name:   "valid",
			mutate: func(*exec.Request) {},
		},
		{
			name:    "missing name",
			mutate:  func(r *exec.Request) { r.Name = "" },
			wantErr: exec.ErrInvalidRequest,
		},
		{
			name:    "negative attempt",
			mutate:  func(r *exec.Request) { r.Attempt = -1 },
			wantErr: exec.ErrInvalidRequest,
		},
		{
			name:   "zero deadline is allowed",
			mutate: func(r *exec.Request) { r.Deadline = time.Time{} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validRequest()
			tt.mutate(req)

			err := req.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRequest_InputPathLookup(t *testing.T) {
	req := validRequest()
	req.Inputs = []exec.InputSlot{{Name: "model", Path: "model/scene.ifc"}}

	if got := req.InputPath("model"); got != "model/scene.ifc" {
		t.Errorf("InputPath(model) = %q, want %q", got, "model/scene.ifc")
	}
	if got := req.InputPath("absent"); got != "" {
		t.Errorf("InputPath(absent) = %q, want empty", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./exec/...`
Expected: FAIL — undefined: `exec.FingerprintOf`, `exec.Request`, `exec.ErrInvalidRequest`.

- [ ] **Step 3: Write the fingerprint implementation**

Create `exec/fingerprint.go`:

```go
package exec

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"sort"
)

// FingerprintOf derives a stable identifier for a handler set and the build
// that contains it.
//
// A sandbox verifies this before running anything. When the sandbox re-execs
// the worker's own binary the check always passes and costs one comparison;
// its purpose is the Policy.Image override, where a stale image would
// otherwise run an old handler and report success. Drift becomes an
// immediate, correctly-classified launch failure instead of a silent wrong
// answer.
func FingerprintOf(names []string, revision string) string {
	sorted := make([]string, len(names))
	copy(sorted, names)
	sort.Strings(sorted)

	h := sha256.New()
	// Length-prefix every element. Joining on a separator would let a
	// handler named "a\nb" hash identically to the pair {"a", "b"}.
	fmt.Fprintf(h, "%d:%s\n", len(revision), revision)
	for _, n := range sorted {
		fmt.Fprintf(h, "%d:%s\n", len(n), n)
	}

	return hex.EncodeToString(h.Sum(nil))
}

// Fingerprint derives the identifier for a handler set using this binary's
// VCS revision. When the revision is unavailable — a build without VCS
// stamping — it falls back to the empty revision, so the fingerprint still
// covers the handler names.
func Fingerprint(names []string) string {
	return FingerprintOf(names, buildRevision())
}

// buildRevision returns the VCS revision this binary was built from.
func buildRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			return s.Value
		}
	}

	return ""
}
```

- [ ] **Step 4: Write the request implementation**

Create `exec/request.go`:

```go
package exec

import (
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// ErrInvalidRequest marks a Request that cannot be executed as given.
var ErrInvalidRequest = errors.New("invalid execution request")

// InputSlot maps a declared input name to its location within InputDir.
// The path is relative, so the same Request describes the inputs whether
// the sandbox mounts them at /dispatch/in or reads them where they lie.
type InputSlot struct {
	Name string
	Path string
}

// PriorOutput is an artifact an earlier attempt of this job committed.
//
// A sandbox keeps its artifact rows in memory and cannot query the store,
// so without these Accessor.Existing would always answer "no" and a
// retried handler would redo work it had already finished. The output
// would still be correct, which is exactly why this is worth carrying
// explicitly: nothing would fail, it would just quietly cost twice.
type PriorOutput struct {
	Name string
	Ref  artifact.Ref
}

// Request is one execution attempt, fully described. Everything the
// handler needs crosses the boundary in this value; nothing is inherited
// from the worker's environment.
type Request struct {
	JobID   id.JobID
	Name    string
	Payload []byte
	Attempt int

	// Deadline is when the attempt must be killed. Zero means no deadline.
	Deadline time.Time

	// Fingerprint identifies the handler set the caller expects.
	Fingerprint string

	// InputDir holds staged inputs and is read-only to the handler.
	InputDir string
	// OutputDir is where the handler writes artifacts.
	OutputDir string

	Inputs       []InputSlot
	PriorOutputs []PriorOutput

	Policy Policy

	// ScopeAppID and ScopeOrgID label the attempt for logs and metrics.
	// They are identifiers, never credentials.
	ScopeAppID string
	ScopeOrgID string

	// Env is passed to out-of-process rungs. It is constructed, never
	// inherited, so the sandbox does not receive the worker's environment.
	Env map[string]string
}

// Validate reports whether the request is well formed.
func (r *Request) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("%w: empty job name", ErrInvalidRequest)
	}
	if r.Attempt < 0 {
		return fmt.Errorf("%w: negative attempt %d", ErrInvalidRequest, r.Attempt)
	}

	return nil
}

// InputPath returns the relative path of a declared input, or an empty
// string when the request carries no such input.
func (r *Request) InputPath(name string) string {
	for _, in := range r.Inputs {
		if in.Name == name {
			return in.Path
		}
	}

	return ""
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./exec/...`
Expected: PASS.

- [ ] **Step 6: Lint and commit**

```bash
golangci-lint run ./exec/...
git add exec/request.go exec/fingerprint.go exec/request_test.go exec/fingerprint_test.go
git commit -m "feat(exec): add the execution request and registry fingerprint

Request fully describes one attempt so nothing is inherited from the
worker's environment. PriorOutputs carries what earlier attempts
committed: a sandbox cannot query the store, so without it Existing would
answer no and a retried handler would silently redo finished work.

The fingerprint length-prefixes its elements rather than joining on a
separator, so a handler name containing the separator cannot impersonate a
different handler set."
```

---

## Task 4: The Executor interface, the executor registry, and the leaf-constraint test

**Files:**
- Create: `exec/executor.go`, `exec/registry.go`
- Test: `exec/registry_test.go`, `exec/deps_test.go`

**Interfaces:**
- Consumes: `Policy`, `Level` (Task 1); `Request`, `Result` (Tasks 2–3).
- Produces: `exec.Executor` interface with `Name() string`, `Level() Level`, `Run(context.Context, *Request) (*Result, error)`, `Reclaim(context.Context, id.WorkerID) error`, `Close() error`; `exec.Registry` with `NewRegistry(def Executor) *Registry`, `(*Registry).Add(Executor)`, `(*Registry).Default() Executor`, `(*Registry).Select(Policy) (Executor, error)`, `(*Registry).Executors() []Executor`; `exec.ErrNoExecutor`.

- [ ] **Step 1: Write the failing tests**

Create `exec/registry_test.go`:

```go
package exec_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
)

// fakeExecutor is a minimal Executor for registry tests.
type fakeExecutor struct {
	name  string
	level exec.Level
}

func (f fakeExecutor) Name() string     { return f.name }
func (f fakeExecutor) Level() exec.Level { return f.level }

func (f fakeExecutor) Run(context.Context, *exec.Request) (*exec.Result, error) {
	return &exec.Result{Status: exec.StatusOK}, nil
}

func (f fakeExecutor) Reclaim(context.Context, id.WorkerID) error { return nil }
func (f fakeExecutor) Close() error                               { return nil }

func TestRegistry_SelectPicksWeakestSufficient(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})
	r.Add(fakeExecutor{name: "subprocess", level: exec.LevelProcess})
	r.Add(fakeExecutor{name: "k8s", level: exec.LevelVM})

	// A job needing process isolation must not be handed the Kubernetes
	// rung when a cheaper sufficient one exists.
	got, err := r.Select(exec.NewPolicy(exec.Isolate(exec.LevelProcess)))
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "subprocess" {
		t.Errorf("Select() = %q, want %q", got.Name(), "subprocess")
	}
}

func TestRegistry_SelectEscalatesWhenExactRungAbsent(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})
	r.Add(fakeExecutor{name: "k8s", level: exec.LevelVM})

	got, err := r.Select(exec.NewPolicy(exec.Isolate(exec.LevelSandboxed)))
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "k8s" {
		t.Errorf("Select() = %q, want %q", got.Name(), "k8s")
	}
}

func TestRegistry_SelectRefusesSilentDowngrade(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})

	_, err := r.Select(exec.NewPolicy(exec.Isolate(exec.LevelSandboxed)))
	if !errors.Is(err, exec.ErrNoExecutor) {
		t.Fatalf("Select() error = %v, want %v", err, exec.ErrNoExecutor)
	}
}

func TestRegistry_SelectAllowsExplicitDowngrade(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})

	got, err := r.Select(exec.NewPolicy(
		exec.Isolate(exec.LevelSandboxed),
		exec.AllowDowngrade(),
	))
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "inprocess" {
		t.Errorf("Select() = %q, want %q", got.Name(), "inprocess")
	}
}

func TestRegistry_SelectDefaultForLevelNone(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})
	r.Add(fakeExecutor{name: "subprocess", level: exec.LevelProcess})

	got, err := r.Select(exec.NewPolicy())
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "inprocess" {
		t.Errorf("Select() = %q, want the default %q", got.Name(), "inprocess")
	}
}

func TestRegistry_AddReplacesSameName(t *testing.T) {
	r := exec.NewRegistry(fakeExecutor{name: "inprocess", level: exec.LevelNone})
	r.Add(fakeExecutor{name: "subprocess", level: exec.LevelProcess})
	r.Add(fakeExecutor{name: "subprocess", level: exec.LevelSandboxed})

	if n := len(r.Executors()); n != 2 {
		t.Fatalf("len(Executors()) = %d, want 2", n)
	}
	got, err := r.Select(exec.NewPolicy(exec.Isolate(exec.LevelSandboxed)))
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got.Name() != "subprocess" {
		t.Errorf("Select() = %q, want %q", got.Name(), "subprocess")
	}
}
```

Create `exec/deps_test.go`:

```go
package exec_test

import (
	"go/build"
	"strings"
	"testing"
)

// TestExecIsALeafPackage guards the import constraint the whole design
// rests on. job imports exec for Options.Execution, so exec importing job
// would be a cycle; importing worker or engine would drag the store, and
// with it the credentials, into a package the sandbox links.
func TestExecIsALeafPackage(t *testing.T) {
	const self = "github.com/xraph/dispatch/exec"

	allowed := map[string]bool{
		"github.com/xraph/dispatch":          true,
		"github.com/xraph/dispatch/id":       true,
		"github.com/xraph/dispatch/scope":    true,
		"github.com/xraph/dispatch/artifact": true,
	}

	pkg, err := build.Import(self, "", 0)
	if err != nil {
		t.Fatalf("import %s: %v", self, err)
	}

	for _, imp := range pkg.Imports {
		if !strings.HasPrefix(imp, "github.com/xraph/dispatch") {
			continue // standard library and third-party are fine
		}
		if !allowed[imp] {
			t.Errorf("exec imports %q, which breaks the leaf constraint", imp)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./exec/...`
Expected: FAIL — undefined: `exec.Executor`, `exec.NewRegistry`, `exec.ErrNoExecutor`.

- [ ] **Step 3: Write the Executor interface**

Create `exec/executor.go`:

```go
package exec

import (
	"context"

	"github.com/xraph/dispatch/id"
)

// Executor runs one job attempt. Implementations form an escalating ladder
// of isolation, and every one of them must pass the shared conformance
// suite in exec/exectest.
type Executor interface {
	// Name identifies the executor in configuration, logs, and metrics.
	Name() string

	// Level reports the isolation this executor actually provides, which
	// is what Registry.Select matches a Policy against.
	Level() Level

	// Run executes one attempt.
	//
	// The returned error is reserved for failures to launch — the handler
	// never ran. A handler that ran and failed is reported through
	// Result.Status, so the caller can tell a business failure from a
	// dead sandbox without inspecting error text.
	Run(ctx context.Context, req *Request) (*Result, error)

	// Reclaim releases sandboxes this worker leaked across a restart. It
	// runs once when the pool starts, and on the leader's behalf for
	// workers the cluster has declared dead.
	Reclaim(ctx context.Context, workerID id.WorkerID) error

	// Close releases the executor's own resources.
	Close() error
}
```

- [ ] **Step 4: Write the registry**

Create `exec/registry.go`:

```go
package exec

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrNoExecutor marks a policy no configured executor can satisfy.
var ErrNoExecutor = errors.New("no executor satisfies the policy")

// Registry holds the executors a deployment has configured and matches
// job policies against them.
//
// It is safe for concurrent use, though in practice it is built once at
// startup and only read afterwards.
type Registry struct {
	mu   sync.RWMutex
	def  Executor
	byName map[string]Executor
}

// NewRegistry creates a registry with a default executor, which is the one
// used by any job that declares no isolation requirement.
func NewRegistry(def Executor) *Registry {
	r := &Registry{
		def:    def,
		byName: make(map[string]Executor),
	}
	if def != nil {
		r.byName[def.Name()] = def
	}

	return r
}

// Add registers an executor, replacing any existing one with the same name.
func (r *Registry) Add(e Executor) {
	if e == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.byName[e.Name()] = e
}

// Default returns the executor used when a job declares no requirement.
func (r *Registry) Default() Executor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.def
}

// Executors returns every registered executor, ordered by name so callers
// and tests see a stable list.
func (r *Registry) Executors() []Executor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.byName))
	for n := range r.byName {
		names = append(names, n)
	}
	sort.Strings(names)

	out := make([]Executor, 0, len(names))
	for _, n := range names {
		out = append(out, r.byName[n])
	}

	return out
}

// Select returns the executor that should run a job with this policy.
//
// It picks the weakest executor that still satisfies the declared level,
// so a job needing a separate process is not handed a Kubernetes pod
// merely because one is configured. When nothing satisfies the policy the
// call fails rather than quietly running the handler with less isolation
// than it asked for — unless the policy opted into a downgrade.
func (r *Registry) Select(p Policy) (Executor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if p.Level == LevelNone {
		if r.def == nil {
			return nil, fmt.Errorf("%w: no default executor configured", ErrNoExecutor)
		}

		return r.def, nil
	}

	var best Executor
	for _, e := range r.byName {
		if e.Level() < p.Level {
			continue
		}
		if best == nil || e.Level() < best.Level() ||
			(e.Level() == best.Level() && e.Name() < best.Name()) {
			best = e
		}
	}
	if best != nil {
		return best, nil
	}

	if p.AllowDowngrade && r.def != nil {
		return r.def, nil
	}

	return nil, fmt.Errorf(
		"%w: policy requires level %s, configured executors are %s",
		ErrNoExecutor, p.Level, r.describeLocked(),
	)
}

// describeLocked renders the configured executors for an error message.
// The caller must hold at least a read lock.
func (r *Registry) describeLocked() string {
	if len(r.byName) == 0 {
		return "(none)"
	}

	names := make([]string, 0, len(r.byName))
	for n, e := range r.byName {
		names = append(names, fmt.Sprintf("%s(%s)", n, e.Level()))
	}
	sort.Strings(names)

	out := names[0]
	for _, n := range names[1:] {
		out += ", " + n
	}

	return out
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./exec/...`
Expected: PASS, including `TestExecIsALeafPackage`.

- [ ] **Step 6: Lint and commit**

```bash
gofmt -s -w exec/
golangci-lint run ./exec/...
git add exec/executor.go exec/registry.go exec/registry_test.go exec/deps_test.go
git commit -m "feat(exec): add the Executor interface and executor registry

Select picks the weakest executor that satisfies the declared level, so a
job needing a separate process is not handed a pod merely because one is
configured. A policy nothing satisfies fails rather than running with less
isolation than it asked for; downgrade is opt-in.

deps_test guards the leaf constraint: job imports exec, so exec importing
job would be a cycle, and importing worker or engine would link the store
into a package the sandbox loads."
```

---

## Task 5: `job.Registrable` and the execution policy on definitions

**Files:**
- Create: `job/registrable.go`
- Modify: `job/options.go`, `job/registry.go`
- Test: `job/registrable_test.go`

**Interfaces:**
- Consumes: `exec.Policy`, `exec.PolicyOption`, `exec.NewPolicy` (Task 1).
- Produces: `job.Registrable` interface with `Register(*Registry)`, `JobName() string`, and `Policy() exec.Policy`; the three corresponding methods on `*Definition[T]`; `job.Options.Execution exec.Policy`; `job.WithExecution(opts ...exec.PolicyOption) Option`; `(*Registry).Policy(name string) exec.Policy`.

**Why this task exists:** Go forbids generic methods, but a method *on* a generic type is legal. That is the only reason a heterogeneous `[]job.Registrable` can exist, and it is what lets Phase 2's credential-free entrypoint register the same handler set the worker uses.

- [ ] **Step 1: Write the failing test**

Create `job/registrable_test.go`:

```go
package job_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/job"
)

type meshPayload struct {
	Detail int `json:"detail"`
}

func TestDefinition_ImplementsRegistrable(t *testing.T) {
	// The whole out-of-process design depends on this compiling: a
	// heterogeneous slice of definitions with different payload types.
	defs := []job.Registrable{
		job.NewDefinition("send-email", func(_ context.Context, _ emailPayload) error { return nil }),
		job.NewDefinition("tessellate", func(_ context.Context, _ meshPayload) error { return nil }),
	}

	r := job.NewRegistry()
	for _, d := range defs {
		d.Register(r)
	}

	for _, want := range []string{"send-email", "tessellate"} {
		if _, ok := r.Get(want); !ok {
			t.Errorf("handler %q not registered", want)
		}
	}
}

func TestDefinition_JobName(t *testing.T) {
	d := job.NewDefinition("tessellate", func(_ context.Context, _ meshPayload) error { return nil })

	if got := d.JobName(); got != "tessellate" {
		t.Errorf("JobName() = %q, want %q", got, "tessellate")
	}
}

func TestWithExecution(t *testing.T) {
	d := job.NewDefinition("tessellate",
		func(_ context.Context, _ meshPayload) error { return nil },
		job.WithExecution(
			exec.Isolate(exec.LevelSandboxed),
			exec.GracePeriod(90*time.Second),
		),
	)

	if d.Opts.Execution.Level != exec.LevelSandboxed {
		t.Errorf("Level = %v, want %v", d.Opts.Execution.Level, exec.LevelSandboxed)
	}
	if d.Opts.Execution.GracePeriod != 90*time.Second {
		t.Errorf("GracePeriod = %v, want %v", d.Opts.Execution.GracePeriod, 90*time.Second)
	}
}

func TestDefaultOptions_HasUsableExecutionPolicy(t *testing.T) {
	// A definition that says nothing about execution must still carry a
	// usable grace period, or later rungs would kill instantly.
	d := job.NewDefinition("plain", func(_ context.Context, _ meshPayload) error { return nil })

	if d.Opts.Execution.Level != exec.LevelNone {
		t.Errorf("Level = %v, want %v", d.Opts.Execution.Level, exec.LevelNone)
	}
	if d.Opts.Execution.GracePeriod != exec.DefaultGracePeriod {
		t.Errorf("GracePeriod = %v, want %v", d.Opts.Execution.GracePeriod, exec.DefaultGracePeriod)
	}
}

func TestRegistry_Policy(t *testing.T) {
	r := job.NewRegistry()
	d := job.NewDefinition("tessellate",
		func(_ context.Context, _ meshPayload) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelVM)),
	)
	d.Register(r)

	if got := r.Policy("tessellate").Level; got != exec.LevelVM {
		t.Errorf("Policy(tessellate).Level = %v, want %v", got, exec.LevelVM)
	}
	// An unregistered name yields the zero policy with usable defaults.
	if got := r.Policy("absent").Level; got != exec.LevelNone {
		t.Errorf("Policy(absent).Level = %v, want %v", got, exec.LevelNone)
	}
	if got := r.Policy("absent").GracePeriod; got != exec.DefaultGracePeriod {
		t.Errorf("Policy(absent).GracePeriod = %v, want %v", got, exec.DefaultGracePeriod)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./job/...`
Expected: FAIL — undefined: `job.Registrable`, `job.WithExecution`, `d.Register`, `r.Policy`.

- [ ] **Step 3: Add the Registrable seam**

Create `job/registrable.go`:

```go
package job

// Registrable is a job definition that can register itself into a Registry
// without the caller knowing its payload type.
//
// Go forbids generic methods, but a method on a generic type is legal, so
// Definition[T] can satisfy this non-generic interface. That is what lets
// definitions with different payload types live in one slice — and a slice
// is what an out-of-process entrypoint can be handed, since it cannot be
// given the engine that would otherwise do the registering.
type Registrable interface {
	// Register adds this definition's handler to the registry.
	Register(r *Registry)

	// JobName returns the name the definition registers under.
	JobName() string

	// Policy returns the execution declaration, so a caller can check
	// that the deployment can satisfy it before registering anything.
	Policy() exec.Policy
}

// Register adds the definition's handler to the registry.
func (d *Definition[T]) Register(r *Registry) { RegisterDefinition(r, d) }

// JobName returns the name this definition registers under.
func (d *Definition[T]) JobName() string { return d.Name }

// Policy returns this definition's execution declaration.
func (d *Definition[T]) Policy() exec.Policy { return d.Opts.Execution }
```

Add the `exec` import to this file:

```go
import "github.com/xraph/dispatch/exec"
```

- [ ] **Step 4: Add the execution policy to Options**

In `job/options.go`, add the `exec` import, the `Execution` field, the default, and the option.

Add to the import block:

```go
	"github.com/xraph/dispatch/exec"
```

Add to the `Options` struct, after `Bindings`:

```go
	// Execution declares the minimum isolation this job's handler
	// requires. The zero value runs in-process, which is what every
	// existing definition gets.
	Execution exec.Policy
```

In `DefaultOptions`, add the field so the grace period is never zero:

```go
func DefaultOptions() Options {
	return Options{
		MaxRetries: 3,
		Queue:      "default",
		Priority:   0,
		Timeout:    5 * time.Minute,
		Execution:  exec.NewPolicy(),
	}
}
```

Append the option at the end of the file:

```go
// WithExecution declares the isolation this job's handler requires.
//
// It mirrors WithArtifactInputs: the exec package builds the value and
// job adapts it, which is what keeps exec a leaf that never imports job.
func WithExecution(opts ...exec.PolicyOption) Option {
	return func(o *Options) {
		p := o.Execution
		for _, opt := range opts {
			opt(&p)
		}
		o.Execution = p
	}
}
```

- [ ] **Step 5: Record the policy in the registry**

In `job/registry.go`, add the `exec` import, a `policies` map, its initialisation, its population, and the accessor.

Add to imports:

```go
	"github.com/xraph/dispatch/exec"
```

Add to the `Registry` struct after `inputs`:

```go
	// policies holds each job's execution declaration. The worker needs
	// it keyed by name for the same reason inputs are: at execution time
	// the typed definition is long gone.
	policies map[string]exec.Policy
```

In `NewRegistry`:

```go
		policies: make(map[string]exec.Policy),
```

In `RegisterDefinition`, after the inputs block:

```go
	r.policies[def.Name] = def.Opts.Execution
```

Add the accessor after `Inputs`:

```go
// Policy returns the execution declaration for a job. An unregistered name
// yields a default policy rather than a zero one, so callers always get a
// usable grace period.
func (r *Registry) Policy(name string) exec.Policy {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if p, ok := r.policies[name]; ok {
		return p
	}

	return exec.NewPolicy()
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./job/... ./exec/...`
Expected: PASS. The existing `job/registry_test.go` tests must still pass unchanged.

- [ ] **Step 7: Verify no import cycle and lint**

Run: `go build ./... && golangci-lint run ./job/... ./exec/...`
Expected: builds cleanly. If Go reports an import cycle, `exec` has gained a `job` import — revisit Task 4's `deps_test.go`.

- [ ] **Step 8: Commit**

```bash
git add job/registrable.go job/options.go job/registry.go job/registrable_test.go
git commit -m "feat(job): add the Registrable seam and execution policy

Go forbids generic methods but permits methods on generic types, so
(*Definition[T]).Register satisfies a non-generic interface. That is the
only reason a heterogeneous []job.Registrable can exist, and it is what
lets an out-of-process entrypoint register the same handler set the worker
uses without being handed an engine.

WithExecution mirrors WithArtifactInputs: exec builds the value and job
adapts it, keeping exec a leaf."
```

---

## Task 6: The in-process executor

**Files:**
- Create: `exec/inproc/inproc.go`, `exec/inproc/doc.go`
- Test: `exec/inproc/inproc_test.go`

**Interfaces:**
- Consumes: `exec.Executor`, `exec.Request`, `exec.Result`, `exec.Status`, `exec.Level` (Tasks 1–4); `job.Registry`, `job.HandlerFunc` (Task 5).
- Produces: `inproc.New(r *job.Registry) *inproc.Executor` satisfying `exec.Executor`, with `Name() == "inprocess"` and `Level() == exec.LevelNone`.

**Note:** `exec/inproc` imports both `exec` and `job`. That is fine and does not violate the leaf rule — the constraint is on `exec` itself, not on its sub-packages.

- [ ] **Step 1: Write the failing test**

Create `exec/inproc/inproc_test.go`:

```go
package inproc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

type payload struct {
	Value int `json:"value"`
}

func TestExecutor_Identity(t *testing.T) {
	e := inproc.New(job.NewRegistry())

	if got := e.Name(); got != "inprocess" {
		t.Errorf("Name() = %q, want %q", got, "inprocess")
	}
	if got := e.Level(); got != exec.LevelNone {
		t.Errorf("Level() = %v, want %v", got, exec.LevelNone)
	}
}

func TestExecutor_Run(t *testing.T) {
	sentinel := errors.New("boom")

	tests := []struct {
		name       string
		handler    func(context.Context, payload) error
		wantStatus exec.Status
		wantErrMsg string
	}{
		{
			name:       "success",
			handler:    func(context.Context, payload) error { return nil },
			wantStatus: exec.StatusOK,
		},
		{
			name:       "handler error",
			handler:    func(context.Context, payload) error { return sentinel },
			wantStatus: exec.StatusHandlerError,
			wantErrMsg: "boom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := job.NewRegistry()
			job.NewDefinition("test.job", tt.handler).Register(r)
			e := inproc.New(r)

			res, err := e.Run(context.Background(), &exec.Request{
				JobID:   id.NewJobID(),
				Name:    "test.job",
				Payload: []byte(`{"value":7}`),
			})
			if err != nil {
				t.Fatalf("Run() error = %v, want nil", err)
			}
			if res.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", res.Status, tt.wantStatus)
			}
			if res.HandlerErr != tt.wantErrMsg {
				t.Errorf("HandlerErr = %q, want %q", res.HandlerErr, tt.wantErrMsg)
			}
		})
	}
}

func TestExecutor_RunPassesPayload(t *testing.T) {
	var got payload
	r := job.NewRegistry()
	job.NewDefinition("test.job", func(_ context.Context, p payload) error {
		got = p
		return nil
	}).Register(r)

	_, err := inproc.New(r).Run(context.Background(), &exec.Request{
		JobID:   id.NewJobID(),
		Name:    "test.job",
		Payload: []byte(`{"value":42}`),
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got.Value != 42 {
		t.Errorf("payload.Value = %d, want 42", got.Value)
	}
}

func TestExecutor_RunUnknownHandlerIsALaunchFailure(t *testing.T) {
	// The handler never ran, so this must not consume the retry budget.
	res, err := inproc.New(job.NewRegistry()).Run(context.Background(), &exec.Request{
		JobID: id.NewJobID(),
		Name:  "absent",
	})
	if err != nil {
		t.Fatalf("Run() error = %v, want a Result", err)
	}
	if res.Status != exec.StatusLaunchFailed {
		t.Fatalf("Status = %q, want %q", res.Status, exec.StatusLaunchFailed)
	}
	if res.Status.CountsAgainstRetries() {
		t.Error("an unknown handler must not consume the retry budget")
	}
}

func TestExecutor_RunInvalidRequest(t *testing.T) {
	_, err := inproc.New(job.NewRegistry()).Run(context.Background(), &exec.Request{})
	if !errors.Is(err, exec.ErrInvalidRequest) {
		t.Fatalf("Run() error = %v, want %v", err, exec.ErrInvalidRequest)
	}
}

func TestExecutor_RunCancelledContext(t *testing.T) {
	r := job.NewRegistry()
	job.NewDefinition("test.job", func(ctx context.Context, _ payload) error {
		<-ctx.Done()
		return ctx.Err()
	}).Register(r)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	res, err := inproc.New(r).Run(ctx, &exec.Request{
		JobID: id.NewJobID(),
		Name:  "test.job",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	// In-process cancellation is cooperative: the handler chose to
	// return, so this is a handler error, not an enforced timeout.
	if res.Status != exec.StatusHandlerError {
		t.Errorf("Status = %q, want %q", res.Status, exec.StatusHandlerError)
	}
}

func TestExecutor_RunRecordsWallTime(t *testing.T) {
	r := job.NewRegistry()
	job.NewDefinition("test.job", func(context.Context, payload) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	}).Register(r)

	res, err := inproc.New(r).Run(context.Background(), &exec.Request{
		JobID: id.NewJobID(),
		Name:  "test.job",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Usage.WallTime <= 0 {
		t.Errorf("Usage.WallTime = %v, want > 0", res.Usage.WallTime)
	}
}

func TestExecutor_ReclaimAndClose(t *testing.T) {
	e := inproc.New(job.NewRegistry())

	if err := e.Reclaim(context.Background(), id.NewWorkerID()); err != nil {
		t.Errorf("Reclaim() = %v, want nil", err)
	}
	if err := e.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./exec/inproc/...`
Expected: FAIL — no Go files in `exec/inproc`.

- [ ] **Step 3: Write the implementation**

Create `exec/inproc/doc.go`:

```go
// Package inproc runs job handlers in the worker process.
//
// This is Dispatch's original behaviour and remains the default. It
// provides no isolation: the handler shares the worker's memory,
// credentials, file descriptors, and network. That is the right trade for
// handlers that do not touch untrusted bytes, where launching a process
// per job would be pure overhead, and the wrong one for anything parsing
// a customer upload with a memory-unsafe library.
package inproc
```

Create `exec/inproc/inproc.go`:

```go
package inproc

import (
	"context"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Name is the identifier this executor registers under.
const Name = "inprocess"

// Executor runs handlers in the worker process.
type Executor struct {
	registry *job.Registry
}

var _ exec.Executor = (*Executor)(nil)

// New creates an in-process executor backed by a handler registry.
func New(r *job.Registry) *Executor {
	return &Executor{registry: r}
}

// Name identifies the executor.
func (e *Executor) Name() string { return Name }

// Level reports that this executor provides no isolation.
func (e *Executor) Level() exec.Level { return exec.LevelNone }

// Run looks the handler up by name and calls it.
func (e *Executor) Run(ctx context.Context, req *exec.Request) (*exec.Result, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	handler, ok := e.registry.Get(req.Name)
	if !ok {
		// The handler never ran, so this is a launch failure rather than
		// a job failure, and must not consume the retry budget.
		return &exec.Result{
			Status:     exec.StatusLaunchFailed,
			HandlerErr: "no handler registered for job " + req.Name,
		}, nil
	}

	start := time.Now()
	err := handler(ctx, req.Payload)
	elapsed := time.Since(start)

	res := &exec.Result{
		Status: exec.StatusOK,
		Usage:  exec.Usage{WallTime: elapsed},
	}
	if err != nil {
		res.Status = exec.StatusHandlerError
		res.HandlerErr = err.Error()
	}

	return res, nil
}

// Reclaim is a no-op. An in-process handler cannot outlive the worker
// that called it, so there is never anything to reclaim.
func (e *Executor) Reclaim(context.Context, id.WorkerID) error { return nil }

// Close is a no-op. The executor owns no resources of its own.
func (e *Executor) Close() error { return nil }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./exec/...`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
golangci-lint run ./exec/...
git add exec/inproc/
git commit -m "feat(exec): add the in-process executor

Preserves today's behaviour exactly and stays the default. An unknown
handler is reported as a launch failure rather than a handler error, so it
does not consume the job's retry budget: the handler never ran, and three
retries against a registration mistake would send the job to the DLQ for
an operator error."
```

---

## Task 7: The conformance suite

**Files:**
- Create: `exec/exectest/doc.go`, `exec/exectest/handlers.go`, `exec/exectest/suite.go`
- Test: `exec/exectest/suite_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1–6.
- Produces: `exectest.Handlers() []job.Registrable` — the fixture handler set every rung must be able to run; `exectest.HandlerNames() []string`; `exectest.Capabilities{Enforces bool; ReportsUsage bool; IsolatesMemory bool}`; `exectest.RunSuite(t *testing.T, name string, newExecutor func(*testing.T) exec.Executor, caps Capabilities)`.

**Why capabilities:** the suite runs against every rung, but the rungs genuinely differ. In-process cannot enforce a deadline or survive an OOM, and asserting it does would make the suite unimplementable. `Capabilities` states what a rung claims, and the suite asserts the shared behaviour for everyone plus the enforcement behaviour only for rungs that claim it. Later phases flip a flag rather than fork the suite.

- [ ] **Step 1: Write the fixture handlers**

Create `exec/exectest/doc.go`:

```go
// Package exectest is the conformance suite every exec.Executor must pass.
//
// The rungs of the isolation ladder are meant to be interchangeable: the
// same handler, the same payload, and the same declared inputs must behave
// the same way whether the handler runs in-process or in a pod. One shared
// table-driven suite is how that stays true, and it is what lets a new rung
// land without redesigning the ones before it.
//
// Rungs differ in what they can enforce — in-process cannot kill a handler
// that ignores its deadline — so a rung declares its Capabilities and the
// suite asserts the enforcement cases only against rungs that claim them.
package exectest
```

Create `exec/exectest/handlers.go`:

```go
package exectest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/xraph/dispatch/job"
)

// Job names the suite installs. Every executor under test must be able to
// run all of them.
const (
	JobOK          = "exectest.ok"
	JobError       = "exectest.error"
	JobPanic       = "exectest.panic"
	JobSlow        = "exectest.slow"
	JobEcho        = "exectest.echo"
	JobWriteOutput = "exectest.write_output"
	JobReadInput   = "exectest.read_input"
)

// ErrIntentional is what JobError returns, so tests can match it exactly.
var ErrIntentional = errors.New("intentional failure")

// EchoPayload is the payload JobEcho round-trips.
type EchoPayload struct {
	Value string `json:"value"`
}

// SlowPayload controls how long JobSlow sleeps.
type SlowPayload struct {
	SleepMillis int  `json:"sleep_millis"`
	IgnoreCtx   bool `json:"ignore_ctx"`
}

// OutputPayload controls what JobWriteOutput writes.
type OutputPayload struct {
	Name  string `json:"name"`
	Bytes int    `json:"bytes"`
}

// InputPayload names the input JobReadInput reads.
type InputPayload struct {
	Name string `json:"name"`
}

// echoed records what JobEcho last received, for the in-process case where
// the suite can observe it directly.
var echoed string

// Echoed returns the value JobEcho last received.
func Echoed() string { return echoed }

// Handlers returns the fixture handler set. Registering these is all an
// executor needs to be run through the suite.
func Handlers() []job.Registrable {
	return []job.Registrable{
		job.NewDefinition(JobOK, func(context.Context, struct{}) error {
			return nil
		}),
		job.NewDefinition(JobError, func(context.Context, struct{}) error {
			return ErrIntentional
		}),
		job.NewDefinition(JobPanic, func(context.Context, struct{}) error {
			panic("intentional panic")
		}),
		job.NewDefinition(JobSlow, func(ctx context.Context, p SlowPayload) error {
			d := time.Duration(p.SleepMillis) * time.Millisecond
			if p.IgnoreCtx {
				// Stands in for a native library that has stopped
				// honouring cancellation. Only a rung that can kill
				// will stop this.
				time.Sleep(d)
				return nil
			}
			select {
			case <-time.After(d):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
		job.NewDefinition(JobEcho, func(_ context.Context, p EchoPayload) error {
			echoed = p.Value
			return nil
		}),
		job.NewDefinition(JobWriteOutput, func(ctx context.Context, p OutputPayload) error {
			return writeOutput(ctx, p)
		}),
		job.NewDefinition(JobReadInput, func(ctx context.Context, p InputPayload) error {
			return readInput(ctx, p)
		}),
	}
}

// HandlerNames returns the fixture job names, which is what a fingerprint
// is derived from.
func HandlerNames() []string {
	defs := Handlers()
	names := make([]string, 0, len(defs))
	for _, d := range defs {
		names = append(names, d.JobName())
	}

	return names
}

// outputDirKey is how the suite tells the fixture handlers where to write
// when they run in-process. Out-of-process rungs set DISPATCH_OUTPUT_DIR
// instead, which is why the handler checks both.
type outputDirKey struct{}

// WithOutputDir attaches an output directory to a context.
func WithOutputDir(ctx context.Context, dir string) context.Context {
	return context.WithValue(ctx, outputDirKey{}, dir)
}

// WithInputDir attaches an input directory to a context.
func WithInputDir(ctx context.Context, dir string) context.Context {
	return context.WithValue(ctx, inputDirKey{}, dir)
}

type inputDirKey struct{}

func dirFrom(ctx context.Context, key any, env string) string {
	if v, ok := ctx.Value(key).(string); ok && v != "" {
		return v
	}

	return os.Getenv(env)
}

func writeOutput(ctx context.Context, p OutputPayload) error {
	dir := dirFrom(ctx, outputDirKey{}, "DISPATCH_OUTPUT_DIR")
	if dir == "" {
		return errors.New("exectest: no output directory")
	}
	buf := make([]byte, p.Bytes)
	for i := range buf {
		buf[i] = byte('a' + i%26)
	}

	//nolint:gosec // fixture output in a test directory
	return os.WriteFile(filepath.Join(dir, p.Name), buf, 0o644)
}

func readInput(ctx context.Context, p InputPayload) error {
	dir := dirFrom(ctx, inputDirKey{}, "DISPATCH_INPUT_DIR")
	if dir == "" {
		return errors.New("exectest: no input directory")
	}
	b, err := os.ReadFile(filepath.Join(dir, p.Name)) //nolint:gosec // fixture input
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return errors.New("exectest: input was empty")
	}

	return nil
}
```

- [ ] **Step 2: Write the suite**

Create `exec/exectest/suite.go`:

```go
package exectest

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
)

// Capabilities describes what a rung can actually do, so the suite asserts
// enforcement only against rungs that provide it.
type Capabilities struct {
	// Enforces means the rung can stop a handler that ignores its
	// deadline. Only out-of-process rungs can.
	Enforces bool

	// ReportsUsage means the rung measures CPU time and peak memory
	// rather than only wall time.
	ReportsUsage bool

	// IsolatesPanic means a panicking handler does not take the caller
	// down, so the rung reports it as a failed attempt rather than
	// relying on the worker's recover middleware.
	IsolatesPanic bool
}

// RunSuite runs the conformance suite against one executor implementation.
//
// newExecutor is called per subtest so each case gets a clean executor.
// The returned executor must already have the fixture Handlers registered.
func RunSuite(t *testing.T, name string, newExecutor func(*testing.T) exec.Executor, caps Capabilities) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		t.Run("Identity", func(t *testing.T) { testIdentity(t, newExecutor) })
		t.Run("Success", func(t *testing.T) { testSuccess(t, newExecutor) })
		t.Run("HandlerError", func(t *testing.T) { testHandlerError(t, newExecutor) })
		t.Run("UnknownHandler", func(t *testing.T) { testUnknownHandler(t, newExecutor) })
		t.Run("InvalidRequest", func(t *testing.T) { testInvalidRequest(t, newExecutor) })
		t.Run("PayloadRoundTrip", func(t *testing.T) { testPayloadRoundTrip(t, newExecutor) })
		t.Run("LargePayload", func(t *testing.T) { testLargePayload(t, newExecutor) })
		t.Run("Cancellation", func(t *testing.T) { testCancellation(t, newExecutor) })
		t.Run("WallTimeRecorded", func(t *testing.T) { testWallTime(t, newExecutor) })
		t.Run("Reclaim", func(t *testing.T) { testReclaim(t, newExecutor) })

		if caps.Enforces {
			t.Run("DeadlineEnforced", func(t *testing.T) { testDeadlineEnforced(t, newExecutor) })
		}
		if caps.IsolatesPanic {
			t.Run("PanicIsolated", func(t *testing.T) { testPanicIsolated(t, newExecutor) })
		}
		if caps.ReportsUsage {
			t.Run("UsageReported", func(t *testing.T) { testUsageReported(t, newExecutor) })
		}
	})
}

func request(name string, payload any) *exec.Request {
	raw, _ := json.Marshal(payload)

	return &exec.Request{
		JobID:       id.NewJobID(),
		Name:        name,
		Payload:     raw,
		Fingerprint: exec.Fingerprint(HandlerNames()),
		Policy:      exec.NewPolicy(),
	}
}

func testIdentity(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	e := newExecutor(t)
	if e.Name() == "" {
		t.Error("Name() is empty")
	}
	if err := e.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

func testSuccess(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(), request(JobOK, struct{}{}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Status != exec.StatusOK {
		t.Errorf("Status = %q, want %q (handler err: %q)", res.Status, exec.StatusOK, res.HandlerErr)
	}
	if res.Err() != nil {
		t.Errorf("Err() = %v, want nil", res.Err())
	}
}

func testHandlerError(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(), request(JobError, struct{}{}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Status != exec.StatusHandlerError {
		t.Fatalf("Status = %q, want %q", res.Status, exec.StatusHandlerError)
	}
	if res.HandlerErr != ErrIntentional.Error() {
		t.Errorf("HandlerErr = %q, want %q", res.HandlerErr, ErrIntentional.Error())
	}
	if !errors.Is(res.Err(), exec.ErrHandler) {
		t.Errorf("Err() = %v, want it to wrap ErrHandler", res.Err())
	}
}

func testUnknownHandler(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(), request("exectest.absent", struct{}{}))
	if err != nil {
		t.Fatalf("Run() error = %v, want a Result", err)
	}
	if res.Status != exec.StatusLaunchFailed {
		t.Fatalf("Status = %q, want %q", res.Status, exec.StatusLaunchFailed)
	}
	if res.Status.CountsAgainstRetries() {
		t.Error("an unknown handler must not consume the retry budget")
	}
}

func testInvalidRequest(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	_, err := newExecutor(t).Run(context.Background(), &exec.Request{})
	if !errors.Is(err, exec.ErrInvalidRequest) {
		t.Fatalf("Run() error = %v, want ErrInvalidRequest", err)
	}
}

func testPayloadRoundTrip(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(),
		request(JobEcho, EchoPayload{Value: "hello boundary"}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want %q (handler err: %q)", res.Status, exec.StatusOK, res.HandlerErr)
	}
}

func testLargePayload(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	// Large enough to exceed a pipe buffer, so any rung that frames the
	// request over a descriptor is exercised rather than accidentally
	// fitting in one write.
	big := make([]byte, 1<<20)
	for i := range big {
		big[i] = byte('a' + i%26)
	}

	res, err := newExecutor(t).Run(context.Background(),
		request(JobEcho, EchoPayload{Value: string(big)}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Status != exec.StatusOK {
		t.Errorf("Status = %q, want %q (handler err: %q)", res.Status, exec.StatusOK, res.HandlerErr)
	}
}

func testCancellation(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	res, err := newExecutor(t).Run(ctx,
		request(JobSlow, SlowPayload{SleepMillis: 5000, IgnoreCtx: false}))
	if err != nil {
		// An out-of-process rung may surface cancellation as a launch
		// error; either shape is acceptable so long as it returns.
		return
	}
	if res.Status == exec.StatusOK {
		t.Error("Status = ok, want a failure after cancellation")
	}
}

func testWallTime(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(),
		request(JobSlow, SlowPayload{SleepMillis: 20}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Usage.WallTime <= 0 {
		t.Errorf("Usage.WallTime = %v, want > 0", res.Usage.WallTime)
	}
}

func testReclaim(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	// Reclaim must be safe to call when there is nothing to reclaim,
	// because the pool calls it unconditionally at startup.
	if err := newExecutor(t).Reclaim(context.Background(), id.NewWorkerID()); err != nil {
		t.Errorf("Reclaim() = %v, want nil", err)
	}
}

func testDeadlineEnforced(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	req := request(JobSlow, SlowPayload{SleepMillis: 30000, IgnoreCtx: true})
	req.Deadline = time.Now().Add(300 * time.Millisecond)
	req.Policy = exec.NewPolicy(exec.GracePeriod(200 * time.Millisecond))

	start := time.Now()
	res, err := newExecutor(t).Run(context.Background(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Status != exec.StatusTimeout {
		t.Errorf("Status = %q, want %q", res.Status, exec.StatusTimeout)
	}
	// The handler asked to sleep 30s and ignores cancellation. Anything
	// close to that means the rung did not actually kill it.
	if elapsed > 10*time.Second {
		t.Errorf("Run() took %v, want the deadline to be enforced", elapsed)
	}
}

func testPanicIsolated(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(), request(JobPanic, struct{}{}))
	if err != nil {
		return // a launch-shaped error is acceptable
	}
	if res.Status == exec.StatusOK {
		t.Error("Status = ok, want a failure for a panicking handler")
	}
}

func testUsageReported(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(),
		request(JobSlow, SlowPayload{SleepMillis: 50}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Usage.PeakRSS <= 0 {
		t.Errorf("Usage.PeakRSS = %d, want > 0", res.Usage.PeakRSS)
	}
}

// TempDirs creates the input and output directories a rung needs, and is
// exported so each rung's test wiring can use the same layout.
func TempDirs(t *testing.T) (inputDir, outputDir string) {
	t.Helper()

	root := t.TempDir()
	inputDir = filepath.Join(root, "in")
	outputDir = filepath.Join(root, "out")
	for _, d := range []string{inputDir, outputDir} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	return inputDir, outputDir
}
```

- [ ] **Step 3: Wire the in-process executor into the suite**

Create `exec/exectest/suite_test.go`:

```go
package exectest_test

import (
	"testing"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/exectest"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/job"
)

func TestInProcessConformance(t *testing.T) {
	exectest.RunSuite(t, "inprocess", func(*testing.T) exec.Executor {
		r := job.NewRegistry()
		for _, d := range exectest.Handlers() {
			d.Register(r)
		}

		return inproc.New(r)
	}, exectest.Capabilities{
		// In-process enforces nothing: it cannot kill a handler that
		// ignores cancellation, it has no separate address space to
		// measure, and a panic propagates to the caller, which is what
		// the worker's recover middleware is for.
		Enforces:      false,
		ReportsUsage:  false,
		IsolatesPanic: false,
	})
}
```

- [ ] **Step 4: Run the suite**

Run: `go test ./exec/... -v -run Conformance`
Expected: PASS. Every subtest listed under `TestInProcessConformance/inprocess/...` runs; the three capability-gated ones are absent.

- [ ] **Step 5: Lint and commit**

```bash
gofmt -s -w exec/
golangci-lint run ./exec/...
git add exec/exectest/
git commit -m "feat(exec): add the executor conformance suite

One table-driven suite every rung must pass, so the ladder stays
interchangeable: the same handler and payload behave the same whether they
run in-process or in a pod.

Rungs declare Capabilities rather than the suite forking per rung.
In-process genuinely cannot enforce a deadline or isolate a panic, and
asserting that it does would make the suite unimplementable; a later rung
flips a flag instead of copying the file."
```

---

## Task 8: `worker.Runner` — rename and delegate to the executor

**Files:**
- Rename: `worker/executor.go` → `worker/runner.go`
- Create: `worker/executor_compat.go`
- Modify: `worker/runner.go`
- Test: `worker/runner_test.go`

**Interfaces:**
- Consumes: `exec.Executor`, `exec.Request`, `exec.Result` (Tasks 1–4); `job.Registry.Policy` (Task 5).
- Produces: `worker.Runner` with `NewRunner(registry *job.Registry, extensions *ext.Registry, store job.Store, dlqService *dlq.Service, bo backoff.Strategy, executors *exec.Registry, logger log.Logger, mws ...middleware.Middleware) *Runner`; `worker.Executor = Runner` type alias; deprecated `worker.NewExecutor` preserving the old signature.

**Backward-compatibility requirement:** `worker.NewExecutor` keeps its exact current parameter list and returns `*Runner`. Existing callers must compile untouched. Passing a nil `*exec.Registry` must fall back to calling the handler directly, so `NewExecutor` needs no executor registry.

- [ ] **Step 1: Write the failing test**

Create `worker/runner_test.go`:

```go
package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/worker"
)

// recordingExecutor captures the Request the runner built.
type recordingExecutor struct {
	got    *exec.Request
	result *exec.Result
	err    error
}

func (r *recordingExecutor) Name() string      { return "recording" }
func (r *recordingExecutor) Level() exec.Level { return exec.LevelProcess }

func (r *recordingExecutor) Run(_ context.Context, req *exec.Request) (*exec.Result, error) {
	r.got = req
	if r.err != nil {
		return nil, r.err
	}
	if r.result != nil {
		return r.result, nil
	}

	return &exec.Result{Status: exec.StatusOK}, nil
}

func (r *recordingExecutor) Reclaim(context.Context, id.WorkerID) error { return nil }
func (r *recordingExecutor) Close() error                              { return nil }

func newTestRunner(t *testing.T, reg *job.Registry, executors *exec.Registry) (*worker.Runner, *fakeJobStore) {
	t.Helper()

	store := newFakeJobStore()

	return worker.NewRunner(
		reg,
		ext.NewRegistry(log.NewNoopLogger()),
		store,
		nil,
		backoff.NewExponential(time.Second, time.Hour),
		executors,
		log.NewNoopLogger(),
	), store
}

func TestRunner_ExecuteBuildsRequestFromJob(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingExecutor{}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	j := &job.Job{
		ID:         id.NewJobID(),
		Name:       "test.job",
		Payload:    []byte(`{"a":1}`),
		RetryCount: 2,
		MaxRetries: 3,
		ScopeAppID: "app_1",
		ScopeOrgID: "org_1",
	}

	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
	if rec.got == nil {
		t.Fatal("executor was not called")
	}
	if rec.got.Name != "test.job" {
		t.Errorf("Request.Name = %q, want %q", rec.got.Name, "test.job")
	}
	if rec.got.Attempt != 2 {
		t.Errorf("Request.Attempt = %d, want 2", rec.got.Attempt)
	}
	if rec.got.ScopeAppID != "app_1" || rec.got.ScopeOrgID != "org_1" {
		t.Errorf("Request scope = (%q, %q), want (app_1, org_1)", rec.got.ScopeAppID, rec.got.ScopeOrgID)
	}
	if rec.got.Policy.Level != exec.LevelProcess {
		t.Errorf("Request.Policy.Level = %v, want %v", rec.got.Policy.Level, exec.LevelProcess)
	}
}

func TestRunner_ExecuteRoutesByPolicy(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("plain.job", func(context.Context, struct{}) error { return nil }).Register(reg)

	rec := &recordingExecutor{}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	// No declared isolation, so this must go to the default executor and
	// never reach the recording one.
	j := &job.Job{ID: id.NewJobID(), Name: "plain.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
	if rec.got != nil {
		t.Error("a job with no declared isolation was routed to the isolated executor")
	}
}

func TestRunner_LaunchFailureDoesNotConsumeRetries(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingExecutor{
		result: &exec.Result{Status: exec.StatusLaunchFailed, HandlerErr: "image pull backoff"},
	}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, store := newTestRunner(t, reg, executors)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	err := runner.Execute(context.Background(), j)
	if err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}
	if j.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0 — a launch failure is infrastructure", j.RetryCount)
	}
	if j.State != job.StatePending && j.State != job.StateRetrying {
		t.Errorf("State = %q, want the job requeued", j.State)
	}
	if store.updates == 0 {
		t.Error("the job was never persisted")
	}
}

func TestRunner_HandlerErrorConsumesRetries(t *testing.T) {
	sentinel := errors.New("bad file")

	reg := job.NewRegistry()
	job.NewDefinition("test.job", func(context.Context, struct{}) error { return sentinel }).Register(reg)

	runner, _ := newTestRunner(t, reg, exec.NewRegistry(inproc.New(reg)))

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}
	if j.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", j.RetryCount)
	}
}

func TestNewExecutor_StillCompilesAndRuns(t *testing.T) {
	// The deprecated constructor must keep working for existing callers.
	reg := job.NewRegistry()
	job.NewDefinition("test.job", func(context.Context, struct{}) error { return nil }).Register(reg)

	e := worker.NewExecutor(
		reg,
		ext.NewRegistry(log.NewNoopLogger()),
		newFakeJobStore(),
		nil,
		backoff.NewExponential(time.Second, time.Hour),
		log.NewNoopLogger(),
	)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := e.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
	if j.State != job.StateCompleted {
		t.Errorf("State = %q, want %q", j.State, job.StateCompleted)
	}
}
```

`worker/pool_test.go` defines no reusable `fakeJobStore`, so add this complete one to `worker/runner_test.go`. All nine `job.Store` methods are stubbed; only `UpdateJob` does anything, because it is the only one the runner calls.

```go
// fakeJobStore is a job.Store that records UpdateJob calls. Only the
// method the runner uses does anything.
type fakeJobStore struct {
	updates int
}

func newFakeJobStore() *fakeJobStore { return &fakeJobStore{} }

func (f *fakeJobStore) UpdateJob(context.Context, *job.Job) error {
	f.updates++
	return nil
}

func (f *fakeJobStore) EnqueueJob(context.Context, *job.Job) error { return nil }

func (f *fakeJobStore) DequeueJobs(context.Context, []string, int) ([]*job.Job, error) {
	return nil, nil
}

func (f *fakeJobStore) GetJob(context.Context, id.JobID) (*job.Job, error) { return nil, nil }

func (f *fakeJobStore) DeleteJob(context.Context, id.JobID) error { return nil }

func (f *fakeJobStore) ListJobsByState(
	context.Context, job.State, job.ListOpts,
) ([]*job.Job, error) {
	return nil, nil
}

func (f *fakeJobStore) HeartbeatJob(context.Context, id.JobID, id.WorkerID) error { return nil }

func (f *fakeJobStore) ReapStaleJobs(context.Context, time.Duration) ([]*job.Job, error) {
	return nil, nil
}

func (f *fakeJobStore) CountJobs(context.Context, job.CountOpts) (int64, error) { return 0, nil }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./worker/...`
Expected: FAIL — undefined: `worker.NewRunner`, `worker.Runner`.

- [ ] **Step 3: Rename the file and the type**

```bash
git mv worker/executor.go worker/runner.go
```

In `worker/runner.go`, rename the type and constructor, add the executor registry field, and change the terminal closure. The struct becomes:

```go
// Runner executes a single job attempt: it selects an executor from the
// job's policy, runs the attempt through the middleware chain, then
// handles retry logic, DLQ push, state updates, and lifecycle events.
//
// Runner orchestrates the attempt. It does not itself invoke the handler —
// that is exec.Executor's job, which is what lets the same attempt run
// in-process or in a pod without this file changing.
type Runner struct {
	registry   *job.Registry
	extensions *ext.Registry
	store      job.Store
	dlqService *dlq.Service
	backoff    backoff.Strategy
	executors  *exec.Registry
	mw         middleware.Middleware
	logger     log.Logger
}

// NewRunner creates a Runner with the given dependencies.
//
// A nil executors registry means handlers are called directly, which is
// the behaviour the deprecated NewExecutor preserves.
func NewRunner(
	registry *job.Registry,
	extensions *ext.Registry,
	store job.Store,
	dlqService *dlq.Service,
	bo backoff.Strategy,
	executors *exec.Registry,
	logger log.Logger,
	mws ...middleware.Middleware,
) *Runner {
	return &Runner{
		registry:   registry,
		extensions: extensions,
		store:      store,
		dlqService: dlqService,
		backoff:    bo,
		executors:  executors,
		mw:         middleware.Chain(mws...),
		logger:     logger,
	}
}
```

Replace the body of `Execute` down to the middleware call. Everything from `elapsed := time.Since(start)` onward stays exactly as it is, except that the receiver becomes `r *Runner` throughout the file and `e.` becomes `r.`:

```go
// Execute runs a job through the middleware chain and its executor.
// On success: marks completed, emits JobCompleted.
// On failure with retries remaining: marks retrying with backoff, emits JobRetrying.
// On failure with retries exhausted: marks failed, pushes to DLQ, emits JobFailed + JobDLQ.
func (r *Runner) Execute(ctx context.Context, j *job.Job) error {
	terminal, err := r.terminalFor(j)
	if err != nil {
		return err
	}

	start := time.Now()
	execErr := r.mw(ctx, j, terminal)
	elapsed := time.Since(start)

	now := time.Now().UTC()
	j.UpdatedAt = now

	if execErr != nil {
		return r.handleFailure(ctx, j, execErr, now)
	}

	return r.handleSuccess(ctx, j, now, elapsed)
}

// terminalFor builds the innermost handler for this job.
//
// Everything cross-cutting — recover, tracing, metrics, logging, scope,
// timeout, and artifact staging — wraps this closure, which is precisely
// why staging keeps running in the worker process and an out-of-process
// handler receives a directory rather than storage credentials.
func (r *Runner) terminalFor(j *job.Job) (middleware.Handler, error) {
	if r.executors == nil {
		handler, ok := r.registry.Get(j.Name)
		if !ok {
			return nil, fmt.Errorf("no handler registered for job %q", j.Name)
		}

		return func(ctx context.Context) error {
			return handler(ctx, j.Payload)
		}, nil
	}

	policy := r.registry.Policy(j.Name)
	executor, err := r.executors.Select(policy)
	if err != nil {
		return nil, fmt.Errorf("dispatch/worker: select executor for job %q: %w", j.Name, err)
	}

	return func(ctx context.Context) error {
		res, runErr := executor.Run(ctx, r.request(j, policy))
		if runErr != nil {
			return runErr
		}

		return res.Err()
	}, nil
}

// request builds the execution request for one attempt.
func (r *Runner) request(j *job.Job, policy exec.Policy) *exec.Request {
	req := &exec.Request{
		JobID:      j.ID,
		Name:       j.Name,
		Payload:    j.Payload,
		Attempt:    j.RetryCount,
		Policy:     policy,
		ScopeAppID: j.ScopeAppID,
		ScopeOrgID: j.ScopeOrgID,
	}
	if j.Timeout > 0 {
		req.Deadline = time.Now().Add(j.Timeout)
	}

	return req
}
```

Add `"github.com/xraph/dispatch/exec"` to the imports.

- [ ] **Step 4: Make launch failures skip the retry counter**

In `handleFailure`, branch before incrementing. Replace the existing body:

```go
// handleFailure either requeues the job or increments the retry counter and
// retries, depending on whether the failure was the work's fault.
func (r *Runner) handleFailure(ctx context.Context, j *job.Job, handlerErr error, now time.Time) error {
	j.LastError = handlerErr.Error()

	// A launch failure means the handler never ran: an image that would
	// not pull, an exhausted quota, a missing runtime. Consuming the
	// retry budget for it would let one bad node send healthy work to
	// the DLQ, so the job is requeued without counting the attempt.
	var execErr *exec.Error
	if errors.As(handlerErr, &execErr) && !execErr.Status.CountsAgainstRetries() {
		return r.requeueAfterLaunchFailure(ctx, j, now)
	}

	j.RetryCount++

	if j.RetryCount <= j.MaxRetries {
		return r.scheduleRetry(ctx, j, now)
	}

	return r.sendToDLQ(ctx, j, handlerErr)
}

// requeueAfterLaunchFailure returns the job to pending with a backoff
// delay derived from the retry count without advancing it.
func (r *Runner) requeueAfterLaunchFailure(ctx context.Context, j *job.Job, now time.Time) error {
	delay := r.backoff.Delay(j.RetryCount + 1)
	j.RunAt = now.Add(delay)
	j.State = job.StatePending

	if updateErr := r.store.UpdateJob(ctx, j); updateErr != nil {
		r.logger.Error("failed to requeue job after launch failure",
			log.String("job_id", j.ID.String()),
			log.String("error", updateErr.Error()),
		)

		return updateErr
	}

	r.logger.Warn("sandbox launch failed; requeued without consuming a retry",
		log.String("job_id", j.ID.String()),
		log.String("job_name", j.Name),
		log.String("error", j.LastError),
		log.Duration("delay", delay),
	)

	return fmt.Errorf("job %s launch failed: %s", j.Name, j.LastError)
}
```

Add `"errors"` to the imports.

- [ ] **Step 5: Add the compatibility shim**

Create `worker/executor_compat.go`:

```go
package worker

import (
	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
)

// Executor is the former name of Runner.
//
// The type was renamed because it orchestrates an attempt — middleware,
// retry, DLQ, state, events — and was never the thing that invokes the
// handler. That is now exec.Executor. This alias keeps existing code
// compiling.
//
// Deprecated: use Runner.
type Executor = Runner

// NewExecutor creates a Runner with no executor registry, so handlers are
// called directly in-process exactly as before.
//
// Deprecated: use NewRunner, which takes an *exec.Registry.
func NewExecutor(
	registry *job.Registry,
	extensions *ext.Registry,
	store job.Store,
	dlqService *dlq.Service,
	bo backoff.Strategy,
	logger log.Logger,
	mws ...middleware.Middleware,
) *Runner {
	return NewRunner(registry, extensions, store, dlqService, bo, nil, logger, mws...)
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./worker/... ./exec/... ./job/...`
Expected: PASS, including the pre-existing `worker/pool_test.go`.

- [ ] **Step 7: Verify the whole tree still builds**

Run: `go build ./... && go vet ./...`
Expected: clean. `engine/engine.go:286` still calls `worker.NewExecutor` and must compile unchanged.

- [ ] **Step 8: Lint and commit**

```bash
golangci-lint run ./worker/...
git add worker/
git commit -m "refactor(worker): rename Executor to Runner and delegate to exec.Executor

Runner orchestrates an attempt: middleware, retry, DLQ, state, events. It
was never the thing that invokes the handler, which is now exec.Executor.
worker.Executor survives as a type alias and NewExecutor as a deprecated
constructor, so existing callers compile untouched.

The terminal closure is the only execution logic that changes, which is
what keeps artifact staging outside the boundary: an out-of-process
handler receives a directory, never storage credentials.

Launch failures now requeue without incrementing RetryCount. An
ImagePullBackOff says nothing about the work, and burning three retries on
one bad node would send healthy jobs to the DLQ."
```

---

## Task 9: Engine wiring

**Files:**
- Modify: `engine/engine.go`
- Create: `engine/execution.go`
- Test: `engine/execution_test.go`

**Interfaces:**
- Consumes: `exec.Registry`, `exec.Policy`, `inproc.New` (Tasks 1–6); `worker.NewRunner` (Task 8); `job.Registrable` (Task 5).
- Produces: `engine.RegisterAll(eng *Engine, defs ...job.Registrable) error`; `engine.WithExecutor(e exec.Executor) Option`; `(*Engine).Executors() *exec.Registry`. `engine.RegisterChecked[T]` gains a policy-satisfiability check.

**No breaking change.** The repo already has the convention this needs: `engine.Register[T]` (`engine/engine.go:383`) returns nothing and registers unconditionally, while `engine.RegisterChecked[T]` (`engine/engine.go:391`) returns an `error` and validates artifact declarations first. The execution-policy check belongs in `RegisterChecked` beside `ValidateArtifactInputs` — same purpose, same failure mode, same signature. `Register` keeps its signature and stays unchecked. `RegisterAll` is new and returns an `error`, matching `RegisterChecked`.

- [ ] **Step 1: Write the failing test**

Create `engine/execution_test.go`.

```go
package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/job"
)

type execPayload struct {
	Value int `json:"value"`
}

func TestEngine_ExecutorsIncludesInProcessByDefault(t *testing.T) {
	eng := newTestEngine(t)

	executors := eng.Executors()
	if executors == nil {
		t.Fatal("Executors() = nil, want a registry")
	}
	def := executors.Default()
	if def == nil {
		t.Fatal("Default() = nil, want the in-process executor")
	}
	if def.Name() != "inprocess" {
		t.Errorf("Default().Name() = %q, want %q", def.Name(), "inprocess")
	}
}

func TestEngine_RegisterRejectsUnsatisfiablePolicy(t *testing.T) {
	// A definition that must be isolated must not silently run
	// unisolated because it was deployed somewhere that cannot isolate.
	eng := newTestEngine(t)

	err := engine.RegisterChecked(eng, job.NewDefinition("needs.sandbox",
		func(context.Context, execPayload) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelSandboxed)),
	))
	if !errors.Is(err, exec.ErrNoExecutor) {
		t.Fatalf("RegisterChecked() = %v, want %v", err, exec.ErrNoExecutor)
	}
}

func TestEngine_RegisterCheckedAllowsExplicitDowngrade(t *testing.T) {
	eng := newTestEngine(t)

	err := engine.RegisterChecked(eng, job.NewDefinition("needs.sandbox.but.ok",
		func(context.Context, execPayload) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelSandboxed), exec.AllowDowngrade()),
	))
	if err != nil {
		t.Fatalf("RegisterChecked() = %v, want nil", err)
	}
}

func TestEngine_RegisterStaysUnchecked(t *testing.T) {
	// Register is the unchecked path by existing convention, and its
	// signature must not change. A policy nothing satisfies is caught by
	// RegisterChecked and by RegisterAll, not here.
	eng := newTestEngine(t)

	engine.Register(eng, job.NewDefinition("unchecked.sandbox",
		func(context.Context, execPayload) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelSandboxed)),
	))

	if _, ok := eng.Registry().Get("unchecked.sandbox"); !ok {
		t.Error("Register did not register the handler")
	}
}

func TestEngine_RegisterAll(t *testing.T) {
	eng := newTestEngine(t)

	defs := []job.Registrable{
		job.NewDefinition("a.job", func(context.Context, execPayload) error { return nil }),
		job.NewDefinition("b.job", func(context.Context, struct{}) error { return nil }),
	}

	if err := engine.RegisterAll(eng, defs...); err != nil {
		t.Fatalf("RegisterAll() = %v, want nil", err)
	}
	for _, name := range []string{"a.job", "b.job"} {
		if _, ok := eng.Registry().Get(name); !ok {
			t.Errorf("handler %q not registered", name)
		}
	}
}
```

`newTestEngine` must reuse the engine-construction helper `engine/engine_test.go` already uses. Run `grep -n "func newTestEngine\|func newEngine\|engine.New(" engine/engine_test.go | head` and call the same path rather than building a second one; if the existing tests construct the engine inline, extract that into `newTestEngine(t *testing.T) *engine.Engine` in the new file and leave the existing tests alone.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./engine/...`
Expected: FAIL — undefined: `eng.Executors`, `engine.RegisterAll`.

- [ ] **Step 3: Add the executor registry to the engine**

Create `engine/execution.go`:

```go
package engine

import (
	"fmt"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/job"
)

// WithExecutor registers an additional executor, making a stronger
// isolation level available to job definitions that ask for it.
//
// The in-process executor is always present as the default, so a
// deployment that adds nothing behaves exactly as it always has.
func WithExecutor(e exec.Executor) Option {
	return func(eng *Engine) {
		eng.extraExecutors = append(eng.extraExecutors, e)
	}
}

// Executors returns the configured executor registry.
func (eng *Engine) Executors() *exec.Registry { return eng.executors }

// buildExecutors assembles the executor registry. It is called once during
// engine construction, before any definition is registered, because
// registration validates policies against it.
func (eng *Engine) buildExecutors() {
	r := exec.NewRegistry(inproc.New(eng.registry))
	for _, e := range eng.extraExecutors {
		r.Add(e)
	}
	eng.executors = r
}

// checkExecutionPolicy reports whether the deployment can satisfy a
// definition's declared isolation.
//
// This runs at registration rather than at execution deliberately. A
// definition that can never be satisfied should fail on a developer's
// machine, not on the first malicious upload in production.
func (eng *Engine) checkExecutionPolicy(name string, p exec.Policy) error {
	if eng.executors == nil {
		return nil
	}
	if _, err := eng.executors.Select(p); err != nil {
		return fmt.Errorf("dispatch/engine: job %q: %w", name, err)
	}

	return nil
}

// RegisterAll registers a set of definitions.
//
// It takes job.Registrable rather than a typed definition so a single
// handler list can be shared between the worker and an out-of-process
// entrypoint, which cannot be handed an engine.
func RegisterAll(eng *Engine, defs ...job.Registrable) error {
	// Validate every definition before registering any of them, so a
	// rejected set leaves the registry as it was rather than half
	// populated.
	for _, d := range defs {
		if err := eng.checkExecutionPolicy(d.JobName(), d.Policy()); err != nil {
			return err
		}
	}
	for _, d := range defs {
		d.Register(eng.registry)
	}

	return nil
}
```

- [ ] **Step 4: Wire the fields and the construction call**

In `engine/engine.go`, add two fields to the `Engine` struct:

```go
	executors      *exec.Registry
	extraExecutors []exec.Executor
```

Call `eng.buildExecutors()` during construction, **after** `eng.registry` is created and **before** any definition is registered or the runner is built.

Change the runner construction at `engine/engine.go:286` from `worker.NewExecutor(...)` to:

```go
	runner := worker.NewRunner(
		eng.registry, eng.extensions, eng.jobStore, eng.dlqService,
		eng.bo, eng.executors, logger, allMws...,
	)
```

and update the `worker.NewPool(...)` call below it to pass `runner`.

Add the policy check to `RegisterChecked[T]`, beside the existing `ValidateArtifactInputs` call. The whole function becomes:

```go
// RegisterChecked registers a definition and validates its artifact
// declarations and execution policy, so a job that could never be staged
// or could never be isolated as it requires fails here rather than on
// every worker that picks it up.
func RegisterChecked[T any](eng *Engine, def *job.Definition[T]) error {
	if err := eng.ValidateArtifactInputs(def.Name, def.Opts.Inputs); err != nil {
		return err
	}
	if err := eng.checkExecutionPolicy(def.Name, def.Opts.Execution); err != nil {
		return err
	}

	job.RegisterDefinition(eng.registry, def)

	return nil
}
```

Leave `Register[T]` exactly as it is. It is the unchecked path by existing convention, and changing its signature would break every caller for no gain.

- [ ] **Step 5: Run the full test suite**

Run: `make test`
Expected: PASS across every package. Pay particular attention to `engine/engine_test.go` and `engine/artifact_test.go`, which exercise the registration path this task changed.

- [ ] **Step 6: Lint and commit**

```bash
make fmt
golangci-lint run ./...
git add engine/
git commit -m "feat(engine): wire the executor registry into registration and execution

The engine always configures the in-process executor as the default, so a
deployment that adds nothing behaves exactly as before. WithExecutor adds
stronger rungs.

Policies are checked in RegisterChecked, beside the existing artifact
validation, rather than at execution: a definition demanding isolation the
deployment cannot provide should fail on a developer's machine, not on the
first malicious upload in production. Register stays the unchecked path
and keeps its signature.

RegisterAll takes job.Registrable so one handler list can be shared
between the worker and an out-of-process entrypoint that cannot be handed
an engine."
```

---

## Task 10: Documentation and the phase gate

**Files:**
- Create: `docs/content/docs/execution-isolation.mdx`
- Modify: `exec/doc.go` (add a usage example)
- Test: `exec/example_test.go`

**Interfaces:**
- Consumes: everything.
- Produces: a runnable `Example` that doubles as documentation.

- [ ] **Step 1: Write the runnable example**

Create `exec/example_test.go`:

```go
package exec_test

import (
	"context"
	"fmt"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/job"
)

type modelInput struct {
	Detail int `json:"detail"`
}

// ExampleRegistry_Select shows how a definition's declared isolation
// chooses the executor that runs it.
func ExampleRegistry_Select() {
	registry := job.NewRegistry()

	// A handler that parses untrusted geometry declares that it needs a
	// separate address space at minimum.
	job.NewDefinition("tessellate.model",
		func(context.Context, modelInput) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(registry)

	executors := exec.NewRegistry(inproc.New(registry))

	_, err := executors.Select(registry.Policy("tessellate.model"))
	fmt.Println(err != nil)

	// A handler that declares nothing runs in-process, as it always has.
	e, err := executors.Select(registry.Policy("send.email"))
	fmt.Println(e.Name(), err)

	// Output:
	// true
	// inprocess <nil>
}
```

- [ ] **Step 2: Run the example**

Run: `go test ./exec/ -run Example -v`
Expected: PASS. The first line prints `true` because no process-level executor is configured in this phase, which is the no-silent-downgrade rule doing its job.

- [ ] **Step 3: Write the user documentation**

Create `docs/content/docs/execution-isolation.mdx` following the frontmatter format of the existing files in that directory — run `head -5 docs/content/docs/*.mdx` to see it. Cover: what the ladder is, why in-process is the default, how to declare a policy with `job.WithExecution`, that only the in-process rung exists today, and that a definition declaring a level the deployment cannot provide fails at startup rather than running unisolated.

- [ ] **Step 4: Full verification**

Run each and confirm before proceeding:

```bash
make fmt
make vet
make lint
make test
go build ./...
```

Expected: all clean. This is the phase gate — do not commit if any of the five fails.

- [ ] **Step 5: Commit**

```bash
git add exec/example_test.go docs/content/docs/execution-isolation.mdx
git commit -m "docs(exec): document the isolation ladder and policy declaration

Adds a runnable example that doubles as the API documentation, including
the no-silent-downgrade behaviour: with only the in-process rung
configured, a definition demanding process isolation fails selection
rather than running unisolated."
```

---

## Phase Completion Checklist

- [ ] `make test` passes across every package
- [ ] `make lint` reports no issues
- [ ] `go build ./...` is clean
- [ ] `TestExecIsALeafPackage` passes — `exec` imports only `id`, `scope`, `artifact`, and the root package
- [ ] `TestInProcessConformance` passes the full shared suite
- [ ] `worker.NewExecutor` still compiles with its original signature
- [ ] `go.mod` is unchanged — no new dependencies
- [ ] Existing behaviour is unchanged: a deployment configuring no executor runs handlers in-process exactly as before

**Next:** Phase 2 — `exec/wire`, `exec/shim`, and `exec/subprocess`. That phase flips `Capabilities{Enforces: true, IsolatesPanic: true}` for its rung and the conformance suite starts asserting that deadlines are actually enforced.
