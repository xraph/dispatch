package subprocess

import (
	"context"
	"fmt"
	"os"
	osexec "os/exec"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/shim"
	"github.com/xraph/dispatch/exec/wire"
	"github.com/xraph/dispatch/id"
)

// Name is the identifier this executor registers under.
const Name = "subprocess"

// requestFD and resultFD are the descriptors the child sees fd 3 and fd 4
// as, once os/exec appends ExtraFiles after stdin/stdout/stderr. They are
// a single source of truth for both the ExtraFiles ordering below and the
// environment variables that tell the shim where to look, so the two can
// never drift apart.
const (
	requestFD = 3
	resultFD  = 4
)

// options holds every Option's effect. Some fields — the configured user,
// AllowSameUser, and Rlimits — are not yet enforced: setting the process's
// credentials and resource limits is Task 5's job, and the kill ladder's
// SIGTERM-then-grace-period-then-SIGKILL sequence is Task 6's. This task
// carries their configuration through so those tasks only have to wire
// behaviour onto values that already exist, not invent a new option API.
// The child's process group is the one piece of SysProcAttr this task does
// set (see sysProcAttr in procattr_unix.go): Task 5 still owns the
// Credential half of the same struct, for the dedicated uid.
type options struct {
	binary        string
	args          []string
	env           map[string]string
	uid           int
	gid           int
	hasUser       bool
	allowSameUser bool
	logger        log.Logger
	rlimits       Rlimits
	hasRlimits    bool
	scratchDir    string
}

// Option configures an Executor.
type Option func(*options)

// WithBinary sets the path to the binary the executor re-execs for every
// attempt. In production this is the worker's own executable, found via
// os.Executable; tests pass os.Args[0] so the re-exec'd child is the test
// binary itself, running the shim instead of go test.
func WithBinary(path string) Option {
	return func(o *options) { o.binary = path }
}

// WithArgs sets extra arguments appended after shim.ArgName when launching
// the child. Most deployments need none of these — the marker argument is
// enough for the binary to know to run the shim.
func WithArgs(args ...string) Option {
	return func(o *options) {
		o.args = append([]string(nil), args...)
	}
}

// WithEnv supplies the base environment for the child. It is merged with
// Request.Env, which wins on any key both sides set, and with a small
// fixed allowlist (PATH, HOME, TMPDIR) copied from the worker's own
// environment. The child never inherits os.Environ() wholesale — that is
// the entire point of this rung, since the worker's environment is where
// its own credentials tend to live.
func WithEnv(env map[string]string) Option {
	return func(o *options) {
		m := make(map[string]string, len(env))
		for k, v := range env {
			m[k] = v
		}
		o.env = m
	}
}

// WithUser configures the uid and gid the child runs as. Task 5 enforces
// this and refuses to start when it matches the worker's own uid, unless
// WithAllowSameUser is also given.
func WithUser(uid, gid int) Option {
	return func(o *options) {
		o.uid = uid
		o.gid = gid
		o.hasUser = true
	}
}

// WithAllowSameUser permits WithUser to name the worker's own uid. Without
// it, Task 5's enforcement refuses to start, because a child running as
// the worker can read every credential the isolation exists to hide.
func WithAllowSameUser() Option {
	return func(o *options) { o.allowSameUser = true }
}

// WithLogger sets where the child's stdout and stderr are streamed, each
// line tagged with the job's id and name. The default is a no-op logger,
// so output is silently discarded rather than reaching os.Stdout, which
// would interleave a handler's own output with the worker's.
func WithLogger(l log.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithRlimits configures POSIX resource limits for the child. Task 5
// applies these; this task only carries the value from configuration
// through to the point Task 5 needs it.
func WithRlimits(r Rlimits) Option {
	return func(o *options) {
		o.rlimits = r
		o.hasRlimits = true
	}
}

// WithScratchDir sets the directory under which each attempt gets a fresh
// working directory for the child's process (Cmd.Dir). It defaults to
// os.TempDir(). This is distinct from Request.OutputDir: that is where the
// handler writes artifacts through the accessor, while this is just a safe
// place for the process to start in, so a handler that writes a relative
// path outside the artifact API lands somewhere disposable instead of the
// worker's own working directory.
func WithScratchDir(path string) Option {
	return func(o *options) { o.scratchDir = path }
}

// Rlimits configures the POSIX resource limits applied to the child
// process. Go cannot set a child's rlimits through SysProcAttr, so Task 5
// applies these child-side, in shim.Main, from environment variables the
// parent sets. Zero means "leave the limit at whatever the worker itself
// runs with."
type Rlimits struct {
	// AddressSpace caps RLIMIT_AS in bytes.
	AddressSpace int64
	// NoFile caps RLIMIT_NOFILE, the open file descriptor count.
	NoFile int64
	// NProc caps RLIMIT_NPROC, the number of processes the child's uid
	// may run — a second line of defence against a forking exploit even
	// with the process group killed on timeout.
	NProc int64
	// Core caps RLIMIT_CORE. Task 5 forces this to zero regardless of
	// what is configured here, so a segfaulting parser cannot dump the
	// input that crashed it, and the worker's memory alongside it, to
	// disk.
	Core int64
	// FSize caps RLIMIT_FSIZE in bytes, bounding how much a runaway
	// handler can write before the kernel kills it outright.
	FSize int64
}

// Executor runs handlers in a child process, re-exec'ing the configured
// binary as the shim for every attempt. It satisfies exec.Executor at
// exec.LevelProcess: a crash or a memory-unsafe parser going off the rails
// takes the child down, not the worker, and the child receives a
// constructed environment rather than the worker's own.
type Executor struct {
	opts options
}

var _ exec.Executor = (*Executor)(nil)

// New creates a subprocess executor from options. Absent WithLogger, child
// output is discarded rather than reaching the worker's own stdout/stderr.
// Absent WithScratchDir, os.TempDir() is used.
func New(opts ...Option) *Executor {
	o := options{
		logger:     log.NewNoopLogger(),
		scratchDir: os.TempDir(),
	}
	for _, opt := range opts {
		opt(&o)
	}

	return &Executor{opts: o}
}

// Name identifies the executor.
func (e *Executor) Name() string { return Name }

// Level reports that this executor isolates the handler into its own
// address space, but does not sandbox it — no mount, network, or PID
// namespace, no seccomp filter.
func (e *Executor) Level() exec.Level { return exec.LevelProcess }

// Run launches the child, feeds it req over fd 3, and waits for either a
// result frame on fd 4 or the process ending on its own. A returned error
// means the child never started; every other outcome, including the child
// crashing or missing its deadline, is reported through Result.Status.
func (e *Executor) Run(ctx context.Context, req *exec.Request) (*exec.Result, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("dispatch/exec/subprocess: invalid request: %w", err)
	}

	// The request pipe: reqR becomes the child's fd 3, reqW is ours to
	// write the frame on. The result pipe: resW becomes the child's fd 4,
	// resR is ours to read the frame from.
	reqR, reqW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("dispatch/exec/subprocess: create request pipe: %w", err)
	}
	resR, resW, err := os.Pipe()
	if err != nil {
		reqR.Close()
		reqW.Close()

		return nil, fmt.Errorf("dispatch/exec/subprocess: create result pipe: %w", err)
	}
	// A second, independent pair per stream, so draining stdout/stderr is
	// entirely decoupled from Cmd.Wait()'s own bookkeeping. Cmd.StdoutPipe
	// ties draining to Wait in a way that is unsafe to run concurrently
	// with a Wait call happening in another goroutine (its own docs say
	// so); assigning a plain *os.File to Cmd.Stdout/Cmd.Stderr instead
	// means os/exec just dups the fd into the child and otherwise leaves
	// it alone, so we can read our own end on our own schedule.
	outR, outW, err := os.Pipe()
	if err != nil {
		reqR.Close()
		reqW.Close()
		resR.Close()
		resW.Close()

		return nil, fmt.Errorf("dispatch/exec/subprocess: create stdout pipe: %w", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		reqR.Close()
		reqW.Close()
		resR.Close()
		resW.Close()
		outR.Close()
		outW.Close()

		return nil, fmt.Errorf("dispatch/exec/subprocess: create stderr pipe: %w", err)
	}

	scratch, err := os.MkdirTemp(e.opts.scratchDir, "dispatch-exec-")
	if err != nil {
		reqR.Close()
		reqW.Close()
		resR.Close()
		resW.Close()
		outR.Close()
		outW.Close()
		errR.Close()
		errW.Close()

		return nil, fmt.Errorf("dispatch/exec/subprocess: create scratch dir: %w", err)
	}
	defer os.RemoveAll(scratch) // best-effort cleanup; a leftover empty scratch dir is not worth failing the attempt over

	args := append([]string{shim.ArgName}, e.opts.args...)
	// CommandContext rather than Command to satisfy noctx; the context
	// passed here is intentionally context.Background(), not the caller's
	// ctx, because cancellation is handled explicitly below by the
	// waitLoop select and killProcess. Wiring the caller's ctx in here too
	// would give os/exec its own independent kill-on-cancel path (with its
	// own WaitDelay semantics) racing the one this function already owns.
	cmd := osexec.CommandContext(context.Background(), e.opts.binary, args...) //nolint:gosec // G204: binary and args come from operator configuration (WithBinary/WithArgs), never from the untrusted job payload
	cmd.Env = e.buildEnv(req)
	cmd.Dir = scratch
	cmd.ExtraFiles = []*os.File{reqR, resW} // index 0 -> fd 3, index 1 -> fd 4, matching requestFD/resultFD above
	cmd.Stdout = outW
	cmd.Stderr = errW
	cmd.SysProcAttr = sysProcAttr() // Setpgid, so killProcess below can reach the whole group, not just this one process

	if err := cmd.Start(); err != nil {
		reqR.Close()
		reqW.Close()
		resR.Close()
		resW.Close()
		outR.Close()
		outW.Close()
		errR.Close()
		errW.Close()

		return &exec.Result{
			Status:     exec.StatusLaunchFailed,
			HandlerErr: err.Error(),
		}, nil
	}

	// The child inherited its own copies of these four fds across
	// fork/exec. Ours are now redundant, and keeping them open is actively
	// harmful: as long as our copy of resW stays open, resR can never see
	// EOF, even after the child exits and closes its own copy — Run would
	// block forever reading a result frame from a process that is already
	// gone. The same reasoning applies to outW/errW for the stdio pipes.
	// reqR only matters for symmetry; nothing reads from our copy anyway.
	reqR.Close()
	resW.Close()
	outW.Close()
	errW.Close()

	// Each of these closes exactly once no matter which of several racing
	// paths gets there first: the dedicated writer goroutine below always
	// closes reqW itself once it is done with it, and the drain-grace
	// timeout further down can also force any of the four closed to
	// unblock a reader or writer stuck on a descendant the child left
	// behind. sync.OnceFunc is what keeps that from ever double-closing a
	// file — recycled fd numbers make a double-close silently break an
	// unrelated descriptor rather than just returning a harmless error.
	closeReqW := sync.OnceFunc(func() { reqW.Close() })
	closeOutR := sync.OnceFunc(func() { outR.Close() })
	closeErrR := sync.OnceFunc(func() { errR.Close() })
	closeResR := sync.OnceFunc(func() { resR.Close() })
	defer closeReqW()
	defer closeOutR()
	defer closeErrR()
	defer closeResR()

	var stdioWG sync.WaitGroup
	stdioWG.Add(2)
	go func() {
		defer stdioWG.Done()
		streamOutput(outR, e.opts.logger, req, "stdout")
	}()
	go func() {
		defer stdioWG.Done()
		streamOutput(errR, e.opts.logger, req, "stderr")
	}()

	type frameRead struct {
		frame *wire.Frame
		err   error
	}
	frameCh := make(chan frameRead, 1)
	go func() {
		f, ferr := wire.Decode(resR)
		frameCh <- frameRead{f, ferr}
	}()

	// writeRequest runs on its own goroutine instead of blocking Run's own
	// goroutine here: a synchronous write large enough to fill the pipe
	// buffer (64KB on Linux, often 16KB on macOS) would block until the
	// child drains it, and if the child is alive but has not yet reached
	// wire.Decode — busy, stopped, traced, or simply not a real shim —
	// that block would sit outside the waitLoop select below, unreachable
	// by both the deadline and ctx. Once the child is killed (or exits on
	// its own), the last reader of reqR goes away and the pending Write
	// unblocks with EPIPE on its own, without this needing to close reqW
	// itself for that to happen.
	encodeCh := make(chan error, 1)
	go func() {
		err := writeRequest(reqW, req)
		closeReqW()
		encodeCh <- err
	}()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	var deadlineCh <-chan time.Time
	if !req.Deadline.IsZero() {
		d := time.Until(req.Deadline)
		if d < 0 {
			d = 0
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		deadlineCh = timer.C
	}

	var (
		timedOut   bool
		callerDone bool
		ctxDoneCh  = ctx.Done()
	)

waitLoop:
	for {
		select {
		case <-waitCh:
			break waitLoop
		case <-deadlineCh:
			timedOut = true
			deadlineCh = nil // this case must not fire again once handled
			killProcess(cmd)
		case <-ctxDoneCh:
			callerDone = true
			ctxDoneCh = nil // ditto, so we do not spin once ctx is done
			killProcess(cmd)
		}
	}

	// The tracked process is reaped, but that guarantees nothing about
	// outR/errR/resR/reqW seeing EOF or completing: anything the handler
	// spawned — a shelled-out ffmpeg, a stray background job — inherits
	// stdout, stderr, and both wire descriptors, and keeps its own copy of
	// each open for as long as it runs. killProcess only reaches the
	// tracked process (Setpgid extends that to its whole group, but only
	// when killProcess actually runs — nothing kills a grandchild left
	// behind by a process that exited cleanly on its own, before any
	// deadline or cancellation ever fired). drainGrace bounds how long the
	// four operations below wait for their own copies to close on their
	// own before this forces them closed instead: long enough for a
	// handler's own trailing writes to flush, short enough that a leaked
	// descendant cannot wedge Run past this point indefinitely.
	const drainGrace = 3 * time.Second

	stdioDone := make(chan struct{})
	go func() {
		stdioWG.Wait()
		close(stdioDone)
	}()

	drainTimer := time.NewTimer(drainGrace)
	defer drainTimer.Stop()

	var (
		fr            frameRead
		encodeErr     error
		stdioPending  = true
		framePending  = true
		encodePending = true
		timerCh       = drainTimer.C
	)

	for stdioPending || framePending || encodePending {
		select {
		case <-stdioDone:
			stdioPending = false
		case fr = <-frameCh:
			framePending = false
		case encodeErr = <-encodeCh:
			encodePending = false
		case <-timerCh:
			// Force every reader/writer still blocked to return, so the
			// goroutines above can finish and this loop can exit rather
			// than waiting on a descendant process that may never close
			// these fds on its own. timerCh is a one-shot channel — it
			// cannot fire twice — so this only ever forces the closes
			// once, then lets the now-unblocked goroutines deliver
			// through the cases above on the next iterations.
			closeOutR()
			closeErrR()
			closeResR()
			closeReqW()
			timerCh = nil
		}
	}

	return e.classify(req, fr.frame, fr.err, encodeErr, cmd.ProcessState, timedOut, callerDone), nil
}

// killProcess best-effort kills the started process's whole group (see
// killGroup in procattr_unix.go). Task 6 replaces the direct kill here
// with the graceful SIGTERM-then-grace-period-then-SIGKILL ladder; what
// this task adds is Setpgid plus signalling the group rather than the one
// process, so a native library's forked helpers die with the handler
// instead of surviving it — the direct kill alone only ever reached the
// process this package started, leaving anything that process forked
// running.
func killProcess(cmd *osexec.Cmd) {
	if cmd.Process == nil {
		return
	}

	// killGroup legitimately errors when the process has already exited —
	// e.g. it happened to finish in the window between the wait channel
	// firing and this call landing, which is a benign race, not a failure
	// this function has anything useful to do about.
	_ = killGroup(cmd) //nolint:errcheck // benign race with the process exiting on its own; nothing useful to do with the error here
}

// writeRequest encodes and writes the single request frame Run ever
// sends. It exists mainly so Run's own body does not have to build the
// wire.Frame inline.
func writeRequest(w *os.File, req *exec.Request) error {
	if err := wire.Encode(w, &wire.Frame{Kind: wire.KindRequest, Request: req}); err != nil {
		return fmt.Errorf("dispatch/exec/subprocess: write request: %w", err)
	}

	return nil
}

// buildEnv constructs the child's environment. It never starts from
// os.Environ(): only a fixed allowlist (PATH, HOME, TMPDIR) is copied from
// the worker's own environment, then the executor's configured base
// (WithEnv), then the request's own Env, which wins any conflict since it
// is the most specific source. The fd variables are set last and cannot be
// overridden by any of the above, because their values are fixed by how
// ExtraFiles was built above, not something any caller should influence.
func (e *Executor) buildEnv(req *exec.Request) []string {
	merged := make(map[string]string, len(e.opts.env)+len(req.Env)+5)

	for _, k := range [...]string{"PATH", "HOME", "TMPDIR"} {
		if v, ok := os.LookupEnv(k); ok {
			merged[k] = v
		}
	}
	for k, v := range e.opts.env {
		merged[k] = v
	}
	for k, v := range req.Env {
		merged[k] = v
	}

	merged[shim.EnvRequestFD] = strconv.Itoa(requestFD)
	merged[shim.EnvResultFD] = strconv.Itoa(resultFD)

	out := make([]string, 0, len(merged))
	for k, v := range merged {
		out = append(out, k+"="+v)
	}
	sort.Strings(out) // deterministic, so the same request produces the same argv/env for debugging

	return out
}

// classify turns what the child reported and what happened to the process
// into a Result.
//
// The rule: the parent trusts the child's frame for what the handler did,
// and its own wait status for what happened to the process. A decoded
// frame from a process that was not signalled is authoritative for the
// status it reports — including when timedOut is set, see below. When the
// process was signalled, its wait status overrides the frame even if the
// frame claims StatusOK: a report of success from a process that was, in
// fact, killed is not to be believed. Absent a usable frame entirely — the
// shim never got the chance to report, or its report was cut off mid-write
// — the process's own exit code or signal is all there is to classify by.
func (e *Executor) classify(
	req *exec.Request,
	frame *wire.Frame,
	frameErr error,
	encodeErr error,
	ps *os.ProcessState,
	timedOut, callerCanceled bool,
) *exec.Result {
	exitCode, signal, signaled := processOutcome(ps)
	frameOK := frameErr == nil && frame != nil && frame.Result != nil

	// A decoded frame from a process that was not signalled wins even over
	// timedOut. The Go spec says select "chooses uniformly at random among
	// [the ready cases]", so when the tracked process finishes at the same
	// instant the deadline timer fires, the waitLoop above can pick the
	// timer case over an already-ready waitCh. A process that was actually
	// killed always reports Signaled() true — there is no way to kill it
	// and have it look like a clean exit — so timedOut with signaled false
	// means only "the timer fired", never "the kill did anything." Trusting
	// the frame here is what keeps that race from turning a successful
	// attempt into a StatusTimeout that burns a retry it never needed.
	if frameOK && !signaled {
		res := *frame.Result
		res.ExitCode = exitCode
		res.Signal = 0

		return &res
	}

	if timedOut {
		return &exec.Result{
			Status:     exec.StatusTimeout,
			HandlerErr: fmt.Sprintf("dispatch/exec/subprocess: deadline %s exceeded", req.Deadline.Format(time.RFC3339)),
			ExitCode:   exitCode,
			Signal:     signal,
		}
	}

	if frameOK { // signaled is true here, or the branch above would have returned
		res := frame.Result

		return &exec.Result{
			Status: exec.StatusKilled,
			HandlerErr: fmt.Sprintf(
				"dispatch/exec/subprocess: process killed by signal %d after reporting %s",
				signal, res.Status,
			),
			ExitCode: exitCode,
			Signal:   signal,
			Usage:    res.Usage,
			// Outputs and Permanent carry through even though the process
			// was signalled: a handler killed right after committing its
			// artifacts should not have them become invisible, and a
			// permanent failure it already flagged should not silently
			// turn retryable just because the signal arrived a moment
			// later than the report did.
			Outputs:   res.Outputs,
			Permanent: res.Permanent,
		}
	}

	reason := "no result frame"
	switch {
	case callerCanceled:
		reason = "context canceled"
	case frameErr != nil:
		reason = frameErr.Error()
	case encodeErr != nil:
		reason = encodeErr.Error()
	}

	return &exec.Result{
		Status: exec.StatusKilled,
		HandlerErr: fmt.Sprintf(
			"dispatch/exec/subprocess: process ended without a usable result (%s); exit=%d signal=%d",
			reason, exitCode, signal,
		),
		ExitCode: exitCode,
		Signal:   signal,
	}
}

// processOutcome reads the exit code and, on the platforms this rung
// supports, the signal that ended the process. signaled reports whether
// the process died on a signal rather than exiting on its own; when it is
// true, exitCode is meaningless and signal is what actually happened.
func processOutcome(ps *os.ProcessState) (exitCode, signal int, signaled bool) {
	if ps == nil {
		return -1, 0, false
	}

	ws, ok := ps.Sys().(syscall.WaitStatus)
	if !ok {
		return ps.ExitCode(), 0, false
	}
	if ws.Signaled() {
		return -1, int(ws.Signal()), true
	}

	return ws.ExitStatus(), 0, false
}

// Reclaim is a no-op for this rung: a child dies with its parent's
// process group when the worker itself dies (nothing survives a worker
// crash to leak), so there is nothing for a later worker to sweep up.
func (e *Executor) Reclaim(context.Context, id.WorkerID) error { return nil }

// Close releases the executor's own resources. Subprocess holds none —
// every pipe and process it creates is scoped to a single Run.
func (e *Executor) Close() error { return nil }
