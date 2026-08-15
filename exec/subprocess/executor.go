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
	"github.com/xraph/dispatch/resource"
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

// options holds every Option's effect. The configured user and Rlimits are
// now enforced: checkLaunch (limits_unix.go / limits_other.go) refuses to
// start when no uid is configured, or when the configured uid matches the
// worker's own, in either case without AllowSameUser, sysProcAttr
// (procattr_unix.go) sets Credential from uid/gid, and
// buildEnv below passes rlimits to the child, which shim.Main applies via
// syscall.Setrlimit — except RLIMIT_AS, which buildEnv lets a job's own
// resource.Memory ceiling (Request.ResourceLimits) override per attempt,
// falling back to rlimits.AddressSpace only when the job declares
// nothing. The kill ladder's SIGTERM-then-grace-period-then-SIGKILL
// sequence runs in terminate (kill_unix.go), called from killProcess
// below.
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
	strictRlimits bool
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

// WithUser configures the uid and gid the child runs as, dropped via
// Credential on sysProcAttr before exec. Run refuses to start when no uid
// is configured at all, and also when one is configured that matches the
// worker's own, unless WithAllowSameUser is also given — see its doc
// comment for why.
//
// A uid or gid that genuinely differs from the worker's own also clears
// the child's supplementary groups, along with the primary uid/gid — see
// the package doc comment's "The uid/gid boundary" section for why that
// falls out of the same privilege check rather than needing separate
// handling, and for the one path (WithAllowSameUser) where it does not
// happen.
func WithUser(uid, gid int) Option {
	return func(o *options) {
		o.uid = uid
		o.gid = gid
		o.hasUser = true
	}
}

// WithAllowSameUser is the single opt-out for running this rung
// unisolated on the uid boundary. Without it, Run refuses to start in
// either of the two shapes that leave the child running as the worker's
// own uid: WithUser never called at all, or WithUser naming the worker's
// own uid explicitly. Both leave the child able to read every credential
// the isolation exists to hide — ~/.aws, /var/run/secrets, the Dispatch
// config itself — so both share this one switch rather than each getting
// its own.
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

// WithRlimits configures POSIX resource limits for the child. buildEnv
// passes non-zero fields to the child as environment variables, and
// shim.Main applies them via syscall.Setrlimit before running the
// handler — see the Rlimits doc comment for why that happens child-side.
func WithRlimits(r Rlimits) Option {
	return func(o *options) {
		o.rlimits = r
		o.hasRlimits = true
	}
}

// WithStrictRlimits makes a configured rlimit that did not actually take
// effect a launch failure instead of a warning — with one exception.
// shim.Main (applyRlimits) still treats the current kernel's own
// structural refusal to support a given limit at all as a warning
// regardless of this option: Darwin rejecting setrlimit(RLIMIT_AS, ...)
// unconditionally is the standing example, and there is nothing a
// caller-supplied value could have done differently about that, so
// making it fatal here would only make WithRlimits{AddressSpace: ...}
// unusable on Darwin rather than catch a real misconfiguration.
//
// Everything else this rung can fail on, it does catch, including two
// shapes worth naming explicitly: a value that exceeds the process's own
// hard limit (EPERM on a platform that does support the resource), and —
// distinct from the kernel refusing the limit — Dispatch itself not
// having verified the raw resource number for the current platform at
// all (RLIMIT_NPROC on most non-Linux/Darwin/FreeBSD Unixes, currently;
// see rlimitNProc in exec/shim/rlimit_unix.go). That second case is a
// library gap, not a platform fact, and without this option it would
// otherwise be indistinguishable, from the outside, from the limit
// simply having applied.
//
// Without this, a configured rlimit that fails to apply is logged to the
// child's stderr and otherwise ignored — see the Rlimits doc comment —
// which is invisible by default unless WithLogger is also configured,
// since WithLogger's own default discards output silently. An operator
// who wants a guarantee that a configured limit actually took effect,
// not just an attempt at one, should use this rather than relying on
// stderr being watched.
func WithStrictRlimits() Option {
	return func(o *options) { o.strictRlimits = true }
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
// process. Go cannot set a child's rlimits through SysProcAttr, so
// buildEnv passes these child-side as environment variables (see
// shim.EnvRlimitAS and friends), and shim.Main applies them via
// syscall.Setrlimit before running the handler. Zero means "leave the
// limit at whatever the worker itself runs with."
type Rlimits struct {
	// AddressSpace caps RLIMIT_AS in bytes. Note Darwin's kernel rejects
	// setrlimit(RLIMIT_AS, ...) outright (EINVAL) regardless of value —
	// shim.Main logs and continues rather than failing the attempt when
	// that happens, since the rest of the isolation (uid boundary,
	// process-group kill, the other limits) still holds.
	AddressSpace int64
	// NoFile caps RLIMIT_NOFILE, the open file descriptor count.
	NoFile int64
	// NProc caps RLIMIT_NPROC, the number of processes the child's uid
	// may run — a second line of defence against a forking exploit even
	// with the process group killed on timeout.
	NProc int64
	// Core caps RLIMIT_CORE. buildEnv forces the child's actual limit to
	// zero unconditionally, regardless of what is set here, so a
	// segfaulting parser cannot dump the input that crashed it, and the
	// worker's memory alongside it, to disk. This field is accepted for
	// API symmetry with the other limits but has no effect.
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

	// checkLaunch refuses before any pipe or process exists: on Unix, no
	// uid configured at all, or a configured uid matching the worker's
	// own, neither without WithAllowSameUser; on every other platform,
	// unconditionally, since this rung has no isolation to offer there.
	// See limits_unix.go / limits_other.go.
	if err := checkLaunch(e.opts); err != nil {
		return &exec.Result{
			Status:     exec.StatusLaunchFailed,
			HandlerErr: err.Error(),
		}, nil
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
	cmd.SysProcAttr = sysProcAttr(e.opts) // Setpgid, so killProcess below can reach the whole group, not just this one process; Credential when a user is configured

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

	// grace is Policy.GracePeriod, falling back to exec.DefaultGracePeriod
	// when the request carries a zero value — a Request built without
	// exec.NewPolicy (which applies that same default itself) leaves
	// Policy as its zero value, and a zero grace period would collapse
	// the ladder in kill_unix.go's terminate back into an immediate
	// SIGKILL, silently losing the whole point of Task 6 for any caller
	// that did not opt in explicitly.
	grace := req.Policy.GracePeriod
	if grace <= 0 {
		grace = exec.DefaultGracePeriod
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
			deadlineCh = nil // this case must not fire again once handled
			// select "chooses uniformly at random among those that can
			// proceed" (Go spec), so if the process finished on its own in
			// the same instant the timer fired, this case can still win
			// even though waitCh is already deliverable. A cooperative
			// handler makes that a live outcome, not a theoretical one:
			// the shim traps SIGTERM and cancels its own handler context,
			// and killProcess below sends SIGTERM as the first rung of
			// its kill ladder, so "the tracked process exits right as the
			// deadline fires" only gets more common, not less. Checking
			// waitCh non-blockingly resolves the tie deterministically in
			// favour of what actually happened to the process, instead of
			// leaving classify to guess from a frame and a signal after
			// the fact.
			select {
			case <-waitCh:
				break waitLoop
			default:
			}
			timedOut = true
			killProcess(cmd, grace)
		case <-ctxDoneCh:
			ctxDoneCh = nil // ditto, so we do not spin once ctx is done
			select {
			case <-waitCh:
				break waitLoop
			default:
			}
			callerDone = true
			killProcess(cmd, grace)
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

// killProcess best-effort runs the kill ladder (terminate, kill_unix.go)
// against the started process's whole group: SIGTERM to the group, up to
// grace for the whole group — not just the tracked leader — to empty out
// on its own, then SIGKILL to the group if any of it is still there once
// grace elapses. Setpgid (sysProcAttr, procattr_unix.go) is what makes
// "the group" reach anything the tracked process forked, not just the one
// process this package started directly — without it, even the SIGKILL
// half would leave a native library's forked helpers running.
//
// This blocks the waitLoop select for up to grace, which is deliberate:
// the alternative is racing terminate against the very channels that
// triggered it, and there is nothing useful for waitLoop to do with a
// second deadline or cancellation signal while a kill is already in
// flight — see terminate's own doc comment for why grace runs from here,
// not from whatever triggered this call.
func killProcess(cmd *osexec.Cmd, grace time.Duration) {
	if cmd.Process == nil {
		return
	}

	terminate(cmd, grace)
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
// is the most specific source. The fd and rlimit variables are set last
// and cannot be overridden by any of the above, because their values are
// fixed by how ExtraFiles and options.rlimits were built, not something
// any caller should influence.
func (e *Executor) buildEnv(req *exec.Request) []string {
	merged := make(map[string]string, len(e.opts.env)+len(req.Env)+10)

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

	// RLIMIT_CORE is forced to zero unconditionally, regardless of
	// whether WithRlimits was ever called: a segfaulting parser would
	// otherwise dump a core containing the malicious input and the
	// process's own memory to disk. The rest are only sent when
	// configured, and only per field — zero means "leave the limit at
	// whatever the worker itself runs with" (see the Rlimits doc
	// comment). shim.Main applies all of these child-side via
	// syscall.Setrlimit, since Go cannot set a child's rlimits through
	// SysProcAttr.
	merged[shim.EnvRlimitCore] = "0"

	// RLIMIT_AS is the one dimension job.WithResourceLimits can actually
	// reach today: resource.Memory maps onto it unambiguously. The job's
	// own ceiling wins when it declares one — a job that asked for a
	// tight limit must get it even when the deployment's WithRlimits sets
	// a looser one — and the deployment-wide AddressSpace is the fallback
	// for the (overwhelming majority of) jobs that declare nothing, so
	// nobody who never touches the resource model loses the protection
	// WithRlimits already gave them.
	//
	// resource.CPU has no comparably clean rlimit — RLIMIT_CPU caps total
	// CPU *time*, not the instantaneous share a millicore budget
	// describes — so it is deliberately left unmapped rather than given
	// invented semantics. Every other Rlimits field (NoFile, NProc,
	// FSize) has no per-job resource.Set counterpart at all, so those
	// stay deployment-wide only, exactly as before.
	addressSpace := int64(0)
	if e.opts.hasRlimits {
		addressSpace = e.opts.rlimits.AddressSpace
	}
	if v := req.ResourceLimits[resource.Memory]; v > 0 {
		addressSpace = v
	}
	if addressSpace != 0 {
		merged[shim.EnvRlimitAS] = strconv.FormatInt(addressSpace, 10)
	}

	if e.opts.hasRlimits {
		if e.opts.rlimits.NoFile != 0 {
			merged[shim.EnvRlimitNoFile] = strconv.FormatInt(e.opts.rlimits.NoFile, 10)
		}
		if e.opts.rlimits.NProc != 0 {
			merged[shim.EnvRlimitNProc] = strconv.FormatInt(e.opts.rlimits.NProc, 10)
		}
		if e.opts.rlimits.FSize != 0 {
			merged[shim.EnvRlimitFSize] = strconv.FormatInt(e.opts.rlimits.FSize, 10)
		}
	}
	if e.opts.strictRlimits {
		merged[shim.EnvRlimitStrict] = "1"
	}

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
// and its own wait status for what happened to the process. A deadline
// expiry is reported as StatusTimeout regardless of what the frame says —
// timedOut is only ever set by the waitLoop above after it has already
// checked, non-blockingly, that waitCh was not already deliverable, so the
// uniform-random select tie that used to reach classify directly is
// resolved before timedOut is ever set. That check is on the channel, not
// the process: if the child happened to exit microseconds earlier and the
// Wait() goroutine simply had not yet delivered to waitCh, timedOut still
// ends up true and killProcess no-ops on an already-gone process. The
// window this leaves is reap-and-deliver latency, not the width of a
// select's random pick — narrowed, not eliminated. Short of that, a
// decoded frame is authoritative for the status it reports, unless the
// process still died on a signal — a frame claiming StatusOK from a
// process that was, in fact, killed is not to be believed, so the process
// status wins that disagreement. Absent a usable frame entirely — the shim
// never got the chance to report, or its report was cut off mid-write —
// the process's own exit code or signal is all there is to classify by.
func (e *Executor) classify(
	req *exec.Request,
	frame *wire.Frame,
	frameErr error,
	encodeErr error,
	ps *os.ProcessState,
	timedOut, callerCanceled bool,
) *exec.Result {
	exitCode, signal, signaled := processOutcome(ps)

	if timedOut {
		return &exec.Result{
			Status:     exec.StatusTimeout,
			HandlerErr: fmt.Sprintf("dispatch/exec/subprocess: deadline %s exceeded", req.Deadline.Format(time.RFC3339)),
			ExitCode:   exitCode,
			Signal:     signal,
		}
	}

	if frameErr == nil && frame != nil && frame.Result != nil {
		res := *frame.Result
		if signaled {
			return &exec.Result{
				Status: exec.StatusKilled,
				HandlerErr: fmt.Sprintf(
					"dispatch/exec/subprocess: process killed by signal %d after reporting %s",
					signal, res.Status,
				),
				ExitCode: exitCode,
				Signal:   signal,
				Usage:    res.Usage,
				// Permanent carries through even though the process was
				// signalled: a permanent failure the handler already
				// flagged should not silently turn retryable just because
				// the kill signal arrived a moment after the report did —
				// Result.Err converts this into exec.Error.Permanent,
				// which worker/runner.go's retry check reads directly.
				//
				// Outputs is deliberately NOT carried through, unlike an
				// earlier version of this branch claimed ("should not
				// become invisible"): it does become invisible regardless
				// of what this field holds, because the only commit call
				// site (worker/runner.go) gates on Status == StatusOK, and
				// prepareOutputDir's own deferred cleanup removes the
				// scratch directory those outputs point into before
				// anything downstream could read them. That is a
				// deliberate choice, not an oversight this comment is
				// papering over: a handler killed moments after writing an
				// artifact may have left it mid-write, and committing a
				// truncated file under the job's real output name is worse
				// than committing nothing. Reviving this would need two
				// changes made together, not one — populating Outputs here
				// again AND widening worker/runner.go's commit gate to
				// include StatusKilled — so a future change to either side
				// alone does not silently start committing partial output.
				Permanent: res.Permanent,
			}
		}
		res.ExitCode = exitCode
		res.Signal = 0

		return &res
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

// Reclaim is a no-op for this rung. That is no longer because nothing can
// leak: the child used to share the worker's own process group, which
// would let a signal aimed at the worker's whole group reach it too, but
// now that it is its own group leader (see sysProcAttr in
// procattr_unix.go, Setpgid), a worker that dies mid-attempt can leave an
// orphaned child running with nothing left to signal it. Actually sweeping
// those up is deferred — it needs a worker identity stable across
// restarts, which this rung does not have today; see the SDD ledger's
// Phase 3 note on the same gap for the stronger rungs.
func (e *Executor) Reclaim(context.Context, id.WorkerID) error { return nil }

// Close releases the executor's own resources. Subprocess holds none —
// every pipe and process it creates is scoped to a single Run.
func (e *Executor) Close() error { return nil }
