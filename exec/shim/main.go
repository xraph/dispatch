package shim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/wire"
	"github.com/xraph/dispatch/job"
)

const (
	// EnvRequestFD names the environment variable that overrides which
	// file descriptor Main reads the exec.Request from. Unset, Main reads
	// fd 3 — the parent's convention for the first descriptor past
	// stdin/stdout/stderr.
	EnvRequestFD = "DISPATCH_EXEC_REQUEST_FD"

	// EnvResultFD names the environment variable that overrides which
	// file descriptor Main writes the exec.Result to. Unset, Main writes
	// fd 4.
	EnvResultFD = "DISPATCH_EXEC_RESULT_FD"

	// ArgName is the argv[0] marker a parent sets when it re-execs its own
	// binary into the shim, distinguishing that invocation from an
	// ordinary run of the worker.
	ArgName = "dispatch-exec"

	// defaultRequestFD is the descriptor Main reads from absent an
	// EnvRequestFD override.
	defaultRequestFD = 3

	// defaultResultFD is the descriptor Main writes to absent an
	// EnvResultFD override.
	defaultResultFD = 4

	// EnvRlimitAS, EnvRlimitNoFile, EnvRlimitNProc, EnvRlimitCore, and
	// EnvRlimitFSize name the environment variables the parent uses to
	// pass POSIX resource limits into the child. Go cannot set a child's
	// rlimits through os/exec's SysProcAttr, so the parent (see buildEnv
	// in exec/subprocess) passes the desired values here, and Main
	// applies them itself via syscall.Setrlimit — see applyRlimits —
	// before running anything. Each is unset when the parent has no
	// value to send, except EnvRlimitCore, which the parent always sends
	// as "0".
	EnvRlimitAS     = "DISPATCH_RLIMIT_AS"
	EnvRlimitNoFile = "DISPATCH_RLIMIT_NOFILE"
	EnvRlimitNProc  = "DISPATCH_RLIMIT_NPROC"
	EnvRlimitCore   = "DISPATCH_RLIMIT_CORE"
	EnvRlimitFSize  = "DISPATCH_RLIMIT_FSIZE"
)

// Main is the sandboxed child's entrypoint. It builds a bare job.Registry
// from defs and a credential-free artifact.Service over a local directory,
// reads one exec.Request from its request descriptor, runs the matching
// handler, and writes one exec.Result to its result descriptor.
//
// Main never returns: it calls os.Exit. A handler returning an error is
// exit 0, since that is a business outcome the Result frame already
// carries as StatusHandlerError. A nonzero exit means the shim itself
// failed the attempt before or instead of running the handler — a request
// that failed to decode, or one whose fingerprint or handler name did not
// resolve — which is what lets the parent distinguish those cases from a
// clean run without having to inspect the frame first: the exit code
// corroborates what the Result already says.
func Main(defs ...job.Registrable) {
	os.Exit(mainExitCode(defs))
}

// mainExitCode does the real work of Main and returns the process exit
// code, rather than calling os.Exit itself. os.Exit does not run deferred
// calls, so a single call to it right at the top of Main — after every
// defer here has already unwound — is what lets the signal handler and
// the cancel func clean up on every path.
//
// It calls run rather than Run because Run's public contract discards the
// Result on success, and the exit code has to be derived from that Result:
// a fingerprint mismatch or an unknown handler makes Run return a nil
// error (the frame is the report, by design — see Run's doc comment), so
// err == nil alone cannot tell mainExitCode apart from a clean success.
// Only StatusHandlerError keeps exit 0; every other non-OK status,
// including one Run reported without an error, is a nonzero exit.
func mainExitCode(defs []job.Registrable) int {
	// Applied before anything else touches the request: RLIMIT_CORE in
	// particular exists to stop a segfaulting handler from dumping the
	// input that crashed it (and the process's own memory) to disk, which
	// only holds if the limit is in place before the handler ever runs.
	applyRlimits()

	//nolint:gosec // G115: fd numbers come from a small, non-negative process descriptor space, never from attacker input.
	in := os.NewFile(uintptr(fdFromEnv(EnvRequestFD, defaultRequestFD)), "dispatch-exec-request")
	//nolint:gosec // G115: fd numbers come from a small, non-negative process descriptor space, never from attacker input.
	out := os.NewFile(uintptr(fdFromEnv(EnvResultFD, defaultResultFD)), "dispatch-exec-result")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// done unblocks the goroutine below once mainExitCode is about to
	// return. signal.Stop alone only unregisters future deliveries; it
	// neither closes sigCh nor wakes a goroutine already parked on
	// <-sigCh, so without this the goroutine leaks on every call that
	// never receives a SIGTERM — which, outside of tests, is every call,
	// since Main's own os.Exit would otherwise mask the leak by killing
	// the process before it could accumulate.
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-done:
		}
	}()

	res, err := run(ctx, in, out, defs)
	if err != nil {
		return 1
	}

	// StatusHandlerError stays exit 0: it is a business outcome, and the
	// parent reads it from the Result frame, not the exit code.
	// StatusLaunchFailed is the shim's own failure and must not look like
	// a clean exit.
	if res.Status == exec.StatusLaunchFailed {
		return 1
	}

	return 0
}

// fdFromEnv reads a file descriptor number from the named environment
// variable, falling back to def when the variable is unset or unparsable.
func fdFromEnv(name string, def int) int {
	v, ok := os.LookupEnv(name)
	if !ok {
		return def
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}

	return n
}

// rlimitSpec pairs the environment variable the parent sets with the raw
// setrlimit(2) resource number and a label for diagnostics.
type rlimitSpec struct {
	env      string
	resource int
	label    string
}

// rlimitSpecs lists every limit applyRlimits knows how to apply.
//
// RLIMIT_NPROC's resource number is hardcoded rather than named from the
// syscall package because Go's syscall package does not export
// RLIMIT_NPROC at all, on any platform — it was trimmed from the
// generated zerrors tables along with RLIMIT_MEMLOCK and RLIMIT_RSS. The
// number itself is not portable either: Linux's <bits/resource.h> puts it
// at 6, while Darwin and the rest of the BSD family put it at 7. This
// rung's CI runs on Linux and its developers on Darwin, so those are the
// two values handled explicitly; runtime.GOOS is read once here rather
// than behind a build tag because nothing else in this function needs
// per-platform source files.
func rlimitSpecs() []rlimitSpec {
	nproc := 6 // Linux RLIMIT_NPROC
	if runtime.GOOS == "darwin" {
		nproc = 7 // Darwin/BSD RLIMIT_NPROC
	}

	return []rlimitSpec{
		// Applied first: this is the one limit the parent always sends,
		// and it is the one the worker's own security promise depends
		// on most directly.
		{EnvRlimitCore, syscall.RLIMIT_CORE, "RLIMIT_CORE"},
		{EnvRlimitAS, syscall.RLIMIT_AS, "RLIMIT_AS"},
		{EnvRlimitNoFile, syscall.RLIMIT_NOFILE, "RLIMIT_NOFILE"},
		{EnvRlimitFSize, syscall.RLIMIT_FSIZE, "RLIMIT_FSIZE"},
		{EnvRlimitNProc, nproc, "RLIMIT_NPROC"},
	}
}

// applyRlimits reads every limit the parent set in the environment (see
// EnvRlimitAS and friends) and applies it via syscall.Setrlimit before the
// handler ever runs.
//
// A limit whose env var is unset is left alone — that is how buildEnv
// says "no opinion" for anything but RLIMIT_CORE, which it always sends.
// A limit that fails to apply is logged to stderr and skipped rather than
// treated as a launch failure: unlike checkLaunch's platform refusal in
// exec/subprocess (an all-or-nothing decision about whether this rung has
// anything to offer at all), an individual rlimit is one layer among
// several — the uid boundary and the process-group kill are still in
// effect regardless. Making it fatal would also make this rung unusable
// in practice on Darwin, whose kernel rejects setrlimit(RLIMIT_AS, ...)
// outright (EINVAL) no matter what value is requested; failing the whole
// launch over a limit the current kernel does not support at all is worse
// than proceeding with the rest of the isolation intact and saying so.
func applyRlimits() {
	for _, s := range rlimitSpecs() {
		v, ok := os.LookupEnv(s.env)
		if !ok {
			continue
		}

		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			fmt.Fprintf(os.Stderr, "dispatch/exec/shim: %s value %q is invalid, skipping\n", s.label, v)
			continue
		}

		//nolint:gosec // G115: n is validated non-negative above; it comes from the parent's own constructed environment, never attacker input.
		lim := &syscall.Rlimit{Cur: uint64(n), Max: uint64(n)}
		if err := syscall.Setrlimit(s.resource, lim); err != nil {
			fmt.Fprintf(os.Stderr, "dispatch/exec/shim: setrlimit %s=%d failed, continuing without it: %v\n", s.label, n, err)
		}
	}
}

// Run is the testable core of the shim: it reads one exec.Request from in,
// runs the matching handler out of defs, and writes one exec.Result to
// out. Splitting it from Main is what lets tests drive it with in-memory
// buffers instead of real file descriptors.
//
// Run returns a non-nil error only when it could not produce a Result
// frame at all — the request failed to decode, or was malformed enough
// that there is no attempt to report on. Every failure that happens once a
// well-formed request is in hand — an unknown handler, a fingerprint
// mismatch, the handler itself erroring — is reported by writing a Result
// frame and returning nil, because the frame is the report, not the
// error: the parent reads Status, not the shim's exit code, to learn what
// happened.
//
// Run discards the Result it produces, keeping its signature exactly what
// callers (and the brief's test) expect. mainExitCode needs that Result to
// derive an exit code, which Run's error alone cannot give it — see run.
func Run(ctx context.Context, in io.Reader, out io.Writer, defs []job.Registrable) error {
	_, err := run(ctx, in, out, defs)

	return err
}

// run is Run's implementation, plus the Result it wrote. mainExitCode is
// the reason this exists as a separate, unexported function: Run's error
// return is nil for a launch failure by design (the frame is the report),
// so the only way mainExitCode can tell a launch failure apart from a
// clean success is to inspect the Result's Status directly, which Run's
// public contract does not expose.
func run(ctx context.Context, in io.Reader, out io.Writer, defs []job.Registrable) (*exec.Result, error) {
	frame, err := wire.Decode(in)
	if err != nil {
		return nil, fmt.Errorf("dispatch/exec/shim: read request: %w", err)
	}

	req := frame.Request
	if req == nil {
		return nil, errors.New("dispatch/exec/shim: frame carries no request")
	}

	if verr := req.Validate(); verr != nil {
		return nil, fmt.Errorf("dispatch/exec/shim: %w", verr)
	}

	registry := job.NewRegistry()
	for _, d := range defs {
		d.Register(registry)
	}

	if req.Fingerprint != "" {
		if got := exec.Fingerprint(registry.Names()); got != req.Fingerprint {
			res := &exec.Result{
				Status: exec.StatusLaunchFailed,
				HandlerErr: fmt.Sprintf(
					"fingerprint mismatch: request wants %s, this binary's handler set is %s",
					req.Fingerprint, got,
				),
			}

			return res, writeResult(out, res)
		}
	}

	handler, ok := registry.Get(req.Name)
	if !ok {
		res := &exec.Result{
			Status:     exec.StatusLaunchFailed,
			HandlerErr: fmt.Sprintf("no handler registered for job %q", req.Name),
		}

		return res, writeResult(out, res)
	}

	svc := newAccessorService(req)
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: req.JobID.String()}

	if len(req.PriorOutputs) > 0 {
		if serr := seedPriorOutputs(ctx, svc, owner, req.PriorOutputs); serr != nil {
			return nil, fmt.Errorf("dispatch/exec/shim: %w", serr)
		}
	}

	ctx = artifact.WithAccessor(ctx, newAccessor(svc, req, owner, req.Attempt))

	if !req.Deadline.IsZero() {
		var cancel context.CancelFunc

		ctx, cancel = context.WithDeadline(ctx, req.Deadline)
		defer cancel()
	}

	start := time.Now()
	handlerErr := handler(ctx, req.Payload)
	wallTime := time.Since(start)

	res := &exec.Result{Usage: exec.Usage{WallTime: wallTime}}
	if handlerErr != nil {
		res.Status = exec.StatusHandlerError
		res.HandlerErr = handlerErr.Error()
		res.Permanent = errors.Is(handlerErr, dispatch.ErrPermanent)
	} else {
		res.Status = exec.StatusOK
	}

	outputs, err := collectOutputs(req.OutputDir)
	if err != nil {
		return nil, fmt.Errorf("dispatch/exec/shim: collect outputs: %w", err)
	}

	res.Outputs = outputs

	return res, writeResult(out, res)
}

// writeResult encodes and writes the single result frame Run ever
// produces.
func writeResult(out io.Writer, res *exec.Result) error {
	if err := wire.Encode(out, &wire.Frame{Kind: wire.KindResult, Result: res}); err != nil {
		return fmt.Errorf("dispatch/exec/shim: encode result: %w", err)
	}

	return nil
}

// collectOutputs walks dir and reports every regular file found as an
// artifact the handler produced.
//
// It reads the filesystem rather than any manifest the handler could have
// populated itself, so a handler cannot claim an output it did not
// actually write. Dot-prefixed entries are skipped: LocalFS's Create
// writes into a hidden temp file before renaming it into place on Commit,
// so a leftover one (a write that was never committed or aborted) is not
// mistaken for a finished artifact. A missing directory is treated as no
// outputs rather than an error, since a handler that wrote nothing never
// causes LocalFS to create it.
func collectOutputs(dir string) ([]exec.OutputFile, error) {
	var outputs []exec.OutputFile

	// dir is req.OutputDir, a path the parent chose and mounted for this
	// attempt, never a value read out of the untrusted payload the
	// handler parses.
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error { //nolint:gosec // G703: dir is the request's own OutputDir, not attacker-controlled.
		if err != nil {
			return err
		}

		if d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", path, err)
		}

		outputs = append(outputs, exec.OutputFile{
			Name: d.Name(),
			Size: info.Size(),
		})

		return nil
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	return outputs, nil
}
