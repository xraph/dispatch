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

	// ArgName is the argv[1] marker a parent sets when it re-execs its own
	// binary into the shim, distinguishing that invocation from an
	// ordinary run of the worker. argv[0] is still the binary path itself,
	// same as any other invocation — subprocess.Executor.Run builds the
	// child's argv as [binary, ArgName, ...WithArgs], so this is the first
	// argument after the binary, not the zeroth.
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

	// EnvRlimitStrict names the environment variable the parent sets
	// (see subprocess.WithStrictRlimits) to ask Main to fail the launch
	// outright when an rlimit it was actually asked to apply fails for a
	// reason other than "this platform does not support that limit at
	// all" — see applyRlimits for the distinction. Set to any non-empty
	// value to enable; unset (the default) keeps every rlimit failure
	// non-fatal.
	EnvRlimitStrict = "DISPATCH_RLIMIT_STRICT"
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
	// true: this is the real child, so mainExitCode's signal teardown
	// must leave SIGTERM ignored rather than restore the default. See the
	// exiting parameter's own comment for why that matters here and not
	// for the in-process callers that pass false.
	os.Exit(mainExitCode(defs, true))
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
func mainExitCode(defs []job.Registrable, exiting bool) int {
	// fdFromEnv guarantees a non-negative descriptor, so the uintptr
	// conversion cannot wrap.
	// #nosec G115 -- fdFromEnv rejects negatives and both defaults are positive, so neither conversion can wrap.
	in := os.NewFile(uintptr(fdFromEnv(EnvRequestFD, defaultRequestFD)), "dispatch-exec-request")
	// #nosec G115 -- fdFromEnv rejects negatives and both defaults are positive, so neither conversion can wrap.
	out := os.NewFile(uintptr(fdFromEnv(EnvResultFD, defaultResultFD)), "dispatch-exec-result")

	// Applied before anything else touches the request: RLIMIT_CORE in
	// particular exists to stop a segfaulting handler from dumping the
	// input that crashed it (and the process's own memory) to disk, which
	// only holds if the limit is in place before the handler ever runs.
	//
	// failures excludes anything applyRlimits judged "this platform does
	// not support that limit at all" — Darwin's blanket refusal of
	// RLIMIT_AS being the standing example — since that is a structural,
	// permanent fact about the kernel this process is running under, not
	// a misconfiguration. What is left is failures a correctly-configured
	// limit should not produce on a platform that claims to support it —
	// EPERM because the requested value exceeds the hard limit is the
	// common shape — which is what EnvRlimitStrict (see
	// subprocess.WithStrictRlimits) opts into treating as a launch
	// failure rather than a warning.
	if failures := applyRlimits(); len(failures) > 0 && os.Getenv(EnvRlimitStrict) != "" {
		res := &exec.Result{
			Status:     exec.StatusLaunchFailed,
			HandlerErr: fmt.Sprintf("dispatch/exec/shim: rlimits requested by WithStrictRlimits failed to apply: %s", joinRlimitFailures(failures)),
		}
		_ = writeResult(out, res) //nolint:errcheck // best-effort: the process is exiting non-zero regardless of whether the frame made it across

		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)

	// Registered before the Stop below so that it runs after it: defers
	// unwind last-in-first-out, and the order is what this is for.
	// signal.Stop puts SIGTERM back on its default disposition, which is
	// fatal, while this process is still alive and still a target. The
	// parent's kill ladder (subprocess.terminate) signals the whole
	// process group the moment a deadline fires, and a handler that
	// cooperated has returned ctx.Err() at that same instant off the
	// child's own copy of the deadline, so the two events collide by
	// design. Without this, a SIGTERM landing in the gap between Stop and
	// the process actually exiting kills a child that had already written
	// its Result frame and settled on exit 0, and the parent reports
	// Signal 15 for a handler that shut down cleanly. Measured at roughly
	// 7% of runs under load before this was added.
	//
	// Only the real child does this. An in-process caller (the tests,
	// which pass exiting false) is not about to exit, and leaving SIGTERM
	// ignored for the rest of the test binary's life would outlive the
	// call that set it.
	if exiting {
		defer signal.Ignore(syscall.SIGTERM)
	}

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

// rlimitFailure describes one configured rlimit that applyRlimits could
// not apply for a reason other than "this platform is known not to
// support this limit at all" — see applyRlimits (rlimit_unix.go) for how
// that distinction is made. Declared here, without a build tag, rather
// than alongside applyRlimits itself, because mainExitCode needs the type
// on every platform: rlimit_other.go's non-Unix stub returns the same
// []rlimitFailure (always empty) so mainExitCode does not need its own
// build-tagged branch just to call applyRlimits.
type rlimitFailure struct {
	label string
	err   error
}

// joinRlimitFailures renders failures as a single diagnostic string for
// the launch-failure Result mainExitCode writes when EnvRlimitStrict is
// set. Every failure that reaches here already went to stderr individually
// by applyRlimits; this is the summary that ends up in the Result the
// caller actually sees, not a replacement for those lines.
func joinRlimitFailures(failures []rlimitFailure) string {
	parts := make([]string, len(failures))
	for i, f := range failures {
		parts[i] = fmt.Sprintf("%s: %v", f.label, f.err)
	}

	return strings.Join(parts, "; ")
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

	// A descriptor is never negative, and callers convert to uintptr, where a
	// negative would wrap to an enormous bogus fd instead of failing.
	if n < 0 {
		return def
	}

	return n
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
	// dir is the request's own OutputDir, not attacker-controlled.
	// #nosec G703 -- dir is req.OutputDir, chosen by the parent and delivered over the request fd, never handler input.
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
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
