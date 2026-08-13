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
)

// Main is the sandboxed child's entrypoint. It builds a bare job.Registry
// from defs and a credential-free artifact.Service over a local directory,
// reads one exec.Request from its request descriptor, runs the matching
// handler, and writes one exec.Result to its result descriptor.
//
// Main never returns: it calls os.Exit. A handler returning an error is
// exit 0, since that is a business outcome the Result frame already
// carries as StatusHandlerError. A nonzero exit means the shim itself
// could not produce a Result frame at all — a request that failed to
// decode, most likely — which is the one case the wire protocol cannot
// report through its own channel.
func Main(defs ...job.Registrable) {
	os.Exit(mainExitCode(defs))
}

// mainExitCode does the real work of Main and returns the process exit
// code, rather than calling os.Exit itself. os.Exit does not run deferred
// calls, so a single call to it right at the top of Main — after every
// defer here has already unwound — is what lets the signal handler and
// the cancel func clean up on every path.
func mainExitCode(defs []job.Registrable) int {
	//nolint:gosec // G115: fd numbers come from a small, non-negative process descriptor space, never from attacker input.
	in := os.NewFile(uintptr(fdFromEnv(EnvRequestFD, defaultRequestFD)), "dispatch-exec-request")
	//nolint:gosec // G115: fd numbers come from a small, non-negative process descriptor space, never from attacker input.
	out := os.NewFile(uintptr(fdFromEnv(EnvResultFD, defaultResultFD)), "dispatch-exec-result")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		if _, ok := <-sigCh; ok {
			cancel()
		}
	}()

	if err := Run(ctx, in, out, defs); err != nil {
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
func Run(ctx context.Context, in io.Reader, out io.Writer, defs []job.Registrable) error {
	frame, err := wire.Decode(in)
	if err != nil {
		return fmt.Errorf("dispatch/exec/shim: read request: %w", err)
	}

	req := frame.Request
	if req == nil {
		return errors.New("dispatch/exec/shim: frame carries no request")
	}

	if verr := req.Validate(); verr != nil {
		return fmt.Errorf("dispatch/exec/shim: %w", verr)
	}

	registry := job.NewRegistry()
	for _, d := range defs {
		d.Register(registry)
	}

	if req.Fingerprint != "" {
		if got := exec.Fingerprint(registry.Names()); got != req.Fingerprint {
			return writeResult(out, &exec.Result{
				Status: exec.StatusLaunchFailed,
				HandlerErr: fmt.Sprintf(
					"fingerprint mismatch: request wants %s, this binary's handler set is %s",
					req.Fingerprint, got,
				),
			})
		}
	}

	handler, ok := registry.Get(req.Name)
	if !ok {
		return writeResult(out, &exec.Result{
			Status:     exec.StatusLaunchFailed,
			HandlerErr: fmt.Sprintf("no handler registered for job %q", req.Name),
		})
	}

	svc := newAccessorService(req)
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: req.JobID.String()}

	if len(req.PriorOutputs) > 0 {
		if serr := seedPriorOutputs(ctx, svc, owner, req.PriorOutputs); serr != nil {
			return fmt.Errorf("dispatch/exec/shim: %w", serr)
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
		return fmt.Errorf("dispatch/exec/shim: collect outputs: %w", err)
	}

	res.Outputs = outputs

	return writeResult(out, res)
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
