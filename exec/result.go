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

	// Permanent means retrying cannot change the outcome, so the job skips
	// its remaining attempts. This is the wire-visible half of permanence:
	// a rung that ran the handler in another process cannot send back a Go
	// error chain, but it can send back this flag.
	Permanent bool

	// Cause is the handler's own error, kept whole. It exists so an
	// in-process attempt loses nothing: errors.Is and errors.As against a
	// caller's sentinels keep working through the exec layer.
	//
	// It is deliberately not serialised — an error chain does not survive a
	// process boundary — which is why Permanent exists alongside it. A rung
	// that marshals a Result leaves this nil and sets Permanent instead.
	Cause error `json:"-"`
}

// Err converts a Result into the error the worker propagates. It returns
// nil for StatusOK and an *Error otherwise.
func (r *Result) Err() error {
	if r == nil || r.Status == StatusOK {
		return nil
	}

	return &Error{
		Status:    r.Status,
		Msg:       r.HandlerErr,
		ExitCode:  r.ExitCode,
		Signal:    r.Signal,
		Permanent: r.Permanent,
		Cause:     r.Cause,
	}
}

// Error is a failed execution attempt. It carries the Status so retry
// policy can branch on how the attempt failed rather than parsing text.
type Error struct {
	Status   Status
	Msg      string
	ExitCode int
	Signal   int

	// Permanent means the job must not be retried, however the attempt was
	// carried. The worker treats it exactly as it treats an error wrapping
	// dispatch.ErrPermanent.
	Permanent bool

	// Cause is the handler's own error when the attempt ran in this
	// process. It is what errors.Is and errors.As reach through Unwrap.
	Cause error
}

// Error implements the error interface.
//
// When a cause survived — an in-process attempt — the handler's own text is
// returned verbatim. The job's LastError, its DLQ entry, and the worker's
// logs then read exactly as they did before execution isolation existed,
// and the exec framing is not stacked on top of an error the caller already
// understands. Only a failure with no cause to speak for it, which is every
// out-of-process shape, is rendered with the status.
func (e *Error) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}

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

// Unwrap returns the status sentinel together with the handler's own error
// when one survived, so errors.Is reaches ErrHandler and errors.Is/errors.As
// reach the caller's own sentinels and error types through the same value.
//
// The multi-error form (Go 1.20) is what lets both hold at once. Returning
// only the sentinel would silently destroy user error identity for every
// in-process attempt, and dispatch.ErrPermanent with it.
func (e *Error) Unwrap() []error {
	var errs []error
	if sentinel := e.sentinel(); sentinel != nil {
		errs = append(errs, sentinel)
	}
	if e.Cause != nil {
		errs = append(errs, e.Cause)
	}

	return errs
}

// sentinel maps the status onto its package-level error value.
func (e *Error) sentinel() error {
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
