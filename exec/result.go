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
