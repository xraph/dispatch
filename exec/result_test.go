package exec_test

import (
	"errors"
	"fmt"
	"strings"
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
			name:     "exit code without a handler message",
			result:   exec.Result{Status: exec.StatusHandlerError, ExitCode: 137},
			wantIs:   exec.ErrHandler,
			wantText: "exit 137",
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
			if !strings.Contains(err.Error(), tt.wantText) {
				t.Errorf("Err() = %q, want it to contain %q", err.Error(), tt.wantText)
			}
		})
	}
}

func TestResult_ErrPreservesTheCause(t *testing.T) {
	// The point of Cause: an in-process attempt must lose nothing. A rung
	// that flattened the handler's error to a string would break every
	// errors.Is and errors.As an extension or the worker performs on it.
	sentinel := errors.New("upstream unavailable")
	cause := fmt.Errorf("fetch config: %w", sentinel)

	err := (&exec.Result{
		Status:     exec.StatusHandlerError,
		HandlerErr: cause.Error(),
		Cause:      cause,
	}).Err()

	if !errors.Is(err, sentinel) {
		t.Errorf("errors.Is(%v, sentinel) = false, want true", err)
	}
	if !errors.Is(err, exec.ErrHandler) {
		t.Errorf("errors.Is(%v, ErrHandler) = false, want true — the status sentinel must still match", err)
	}
	// The handler's own text, with no exec framing: this is what lands in
	// job.LastError, the DLQ entry, and the logs.
	if got, want := err.Error(), cause.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	// errors.As has to reach a caller's own error type through the same
	// value, since that is how extensions inspect a failure.
	var target *typedError
	typed := &typedError{field: "name"}
	err = (&exec.Result{Status: exec.StatusHandlerError, Cause: fmt.Errorf("wrapped: %w", typed)}).Err()
	if !errors.As(err, &target) {
		t.Fatalf("errors.As(%v, **typedError) = false, want true", err)
	}
	if target.field != "name" {
		t.Errorf("target.field = %q, want %q", target.field, "name")
	}
}

// typedError is a caller-defined error type, standing in for the ones an
// extension matches with errors.As.
type typedError struct{ field string }

func (e *typedError) Error() string { return "invalid field " + e.field }

func TestResult_ErrCarriesPermanence(t *testing.T) {
	// Permanent is the wire-visible signal: a rung that ran the handler in
	// another process cannot send back an error chain, so this flag is how
	// it declines the retry schedule.
	err := (&exec.Result{
		Status:     exec.StatusHandlerError,
		HandlerErr: "malformed input",
		Permanent:  true,
	}).Err()

	var execErr *exec.Error
	if !errors.As(err, &execErr) {
		t.Fatalf("errors.As(%v, **exec.Error) = false, want true", err)
	}
	if !execErr.Permanent {
		t.Error("Error.Permanent = false, want true")
	}

	notPermanent := (&exec.Result{Status: exec.StatusHandlerError, HandlerErr: "transient"}).Err()
	if !errors.As(notPermanent, &execErr) {
		t.Fatalf("errors.As(%v, **exec.Error) = false, want true", notPermanent)
	}
	if execErr.Permanent {
		t.Error("Error.Permanent = true for a plain handler error, want false")
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
