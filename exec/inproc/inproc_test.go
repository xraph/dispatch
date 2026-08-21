package inproc_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch"
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

func TestExecutor_RunKeepsTheHandlerErrorWhole(t *testing.T) {
	// In-process there is no boundary to lose the chain at, so the Result
	// carries the handler's error itself. Without this, errors.Is against
	// dispatch.ErrPermanent — and against any sentinel an extension owns —
	// stops matching the moment a job is routed through an executor.
	sentinel := errors.New("upstream gone")

	r := job.NewRegistry()
	job.NewDefinition("test.job", func(context.Context, payload) error {
		return fmt.Errorf("fetch: %w", sentinel)
	}).Register(r)

	res, err := inproc.New(r).Run(context.Background(), &exec.Request{
		JobID: id.NewJobID(),
		Name:  "test.job",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !errors.Is(res.Cause, sentinel) {
		t.Errorf("errors.Is(Cause, sentinel) = false, want true (Cause = %v)", res.Cause)
	}
	if res.Permanent {
		t.Error("Permanent = true for an ordinary handler error, want false")
	}
	if !errors.Is(res.Err(), sentinel) {
		t.Errorf("errors.Is(Err(), sentinel) = false, want true (Err() = %v)", res.Err())
	}
}

func TestExecutor_RunFlagsPermanentFailures(t *testing.T) {
	// The flag is what an out-of-process rung will have to send instead of
	// an error chain, so the in-process rung computes it too and the worker
	// reads permanence one way for every rung.
	r := job.NewRegistry()
	job.NewDefinition("test.job", func(context.Context, payload) error {
		return fmt.Errorf("malformed payload: %w", dispatch.ErrPermanent)
	}).Register(r)

	res, err := inproc.New(r).Run(context.Background(), &exec.Request{
		JobID: id.NewJobID(),
		Name:  "test.job",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !res.Permanent {
		t.Error("Permanent = false for an error wrapping dispatch.ErrPermanent, want true")
	}
	if !errors.Is(res.Err(), dispatch.ErrPermanent) {
		t.Errorf("errors.Is(Err(), ErrPermanent) = false, want true (Err() = %v)", res.Err())
	}
	if got, want := res.Err().Error(), "malformed payload: dispatch: permanent failure"; got != want {
		t.Errorf("Err() = %q, want %q — the handler's own text, unframed", got, want)
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
