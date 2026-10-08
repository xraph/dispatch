package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

type delayedReopen struct {
	*memory.Store
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (s *delayedReopen) ReopenRun(ctx context.Context, runID id.RunID, generation int64) error {
	if s.calls.Add(1) == 1 {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.ReopenRun(ctx, runID, generation)
}

func TestReplayFrom_RejectsOverlappingPlanAfterWinnerCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := &delayedReopen{Store: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	r, reg, ends := newReplayRunner(t, s, s.Store)
	var executions atomic.Int32
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("overlap", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("first", func(context.Context) error { return nil }); err != nil {
			return err
		}
		executions.Add(1)
		return nil
	}))
	run, err := r.StartRaw(ctx, "overlap", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	waitEnd(t, ends)
	delayed := make(chan error, 1)
	go func() {
		_, replayErr := r.ReplayFrom(ctx, run.ID, "first")
		delayed <- replayErr
	}()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("first replay did not reach its claim")
	}
	_, err = r.ReplayFrom(ctx, run.ID, "first")
	if err != nil {
		close(s.release)
		t.Fatal(err)
	}
	waitEnd(t, ends)
	close(s.release)
	if err := <-delayed; !errors.Is(err, dispatch.ErrInvalidState) {
		t.Fatalf("delayed replay error = %v, want ErrInvalidState", err)
	}
	if got := executions.Load(); got != 2 {
		t.Fatalf("executions = %d, want original plus one replay", got)
	}
	// A fresh operator request, planned after completion, may replay again.
	if _, err := r.ReplayFrom(ctx, run.ID, "first"); err != nil {
		t.Fatal(err)
	}
	waitEnd(t, ends)
	if got := executions.Load(); got != 3 {
		t.Fatalf("executions after fresh replay = %d, want 3", got)
	}
}
