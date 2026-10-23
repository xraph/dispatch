package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/memory"
)

// unreachableClaims times out every claim while down is set.
type unreachableClaims struct {
	*memory.Store
	down atomic.Bool
}

func (s *unreachableClaims) ClaimTask(ctx context.Context, r durable.ClaimRequest) (*durable.Task, error) {
	if s.down.Load() {
		return nil, fmt.Errorf("dispatch/postgres: claim execution task: timeout: %w", context.DeadlineExceeded)
	}
	return s.Store.ClaimTask(ctx, r)
}

func TestDurableEngineRidesOutClaimTimeouts(t *testing.T) {
	s := &unreachableClaims{Store: memory.New()}
	s.down.Store(true)
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	options := drt.Options{Namespace: t.Name(), Queue: "durable", BuildID: "v1", Owner: "worker",
		PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{"order": func(*drt.Workflow, []byte) ([]byte, error) { return []byte("done"), nil }}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(options))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	request := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"},
		RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}
	if _, err = eng.StartDurableWorkflow(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err = eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 2*time.Second, func() bool {
		return errors.Is(eng.Health(t.Context()), drt.ErrClaimUnknown)
	})
	s.down.Store(false)
	waitFor(t, 5*time.Second, func() bool {
		execution, getErr := s.GetExecution(t.Context(), request.Key)
		return getErr == nil && execution.State == durable.StateCompleted
	})
	waitFor(t, 5*time.Second, func() bool { return eng.Health(t.Context()) == nil })
	if status, _ := eng.DurableStatus(); status.State != drt.WorkerRunning {
		t.Fatalf("worker state: %s", status.State)
	}
}
