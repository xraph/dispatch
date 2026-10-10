package runtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

type drainFailureError struct {
	entered, release chan struct{}
	once             sync.Once
}

func (e *drainFailureError) Error() string { return "definitive claim failure" }
func (e *drainFailureError) Is(target error) bool {
	if target == drt.ErrWorkerDraining {
		e.once.Do(func() { close(e.entered) })
		<-e.release
	}
	return target == durable.ErrInvalid
}

type drainFailureStore struct {
	durable.Store
	failure *drainFailureError
}

func (s *drainFailureStore) ClaimTask(ctx context.Context, r durable.ClaimRequest) (*durable.Task, error) {
	if r.Kind == durable.TaskWorkflow {
		return nil, s.failure
	}
	return s.Store.ClaimTask(ctx, r)
}
func TestDrainFailurePublishedBeforeRunReturns(t *testing.T) {
	e := &drainFailureError{entered: make(chan struct{}), release: make(chan struct{})}
	var released sync.Once
	defer released.Do(func() { close(e.release) })
	w := newWorker(t, &drainFailureStore{Store: memory.New(), failure: e}, workerOptions(t))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-e.entered:
	case <-ctx.Done():
		t.Fatal("failure barrier not reached")
	}
	h, err := w.BeginDrain(ctx, drt.DrainRequest{OperationID: "review", Deadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	before, err := w.WaitDrain(ctx, h)
	if !errors.Is(err, drt.ErrDrainIncomplete) || before.Complete {
		t.Fatalf("before: %+v %v", before, err)
	}
	released.Do(func() { close(e.release) })
	select {
	case err = <-done:
		if !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("Run: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Run did not finish")
	}
	after, err := w.WaitDrain(ctx, h)
	t.Logf("before Complete=%t; after Complete=%t error=%v state=%s", before.Complete, after.Complete, err, w.Status().State)
	if before.Complete != after.Complete || !errors.Is(err, drt.ErrDrainIncomplete) {
		t.Fatal("accepted drain outcome changed after returning failure")
	}
	if w.Status().State != drt.WorkerFailed {
		t.Fatal("failure status was lost")
	}
}
