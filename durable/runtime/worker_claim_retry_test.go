package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

// claimTimeoutStore times out workflow claims the way Postgres does when
// its host is overloaded, until failures runs out. With commit set, each
// claim takes its task first, so only the reply is lost.
type claimTimeoutStore struct {
	durable.Store
	commit   bool
	failures atomic.Int64
}

func (s *claimTimeoutStore) ClaimTask(ctx context.Context, r durable.ClaimRequest) (*durable.Task, error) {
	if r.Kind != durable.TaskWorkflow || s.failures.Add(-1) < 0 {
		return s.Store.ClaimTask(ctx, r)
	}
	if s.commit {
		if _, err := s.Store.ClaimTask(ctx, r); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("dispatch/postgres: claim execution task: timeout: %w", context.DeadlineExceeded)
}

func TestRunRetriesClaimsWithUnknownOutcome(t *testing.T) {
	for _, tc := range []struct {
		name   string
		commit bool
	}{{"claim rolled back", false}, {"claim committed, reply lost", true}} {
		t.Run(tc.name, func(t *testing.T) {
			s := memory.New()
			store := &claimTimeoutStore{Store: s, commit: tc.commit}
			store.failures.Store(3)
			options := workerOptions(t)
			var calls atomic.Int64
			options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				return w.Activity("charge", "charge", "", nil).Get()
			}
			options.Activities["charge"] = func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) {
				calls.Add(1)
				return []byte("paid"), nil
			}
			w := newWorker(t, store, options)
			key := startWorkerRun(t, w, options)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()

			for {
				execution, err := s.GetExecution(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				if execution.State == durable.StateCompleted {
					break
				}
				select {
				case err = <-done:
					t.Fatalf("Run stopped on a claim timeout: %v", err)
				case <-ctx.Done():
					t.Fatalf("run never completed: %s", execution.State)
				case <-time.After(time.Millisecond):
				}
			}
			if calls.Load() != 1 {
				t.Errorf("activity ran %d times", calls.Load())
			}
			status := w.Status()
			if status.State != drt.WorkerRunning || status.ClaimFailures != 0 || !status.ClaimFailingSince.IsZero() {
				t.Errorf("status after recovery: %+v", status)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Run: %v", err)
			}
		})
	}
}

// A claim error the store can't classify might never clear, so the worker
// keeps retrying but reports it, instead of failing or going quiet.
func TestRunReportsClaimsThatKeepFailing(t *testing.T) {
	store := &claimTimeoutStore{Store: memory.New()}
	store.failures.Store(1 << 40)
	options := workerOptions(t)
	options.Workflows["order"] = func(*drt.Workflow, []byte) ([]byte, error) { return nil, nil }
	w := newWorker(t, store, options)
	startWorkerRun(t, w, options)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	for w.Status().ClaimFailures < 3 {
		select {
		case err := <-done:
			t.Fatalf("Run stopped on a claim timeout: %v", err)
		case <-ctx.Done():
			t.Fatal("claim failures were never reported")
		case <-time.After(time.Millisecond):
		}
	}
	status := w.Status()
	if status.State != drt.WorkerRunning || status.ClaimFailingSince.IsZero() || status.UnknownClaims == 0 {
		t.Errorf("status while claims fail: %+v", status)
	}
	if _, err := w.RunOnce(ctx, durable.TaskWorkflow); !errors.Is(err, drt.ErrClaimUnknown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("RunOnce: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
