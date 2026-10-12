package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func expiredWorkerRun(t *testing.T, s durable.Store, options drt.Options) durable.Key {
	t.Helper()
	key := durable.Key{Namespace: options.Namespace, WorkflowID: "expired", RunID: "run"}
	if _, err := s.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "removed", BuildID: "retired", Queue: "retired", RunTimeout: time.Microsecond}); err != nil {
		t.Fatal(err)
	}
	return key
}
func TestWorkerExecutionTimeoutRetiredBuild(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	key := expiredWorkerRun(t, s, options)
	w := newWorker(t, s, options)
	runTask(t, w, drt.TaskExecutionTimeout)
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateTimedOut {
		t.Fatalf("retired build timeout: %+v %v", e, err)
	}
	if worked, err := w.RunOnce(t.Context(), drt.TaskExecutionTimeout); err != nil || worked {
		t.Fatalf("duplicate timeout: %t %v", worked, err)
	}
}
func TestWorkerExecutionTimeoutAutomaticPolling(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	key := expiredWorkerRun(t, s, options)
	w := newWorker(t, s, options)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	for {
		e, err := s.GetExecution(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if e.State == durable.StateTimedOut {
			break
		}
		select {
		case err = <-done:
			t.Fatalf("worker stopped before timeout: %v", err)
		case <-ctx.Done():
			t.Fatal("timeout poller did not close execution")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout poller did not stop")
	}
}

type timeoutRetryStore struct {
	durable.Store
	requests []durable.ExecutionTimeoutRequest
	failure  error
	lose     bool
}

func (s *timeoutRetryStore) ApplyExecutionTimeout(ctx context.Context, r durable.ExecutionTimeoutRequest) (durable.Receipt, error) {
	s.requests = append(s.requests, r)
	if s.failure != nil {
		return durable.Receipt{}, s.failure
	}
	receipt, err := s.Store.ApplyExecutionTimeout(ctx, r)
	if err == nil && s.lose && len(s.requests) == 1 {
		return durable.Receipt{}, errors.New("lost timeout response")
	}
	return receipt, err
}
func TestWorkerExecutionTimeoutRetry(t *testing.T) {
	for _, mode := range []string{"lost", "transient", "definitive"} {
		t.Run(mode, func(t *testing.T) {
			s := &timeoutRetryStore{Store: memory.New(), lose: mode == "lost"}
			options := workerOptions(t)
			expiredWorkerRun(t, s, options)
			if mode == "transient" {
				s.failure = errors.New("database unavailable")
			}
			if mode == "definitive" {
				s.failure = durable.ErrLeaseLost
			}
			worked, err := newWorker(t, s, options).RunOnce(t.Context(), drt.TaskExecutionTimeout)
			want := 3
			if mode == "lost" {
				want = 2
			}
			if mode == "definitive" {
				want = 1
			}
			if !worked || len(s.requests) != want || (mode == "lost" && err != nil) || (s.failure != nil && !errors.Is(err, s.failure)) {
				t.Fatalf("retry: worked=%t requests=%d err=%v", worked, len(s.requests), err)
			}
			for _, r := range s.requests {
				if r != s.requests[0] {
					t.Fatal("retry rebuilt timeout intent")
				}
			}
		})
	}
}
func TestWorkerChildExecutionTimeout(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		_, err := w.ChildWorkflow("child", "removed", nil, drt.ChildOptions{BuildID: "retired", Queue: "retired", RunTimeout: time.Microsecond, ExecutionTimeout: time.Hour}).Get()
		if errors.Is(err, drt.ErrChildTimedOut) {
			return []byte("child timed out"), nil
		}
		return nil, err
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	link, err := s.GetChildExecution(t.Context(), key, "child")
	if err != nil || link.Start.RunTimeout != time.Microsecond || link.Start.ExecutionTimeout != time.Hour {
		t.Fatalf("child options: %+v %v", link, err)
	}
	runTask(t, w, drt.TaskExecutionTimeout)
	runTask(t, w, drt.TaskChildDelivery)
	runTask(t, w, durable.TaskWorkflow)
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.State != durable.StateCompleted || string(e.Output) != "child timed out" {
		t.Fatalf("parent: %+v %v", e, err)
	}
	before, err := s.GetExecution(t.Context(), link.Start.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.RunOnce(t.Context(), drt.TaskExecutionTimeout); err != nil {
		t.Fatal(err)
	}
	after, err := s.GetExecution(t.Context(), link.Start.Key)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("reprocessed child: %+v %v", after, err)
	}
}

type timeoutClaimStore struct {
	durable.Store
	edit    func(*durable.ExecutionTimeoutTask)
	failure error
	applied bool
}

func (s *timeoutClaimStore) ClaimExecutionTimeout(ctx context.Context, r durable.ExecutionTimeoutClaimRequest) (*durable.ExecutionTimeoutTask, error) {
	if s.failure != nil {
		return nil, s.failure
	}
	grant, err := s.Store.ClaimExecutionTimeout(ctx, r)
	if grant != nil && s.edit != nil {
		s.edit(grant)
	}
	return grant, err
}
func (s *timeoutClaimStore) ApplyExecutionTimeout(ctx context.Context, r durable.ExecutionTimeoutRequest) (durable.Receipt, error) {
	s.applied = true
	return s.Store.ApplyExecutionTimeout(ctx, r)
}
func TestWorkerExecutionTimeoutRejectsMalformedGrant(t *testing.T) {
	for _, mode := range []string{"namespace", "owner", "epoch", "attempt", "lease", "kind", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			s := &timeoutClaimStore{Store: memory.New(), edit: func(grant *durable.ExecutionTimeoutTask) {
				switch mode {
				case "namespace":
					grant.Namespace = "other"
				case "owner":
					grant.Owner = "other"
				case "epoch":
					grant.Epoch = 0
				case "attempt":
					grant.Attempt = 0
				case "lease":
					grant.LeaseUntil = time.Time{}
				case "kind":
					grant.Kind = "unknown"
				case "deadline":
					grant.DeadlineAt = time.Time{}
				}
			}}
			options := workerOptions(t)
			expiredWorkerRun(t, s, options)
			worked, err := newWorker(t, s, options).RunOnce(t.Context(), drt.TaskExecutionTimeout)
			if !worked || !errors.Is(err, durable.ErrInvalid) || s.applied {
				t.Fatalf("malformed grant applied: %t %t %v", worked, s.applied, err)
			}
		})
	}
}
func TestWorkerExecutionTimeoutPollerFailure(t *testing.T) {
	failure := errors.New("timeout claim unavailable")
	s := &timeoutClaimStore{Store: memory.New(), failure: failure}
	options := workerOptions(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := newWorker(t, s, options).Run(ctx); !errors.Is(err, failure) {
		t.Fatalf("timeout poller hid failure: %v", err)
	}
}
