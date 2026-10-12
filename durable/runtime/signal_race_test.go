package runtime_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

type signalDecisionRaceStore struct {
	durable.Store
	signal    durable.SignalRequest
	injected  atomic.Bool
	conflicts atomic.Int64
}

func (s *signalDecisionRaceStore) CommitTransition(ctx context.Context, request durable.CommitRequest) (durable.Receipt, error) {
	if s.injected.CompareAndSwap(false, true) {
		if _, err := s.SignalExecution(ctx, s.signal); err != nil {
			return durable.Receipt{}, err
		}
	}
	receipt, err := s.Store.CommitTransition(ctx, request)
	if errors.Is(err, durable.ErrRevisionConflict) {
		s.conflicts.Add(1)
	}
	return receipt, err
}

func TestSignalDuringDecisionAndPendingActivity(t *testing.T) {
	s := &signalDecisionRaceStore{Store: memory.New()}
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		first, err := w.ReceiveSignal("first", "approve").Get()
		if err != nil {
			return nil, err
		}
		if _, err = w.Activity("wait", "wait", "", nil).Get(); err != nil {
			return nil, err
		}
		second, err := w.ReceiveSignal("second", "approve").Get()
		return append(first, second...), err
	}
	options.Activities["wait"] = func(_ context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) { return nil, nil }
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	request := durable.SignalRequest{Key: key, RequestID: "first", BuildID: options.BuildID, Name: "approve", Input: []byte("A")}
	if _, err := worker.SignalExecution(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	s.signal = request
	s.signal.RequestID = "second"
	s.signal.Input = []byte("B")
	runTask(t, worker, durable.TaskWorkflow)
	if s.conflicts.Load() != 1 {
		t.Fatalf("test did not fence stale decision: %d", s.conflicts.Load())
	}
	runTask(t, worker, durable.TaskWorkflow) // The accepted signal wakes a still pending activity.
	runTask(t, worker, durable.TaskActivity)
	runTask(t, worker, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || string(execution.Output) != "AB" {
		t.Fatalf("message lost during decision: %+v %v", execution, err)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	consumptions := 0
	for _, event := range events {
		if event.Type == drt.EventSignalConsumed {
			consumptions++
		}
	}
	if consumptions != 2 {
		t.Fatalf("retry duplicated consumption: %d", consumptions)
	}
}

type signalClientFaultStore struct {
	durable.Store
	calls      int
	digest     string
	mutate     func()
	failAlways bool
}

var errSignalClientResponseLost = errors.New("signal response lost after acceptance")

func (s *signalClientFaultStore) observe(value any) error {
	digest, err := durable.Fingerprint("test", value)
	if err != nil {
		return err
	}
	if s.calls > 0 && digest != s.digest {
		return durable.ErrRequestConflict
	}
	s.digest = digest
	s.calls++
	if s.calls == 1 && s.mutate != nil {
		s.mutate()
	}
	if s.calls < 3 || s.failAlways {
		return errSignalClientResponseLost
	}
	return nil
}
func (s *signalClientFaultStore) SignalExecution(ctx context.Context, request durable.SignalRequest) (durable.SignalReceipt, error) {
	receipt, err := s.Store.SignalExecution(ctx, request)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	if err = s.observe(request); err != nil {
		return durable.SignalReceipt{}, err
	}
	return receipt, nil
}
func (s *signalClientFaultStore) SignalWithStart(ctx context.Context, request durable.SignalWithStartRequest) (durable.SignalReceipt, error) {
	receipt, err := s.Store.SignalWithStart(ctx, request)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	if err = s.observe(request); err != nil {
		return durable.SignalReceipt{}, err
	}
	return receipt, nil
}

func TestSignalClientImmutableRetries(t *testing.T) {
	for _, mode := range []string{"signal", "with_start", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			s := &signalClientFaultStore{Store: memory.New(), failAlways: mode == "exhausted"}
			options := workerOptions(t)
			worker := newWorker(t, s, options)
			start := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}, RequestID: "start", BuildID: options.BuildID, Queue: options.Queue, WorkflowType: "order", Input: []byte("start")}
			input := []byte("approved")
			s.mutate = func() { input[0] = 'X'; start.Input[0] = 'Y' }
			var receipt durable.SignalReceipt
			var err error
			if mode == "signal" {
				if _, err = s.StartExecution(t.Context(), start); err != nil {
					t.Fatal(err)
				}
				receipt, err = worker.SignalExecution(t.Context(), durable.SignalRequest{Key: start.Key, RequestID: "approval", BuildID: options.BuildID, Name: "approve", Input: input})
			} else {
				receipt, err = worker.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: start, Name: "approve", Input: input})
			}
			if mode == "exhausted" {
				if !errors.Is(err, errSignalClientResponseLost) {
					t.Fatalf("unknown result hidden: %v", err)
				}
			} else if err != nil || receipt.Key != start.Key {
				t.Fatalf("immutable retry: %+v %v", receipt, err)
			}
			if s.calls != 3 {
				t.Fatalf("retry bound: %d", s.calls)
			}
			events, err := s.ReadHistory(t.Context(), start.Key, 0, 100)
			if err != nil || len(events) != 2 {
				t.Fatalf("duplicate acceptance: %d %v", len(events), err)
			}
		})
	}
}

func TestSignalConcurrentClientAcceptance(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.ReceiveSignal("approval", "approve").Get() }
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	request := durable.SignalRequest{Key: key, RequestID: "same", BuildID: options.BuildID, Name: "approve", Input: []byte("yes")}
	var wg sync.WaitGroup
	receipts := make(chan durable.SignalReceipt, 16)
	for range 16 {
		wg.Go(func() {
			r, err := worker.SignalExecution(t.Context(), request)
			if err != nil {
				t.Error(err)
			}
			receipts <- r
		})
	}
	wg.Wait()
	close(receipts)
	var first durable.SignalReceipt
	for r := range receipts {
		if first.Revision != 0 && first != r {
			t.Error("duplicate receipts")
		}
		first = r
	}
	runTask(t, worker, durable.TaskWorkflow)
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil || len(events) != 5 {
		t.Fatalf("duplicate receive: %d %v", len(events), err)
	}
}

func TestSignalCancelledClientCannotPublish(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	worker := newWorker(t, s, options)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := durable.SignalWithStartRequest{Start: durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}, RequestID: "start", BuildID: options.BuildID, Queue: options.Queue, WorkflowType: "order"}, Name: "approve"}
	if _, err := worker.SignalWithStart(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	if _, err := s.GetExecution(t.Context(), request.Start.Key); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("cancelled request published: %v", err)
	}
}
