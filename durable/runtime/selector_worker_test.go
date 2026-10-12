package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

type selectionFaultStore struct {
	durable.Store
	signal    durable.SignalRequest
	injected  bool
	conflicts int
	lost      bool
	digest    string
	retries   int
}

func (s *selectionFaultStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	selected := false
	for _, event := range r.Events {
		if event.Type == drt.EventSelected {
			selected = true
		}
	}
	if selected && !s.injected {
		s.injected = true
		if _, err := s.SignalExecution(ctx, s.signal); err != nil {
			return durable.Receipt{}, err
		}
	}
	if selected && s.lost {
		digest, err := durable.Fingerprint("selection", r)
		if err != nil {
			return durable.Receipt{}, err
		}
		if digest != s.digest {
			return durable.Receipt{}, durable.ErrRequestConflict
		}
		s.retries++
	}
	receipt, err := s.Store.CommitTransition(ctx, r)
	if errors.Is(err, durable.ErrRevisionConflict) {
		s.conflicts++
	}
	if err == nil && selected && !s.lost {
		s.lost = true
		s.digest, err = durable.Fingerprint("selection", r)
		if err != nil {
			return durable.Receipt{}, err
		}
		return durable.Receipt{}, errors.New("selection response lost after commit")
	}
	return receipt, err
}

func TestSelectWorkerAtomicRecovery(t *testing.T) {
	s := &selectionFaultStore{Store: memory.New()}
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state := "pending"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		approval := w.ReceiveSignal("approval", "approve")
		timer := w.Timer("deadline", time.Millisecond)
		winner := w.Select("race", approval, timer)
		state = "timeout"
		if winner == approval {
			value, err := winner.Get()
			if err != nil {
				return nil, err
			}
			state = string(value)
		}
		if _, err := w.ReceiveSignal("finish", "finish").Get(); err != nil {
			return nil, err
		}
		return []byte(state), nil
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	runTask(t, worker, durable.TaskWorkflow)
	if _, err := s.GetTask(t.Context(), key, "command:3"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("selection created a polled task: %v", err)
	}
	s.signal = durable.SignalRequest{Key: key, RequestID: "during-decision", BuildID: options.BuildID, Name: "note", Input: []byte("concurrent")}
	if _, err := worker.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "approval", BuildID: options.BuildID, Name: "approve", Input: []byte("approved")}); err != nil {
		t.Fatal(err)
	}
	runTask(t, worker, durable.TaskWorkflow)
	if s.conflicts != 1 || !s.lost || s.retries != 1 {
		t.Fatalf("faults not exercised: conflicts=%d lost=%t retries=%d", s.conflicts, s.lost, s.retries)
	}
	loser, err := s.GetTask(t.Context(), key, "command:2")
	if err != nil || loser.Done {
		t.Fatalf("losing timer canceled: %+v %v", loser, err)
	}
	options.Owner = "replacement"
	worker = newWorker(t, s, options)
	query := drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"}
	result, err := worker.QueryExecution(t.Context(), query)
	if err != nil || string(result.Output) != "approved" {
		t.Fatalf("saved winner query: %+v %v", result, err)
	}
	time.Sleep(2 * time.Millisecond)
	runTask(t, worker, durable.TaskTimer)
	runTask(t, worker, durable.TaskWorkflow)
	if _, err = worker.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "finish", BuildID: options.BuildID, Name: "finish"}); err != nil {
		t.Fatal(err)
	}
	runTask(t, worker, durable.TaskWorkflow)
	result, err = worker.QueryExecution(t.Context(), query)
	if err != nil || result.State != durable.StateCompleted || string(result.Output) != "approved" {
		t.Fatalf("terminal winner: %+v %v", result, err)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	selections, consumptions := 0, 0
	for _, event := range events {
		if event.Type == drt.EventSelected {
			selections++
		}
		if event.Type == drt.EventSignalConsumed {
			consumptions++
		}
	}
	if selections != 1 || consumptions != 2 {
		t.Fatalf("duplicate or missing decision: selections=%d consumptions=%d", selections, consumptions)
	}
}
