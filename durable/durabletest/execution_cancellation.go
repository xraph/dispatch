package durabletest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func cancellationAcceptance(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	request := durable.CancelExecutionRequest{Key: r.Key, RequestID: "cancel", BuildID: r.BuildID, Reason: "stop"}
	request.RunID = ""
	receipt, err := s.RequestCancelExecution(t.Context(), request)
	if err != nil || receipt.Key != r.Key || receipt.Revision != 2 || receipt.FirstSequence != 2 || receipt.LastSequence != 2 {
		t.Fatalf("accept: %+v %v", receipt, err)
	}
	wake, err := s.GetTask(t.Context(), r.Key, "workflow:cancel-request:2")
	if err != nil || wake.Queue != r.Queue || wake.Kind != durable.TaskWorkflow || wake.Done {
		t.Fatalf("wake: %+v %v", wake, err)
	}
	events, err := s.ReadHistory(t.Context(), r.Key, 1, 100)
	if err != nil || len(events) != 1 {
		t.Fatalf("events: %+v %v", events, err)
	}
	var input durable.ExecutionCancellation
	if err = json.Unmarshal(events[0].Payload, &input); err != nil || input.Version != 1 || input.RequestID != "cancel" || input.Reason != "stop" || events[0].Type != durable.EventCancellationRequested {
		t.Fatalf("input: %+v %v", input, err)
	}
	closeRequest := completion(r, task)
	closeRequest.State = durable.StateCancelled
	if _, err = s.CommitTransition(t.Context(), closeRequest); !errors.Is(err, durable.ErrRevisionConflict) {
		t.Fatalf("stale decision: %v", err)
	}
	closeRequest.ExpectedRevision = 2
	if _, err = s.CommitTransition(t.Context(), closeRequest); err != nil {
		t.Fatal(err)
	}
	next := r
	next.RunID = "next"
	next.RequestID = "next-start"
	if _, err = s.StartExecution(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if again, retryErr := s.RequestCancelExecution(t.Context(), request); retryErr != nil || again != receipt {
		t.Fatalf("replacement retry: %+v %v", again, retryErr)
	}
	for _, field := range []string{"reason", "build", "run"} {
		changed := request
		switch field {
		case "reason":
			changed.Reason = "different"
		case "build":
			changed.BuildID = "v2"
		case "run":
			changed.RunID = "next"
		}
		if _, err = s.RequestCancelExecution(t.Context(), changed); !errors.Is(err, durable.ErrRequestConflict) {
			t.Fatalf("changed %s: %v", field, err)
		}
	}
	events, err = s.ReadHistory(t.Context(), next.Key, 0, 100)
	if err != nil || len(events) != 1 {
		t.Fatalf("replacement changed: %+v %v", events, err)
	}
	request.Key = r.Key
	request.RequestID = "late"
	if _, err = s.RequestCancelExecution(t.Context(), request); !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("closed request: %v", err)
	}
}

func cancellationIsolation(t *testing.T, s durable.Store) {
	for _, side := range []string{"left", "right"} {
		r := durable.StartRequest{Key: durable.Key{Namespace: t.Name() + "/" + side, WorkflowID: "same", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: strings.Repeat("b", 512), Queue: "orders"}
		if _, err := s.StartExecution(t.Context(), r); err != nil {
			t.Fatal(err)
		}
		request := durable.CancelExecutionRequest{Key: r.Key, RequestID: strings.Repeat("r", 512), BuildID: r.BuildID, Reason: strings.Repeat("x", 4096)}
		bad := request
		bad.BuildID = "wrong"
		if _, err := s.RequestCancelExecution(t.Context(), bad); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("wrong build: %v", err)
		}
		missing := request
		missing.RunID = "missing"
		if _, err := s.RequestCancelExecution(t.Context(), missing); !errors.Is(err, durable.ErrNotFound) {
			t.Fatalf("missing: %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := s.RequestCancelExecution(ctx, request); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled context: %v", err)
		}
		if got, err := s.RequestCancelExecution(t.Context(), request); err != nil || got.Key != r.Key {
			t.Fatalf("namespace acceptance: %+v %v", got, err)
		}
	}
}

func cancellationConcurrent(t *testing.T, s durable.Store) {
	r := start(t, s)
	request := durable.CancelExecutionRequest{Key: r.Key, RequestID: "same", BuildID: r.BuildID}
	var wg sync.WaitGroup
	receipts := make(chan durable.CancelExecutionReceipt, 8)
	failures := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			receipt, err := s.RequestCancelExecution(t.Context(), request)
			receipts <- receipt
			failures <- err
		})
	}
	wg.Wait()
	close(receipts)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	for got := range receipts {
		if got.Key != r.Key || got.Revision != 2 || got.LastSequence != 2 {
			t.Fatalf("concurrent duplicate: %+v", got)
		}
	}
	for i := range 3 {
		request.RequestID = fmt.Sprintf("request-%d", i)
		request.Reason = request.RequestID
		if _, err := s.RequestCancelExecution(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || e.Revision != 5 || e.LastSequence != 5 || e.State != durable.StateRunning {
		t.Fatalf("distinct requests: %+v %v", e, err)
	}
}

func cancellationFence(t *testing.T, s durable.Store) {
	r := start(t, s)
	source := claim(t, s, r, time.Minute)
	seed := completion(r, source)
	seed.Tasks = []durable.TaskSpec{{ID: "decide", Kind: durable.TaskWorkflow, Queue: r.Queue}, {ID: "activity", Kind: durable.TaskActivity, Queue: r.Queue}, {ID: "timer", Kind: durable.TaskTimer, Queue: r.Queue, AvailableAfter: time.Hour}}
	if _, err := s.CommitTransition(t.Context(), seed); err != nil {
		t.Fatal(err)
	}
	active, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Kind: durable.TaskActivity, Queue: r.Queue, Owner: "activity", LeaseDuration: time.Minute})
	if err != nil || active == nil {
		t.Fatalf("activity: %+v %v", active, err)
	}
	source = claim(t, s, r, time.Minute)
	request := completion(r, source)
	request.RequestID = "fence"
	request.ExpectedRevision = 2
	request.CancelPendingTasks = true
	request.Tasks = []durable.TaskSpec{{ID: "cleanup", Kind: durable.TaskWorkflow, Queue: r.Queue}}
	invalid := request
	invalid.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep}
	if _, err = s.CommitTransition(t.Context(), invalid); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("retained source: %v", err)
	}
	conflict := request
	conflict.Tasks = []durable.TaskSpec{{ID: "timer", Kind: durable.TaskActivity, Queue: r.Queue}}
	if _, err = s.CommitTransition(t.Context(), conflict); err == nil {
		t.Fatal("duplicate new task accepted")
	}
	unchanged, err := s.GetTask(t.Context(), r.Key, "activity")
	if err != nil || unchanged.Done || unchanged.Version != active.Version {
		t.Fatalf("partial fence: %+v %v", unchanged, err)
	}
	receipt, err := s.CommitTransition(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"decide", "activity", "timer"} {
		task, readErr := s.GetTask(t.Context(), r.Key, id)
		if readErr != nil || !task.Done {
			t.Fatalf("unfenced %s: %+v %v", id, task, readErr)
		}
	}
	if _, err = s.RenewTask(t.Context(), r.Key, active.Token(), time.Minute); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("active renewal: %v", err)
	}
	cleanup := claim(t, s, r, time.Minute)
	if cleanup.ID != "cleanup" {
		t.Fatalf("wrong cleanup: %+v", cleanup)
	}
	if again, retryErr := s.CommitTransition(t.Context(), request); retryErr != nil || again != receipt {
		t.Fatalf("fence retry: %+v %v", again, retryErr)
	}
	retained, err := s.GetTask(t.Context(), r.Key, "cleanup")
	if err != nil || retained.Done || retained.Token() != cleanup.Token() {
		t.Fatalf("retry fenced new work: %+v %v", retained, err)
	}
}

func cancellationClosureRace(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	request := durable.CancelExecutionRequest{Key: r.Key, RequestID: "cancel", BuildID: r.BuildID}
	closeRequest := completion(r, task)
	closeRequest.State = durable.StateCompleted
	acceptance := make(chan error, 1)
	closure := make(chan error, 1)
	begin := make(chan struct{})
	go func() { <-begin; _, err := s.RequestCancelExecution(t.Context(), request); acceptance <- err }()
	go func() { <-begin; _, err := s.CommitTransition(t.Context(), closeRequest); closure <- err }()
	close(begin)
	acceptErr, closeErr := <-acceptance, <-closure
	if acceptErr == nil {
		if !errors.Is(closeErr, durable.ErrRevisionConflict) {
			t.Fatalf("acceptance won but close returned %v", closeErr)
		}
		closeRequest.ExpectedRevision = 2
		if _, err := s.CommitTransition(t.Context(), closeRequest); err != nil {
			t.Fatal(err)
		}
		if _, err := s.RequestCancelExecution(t.Context(), request); err != nil {
			t.Fatalf("accepted receipt lost: %v", err)
		}
	} else if !errors.Is(acceptErr, durable.ErrClosed) || closeErr != nil {
		t.Fatalf("race: accept=%v close=%v", acceptErr, closeErr)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || e.State != durable.StateCompleted {
		t.Fatalf("closure: %+v %v", e, err)
	}
}
