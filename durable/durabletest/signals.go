package durabletest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func signalAcceptance(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	request := durable.SignalRequest{Key: r.Key, RequestID: "approval", BuildID: r.BuildID, Name: "approve", Input: []byte("yes")}
	request.RunID = ""
	receipt, err := s.SignalExecution(t.Context(), request)
	if err != nil || receipt.Key != r.Key || receipt.Started || receipt.Revision != 2 || receipt.FirstSequence != 2 || receipt.LastSequence != 2 {
		t.Fatalf("signal: %+v %v", receipt, err)
	}
	wake, err := s.GetTask(t.Context(), r.Key, "workflow:signal:2")
	if err != nil || wake.Kind != durable.TaskWorkflow || wake.Queue != r.Queue || wake.Done {
		t.Fatalf("signal wakeup: %+v %v", wake, err)
	}
	stale := completion(r, task)
	stale.State = durable.StateCompleted
	if _, err = s.CommitTransition(t.Context(), stale); !errors.Is(err, durable.ErrRevisionConflict) {
		t.Fatalf("stale decision ignored accepted signal: %v", err)
	}
	stale.ExpectedRevision = 2
	if _, err = s.CommitTransition(t.Context(), stale); err != nil {
		t.Fatal(err)
	}
	next := r
	next.RunID, next.RequestID = "replacement", "replacement-start"
	if _, err = s.StartExecution(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if got, replayErr := s.SignalExecution(t.Context(), request); replayErr != nil || got != receipt {
		t.Fatalf("receipt after run replacement: %+v %v", got, replayErr)
	}
	request.Input = []byte("different")
	if _, err = s.SignalExecution(t.Context(), request); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed request targeted replacement: %v", err)
	}
	events, err := s.ReadHistory(t.Context(), next.Key, 0, 100)
	if err != nil || len(events) != 1 {
		t.Fatalf("retry changed replacement run: %+v %v", events, err)
	}
}

func signalOrdering(t *testing.T, s durable.Store) {
	r := start(t, s)
	for i, input := range []string{"first", "second"} {
		request := durable.SignalRequest{Key: r.Key, RequestID: fmt.Sprintf("signal-%d", i), BuildID: r.BuildID, Name: "approve", Input: []byte(input)}
		if _, err := s.SignalExecution(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		request.Input[0] = 'X'
	}
	events, err := s.ReadHistory(t.Context(), r.Key, 0, 100)
	if err != nil || len(events) != 3 {
		t.Fatalf("signal history: %+v %v", events, err)
	}
	for i, input := range []string{"first", "second"} {
		var signal durable.Signal
		event := events[i+1]
		if err = json.Unmarshal(event.Payload, &signal); err != nil || event.Type != "workflow.signal_received" || event.Sequence != int64(i+2) || signal.Version != 1 || signal.Name != "approve" || string(signal.Input) != input {
			t.Fatalf("signal order or ownership: %+v %+v %v", event, signal, err)
		}
	}
}

func signalIsolationAndLimits(t *testing.T, s durable.Store) {
	for _, namespace := range []string{t.Name() + "/left", t.Name() + "/right"} {
		r := durable.SignalWithStartRequest{Start: durable.StartRequest{
			Key:       durable.Key{Namespace: namespace, WorkflowID: "shared", RunID: "run"},
			RequestID: strings.Repeat("r", 512), BuildID: strings.Repeat("b", 512),
			WorkflowType: "order", Queue: "orders", Input: []byte(namespace),
		}, Name: strings.Repeat("n", 200), Input: []byte(namespace)}
		got, err := s.SignalWithStart(t.Context(), r)
		if err != nil || got.Key != r.Start.Key || !got.Started {
			t.Fatalf("namespace receipt: %+v %v", got, err)
		}
		r.Start.Input[0] = 'X'
		r.Input[0] = 'Y'
		events, err := s.ReadHistory(t.Context(), got.Key, 0, 100)
		if err != nil || len(events) != 2 || string(events[0].Payload) != namespace {
			t.Fatalf("start input ownership: %+v %v", events, err)
		}
		var message durable.Signal
		if err = json.Unmarshal(events[1].Payload, &message); err != nil || string(message.Input) != namespace {
			t.Fatalf("message ownership: %+v %v", message, err)
		}
		if _, err = s.SignalExecution(t.Context(), durable.SignalRequest{Key: got.Key, RequestID: strings.Repeat("s", 512), BuildID: r.Start.BuildID, Name: r.Name}); err != nil {
			t.Fatalf("long signal identity: %v", err)
		}
	}
}

func signalWithStart(t *testing.T, s durable.Store) {
	request := durable.SignalWithStartRequest{Start: durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "proposed"},
		RequestID: "first", WorkflowType: "order", BuildID: "v1", Queue: "orders", Input: []byte("start")}, Name: "approve", Input: []byte("first")}
	receipt, err := s.SignalWithStart(t.Context(), request)
	if err != nil || receipt.Key != request.Start.Key || !receipt.Started || receipt.Revision != 1 || receipt.FirstSequence != 1 || receipt.LastSequence != 2 {
		t.Fatalf("signal-with-start: %+v %v", receipt, err)
	}
	events, err := s.ReadHistory(t.Context(), receipt.Key, 0, 100)
	if err != nil || len(events) != 2 || events[0].Type != "execution.started" || string(events[0].Payload) != "start" || events[1].Type != "workflow.signal_received" {
		t.Fatalf("atomic initial history: %+v %v", events, err)
	}
	second := request
	second.Start.RequestID, second.Start.RunID, second.Start.Queue, second.Start.WorkflowType = "second", "unused", "other-queue", "other-type"
	second.Start.Input, second.Input = []byte("other-start"), []byte("second")
	got, err := s.SignalWithStart(t.Context(), second)
	if err != nil || got.Key != receipt.Key || got.Started || got.Revision != 2 || got.LastSequence != 3 {
		t.Fatalf("reuse open run: %+v %v", got, err)
	}
	if _, err = s.GetExecution(t.Context(), second.Start.Key); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("unused proposed run exists: %v", err)
	}
	wake, err := s.GetTask(t.Context(), receipt.Key, "workflow:signal:2")
	if err != nil || wake.Queue != "orders" {
		t.Fatalf("proposed queue changed routing: %+v %v", wake, err)
	}
	current, err := s.GetExecution(t.Context(), receipt.Key)
	if err != nil || current.WorkflowType != "order" || string(current.Input) != "start" {
		t.Fatalf("proposed start changed existing execution: %+v %v", current, err)
	}
	task := claim(t, s, request.Start, time.Minute)
	closeRequest := completion(request.Start, task)
	closeRequest.ExpectedRevision, closeRequest.State = 2, durable.StateCompleted
	if _, err = s.CommitTransition(t.Context(), closeRequest); err != nil {
		t.Fatal(err)
	}
	reuse := request
	reuse.Start.RequestID = "new-operation"
	if _, err = s.SignalWithStart(t.Context(), reuse); !errors.Is(err, durable.ErrExists) {
		t.Fatalf("closed proposed run reused: %v", err)
	}
	next := request.Start
	next.RunID, next.RequestID = "next-run", "next-start"
	if _, err = s.StartExecution(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if recovered, replayErr := s.SignalWithStart(t.Context(), request); replayErr != nil || recovered != receipt {
		t.Fatalf("signal-with-start receipt followed new run: %+v %v", recovered, replayErr)
	}
	ordinary := durable.SignalRequest{Key: receipt.Key, RequestID: request.Start.RequestID, BuildID: "v1", Name: request.Name, Input: request.Input}
	if _, err = s.SignalExecution(t.Context(), ordinary); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("different operation reused signal receipt: %v", err)
	}
	events, err = s.ReadHistory(t.Context(), next.Key, 0, 100)
	if err != nil || len(events) != 1 {
		t.Fatalf("retry signaled next run: %+v %v", events, err)
	}
}

func signalRejections(t *testing.T, s durable.Store) {
	r := start(t, s)
	for _, mode := range []string{"namespace", "run", "build", "missing_open"} {
		req := durable.SignalRequest{Key: r.Key, RequestID: "reject", BuildID: r.BuildID, Name: "approve"}
		want := durable.ErrNotFound
		switch mode {
		case "namespace":
			req.Namespace += "-other"
		case "run":
			req.RunID += "-other"
		case "build":
			req.BuildID = "other"
			want = durable.ErrInvalid
		case "missing_open":
			req.WorkflowID, req.RunID = "missing", ""
		}
		if _, err := s.SignalExecution(t.Context(), req); !errors.Is(err, want) {
			t.Fatalf("%s rejection: %v", mode, err)
		}
	}
	badStart := durable.SignalWithStartRequest{Start: r, Name: "approve"}
	badStart.Start.BuildID = "other"
	if _, err := s.SignalWithStart(t.Context(), badStart); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("existing-run build mismatch: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.SignalExecution(ctx, durable.SignalRequest{Key: r.Key, RequestID: "cancelled", BuildID: r.BuildID, Name: "approve"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled signal: %v", err)
	}
	task := claim(t, s, r, time.Minute)
	closed := completion(r, task)
	closed.State = durable.StateCompleted
	if _, err := s.CommitTransition(t.Context(), closed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: "closed", BuildID: r.BuildID, Name: "approve"}); !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("closed signal: %v", err)
	}
	fresh := durable.SignalWithStartRequest{Start: r, Name: "approve"}
	fresh.Start.WorkflowID = "fresh"
	if _, err := s.SignalWithStart(ctx, fresh); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled start: %v", err)
	}
	if _, err := s.GetExecution(t.Context(), fresh.Start.Key); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("cancelled start created run: %v", err)
	}
}

func signalConcurrent(t *testing.T, s durable.Store) {
	r := start(t, s)
	req := durable.SignalRequest{Key: r.Key, RequestID: "same", BuildID: r.BuildID, Name: "approve", Input: []byte("yes")}
	var wg sync.WaitGroup
	results := make(chan durable.SignalReceipt, 16)
	for range 16 {
		wg.Go(func() {
			receipt, err := s.SignalExecution(t.Context(), req)
			if err != nil {
				t.Error(err)
			}
			results <- receipt
		})
	}
	wg.Wait()
	close(results)
	var first durable.SignalReceipt
	for result := range results {
		if first.Revision != 0 && result != first {
			t.Error("identical requests produced different receipts")
		}
		first = result
	}
	events, err := s.ReadHistory(t.Context(), r.Key, 0, 100)
	if err != nil || len(events) != 2 {
		t.Fatalf("duplicate signal history: %d %v", len(events), err)
	}
}

func signalStartRace(t *testing.T, s durable.Store) {
	base := durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "ordinary"}, RequestID: "ordinary", WorkflowType: "order", BuildID: "v1", Queue: "orders"}
	var wg sync.WaitGroup
	var created atomic.Int64
	results := make(chan durable.SignalReceipt, 12)
	wg.Go(func() {
		_, err := s.StartExecution(t.Context(), base)
		if err != nil && !errors.Is(err, durable.ErrExists) {
			t.Error(err)
		}
	})
	for i := range 12 {
		wg.Go(func() {
			r := base
			r.RunID = fmt.Sprintf("candidate-%d", i)
			r.RequestID = fmt.Sprintf("signal-%d", i)
			got, err := s.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: r, Name: "approve", Input: []byte(r.RequestID)})
			if err != nil {
				t.Error(err)
			}
			if got.Started {
				created.Add(1)
			}
			results <- got
		})
	}
	wg.Wait()
	close(results)
	var target durable.Key
	for got := range results {
		if target.RunID != "" && target != got.Key {
			t.Error("signals split across open runs")
		}
		target = got.Key
	}
	if created.Load() > 1 {
		t.Fatal("created multiple open runs")
	}
	events, err := s.ReadHistory(t.Context(), target, 0, 100)
	if err != nil || len(events) != 13 {
		t.Fatalf("concurrent signal history: %d %v", len(events), err)
	}
}

func signalConflictRace(t *testing.T, s durable.Store) {
	r := start(t, s)
	var accepted, conflicted atomic.Int64
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for _, input := range []string{"left", "right"} {
		wg.Go(func() {
			<-gate
			_, err := s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: "one-id", BuildID: r.BuildID, Name: "approve", Input: []byte(input)})
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, durable.ErrRequestConflict):
				conflicted.Add(1)
			default:
				t.Error(err)
			}
		})
	}
	close(gate)
	wg.Wait()
	events, err := s.ReadHistory(t.Context(), r.Key, 0, 100)
	if err != nil || accepted.Load() != 1 || conflicted.Load() != 1 || len(events) != 2 {
		t.Fatalf("conflicting signal acceptance: accepted=%d conflicts=%d history=%d %v", accepted.Load(), conflicted.Load(), len(events), err)
	}
}

func signalClosureRace(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	closing := completion(r, task)
	closing.State = durable.StateCompleted
	gate := make(chan struct{})
	signalResult, closeResult := make(chan error, 1), make(chan error, 1)
	go func() {
		<-gate
		_, err := s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: "race", BuildID: r.BuildID, Name: "approve"})
		signalResult <- err
	}()
	go func() { <-gate; _, err := s.CommitTransition(t.Context(), closing); closeResult <- err }()
	close(gate)
	signalErr, closeErr := <-signalResult, <-closeResult
	current, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	if signalErr == nil {
		if !errors.Is(closeErr, durable.ErrRevisionConflict) || current.State != durable.StateRunning {
			t.Fatalf("signal won without fencing stale closure: %v %+v", closeErr, current)
		}
	} else if !errors.Is(signalErr, durable.ErrClosed) || closeErr != nil || current.State != durable.StateCompleted {
		t.Fatalf("closure race: signal=%v close=%v execution=%+v", signalErr, closeErr, current)
	}
	events, err := s.ReadHistory(t.Context(), r.Key, 0, 100)
	if err != nil || len(events) != 2 || current.Revision != 2 {
		t.Fatalf("partial race history: %d revision=%d %v", len(events), current.Revision, err)
	}
}
