package durabletest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func intentDigest(t *testing.T, operation string, key durable.Key, token durable.TaskToken, secret string, payload []byte) string {
	t.Helper()
	digest, err := durable.Fingerprint(operation, struct {
		Key     durable.Key
		Token   durable.TaskToken
		Secret  string
		Payload []byte
	}{key, token, secret, payload})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func intentQuery(r durable.CommitRequest) durable.ReceiptRequest {
	return durable.ReceiptRequest{Key: r.Key, RequestID: r.RequestID, IntentDigest: r.IntentDigest}
}

func requireIntentReceipt(t *testing.T, s durable.Store, r durable.CommitRequest, want durable.Receipt) {
	t.Helper()
	got, found, err := s.LookupReceipt(t.Context(), intentQuery(r))
	if err != nil || !found || got != want {
		t.Fatalf("intent lookup: %+v %v %v, want %+v", got, found, err, want)
	}
}

func intentReceipts(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	req := completion(r, task)
	req.IntentDigest = intentDigest(t, "progress", r.Key, task.Token(), "", []byte("first"))
	req.Tasks = []durable.TaskSpec{{ID: "next", Kind: durable.TaskWorkflow, Queue: r.Queue}}
	if got, found, err := s.LookupReceipt(t.Context(), intentQuery(req)); err != nil || found || got != (durable.Receipt{}) {
		t.Fatalf("missing receipt: %+v %v %v", got, found, err)
	}
	receipt, err := s.CommitTransition(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	requireIntentReceipt(t, s, req, receipt)
	next := claim(t, s, r, time.Minute)
	finish := completion(r, next)
	finish.RequestID, finish.ExpectedRevision, finish.State = "finish", 2, durable.StateCompleted
	if _, err = s.CommitTransition(t.Context(), finish); err != nil {
		t.Fatal(err)
	}
	requireIntentReceipt(t, s, req, receipt)
	if got, replayErr := s.CommitTransition(t.Context(), req); replayErr != nil || got != receipt {
		t.Fatalf("exact replay: %+v %v", got, replayErr)
	}
	changed := req
	changed.ExpectedRevision++
	if _, err = s.CommitTransition(t.Context(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("intent weakened exact replay: %v", err)
	}
	requireIntentReceipt(t, s, changed, receipt)
	for _, id := range []string{r.RequestID, finish.RequestID} {
		query := intentQuery(req)
		query.RequestID = id
		if got, found, lookupErr := s.LookupReceipt(t.Context(), query); !errors.Is(lookupErr, durable.ErrRequestConflict) || found || got != (durable.Receipt{}) {
			t.Fatalf("legacy receipt lookup: %+v %v %v", got, found, lookupErr)
		}
	}
}

func intentIsolation(t *testing.T, s durable.Store) {
	r, task, _ := awaitActivity(t, s, 0, time.Minute)
	req := asyncFinish(r, task)
	req.IntentDigest = intentDigest(t, "async.complete", r.Key, task.Token(), req.AsyncSecret, req.Output)
	receipt, err := s.CommitTransition(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"operation", "secret", "payload", "epoch"} {
		operation, secret, payload, token := "async.complete", req.AsyncSecret, req.Output, req.Token
		switch mode {
		case "operation":
			operation = "async.fail"
		case "secret":
			secret = strings.Repeat("02", 32)
		case "payload":
			payload = []byte("different")
		case "epoch":
			token.Epoch++
		}
		query := intentQuery(req)
		query.IntentDigest = intentDigest(t, operation, req.Key, token, secret, payload)
		if got, found, lookupErr := s.LookupReceipt(t.Context(), query); !errors.Is(lookupErr, durable.ErrRequestConflict) || found || got != (durable.Receipt{}) {
			t.Fatalf("changed %s reused receipt: %+v %v %v", mode, got, found, lookupErr)
		}
	}
	for _, mode := range []string{"namespace", "workflow", "run"} {
		other := r
		switch mode {
		case "namespace":
			other.Namespace += "-other"
		case "workflow":
			other.WorkflowID += "-other"
		case "run":
			other.RunID += "-other"
		}
		query := intentQuery(req)
		query.Key = other.Key
		if _, found, lookupErr := s.LookupReceipt(t.Context(), query); !errors.Is(lookupErr, durable.ErrNotFound) || found {
			t.Fatalf("foreign missing %s: %v %v", mode, found, lookupErr)
		}
		if _, err = s.StartExecution(t.Context(), other); err != nil {
			t.Fatal(err)
		}
		if _, found, lookupErr := s.LookupReceipt(t.Context(), query); lookupErr != nil || found {
			t.Fatalf("cross %s receipt: %v %v", mode, found, lookupErr)
		}
		otherTask := claim(t, s, other, time.Minute)
		otherRequest := completion(other, otherTask)
		otherRequest.RequestID = req.RequestID
		otherRequest.IntentDigest = intentDigest(t, "progress", other.Key, otherTask.Token(), "", nil)
		if _, err = s.CommitTransition(t.Context(), otherRequest); err != nil {
			t.Fatal(err)
		}
		if _, found, lookupErr := s.LookupReceipt(t.Context(), query); !errors.Is(lookupErr, durable.ErrRequestConflict) || found {
			t.Fatalf("colliding %s receipt exposed: %v %v", mode, found, lookupErr)
		}
	}
	requireIntentReceipt(t, s, req, receipt)
}

func intentAsyncRetry(t *testing.T, s durable.Store) {
	r, task, _ := awaitActivity(t, s, 0, time.Minute)
	req := asyncFinish(r, task)
	req.State, req.Output = "", nil
	req.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskRetry, RetryAt: task.AvailableAt}
	req.IntentDigest = intentDigest(t, "async.fail", req.Key, req.Token, req.AsyncSecret, []byte("retryable"))
	receipt, err := s.CommitTransition(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: task.Queue, Kind: durable.TaskActivity, Owner: "new-worker", LeaseDuration: time.Minute})
	if err != nil || next == nil || next.Epoch <= task.Epoch {
		t.Fatalf("retry claim: %+v %v", next, err)
	}
	requireIntentReceipt(t, s, req, receipt)
}

func intentAsyncTimeout(t *testing.T, s durable.Store) {
	r, task := heartbeatActivity(t, s, 100*time.Millisecond, time.Minute)
	hash, err := durable.HashAsyncSecret(asyncSecret)
	if err != nil {
		t.Fatal(err)
	}
	handoff := durable.CommitRequest{Key: r.Key, RequestID: "await", ExpectedRevision: 3, Token: task.Token(), Events: []durable.EventInput{{Type: "activity.awaited"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskAwait, AsyncKeyHash: hash}}
	handoff.IntentDigest = intentDigest(t, "async.handoff", r.Key, task.Token(), asyncSecret, nil)
	receipt, err := s.CommitTransition(t.Context(), handoff)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(task.DeadlineAt) + time.Millisecond)
	timeout, err := s.ClaimTimeoutTask(t.Context(), durable.TimeoutClaimRequest{Namespace: r.Namespace, Owner: "timeout-worker", LeaseDuration: time.Minute})
	if err != nil || timeout == nil || timeout.Epoch <= task.Epoch {
		t.Fatalf("timeout claim: %+v %v", timeout, err)
	}
	requireIntentReceipt(t, s, handoff, receipt)
}

func intentRejection(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	req := completion(r, task)
	req.IntentDigest = intentDigest(t, "progress", r.Key, task.Token(), "", nil)
	req.Tasks = []durable.TaskSpec{{ID: task.ID, Kind: durable.TaskWorkflow, Queue: r.Queue}}
	if _, err := s.CommitTransition(t.Context(), req); !errors.Is(err, durable.ErrExists) {
		t.Fatalf("duplicate task accepted: %v", err)
	}
	if _, found, err := s.LookupReceipt(t.Context(), intentQuery(req)); err != nil || found {
		t.Fatalf("rejected transition has receipt: %v %v", found, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, found, err := s.LookupReceipt(ctx, intentQuery(req)); !errors.Is(err, context.Canceled) || found {
		t.Fatalf("cancelled lookup: %v %v", found, err)
	}
	query := intentQuery(req)
	query.IntentDigest = "invalid"
	if _, found, err := s.LookupReceipt(t.Context(), query); !errors.Is(err, durable.ErrInvalid) || found {
		t.Fatalf("invalid lookup: %v %v", found, err)
	}
	current, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || current.Revision != 1 || current.LastSequence != 1 {
		t.Fatalf("rejected request mutated state: %+v %v", current, err)
	}
	req.Tasks = nil
	receipt, err := s.CommitTransition(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	requireIntentReceipt(t, s, req, receipt)
}

func intentConcurrent(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	req := completion(r, task)
	req.IntentDigest = intentDigest(t, "progress", r.Key, task.Token(), "", []byte("same-client-intent"))
	results := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			derived := req
			derived.Events = []durable.EventInput{{Type: "progress", Payload: []byte(fmt.Sprintf("derived-%d", i))}}
			_, err := s.CommitTransition(t.Context(), derived)
			results <- err
		})
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !errors.Is(err, durable.ErrRequestConflict) {
			t.Fatalf("concurrent intent: %v", err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d different transactions for one intent", accepted)
	}
	requireIntentReceipt(t, s, req, durable.Receipt{Revision: 2, FirstSequence: 2, LastSequence: 2})
	history, err := s.ReadHistory(t.Context(), r.Key, 0, 1000)
	if err != nil || len(history) != 2 {
		t.Fatalf("duplicate history: %d %v", len(history), err)
	}
}
