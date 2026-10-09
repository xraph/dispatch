package memory

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestExecutionTimeoutRollback(t *testing.T) {
	s := New()
	r := durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "w", RunID: "r"}, RequestID: "start", WorkflowType: "w", BuildID: "v1", Queue: "q", RunTimeout: time.Microsecond}
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	grant, err := s.ClaimExecutionTimeout(t.Context(), durable.ExecutionTimeoutClaimRequest{Namespace: r.Namespace, Owner: "expiry", LeaseDuration: time.Minute})
	if err != nil || grant == nil {
		t.Fatalf("grant: %+v %v", grant, err)
	}
	request := durable.ExecutionTimeoutRequest{Key: r.Key, RequestID: "timeout", Owner: grant.Owner, Epoch: grant.Epoch}
	s.mu.Lock()
	s.executions[r.Key].tasks["workflow:1"].Version = math.MaxInt64
	s.mu.Unlock()
	if _, err = s.ApplyExecutionTimeout(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("overflow accepted: %v", err)
	}
	s.mu.Lock()
	record := s.executions[r.Key]
	if record.execution.State != durable.StateRunning || record.execution.Revision != 1 || len(record.history) != 1 || record.tasks["workflow:1"].Done || record.timeout != *grant {
		t.Error("partial timeout publication")
	}
	if _, ok := record.receipts[request.RequestID]; ok {
		t.Error("partial timeout receipt")
	}
	record.tasks["workflow:1"].Version = 1
	s.mu.Unlock()
	if _, err = s.ApplyExecutionTimeout(t.Context(), request); err != nil {
		t.Fatalf("retry: %v", err)
	}
}
