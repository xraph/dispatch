package durable_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestExecutionDeadline(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 123456789, time.UTC)
	r := durable.StartRequest{Key: durable.Key{Namespace: "n", WorkflowID: "w", RunID: "r"}, RequestID: "s", WorkflowType: "w", BuildID: "v1", Queue: "q"}
	for _, d := range []time.Duration{-1, 1, time.Microsecond - 1} {
		invalid := r
		invalid.RunTimeout = d
		if err := invalid.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("run timeout %v: %v", d, err)
		}
		invalid = r
		invalid.ExecutionTimeout = d
		if err := invalid.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("execution timeout %v: %v", d, err)
		}
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"namespace":"n","workflow_id":"w","run_id":"r","request_id":"s","workflow_type":"w","build_id":"v1","queue":"q"}`
	if string(encoded) != want {
		t.Fatalf("zero timeout fingerprint shape: %s", encoded)
	}
	for _, tc := range []struct {
		run, execution time.Duration
		kind           durable.ExecutionTimeoutKind
		delta          time.Duration
	}{
		{0, 0, "", 0}, {time.Second, 0, durable.TimeoutRun, time.Second}, {0, time.Second, durable.TimeoutExecution, time.Second},
		{time.Second, 2 * time.Second, durable.TimeoutRun, time.Second}, {2 * time.Second, time.Second, durable.TimeoutExecution, time.Second},
		{time.Second, time.Second, durable.TimeoutExecution, time.Second},
	} {
		r.RunTimeout, r.ExecutionTimeout = tc.run, tc.execution
		run, execution, e := durable.ResolveExecutionDeadlines(r, now)
		if e != nil {
			t.Fatal(e)
		}
		projection := durable.Execution{State: durable.StateRunning, RunDeadlineAt: run, ExecutionDeadlineAt: execution}
		kind, deadline := projection.Deadline()
		if kind != tc.kind {
			t.Fatalf("kind: %s want %s", kind, tc.kind)
		}
		if tc.delta == 0 {
			if !deadline.IsZero() {
				t.Fatal(deadline)
			}
			continue
		}
		if !deadline.Equal(durable.Timestamp(now).Add(tc.delta)) {
			t.Fatal(deadline)
		}
		if e = durable.CheckExecutionDeadline(projection, deadline.Add(-time.Microsecond)); e != nil {
			t.Fatal(e)
		}
		if e = durable.CheckExecutionDeadline(projection, deadline); !errors.Is(e, durable.ErrExecutionDeadline) {
			t.Fatalf("at deadline: %v", e)
		}
	}
	r.RunTimeout = time.Hour
	if _, _, err = durable.ResolveExecutionDeadlines(r, time.Date(9999, 12, 31, 23, 30, 0, 0, time.UTC)); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("date overflow: %v", err)
	}
}
