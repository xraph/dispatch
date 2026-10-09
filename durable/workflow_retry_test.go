package durable_test

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestWorkflowRetryOptInNormalization(t *testing.T) {
	for _, policy := range []*durable.WorkflowRetryPolicy{nil, {}} {
		normalized, err := durable.NormalizeWorkflowRetryPolicy(policy)
		if err != nil || normalized != nil {
			t.Fatalf("implicit retry enabled: %+v %v", normalized, err)
		}
	}
	input := &durable.WorkflowRetryPolicy{MaximumAttempts: 4, NonRetryableTypes: []string{"payment", "invalid", "payment"}}
	got, err := durable.NormalizeWorkflowRetryPolicy(input)
	if err != nil || got.InitialInterval != time.Second || got.MaximumInterval != 100*time.Second || got.BackoffCoefficient != 2 || got.MaximumAttempts != 4 || !reflect.DeepEqual(got.NonRetryableTypes, []string{"invalid", "payment"}) {
		t.Fatalf("normalization: %+v %v", got, err)
	}
	if !reflect.DeepEqual(input.NonRetryableTypes, []string{"payment", "invalid", "payment"}) {
		t.Fatal("normalization mutated caller")
	}
	got.NonRetryableTypes[0] = "changed"
	if input.NonRetryableTypes[0] != "payment" {
		t.Fatal("policy aliases caller")
	}
	for _, bad := range []durable.WorkflowRetryPolicy{
		{InitialInterval: -1}, {InitialInterval: time.Nanosecond}, {MaximumInterval: -1}, {InitialInterval: time.Second, MaximumInterval: time.Millisecond},
		{BackoffCoefficient: 0.5}, {BackoffCoefficient: math.NaN()}, {BackoffCoefficient: math.Inf(1)}, {MaximumAttempts: -1}, {NonRetryableTypes: []string{""}}, {NonRetryableTypes: make([]string, 1001)},
	} {
		if _, badErr := durable.NormalizeWorkflowRetryPolicy(&bad); !errors.Is(badErr, durable.ErrInvalid) {
			t.Fatalf("invalid policy accepted: %+v %v", bad, badErr)
		}
	}
}

func TestWorkflowRetryCappedBackoffAndFinality(t *testing.T) {
	p := &durable.WorkflowRetryPolicy{InitialInterval: 2 * time.Second, MaximumInterval: 5 * time.Second, BackoffCoefficient: 1.5, MaximumAttempts: 6, NonRetryableTypes: []string{"permanent"}}
	for _, tc := range []struct {
		attempt int64
		kind    string
		final   bool
		want    time.Duration
	}{
		{1, "transient", false, 2 * time.Second}, {2, "transient", false, 3 * time.Second}, {3, "transient", false, 4500 * time.Millisecond},
		{4, "transient", false, 5 * time.Second}, {5, "transient", false, 5 * time.Second}, {6, "transient", false, 0},
		{1, "permanent", false, 0}, {1, "transient", true, 0}, {math.MaxInt64, "transient", false, 0},
	} {
		got, err := durable.WorkflowRetryDelay(p, tc.attempt, tc.kind, tc.final)
		if err != nil || got != tc.want {
			t.Fatalf("attempt %d %s: %v %v, want %v", tc.attempt, tc.kind, got, err, tc.want)
		}
	}
	p = &durable.WorkflowRetryPolicy{InitialInterval: time.Duration(math.MaxInt64), BackoffCoefficient: math.MaxFloat64}
	for _, attempt := range []int64{1, 2, 1000000000} {
		got, err := durable.WorkflowRetryDelay(p, attempt, "transient", false)
		if err != nil || got != time.Duration(math.MaxInt64) {
			t.Fatalf("overflow changed delay: %v %v", got, err)
		}
	}
	if _, err := durable.WorkflowRetryDelay(p, 0, "transient", false); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("zero attempt: %v", err)
	}
}

func TestWorkflowRetryMetadataRequiresBackoff(t *testing.T) {
	e, err := durable.NewExecution(rootRequest(), time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	e.RunID, e.PreviousRunID, e.RunNumber, e.RetryAttempt = "retry", "r", 2, 2
	for _, available := range []time.Time{e.CreatedAt, e.ExecutionDeadlineAt} {
		e.RunAvailableAt = available
		e.RunDeadlineAt, _, err = durable.ResolveExecutionDeadlines(durable.StartRequest{RunTimeout: e.RunTimeout}, available)
		if err != nil {
			t.Fatal(err)
		}
		if err := durable.RunMetadataOf(e).Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("impossible retry availability accepted: %v %v", available, err)
		}
	}
}
