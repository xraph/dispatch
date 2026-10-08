package runtime_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestActivityPolicyDefaultsAndReplay(t *testing.T) {
	f := newHistory()
	options := drt.ActivityOptions{}
	handler := retryWorkflow(options)
	first, err := drt.Evaluate(f.execution, f.events, handler)
	if err != nil || len(first.Commands) != 1 {
		t.Fatalf("options command: %+v %v", first, err)
	}
	command := first.Commands[0]
	policy := command.ActivityOptions.RetryPolicy
	if command.Version != 2 || policy.InitialInterval != time.Second || policy.MaximumInterval != 100*time.Second || policy.BackoffCoefficient != 2 || policy.MaximumAttempts != 0 {
		t.Fatalf("defaults: %+v", command)
	}
	f.commands(t, first.Commands)
	if _, err = drt.Evaluate(f.execution, f.events, retryWorkflow(retryOptions(2))); !errors.Is(err, drt.ErrNondeterministic) {
		t.Fatalf("changed retry policy accepted: %v", err)
	}
	if _, err = drt.Evaluate(f.execution, f.events, handler); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []drt.RetryPolicy{{InitialInterval: -1}, {BackoffCoefficient: 0.5}, {BackoffCoefficient: math.NaN()}, {BackoffCoefficient: math.Inf(1)}, {InitialInterval: time.Second, MaximumInterval: time.Millisecond}, {MaximumAttempts: -1}, {NonRetryableTypes: []string{""}}} {
		empty := newHistory()
		_, err = drt.Evaluate(empty.execution, empty.events, retryWorkflow(drt.ActivityOptions{RetryPolicy: &invalid}))
		if !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid retry policy accepted: %+v %v", invalid, err)
		}
	}
}

func TestActivityPolicyCapturesTypes(t *testing.T) {
	f := newHistory()
	types := []string{"invalid", "denied"}
	options := retryOptions(2)
	options.RetryPolicy.NonRetryableTypes = types
	first, err := drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) {
		future := w.ActivityWithOptions("charge", "charge", "", nil, options)
		types[0] = "changed"
		options.RetryPolicy.InitialInterval = time.Hour
		return future.Get()
	})
	if err != nil || first.Commands[0].ActivityOptions.RetryPolicy.InitialInterval != 30*time.Millisecond || first.Commands[0].ActivityOptions.RetryPolicy.NonRetryableTypes[1] != "invalid" {
		t.Fatalf("mutable activity options: %+v %v", first, err)
	}
}

func TestActivityHistoryRejectsInvalidAttemptTransitions(t *testing.T) {
	for _, mode := range []string{"skipped", "reused_epoch", "early", "wrong_delay", "missing_start", "mismatched_final", "retry_after_limit", "duplicate_start"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			options := retryOptions(2)
			handler := retryWorkflow(options)
			first, err := drt.Evaluate(f.execution, f.events, handler)
			if err != nil {
				t.Fatal(err)
			}
			f.commands(t, first.Commands)
			at := f.execution.CreatedAt
			started := drt.ActivityAttempt{Version: 1, CommandID: "charge", Attempt: 1, Epoch: 1}
			if mode == "skipped" {
				started.Attempt = 2
			}
			if mode != "missing_start" {
				f.append(drt.EventActivityAttemptStarted, encode(t, started), at)
			}
			failed := started
			failed.Failure = &drt.ApplicationError{Type: "application", Message: "retry"}
			failed.RetryAfter = 30 * time.Millisecond
			switch mode {
			case "skipped":
			case "duplicate_start":
				f.append(drt.EventActivityAttemptStarted, encode(t, started), at)
			case "missing_start":
				f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 2, CommandID: "charge", Attempt: 1}), at)
			case "mismatched_final":
				f.append(drt.EventActivityCompleted, encode(t, drt.Outcome{Version: 2, CommandID: "charge", Attempt: 2}), at)
			case "wrong_delay":
				failed.RetryAfter = time.Second
				f.append(drt.EventActivityAttemptFailed, encode(t, failed), at)
			case "reused_epoch", "early":
				f.append(drt.EventActivityAttemptFailed, encode(t, failed), at)
				started.Attempt = 2
				started.Epoch = 2
				if mode == "reused_epoch" {
					started.Epoch = 1
				}
				next := at.Add(30 * time.Millisecond)
				if mode == "early" {
					next = at
				}
				f.append(drt.EventActivityAttemptStarted, encode(t, started), next)
			case "retry_after_limit":
				f.append(drt.EventActivityAttemptFailed, encode(t, failed), at)
				started.Attempt = 2
				started.Epoch = 2
				at = at.Add(30 * time.Millisecond)
				f.append(drt.EventActivityAttemptStarted, encode(t, started), at)
				failed.Attempt = 2
				failed.Epoch = 2
				failed.RetryAfter = 50 * time.Millisecond
				f.append(drt.EventActivityAttemptFailed, encode(t, failed), at)
			}
			if _, err = drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("invalid attempt %s accepted: %v", mode, err)
			}
		})
	}
}
