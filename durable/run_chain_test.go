package durable_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func rootRequest() durable.StartRequest {
	return durable.StartRequest{Key: durable.Key{Namespace: "n", WorkflowID: "w", RunID: "r"}, RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders", Input: []byte("input"), RunTimeout: time.Second + 1234*time.Nanosecond, ExecutionTimeout: time.Hour}
}

func TestRunChainRoot(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 123456789, time.UTC)
	r := rootRequest()
	e, err := durable.NewExecution(r, now)
	if err != nil || e.Key != r.Key || e.FirstRunID != r.RunID || e.PreviousRunID != "" || e.NextRunID != "" || e.RunNumber != 1 || !e.FirstStartedAt.Equal(e.CreatedAt) || !e.CreatedAt.Equal(durable.Timestamp(now)) || e.RunTimeout != r.RunTimeout || e.Revision != 1 || e.LastSequence != 1 || e.State != durable.StateRunning {
		t.Fatalf("root: %+v %v", e, err)
	}
	if err = durable.ValidateRunMetadata(e); err != nil {
		t.Fatal(err)
	}
	e.Input[0] = 'X'
	if string(r.Input) != "input" {
		t.Fatal("constructor retained caller input")
	}
	for _, now := range []time.Time{{}, time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)} {
		if _, err = durable.NewExecution(r, now); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid creation time accepted: %v %v", now, err)
		}
	}
	r.RunTimeout = time.Duration(math.MaxInt64)
	if _, err = durable.NewExecution(r, time.Date(9900, 1, 1, 0, 0, 0, 0, time.UTC)); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("overflow: %v", err)
	}
}

func TestRunChainMetadataValidation(t *testing.T) {
	root, err := durable.NewExecution(rootRequest(), time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"first", "previous", "number", "first_time", "precision", "timeout", "deadline", "next_self", "next_running", "successor_root", "successor_previous", "execution_limit"} {
		t.Run(mode, func(t *testing.T) {
			e := root
			switch mode {
			case "first":
				e.FirstRunID = "other"
			case "previous":
				e.PreviousRunID = "earlier"
			case "number":
				e.RunNumber = 0
			case "first_time":
				e.FirstStartedAt = e.CreatedAt.Add(-time.Second)
			case "precision":
				e.FirstStartedAt = e.FirstStartedAt.Add(time.Nanosecond)
			case "timeout":
				e.RunTimeout = 1
			case "deadline":
				e.RunDeadlineAt = e.RunDeadlineAt.Add(time.Microsecond)
			case "next_self":
				e.State = durable.StateContinuedAsNew
				e.NextRunID = e.RunID
			case "next_running":
				e.NextRunID = "next"
			case "successor_root":
				e.RunNumber = 2
				e.PreviousRunID = "previous"
			case "successor_previous":
				e.RunID = "next"
				e.RunNumber = 2
				e.PreviousRunID = "next"
			case "execution_limit":
				e.ExecutionDeadlineAt = e.FirstStartedAt
			}
			if metadataErr := durable.ValidateRunMetadata(e); !errors.Is(metadataErr, durable.ErrInvalid) {
				t.Fatalf("invalid lineage accepted: %+v %v", e, metadataErr)
			}
		})
	}
	root.State = durable.StateContinuedAsNew
	root.NextRunID = "next"
	if err = durable.ValidateRunMetadata(root); err != nil {
		t.Fatal(err)
	}
	next := root
	next.RunID = "next"
	next.PreviousRunID = "r"
	next.NextRunID = ""
	next.RunNumber = 2
	next.State = durable.StateRunning
	if err = durable.ValidateRunMetadata(next); err != nil {
		t.Fatalf("valid successor metadata: %v", err)
	}
}
