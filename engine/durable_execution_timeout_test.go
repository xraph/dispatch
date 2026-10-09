package engine_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

func TestDurableEngineExecutionTimeout(t *testing.T) {
	eng, s, start := buildDurableEngine(t, func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.SetQueryHandler("status", func([]byte) ([]byte, error) { return []byte("awaiting approval"), nil })
		return w.ReceiveSignal("approval", "approve").Get()
	})
	start.RunTimeout = time.Microsecond
	if _, err := eng.StartDurableWorkflow(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for {
		e, err := s.GetExecution(ctx, start.Key)
		if err != nil {
			t.Fatal(err)
		}
		if e.State == durable.StateTimedOut {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("engine did not close expired workflow")
		case <-time.After(time.Millisecond):
		}
	}
	if err := eng.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetExecution(ctx, start.Key)
	if err != nil {
		t.Fatal(err)
	}
	history, err := s.ReadHistory(ctx, start.Key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	result, err := eng.QueryDurableWorkflow(ctx, drt.QueryRequest{Key: start.Key, BuildID: start.BuildID, Name: "status"})
	if err != nil || result.State != durable.StateTimedOut || string(result.Output) != "awaiting approval" || result.Revision != before.Revision || result.LastSequence != before.LastSequence {
		t.Fatalf("timed-out query: %+v %v", result, err)
	}
	after, err := s.GetExecution(ctx, start.Key)
	if err != nil {
		t.Fatal(err)
	}
	afterHistory, err := s.ReadHistory(ctx, start.Key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(history, afterHistory) {
		t.Fatal("query mutated closed execution")
	}
}
