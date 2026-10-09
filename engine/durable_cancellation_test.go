package engine_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/memory"
)

func TestDurableEngineWorkflowCancellation(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := engine.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = disabled.RequestCancelDurableWorkflow(t.Context(), durable.CancelExecutionRequest{}); !errors.Is(err, engine.ErrDurableDisabled) {
		t.Fatalf("disabled: %v", err)
	}
	eng, s, request := buildDurableEngine(t, func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte("waiting"), nil })
		return w.Timer("wait", time.Hour).Get()
	})
	if _, err = eng.StartDurableWorkflow(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	cancel := durable.CancelExecutionRequest{Key: request.Key, RequestID: "cancel", BuildID: request.BuildID, Reason: "operator"}
	receipt, err := eng.RequestCancelDurableWorkflow(t.Context(), cancel)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if worked, runErr := eng.DurableWorker().RunOnce(t.Context(), durable.TaskWorkflow); runErr != nil || !worked {
			t.Fatalf("workflow: %t %v", worked, runErr)
		}
	}
	e, err := s.GetExecution(t.Context(), request.Key)
	if err != nil || e.State != durable.StateCancelled {
		t.Fatalf("cancelled: %+v %v", e, err)
	}
	if again, retryErr := eng.RequestCancelDurableWorkflow(t.Context(), cancel); retryErr != nil || again != receipt {
		t.Fatalf("retry: %+v %v", again, retryErr)
	}
	q, err := eng.QueryDurableWorkflow(t.Context(), drt.QueryRequest{Key: request.Key, BuildID: request.BuildID, Name: "status"})
	if err != nil || q.State != durable.StateCancelled || string(q.Output) != "waiting" {
		t.Fatalf("cancelled query: %+v %v", q, err)
	}
}
