package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"

	"github.com/xraph/dispatch/workflow"
)

type orderInput struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := workflow.NewRegistry()

	var got orderInput
	def := workflow.NewWorkflow("process-order", func(_ *workflow.Workflow, input orderInput) error {
		got = input
		return nil
	})

	workflow.RegisterDefinition(r, def)

	runner, ok := r.Get("process-order")
	if !ok {
		t.Fatal("expected runner to be registered")
	}

	payload, _ := json.Marshal(orderInput{OrderID: "ord_123", Amount: 100})
	// runner needs a *Workflow — we can pass nil for this unit test since
	// we never call Step/etc. inside the handler above. But RunnerFunc
	// requires *Workflow, so we need to construct a minimal one.
	// Since the handler only captures input and doesn't call wf methods,
	// we pass nil-safe through the closure. But RunnerFunc signature is
	// func(wf *Workflow, input []byte) error — wf is forwarded to the
	// typed handler as-is. We'll just pass nil here and the typed handler
	// above doesn't use wf.
	err := runner(nil, payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.OrderID != "ord_123" {
		t.Errorf("OrderID = %q, want %q", got.OrderID, "ord_123")
	}
	if got.Amount != 100 {
		t.Errorf("Amount = %d, want %d", got.Amount, 100)
	}
}

func TestRegistry_GetUnknown(t *testing.T) {
	r := workflow.NewRegistry()
	_, ok := r.Get("nonexistent")
	if ok {
		t.Fatal("expected no runner for unregistered workflow")
	}
}

func TestRegistry_Names(t *testing.T) {
	r := workflow.NewRegistry()

	workflow.RegisterDefinition(r, workflow.NewWorkflow("wf-a", func(_ *workflow.Workflow, _ struct{}) error { return nil }))
	workflow.RegisterDefinition(r, workflow.NewWorkflow("wf-b", func(_ *workflow.Workflow, _ struct{}) error { return nil }))
	workflow.RegisterDefinition(r, workflow.NewWorkflow("wf-c", func(_ *workflow.Workflow, _ struct{}) error { return nil }))

	names := r.Names()
	sort.Strings(names)
	if len(names) != 3 {
		t.Fatalf("expected 3 names, got %d", len(names))
	}
	expected := []string{"wf-a", "wf-b", "wf-c"}
	for i, want := range expected {
		if names[i] != want {
			t.Errorf("names[%d] = %q, want %q", i, names[i], want)
		}
	}
}

func TestRegistry_InvalidJSON(t *testing.T) {
	r := workflow.NewRegistry()
	workflow.RegisterDefinition(r, workflow.NewWorkflow("typed-wf", func(_ *workflow.Workflow, _ orderInput) error {
		t.Fatal("handler should not be called with invalid JSON")
		return nil
	}))

	runner, _ := r.Get("typed-wf")
	err := runner(nil, []byte(`{invalid json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestRegistry_EmptyPayload(t *testing.T) {
	r := workflow.NewRegistry()
	called := false
	workflow.RegisterDefinition(r, workflow.NewWorkflow("no-input", func(_ *workflow.Workflow, _ struct{}) error {
		called = true
		return nil
	}))

	runner, _ := r.Get("no-input")
	err := runner(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("handler not called with empty payload")
	}
}

func TestRegistry_HandlerError(t *testing.T) {
	r := workflow.NewRegistry()
	want := errors.New("handler failed")
	workflow.RegisterDefinition(r, workflow.NewWorkflow("failing", func(_ *workflow.Workflow, _ struct{}) error {
		return want
	}))

	runner, _ := r.Get("failing")
	err := runner(nil, nil)
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}

func TestRegistry_Overwrite(t *testing.T) {
	r := workflow.NewRegistry()

	workflow.RegisterDefinition(r, workflow.NewWorkflow("overwrite", func(_ *workflow.Workflow, _ struct{}) error {
		return errors.New("old")
	}))
	workflow.RegisterDefinition(r, workflow.NewWorkflow("overwrite", func(_ *workflow.Workflow, _ struct{}) error {
		return errors.New("new")
	}))

	runner, _ := r.Get("overwrite")
	err := runner(nil, nil)
	if err == nil || err.Error() != "new" {
		t.Fatalf("expected 'new' error, got %v", err)
	}
}

// Ensure the typed handler receives the correct *Workflow reference.
func TestRegistry_WorkflowContextPassthrough(t *testing.T) {
	r := workflow.NewRegistry()

	var gotCtx context.Context
	workflow.RegisterDefinition(r, workflow.NewWorkflow("ctx-test", func(wf *workflow.Workflow, _ struct{}) error {
		if wf != nil {
			gotCtx = wf.Context()
		}
		return nil
	}))

	// We can't easily construct a full *Workflow without the store,
	// so verify nil passthrough doesn't panic in the closure.
	runner, _ := r.Get("ctx-test")
	err := runner(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// gotCtx should be zero value since wf was nil.
	if gotCtx != nil {
		t.Errorf("expected nil context for nil workflow, got %v", gotCtx)
	}
}
