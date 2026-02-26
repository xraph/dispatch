package workflow_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// ── Reactive Event Patterns ─────────────────────────

func TestWaitForAll_AllArrived(t *testing.T) {
	s := memory.New()
	runner, reg := newTestRunnerWithStore(s)

	var gotEvents []*event.Event
	waiting := make(chan struct{})
	resultCh := make(chan *workflow.Run, 1)

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("waitall-wf", func(wf *workflow.Workflow, _ struct{}) error {
		close(waiting)
		events, err := wf.WaitForAll([]string{"evt-a", "evt-b", "evt-c"}, 5*time.Second)
		if err != nil {
			return err
		}
		gotEvents = events
		return nil
	}))

	// Start workflow in goroutine (it blocks on WaitForAll).
	go func() {
		run, _ := workflow.Start(context.Background(), runner, "waitall-wf", struct{}{})
		resultCh <- run
	}()

	// Wait for workflow to enter WaitForAll.
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for workflow to start")
	}

	time.Sleep(50 * time.Millisecond)

	// Publish all 3 events.
	bus := newEventBus(s)
	if _, err := bus.Publish(context.Background(), "evt-a", []byte(`"a"`), "", ""); err != nil {
		t.Fatalf("Publish evt-a: %v", err)
	}
	if _, err := bus.Publish(context.Background(), "evt-b", []byte(`"b"`), "", ""); err != nil {
		t.Fatalf("Publish evt-b: %v", err)
	}
	if _, err := bus.Publish(context.Background(), "evt-c", []byte(`"c"`), "", ""); err != nil {
		t.Fatalf("Publish evt-c: %v", err)
	}

	select {
	case run := <-resultCh:
		if run.State != workflow.RunStateCompleted {
			t.Errorf("state = %q, want completed, error = %q", run.State, run.Error)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for workflow to complete")
	}

	if len(gotEvents) != 3 {
		t.Fatalf("events count = %d, want 3", len(gotEvents))
	}
	for i, evt := range gotEvents {
		if evt == nil {
			t.Errorf("events[%d] is nil", i)
		}
	}
}

func TestWaitForAll_Timeout(t *testing.T) {
	s := memory.New()
	runner, reg := newTestRunnerWithStore(s)

	waiting := make(chan struct{})
	resultCh := make(chan *workflow.Run, 1)

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("waitall-timeout", func(wf *workflow.Workflow, _ struct{}) error {
		close(waiting)
		_, err := wf.WaitForAll([]string{"evt-x", "evt-y"}, 200*time.Millisecond)
		return err
	}))

	go func() {
		run, _ := workflow.Start(context.Background(), runner, "waitall-timeout", struct{}{})
		resultCh <- run
	}()

	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for workflow to start")
	}

	time.Sleep(50 * time.Millisecond)

	// Only publish 1 of 2 events — should timeout.
	bus := newEventBus(s)
	if _, err := bus.Publish(context.Background(), "evt-x", []byte(`"x"`), "", ""); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case run := <-resultCh:
		if run.State != workflow.RunStateFailed {
			t.Errorf("state = %q, want failed (timeout)", run.State)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for workflow to complete")
	}
}

func TestWaitForAny_FirstArrives(t *testing.T) {
	s := memory.New()
	runner, reg := newTestRunnerWithStore(s)

	var gotEvent *event.Event
	waiting := make(chan struct{})
	resultCh := make(chan *workflow.Run, 1)

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("waitany-wf", func(wf *workflow.Workflow, _ struct{}) error {
		close(waiting)
		evt, err := wf.WaitForAny([]string{"fast", "slow", "never"}, 5*time.Second)
		if err != nil {
			return err
		}
		gotEvent = evt
		return nil
	}))

	go func() {
		run, _ := workflow.Start(context.Background(), runner, "waitany-wf", struct{}{})
		resultCh <- run
	}()

	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for workflow to start")
	}

	time.Sleep(50 * time.Millisecond)

	// Publish only "fast" event.
	bus := newEventBus(s)
	if _, err := bus.Publish(context.Background(), "fast", []byte(`{"speed":"fast"}`), "", ""); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case run := <-resultCh:
		if run.State != workflow.RunStateCompleted {
			t.Errorf("state = %q, want completed, error = %q", run.State, run.Error)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for workflow to complete")
	}

	if gotEvent == nil {
		t.Fatal("expected to receive an event")
	}
	if gotEvent.Name != "fast" {
		t.Errorf("event name = %q, want %q", gotEvent.Name, "fast")
	}
}

func TestWaitForMatch_PredicateFilter(t *testing.T) {
	s := memory.New()
	runner, reg := newTestRunnerWithStore(s)

	var matchedPayload string
	var gotEvent *event.Event
	waiting := make(chan struct{})
	resultCh := make(chan *workflow.Run, 1)

	workflow.RegisterDefinition(reg, workflow.NewWorkflow("waitmatch-wf", func(wf *workflow.Workflow, _ struct{}) error {
		close(waiting)
		evt, err := wf.WaitForMatch("order.status", 5*time.Second, func(e *event.Event) bool {
			// Only match events with "confirmed" in the payload.
			var payload map[string]string
			if jsonErr := json.Unmarshal(e.Payload, &payload); jsonErr != nil {
				return false
			}
			return payload["status"] == "confirmed"
		})
		if err != nil {
			return err
		}
		gotEvent = evt
		if evt != nil {
			matchedPayload = string(evt.Payload)
		}
		return nil
	}))

	go func() {
		run, _ := workflow.Start(context.Background(), runner, "waitmatch-wf", struct{}{})
		resultCh <- run
	}()

	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for workflow to start")
	}

	time.Sleep(50 * time.Millisecond)

	bus := newEventBus(s)

	// First event doesn't match the predicate.
	if _, err := bus.Publish(context.Background(), "order.status", []byte(`{"status":"pending"}`), "", ""); err != nil {
		t.Fatalf("Publish pending: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Second event matches.
	if _, err := bus.Publish(context.Background(), "order.status", []byte(`{"status":"confirmed"}`), "", ""); err != nil {
		t.Fatalf("Publish confirmed: %v", err)
	}

	select {
	case run := <-resultCh:
		if run.State != workflow.RunStateCompleted {
			t.Errorf("state = %q, want completed, error = %q", run.State, run.Error)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for workflow to complete")
	}

	if gotEvent == nil {
		t.Fatal("expected matched event")
	}
	if matchedPayload != `{"status":"confirmed"}` {
		t.Errorf("payload = %q, want %q", matchedPayload, `{"status":"confirmed"}`)
	}
}

// ── Helpers ─────────────────────────────────────────

// newEventBus creates an event.Bus backed by the given memory store.
func newEventBus(s *memory.Store) *event.Bus {
	return event.NewBus(s)
}
