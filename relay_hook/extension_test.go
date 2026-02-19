package relayhook_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/xraph/relay"
	revent "github.com/xraph/relay/event"
	"github.com/xraph/relay/store/memory"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	rh "github.com/xraph/dispatch/relay_hook"
	"github.com/xraph/dispatch/workflow"
)

// ── Helpers ─────────────────────────────────────────

func newTestRelay(t *testing.T) *relay.Relay {
	t.Helper()
	r, err := relay.New(relay.WithStore(memory.New()))
	if err != nil {
		t.Fatalf("failed to create relay: %v", err)
	}
	if err := rh.RegisterAll(context.Background(), r); err != nil {
		t.Fatalf("failed to register event types: %v", err)
	}
	return r
}

func newTestJob() *job.Job {
	return &job.Job{
		ID:         id.NewJobID(),
		Name:       "send-email",
		Queue:      "default",
		ScopeAppID: "app-1",
		ScopeOrgID: "org-1",
	}
}

func newTestRun() *workflow.Run {
	return &workflow.Run{
		ID:         id.NewRunID(),
		Name:       "order-flow",
		ScopeAppID: "app-1",
		ScopeOrgID: "org-1",
	}
}

// lastEvent retrieves the most recent event from the relay store with the
// given type. It fails the test if no matching event is found.
func lastEvent(t *testing.T, r *relay.Relay, eventType string) *revent.Event {
	t.Helper()
	events, err := r.Store().ListEvents(context.Background(), revent.ListOpts{
		Type:  eventType,
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("ListEvents failed: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("no %s event found", eventType)
	}
	return events[0]
}

// ── Tests ───────────────────────────────────────────

func TestRelayHookExtension_Name(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)
	if h.Name() != "relay-hook" {
		t.Errorf("expected name %q, got %q", "relay-hook", h.Name())
	}
}

func TestRelayHookExtension_JobEnqueued(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnJobEnqueued(context.Background(), newTestJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventJobEnqueued)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_JobStarted(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnJobStarted(context.Background(), newTestJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventJobStarted)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_JobCompleted(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnJobCompleted(context.Background(), newTestJob(), 150*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventJobCompleted)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_JobFailed(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnJobFailed(context.Background(), newTestJob(), errors.New("boom")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventJobFailed)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_JobRetrying(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)
	nextRun := time.Now().Add(time.Minute)

	if err := h.OnJobRetrying(context.Background(), newTestJob(), 2, nextRun); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventJobRetrying)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_JobDLQ(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnJobDLQ(context.Background(), newTestJob(), errors.New("terminal")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventJobDLQ)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_WorkflowStarted(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnWorkflowStarted(context.Background(), newTestRun()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventWorkflowStarted)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_WorkflowStepCompleted(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnWorkflowStepCompleted(context.Background(), newTestRun(), "validate-order", 200*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventWorkflowStepCompleted)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_WorkflowStepFailed(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnWorkflowStepFailed(context.Background(), newTestRun(), "charge-payment", errors.New("card declined")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventWorkflowStepFailed)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_WorkflowCompleted(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnWorkflowCompleted(context.Background(), newTestRun(), 2*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventWorkflowCompleted)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_WorkflowFailed(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	if err := h.OnWorkflowFailed(context.Background(), newTestRun(), errors.New("step failed")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventWorkflowFailed)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
}

func TestRelayHookExtension_CronFired(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)
	jobID := id.NewJobID()

	if err := h.OnCronFired(context.Background(), "daily-cleanup", jobID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventCronFired)
	// Cron events are system-level, no tenant.
	if evt.TenantID != "" {
		t.Errorf("TenantID: want empty, got %q", evt.TenantID)
	}
}

func TestRelayHookExtension_WithEvents_FiltersDisabled(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r, rh.WithEvents(rh.EventJobCompleted))

	ctx := context.Background()
	j := newTestJob()

	// Enqueued is NOT in the enabled set — should be silently skipped.
	if err := h.OnJobEnqueued(ctx, j); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events, err := r.Store().ListEvents(ctx, revent.ListOpts{Type: rh.EventJobEnqueued, Limit: 10})
	if err != nil {
		t.Fatalf("ListEvents failed: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 enqueued events (disabled), got %d", len(events))
	}

	// Completed IS enabled — should be sent.
	err = h.OnJobCompleted(ctx, j, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events, err = r.Store().ListEvents(ctx, revent.ListOpts{Type: rh.EventJobCompleted, Limit: 10})
	if err != nil {
		t.Fatalf("ListEvents failed: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 completed event, got %d", len(events))
	}
}

func TestRelayHookExtension_ViaRegistry(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)
	logger := slog.Default()

	reg := ext.NewRegistry(logger)
	reg.Register(h)

	ctx := context.Background()
	j := newTestJob()
	run := newTestRun()

	reg.EmitJobEnqueued(ctx, j)
	reg.EmitJobStarted(ctx, j)
	reg.EmitJobCompleted(ctx, j, 50*time.Millisecond)
	reg.EmitJobFailed(ctx, j, errors.New("fail"))
	reg.EmitJobRetrying(ctx, j, 1, time.Now())
	reg.EmitJobDLQ(ctx, j, errors.New("dead"))
	reg.EmitWorkflowStarted(ctx, run)
	reg.EmitWorkflowStepCompleted(ctx, run, "step-1", time.Second)
	reg.EmitWorkflowStepFailed(ctx, run, "step-2", errors.New("bad"))
	reg.EmitWorkflowCompleted(ctx, run, 2*time.Second)
	reg.EmitWorkflowFailed(ctx, run, errors.New("wf fail"))
	reg.EmitCronFired(ctx, "hourly", id.NewJobID())

	// Verify all 12 event types were persisted.
	allTypes := []string{
		rh.EventJobEnqueued,
		rh.EventJobStarted,
		rh.EventJobCompleted,
		rh.EventJobFailed,
		rh.EventJobRetrying,
		rh.EventJobDLQ,
		rh.EventWorkflowStarted,
		rh.EventWorkflowStepCompleted,
		rh.EventWorkflowStepFailed,
		rh.EventWorkflowCompleted,
		rh.EventWorkflowFailed,
		rh.EventCronFired,
	}

	for _, et := range allTypes {
		events, err := r.Store().ListEvents(ctx, revent.ListOpts{Type: et, Limit: 10})
		if err != nil {
			t.Fatalf("ListEvents(%s) failed: %v", et, err)
		}
		if len(events) != 1 {
			t.Errorf("%s: want 1 event, got %d", et, len(events))
		}
	}
}
