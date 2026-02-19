package audithook_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	ah "github.com/xraph/dispatch/audit_hook"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// ── Mock recorder ────────────────────────────────────

// mockRecorder captures audit events for verification.
type mockRecorder struct {
	mu     sync.Mutex
	events []*ah.AuditEvent
}

func (m *mockRecorder) Record(_ context.Context, evt *ah.AuditEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, evt)
	return nil
}

func (m *mockRecorder) last() *ah.AuditEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.events) == 0 {
		return nil
	}
	return m.events[len(m.events)-1]
}

func (m *mockRecorder) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func (m *mockRecorder) findByAction(action string) *ah.AuditEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, evt := range m.events {
		if evt.Action == action {
			return evt
		}
	}
	return nil
}

// ── Test helpers ─────────────────────────────────────

func newTestJob() *job.Job {
	return &job.Job{
		ID:         id.NewJobID(),
		Name:       "send-email",
		Queue:      "default",
		ScopeAppID: "app-1",
		ScopeOrgID: "org-1",
		MaxRetries: 3,
		RetryCount: 1,
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

// ── Tests ────────────────────────────────────────────

func TestExtension_Name(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)
	if e.Name() != "audit-hook" {
		t.Errorf("expected name %q, got %q", "audit-hook", e.Name())
	}
}

// ── Job lifecycle tests ──────────────────────────────

func TestExtension_JobEnqueued(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)
	ctx := context.Background()
	j := newTestJob()

	if err := e.OnJobEnqueued(ctx, j); err != nil {
		t.Fatalf("OnJobEnqueued: %v", err)
	}

	evt := rec.last()
	if evt == nil {
		t.Fatal("no event recorded")
	}
	if evt.Action != ah.ActionJobEnqueued {
		t.Errorf("Action: want %q, got %q", ah.ActionJobEnqueued, evt.Action)
	}
	if evt.Resource != ah.ResourceJob {
		t.Errorf("Resource: want %q, got %q", ah.ResourceJob, evt.Resource)
	}
	if evt.Category != ah.CategoryJob {
		t.Errorf("Category: want %q, got %q", ah.CategoryJob, evt.Category)
	}
	if evt.ResourceID != j.ID.String() {
		t.Errorf("ResourceID: want %q, got %q", j.ID.String(), evt.ResourceID)
	}
	if evt.Severity != ah.SeverityInfo {
		t.Errorf("Severity: want %q, got %q", ah.SeverityInfo, evt.Severity)
	}
	if evt.Outcome != ah.OutcomeSuccess {
		t.Errorf("Outcome: want %q, got %q", ah.OutcomeSuccess, evt.Outcome)
	}
	if evt.Metadata["job_name"] != "send-email" {
		t.Errorf("Metadata[job_name]: want %q, got %v", "send-email", evt.Metadata["job_name"])
	}
	if evt.Metadata["queue"] != "default" {
		t.Errorf("Metadata[queue]: want %q, got %v", "default", evt.Metadata["queue"])
	}
}

func TestExtension_JobStarted(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	j := newTestJob()
	j.WorkerID = id.NewWorkerID()

	if err := e.OnJobStarted(context.Background(), j); err != nil {
		t.Fatalf("OnJobStarted: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionJobStarted {
		t.Errorf("Action: want %q, got %q", ah.ActionJobStarted, evt.Action)
	}
	if evt.Metadata["worker_id"] != j.WorkerID.String() {
		t.Errorf("Metadata[worker_id]: want %q, got %v", j.WorkerID.String(), evt.Metadata["worker_id"])
	}
}

func TestExtension_JobCompleted(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	j := newTestJob()
	elapsed := 150 * time.Millisecond

	if err := e.OnJobCompleted(context.Background(), j, elapsed); err != nil {
		t.Fatalf("OnJobCompleted: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionJobCompleted {
		t.Errorf("Action: want %q, got %q", ah.ActionJobCompleted, evt.Action)
	}
	if evt.Severity != ah.SeverityInfo {
		t.Errorf("Severity: want %q, got %q", ah.SeverityInfo, evt.Severity)
	}
	if evt.Metadata["elapsed_ms"] != elapsed.Milliseconds() {
		t.Errorf("Metadata[elapsed_ms]: want %d, got %v", elapsed.Milliseconds(), evt.Metadata["elapsed_ms"])
	}
}

func TestExtension_JobFailed(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	j := newTestJob()
	jobErr := errors.New("connection timeout")

	if err := e.OnJobFailed(context.Background(), j, jobErr); err != nil {
		t.Fatalf("OnJobFailed: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionJobFailed {
		t.Errorf("Action: want %q, got %q", ah.ActionJobFailed, evt.Action)
	}
	if evt.Severity != ah.SeverityCritical {
		t.Errorf("Severity: want %q, got %q", ah.SeverityCritical, evt.Severity)
	}
	if evt.Outcome != ah.OutcomeFailure {
		t.Errorf("Outcome: want %q, got %q", ah.OutcomeFailure, evt.Outcome)
	}
	if evt.Reason != "connection timeout" {
		t.Errorf("Reason: want %q, got %q", "connection timeout", evt.Reason)
	}
	if evt.Metadata["error"] != "connection timeout" {
		t.Errorf("Metadata[error]: want %q, got %v", "connection timeout", evt.Metadata["error"])
	}
	if evt.Metadata["retry_count"] != 1 {
		t.Errorf("Metadata[retry_count]: want %d, got %v", 1, evt.Metadata["retry_count"])
	}
}

func TestExtension_JobRetrying(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	j := newTestJob()
	nextRun := time.Now().Add(30 * time.Second)

	if err := e.OnJobRetrying(context.Background(), j, 2, nextRun); err != nil {
		t.Fatalf("OnJobRetrying: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionJobRetrying {
		t.Errorf("Action: want %q, got %q", ah.ActionJobRetrying, evt.Action)
	}
	if evt.Severity != ah.SeverityWarning {
		t.Errorf("Severity: want %q, got %q", ah.SeverityWarning, evt.Severity)
	}
	if evt.Outcome != ah.OutcomeFailure {
		t.Errorf("Outcome: want %q, got %q", ah.OutcomeFailure, evt.Outcome)
	}
	if evt.Metadata["attempt"] != 2 {
		t.Errorf("Metadata[attempt]: want %d, got %v", 2, evt.Metadata["attempt"])
	}
}

func TestExtension_JobDLQ(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	j := newTestJob()
	jobErr := errors.New("max retries exceeded")

	if err := e.OnJobDLQ(context.Background(), j, jobErr); err != nil {
		t.Fatalf("OnJobDLQ: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionJobDLQ {
		t.Errorf("Action: want %q, got %q", ah.ActionJobDLQ, evt.Action)
	}
	if evt.Severity != ah.SeverityCritical {
		t.Errorf("Severity: want %q, got %q", ah.SeverityCritical, evt.Severity)
	}
	if evt.Metadata["error"] != "max retries exceeded" {
		t.Errorf("Metadata[error]: want %q, got %v", "max retries exceeded", evt.Metadata["error"])
	}
}

// ── Workflow lifecycle tests ─────────────────────────

func TestExtension_WorkflowStarted(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	r := newTestRun()

	if err := e.OnWorkflowStarted(context.Background(), r); err != nil {
		t.Fatalf("OnWorkflowStarted: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionWorkflowStarted {
		t.Errorf("Action: want %q, got %q", ah.ActionWorkflowStarted, evt.Action)
	}
	if evt.Resource != ah.ResourceWorkflow {
		t.Errorf("Resource: want %q, got %q", ah.ResourceWorkflow, evt.Resource)
	}
	if evt.Category != ah.CategoryWorkflow {
		t.Errorf("Category: want %q, got %q", ah.CategoryWorkflow, evt.Category)
	}
	if evt.Metadata["workflow_name"] != "order-flow" {
		t.Errorf("Metadata[workflow_name]: want %q, got %v", "order-flow", evt.Metadata["workflow_name"])
	}
}

func TestExtension_WorkflowStepCompleted(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	r := newTestRun()

	if err := e.OnWorkflowStepCompleted(context.Background(), r, "validate-order", 200*time.Millisecond); err != nil {
		t.Fatalf("OnWorkflowStepCompleted: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionWorkflowStepCompleted {
		t.Errorf("Action: want %q, got %q", ah.ActionWorkflowStepCompleted, evt.Action)
	}
	if evt.Metadata["step_name"] != "validate-order" {
		t.Errorf("Metadata[step_name]: want %q, got %v", "validate-order", evt.Metadata["step_name"])
	}
	if evt.Metadata["elapsed_ms"] != int64(200) {
		t.Errorf("Metadata[elapsed_ms]: want %d, got %v", 200, evt.Metadata["elapsed_ms"])
	}
}

func TestExtension_WorkflowStepFailed(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	r := newTestRun()
	stepErr := errors.New("card declined")

	if err := e.OnWorkflowStepFailed(context.Background(), r, "charge-payment", stepErr); err != nil {
		t.Fatalf("OnWorkflowStepFailed: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionWorkflowStepFailed {
		t.Errorf("Action: want %q, got %q", ah.ActionWorkflowStepFailed, evt.Action)
	}
	if evt.Severity != ah.SeverityWarning {
		t.Errorf("Severity: want %q, got %q", ah.SeverityWarning, evt.Severity)
	}
	if evt.Reason != "card declined" {
		t.Errorf("Reason: want %q, got %q", "card declined", evt.Reason)
	}
}

func TestExtension_WorkflowCompleted(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	r := newTestRun()

	if err := e.OnWorkflowCompleted(context.Background(), r, 2*time.Second); err != nil {
		t.Fatalf("OnWorkflowCompleted: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionWorkflowCompleted {
		t.Errorf("Action: want %q, got %q", ah.ActionWorkflowCompleted, evt.Action)
	}
	if evt.Metadata["elapsed_ms"] != int64(2000) {
		t.Errorf("Metadata[elapsed_ms]: want %d, got %v", 2000, evt.Metadata["elapsed_ms"])
	}
}

func TestExtension_WorkflowFailed(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	r := newTestRun()
	runErr := errors.New("step failed")

	if err := e.OnWorkflowFailed(context.Background(), r, runErr); err != nil {
		t.Fatalf("OnWorkflowFailed: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionWorkflowFailed {
		t.Errorf("Action: want %q, got %q", ah.ActionWorkflowFailed, evt.Action)
	}
	if evt.Severity != ah.SeverityCritical {
		t.Errorf("Severity: want %q, got %q", ah.SeverityCritical, evt.Severity)
	}
}

// ── Cron lifecycle tests ─────────────────────────────

func TestExtension_CronFired(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)
	jobID := id.NewJobID()

	if err := e.OnCronFired(context.Background(), "daily-cleanup", jobID); err != nil {
		t.Fatalf("OnCronFired: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionCronFired {
		t.Errorf("Action: want %q, got %q", ah.ActionCronFired, evt.Action)
	}
	if evt.Resource != ah.ResourceCron {
		t.Errorf("Resource: want %q, got %q", ah.ResourceCron, evt.Resource)
	}
	if evt.Category != ah.CategoryCron {
		t.Errorf("Category: want %q, got %q", ah.CategoryCron, evt.Category)
	}
	if evt.ResourceID != "daily-cleanup" {
		t.Errorf("ResourceID: want %q, got %q", "daily-cleanup", evt.ResourceID)
	}
	if evt.Metadata["job_id"] != jobID.String() {
		t.Errorf("Metadata[job_id]: want %q, got %v", jobID.String(), evt.Metadata["job_id"])
	}
}

// ── WithActions filter tests ─────────────────────────

func TestExtension_WithActions_FiltersDisabled(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec, ah.WithActions(ah.ActionJobCompleted, ah.ActionJobFailed))

	ctx := context.Background()
	j := newTestJob()

	// Enqueued is NOT enabled — should be silently skipped.
	if err := e.OnJobEnqueued(ctx, j); err != nil {
		t.Fatalf("OnJobEnqueued: %v", err)
	}
	if rec.count() != 0 {
		t.Errorf("expected 0 events (enqueued disabled), got %d", rec.count())
	}

	// Completed IS enabled — should be recorded.
	if err := e.OnJobCompleted(ctx, j, 50*time.Millisecond); err != nil {
		t.Fatalf("OnJobCompleted: %v", err)
	}
	if rec.count() != 1 {
		t.Errorf("expected 1 event (completed enabled), got %d", rec.count())
	}

	// Failed IS enabled — should be recorded.
	if err := e.OnJobFailed(ctx, j, errors.New("boom")); err != nil {
		t.Fatalf("OnJobFailed: %v", err)
	}
	if rec.count() != 2 {
		t.Errorf("expected 2 events, got %d", rec.count())
	}
}

// ── RecorderFunc adapter test ────────────────────────

func TestRecorderFunc(t *testing.T) {
	var captured *ah.AuditEvent
	fn := ah.RecorderFunc(func(_ context.Context, evt *ah.AuditEvent) error {
		captured = evt
		return nil
	})

	e := ah.New(fn)
	j := newTestJob()

	if err := e.OnJobEnqueued(context.Background(), j); err != nil {
		t.Fatalf("OnJobEnqueued: %v", err)
	}
	if captured == nil {
		t.Fatal("RecorderFunc was not called")
	}
	if captured.Action != ah.ActionJobEnqueued {
		t.Errorf("Action: want %q, got %q", ah.ActionJobEnqueued, captured.Action)
	}
}

// ── Recorder error handling test ─────────────────────

func TestExtension_RecorderError_DoesNotPropagate(t *testing.T) {
	failingRecorder := ah.RecorderFunc(func(_ context.Context, _ *ah.AuditEvent) error {
		return errors.New("audit backend down")
	})

	e := ah.New(failingRecorder)
	j := newTestJob()

	// Hook should NOT return an error — audit failures must not block
	// the job pipeline.
	if err := e.OnJobEnqueued(context.Background(), j); err != nil {
		t.Fatalf("expected no error (audit failure swallowed), got: %v", err)
	}
}

// ── Registry integration test ────────────────────────

func TestExtension_ViaRegistry(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)
	logger := slog.Default()

	reg := ext.NewRegistry(logger)
	reg.Register(e)

	ctx := context.Background()
	j := newTestJob()
	r := newTestRun()

	reg.EmitJobEnqueued(ctx, j)
	reg.EmitJobStarted(ctx, j)
	reg.EmitJobCompleted(ctx, j, 50*time.Millisecond)
	reg.EmitJobFailed(ctx, j, errors.New("fail"))
	reg.EmitJobRetrying(ctx, j, 1, time.Now())
	reg.EmitJobDLQ(ctx, j, errors.New("dead"))
	reg.EmitWorkflowStarted(ctx, r)
	reg.EmitWorkflowStepCompleted(ctx, r, "step-1", time.Second)
	reg.EmitWorkflowStepFailed(ctx, r, "step-2", errors.New("bad"))
	reg.EmitWorkflowCompleted(ctx, r, 2*time.Second)
	reg.EmitWorkflowFailed(ctx, r, errors.New("wf fail"))
	reg.EmitCronFired(ctx, "hourly", id.NewJobID())

	// Verify all 12 event types were recorded.
	allActions := ah.AllActions()
	if rec.count() != len(allActions) {
		t.Fatalf("expected %d events, got %d", len(allActions), rec.count())
	}

	for _, action := range allActions {
		evt := rec.findByAction(action)
		if evt == nil {
			t.Errorf("missing event for action %q", action)
		}
	}
}

// ── AllActions test ──────────────────────────────────

func TestAllActions(t *testing.T) {
	actions := ah.AllActions()
	if len(actions) != 12 {
		t.Errorf("expected 12 actions, got %d", len(actions))
	}
}
