package audithook_test

import (
	"context"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	ah "github.com/xraph/dispatch/audit_hook"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
)

func TestExtension_JobCancelled(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)
	j := newTestJob()

	if err := e.OnJobCancelled(context.Background(), j); err != nil {
		t.Fatalf("OnJobCancelled: %v", err)
	}

	evt := rec.last()
	if evt == nil {
		t.Fatal("no event recorded")
	}
	if evt.Action != ah.ActionJobCancelled {
		t.Errorf("Action: want %q, got %q", ah.ActionJobCancelled, evt.Action)
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
	if evt.Severity != ah.SeverityWarning {
		t.Errorf("Severity: want %q, got %q", ah.SeverityWarning, evt.Severity)
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

func TestExtension_OperatorJobCancelled(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)
	jobID := id.NewJobID()
	at := time.Date(2026, 10, 7, 12, 30, 0, 0, time.UTC)

	err := e.OnOperatorAction(context.Background(), ext.Action{
		Kind:  ext.ActionJobCancelled,
		Actor: "user_42",
		JobID: jobID,
		At:    at,
	})
	if err != nil {
		t.Fatalf("OnOperatorAction: %v", err)
	}

	evt := rec.last()
	if evt == nil {
		t.Fatal("no event recorded")
	}
	if evt.Action != ah.ActionOperatorJobCancelled {
		t.Errorf("Action: want %q, got %q", ah.ActionOperatorJobCancelled, evt.Action)
	}
	if evt.Resource != ah.ResourceJob {
		t.Errorf("Resource: want %q, got %q", ah.ResourceJob, evt.Resource)
	}
	if evt.Category != ah.CategoryOperator {
		t.Errorf("Category: want %q, got %q", ah.CategoryOperator, evt.Category)
	}
	if evt.ResourceID != jobID.String() {
		t.Errorf("ResourceID: want %q, got %q", jobID.String(), evt.ResourceID)
	}
	if evt.Severity != ah.SeverityWarning {
		t.Errorf("Severity: want %q, got %q", ah.SeverityWarning, evt.Severity)
	}
	if evt.Outcome != ah.OutcomeSuccess {
		t.Errorf("Outcome: want %q, got %q", ah.OutcomeSuccess, evt.Outcome)
	}
	if evt.Metadata["actor"] != "user_42" {
		t.Errorf("Metadata[actor]: want %q, got %v", "user_42", evt.Metadata["actor"])
	}
	if evt.Metadata["kind"] != "job.cancelled" {
		t.Errorf("Metadata[kind]: want %q, got %v", "job.cancelled", evt.Metadata["kind"])
	}
	if evt.Metadata["at"] != "2026-10-07T12:30:00Z" {
		t.Errorf("Metadata[at]: want %q, got %v", "2026-10-07T12:30:00Z", evt.Metadata["at"])
	}
	if evt.Metadata["job_id"] != jobID.String() {
		t.Errorf("Metadata[job_id]: want %q, got %v", jobID.String(), evt.Metadata["job_id"])
	}
	for _, absent := range []string{"new_job_id", "dlq_id", "cron_id", "run_id", "step", "count"} {
		if v, ok := evt.Metadata[absent]; ok {
			t.Errorf("Metadata[%s]: want absent for a cancel, got %v", absent, v)
		}
	}
}

func TestExtension_OperatorDLQPurgedKeepsZeroCount(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	err := e.OnOperatorAction(context.Background(), ext.Action{
		Kind:  ext.ActionDLQPurged,
		Actor: "user_42",
		At:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("OnOperatorAction: %v", err)
	}

	evt := rec.last()
	if evt.Action != ah.ActionOperatorDLQPurged {
		t.Errorf("Action: want %q, got %q", ah.ActionOperatorDLQPurged, evt.Action)
	}
	if evt.Resource != ah.ResourceDLQ {
		t.Errorf("Resource: want %q, got %q", ah.ResourceDLQ, evt.Resource)
	}
	if evt.ResourceID != "" {
		t.Errorf("ResourceID: want empty for a purge, got %q", evt.ResourceID)
	}
	if evt.Metadata["count"] != int64(0) {
		t.Errorf("Metadata[count]: want int64(0) (a purge of nothing is still a purge), got %#v", evt.Metadata["count"])
	}
}

func TestExtension_OperatorActionsMapEveryKind(t *testing.T) {
	jobID, newJobID := id.NewJobID(), id.NewJobID()
	dlqID, cronID, runID := id.NewDLQID(), id.NewCronID(), id.NewRunID()

	cases := []struct {
		action    ext.Action
		wantName  string
		wantRes   string
		wantResID string
		wantSev   string
	}{
		{ext.Action{Kind: ext.ActionJobCancelled, JobID: jobID}, ah.ActionOperatorJobCancelled, ah.ResourceJob, jobID.String(), ah.SeverityWarning},
		{ext.Action{Kind: ext.ActionJobRetried, JobID: jobID}, ah.ActionOperatorJobRetried, ah.ResourceJob, jobID.String(), ah.SeverityInfo},
		{ext.Action{Kind: ext.ActionDLQReplayed, DLQID: dlqID, NewJobID: newJobID}, ah.ActionOperatorDLQReplayed, ah.ResourceDLQ, dlqID.String(), ah.SeverityInfo},
		{ext.Action{Kind: ext.ActionDLQDeleted, DLQID: dlqID}, ah.ActionOperatorDLQDeleted, ah.ResourceDLQ, dlqID.String(), ah.SeverityWarning},
		{ext.Action{Kind: ext.ActionDLQPurged, Count: 4}, ah.ActionOperatorDLQPurged, ah.ResourceDLQ, "", ah.SeverityWarning},
		{ext.Action{Kind: ext.ActionCronEnabled, CronID: cronID}, ah.ActionOperatorCronEnabled, ah.ResourceCron, cronID.String(), ah.SeverityInfo},
		{ext.Action{Kind: ext.ActionCronDisabled, CronID: cronID}, ah.ActionOperatorCronDisabled, ah.ResourceCron, cronID.String(), ah.SeverityWarning},
		{ext.Action{Kind: ext.ActionCronDeleted, CronID: cronID}, ah.ActionOperatorCronDeleted, ah.ResourceCron, cronID.String(), ah.SeverityWarning},
		{ext.Action{Kind: ext.ActionCronTriggered, CronID: cronID, NewJobID: newJobID}, ah.ActionOperatorCronTriggered, ah.ResourceCron, cronID.String(), ah.SeverityInfo},
		{ext.Action{Kind: ext.ActionWorkflowReplayed, RunID: runID, Step: "charge"}, ah.ActionOperatorWorkflowReplayed, ah.ResourceWorkflow, runID.String(), ah.SeverityInfo},
	}

	for _, tc := range cases {
		t.Run(string(tc.action.Kind), func(t *testing.T) {
			rec := &mockRecorder{}
			e := ah.New(rec)
			if err := e.OnOperatorAction(context.Background(), tc.action); err != nil {
				t.Fatalf("OnOperatorAction: %v", err)
			}
			evt := rec.last()
			if evt == nil {
				t.Fatal("no event recorded")
			}
			if evt.Action != tc.wantName {
				t.Errorf("Action: want %q, got %q", tc.wantName, evt.Action)
			}
			if evt.Category != ah.CategoryOperator {
				t.Errorf("Category: want %q, got %q", ah.CategoryOperator, evt.Category)
			}
			if evt.Resource != tc.wantRes {
				t.Errorf("Resource: want %q, got %q", tc.wantRes, evt.Resource)
			}
			if evt.ResourceID != tc.wantResID {
				t.Errorf("ResourceID: want %q, got %q", tc.wantResID, evt.ResourceID)
			}
			if evt.Severity != tc.wantSev {
				t.Errorf("Severity: want %q, got %q", tc.wantSev, evt.Severity)
			}
		})
	}
}

func TestExtension_OperatorActionUnknownKind(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec)

	if err := e.OnOperatorAction(context.Background(), ext.Action{Kind: "queue.paused", Actor: "user_42"}); err != nil {
		t.Fatalf("OnOperatorAction: %v", err)
	}

	evt := rec.last()
	if evt == nil {
		t.Fatal("an unmapped kind must still be audited")
	}
	if evt.Action != "operator.queue_paused" {
		t.Errorf("Action: want %q, got %q", "operator.queue_paused", evt.Action)
	}
	if evt.Category != ah.CategoryOperator {
		t.Errorf("Category: want %q, got %q", ah.CategoryOperator, evt.Category)
	}
	if evt.Metadata["actor"] != "user_42" {
		t.Errorf("Metadata[actor]: want %q, got %v", "user_42", evt.Metadata["actor"])
	}
}

func TestExtension_OperatorActionViaRegistryTakesActorFromContext(t *testing.T) {
	rec := &mockRecorder{}
	reg := ext.NewRegistry(log.NewNoopLogger())
	reg.Register(ah.New(rec))

	ctx := ext.WithActor(context.Background(), "user_9")
	reg.EmitOperatorAction(ctx, ext.Action{Kind: ext.ActionJobRetried, JobID: id.NewJobID()})

	evt := rec.findByAction(ah.ActionOperatorJobRetried)
	if evt == nil {
		t.Fatal("no operator.job_retried event recorded")
	}
	if evt.Metadata["actor"] != "user_9" {
		t.Errorf("Metadata[actor]: want %q, got %v", "user_9", evt.Metadata["actor"])
	}
	if at, ok := evt.Metadata["at"].(string); !ok || at == "" {
		t.Errorf("Metadata[at]: want the registry's default time, got %v", evt.Metadata["at"])
	}
}

func TestExtension_WithActions_FiltersOperatorActions(t *testing.T) {
	rec := &mockRecorder{}
	e := ah.New(rec, ah.WithActions(ah.ActionOperatorDLQPurged))
	ctx := context.Background()

	if err := e.OnOperatorAction(ctx, ext.Action{Kind: ext.ActionJobCancelled, JobID: id.NewJobID()}); err != nil {
		t.Fatalf("OnOperatorAction: %v", err)
	}
	if rec.count() != 0 {
		t.Fatalf("want 0 events (operator.job_cancelled disabled), got %d", rec.count())
	}
	if err := e.OnOperatorAction(ctx, ext.Action{Kind: ext.ActionDLQPurged, Count: 2}); err != nil {
		t.Fatalf("OnOperatorAction: %v", err)
	}
	if rec.count() != 1 {
		t.Fatalf("want 1 event (operator.dlq_purged enabled), got %d", rec.count())
	}
}
