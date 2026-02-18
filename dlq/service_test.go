package dlq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	dispatchDLQ "github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

func newTestJob(name string, payload []byte) *job.Job {
	now := time.Now().UTC()
	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      "default",
		Payload:    payload,
		State:      job.StateFailed,
		MaxRetries: 3,
		RetryCount: 3,
		LastError:  "test error",
		ScopeAppID: "app_test",
		ScopeOrgID: "org_test",
		RunAt:      now,
	}
	return j
}

func TestService_Push_BuildsEntryFromJob(t *testing.T) {
	s := memory.New()
	svc := dispatchDLQ.NewService(s, s)
	ctx := context.Background()

	j := newTestJob("send-email", []byte(`{"to":"alice@example.com"}`))
	jobErr := errors.New("smtp timeout")

	if err := svc.Push(ctx, j, jobErr); err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Verify entry in store.
	entries, err := s.ListDLQ(ctx, dispatchDLQ.ListOpts{Limit: 10})
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.JobID != j.ID {
		t.Errorf("JobID = %v, want %v", entry.JobID, j.ID)
	}
	if entry.JobName != "send-email" {
		t.Errorf("JobName = %q, want %q", entry.JobName, "send-email")
	}
	if entry.Queue != "default" {
		t.Errorf("Queue = %q, want %q", entry.Queue, "default")
	}
	if string(entry.Payload) != `{"to":"alice@example.com"}` {
		t.Errorf("Payload = %q, want %q", entry.Payload, `{"to":"alice@example.com"}`)
	}
	if entry.Error != "smtp timeout" {
		t.Errorf("Error = %q, want %q", entry.Error, "smtp timeout")
	}
	if entry.RetryCount != 3 {
		t.Errorf("RetryCount = %d, want %d", entry.RetryCount, 3)
	}
	if entry.ScopeAppID != "app_test" {
		t.Errorf("ScopeAppID = %q, want %q", entry.ScopeAppID, "app_test")
	}
	if entry.ScopeOrgID != "org_test" {
		t.Errorf("ScopeOrgID = %q, want %q", entry.ScopeOrgID, "org_test")
	}
	if entry.FailedAt.IsZero() {
		t.Error("expected FailedAt to be set")
	}
	if entry.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be set")
	}
}

func TestService_Push_CountIncreases(t *testing.T) {
	s := memory.New()
	svc := dispatchDLQ.NewService(s, s)
	ctx := context.Background()

	for i := range 3 {
		j := newTestJob("job-"+string(rune('a'+i)), nil)
		if err := svc.Push(ctx, j, errors.New("fail")); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}

	count, err := s.CountDLQ(ctx)
	if err != nil {
		t.Fatalf("CountDLQ: %v", err)
	}
	if count != 3 {
		t.Errorf("CountDLQ = %d, want 3", count)
	}
}

func TestService_Replay_CreatesNewPendingJob(t *testing.T) {
	s := memory.New()
	svc := dispatchDLQ.NewService(s, s)
	ctx := context.Background()

	// Push a failed job to DLQ.
	original := newTestJob("replay-me", []byte(`{"key":"value"}`))
	if err := svc.Push(ctx, original, errors.New("original error")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Get the DLQ entry ID.
	entries, err := s.ListDLQ(ctx, dispatchDLQ.ListOpts{Limit: 1})
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(entries))
	}
	entryID := entries[0].ID

	// Replay.
	replayed, err := svc.Replay(ctx, entryID)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	// Verify new job.
	if replayed.ID == original.ID {
		t.Error("replayed job should have a new ID")
	}
	if replayed.State != job.StatePending {
		t.Errorf("State = %q, want %q", replayed.State, job.StatePending)
	}
	if replayed.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", replayed.RetryCount)
	}
	if replayed.Name != "replay-me" {
		t.Errorf("Name = %q, want %q", replayed.Name, "replay-me")
	}
	if string(replayed.Payload) != `{"key":"value"}` {
		t.Errorf("Payload = %q, want %q", replayed.Payload, `{"key":"value"}`)
	}
	if replayed.ScopeAppID != "app_test" {
		t.Errorf("ScopeAppID = %q, want %q", replayed.ScopeAppID, "app_test")
	}

	// Verify the job exists in the job store.
	got, err := s.GetJob(ctx, replayed.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.State != job.StatePending {
		t.Errorf("stored job State = %q, want %q", got.State, job.StatePending)
	}
}

func TestService_Replay_MarksDLQEntryAsReplayed(t *testing.T) {
	s := memory.New()
	svc := dispatchDLQ.NewService(s, s)
	ctx := context.Background()

	j := newTestJob("replay-mark", nil)
	if err := svc.Push(ctx, j, errors.New("fail")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	entries, err := s.ListDLQ(ctx, dispatchDLQ.ListOpts{Limit: 1})
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	entryID := entries[0].ID

	if _, replayErr := svc.Replay(ctx, entryID); replayErr != nil {
		t.Fatalf("Replay: %v", replayErr)
	}

	// Check that ReplayedAt is set.
	entry, err := s.GetDLQ(ctx, entryID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if entry.ReplayedAt == nil {
		t.Error("expected ReplayedAt to be set after replay")
	}
}

func TestService_Replay_NotFoundReturnsError(t *testing.T) {
	s := memory.New()
	svc := dispatchDLQ.NewService(s, s)
	ctx := context.Background()

	fakeID := id.NewDLQID()
	_, err := svc.Replay(ctx, fakeID)
	if err == nil {
		t.Fatal("expected error for non-existent DLQ entry")
	}
}
