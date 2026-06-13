package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"
	_ "github.com/xraph/grove/drivers/sqlitedriver/sqlitemigrate" // registers the sqlite migrate executor

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	sqlitestore "github.com/xraph/dispatch/store/sqlite"
)

func openSqliteStore(t *testing.T) *sqlitestore.Store {
	t.Helper()
	ctx := context.Background()

	drv := sqlitedriver.New()
	if err := drv.Open(ctx, filepath.Join(t.TempDir(), "dispatch.db")); err != nil {
		t.Fatalf("open sqlitedriver: %v", err)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatalf("grove open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s := sqlitestore.New(db)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func runningJob(name string, startedAgo time.Duration) *job.Job {
	now := time.Now().UTC()
	started := now.Add(-startedAgo)
	return &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StateRunning,
		MaxRetries: 3,
		RunAt:      now.Add(-startedAgo),
		StartedAt:  &started,
	}
}

// TestReapStaleJobs_OrphanNilHeartbeat covers the crashed-worker window: a
// job claimed (running, started_at set) whose worker died before the first
// heartbeat has heartbeat_at == NULL and must still be reaped once its start
// time passes the threshold. A freshly claimed job must not be.
func TestReapStaleJobs_OrphanNilHeartbeat(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	orphan := runningJob("orphan", time.Minute)
	if err := s.EnqueueJob(ctx, orphan); err != nil {
		t.Fatalf("enqueue orphan: %v", err)
	}
	fresh := runningJob("fresh", 0)
	if err := s.EnqueueJob(ctx, fresh); err != nil {
		t.Fatalf("enqueue fresh: %v", err)
	}

	// Precondition: the store persisted the claimed-but-unheartbeated state.
	got, err := s.GetJob(ctx, orphan.ID)
	if err != nil {
		t.Fatalf("get orphan: %v", err)
	}
	if got.State != job.StateRunning || got.HeartbeatAt != nil {
		t.Fatalf("precondition: want running with nil heartbeat, got state=%s heartbeat=%v", got.State, got.HeartbeatAt)
	}

	stale, err := s.ReapStaleJobs(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 orphaned stale job, got %d", len(stale))
	}
	if stale[0].ID.String() != orphan.ID.String() {
		t.Fatalf("reaped wrong job: %s", stale[0].Name)
	}
}
