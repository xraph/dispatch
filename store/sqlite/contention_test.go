package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/sqlitedriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	sqlitestore "github.com/xraph/dispatch/store/sqlite"
	"github.com/xraph/dispatch/workflow"
)

func contentionHandle(t *testing.T, path string, options ...driver.Option) (*sqlitestore.Store, *sqlitedriver.SqliteDB) {
	t.Helper()
	drv := sqlitedriver.New()
	if err := drv.Open(t.Context(), path, options...); err != nil {
		t.Fatal(err)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return sqlitestore.New(db), drv
}
func contentionRun(t *testing.T, s *sqlitestore.Store) *workflow.Run {
	t.Helper()
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	parent := &workflow.Run{Entity: dispatch.NewEntity(), ID: id.NewRunID(), Name: "parent", State: workflow.RunStateRunning, StartedAt: time.Now().UTC()}
	if err := s.CreateRun(t.Context(), parent); err != nil {
		t.Fatal(err)
	}
	completed := time.Now().UTC()
	run := &workflow.Run{Entity: dispatch.NewEntity(), ID: id.NewRunID(), Name: "contended-child", State: workflow.RunStateFailed, Input: []byte(`{"input":1}`), Output: []byte(`{"output":2}`), Error: "failed", ScopeAppID: "app", ScopeOrgID: "org", StartedAt: completed.Add(-time.Minute), CompletedAt: &completed, Version: 3, ParentRunID: &parent.ID}
	if err := s.CreateRun(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	saved, err := s.GetRun(t.Context(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	return saved
}
func heldWriter(t *testing.T, db *sqlitedriver.SqliteDB, run *workflow.Run) driver.Tx {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(t.Context(), "UPDATE dispatch_workflow_runs SET updated_at=updated_at WHERE id=?", run.ID.String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	return tx
}

func TestReopenRunAcrossHeldWriterAndIndependentHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contended.db")
	first, writer := contentionHandle(t, path)
	before := contentionRun(t, first)
	second, other := contentionHandle(t, path)
	for _, db := range []*sqlitedriver.SqliteDB{writer, other} {
		var timeout int
		if err := db.QueryRow(t.Context(), "PRAGMA busy_timeout").Scan(&timeout); err != nil || timeout != 0 {
			t.Fatalf("default connection profile changed: timeout=%d error=%v", timeout, err)
		}
	}
	tx := heldWriter(t, writer, before)
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	defer func() { cancel(); _ = tx.Rollback(); wg.Wait() }()
	const contenders = 16
	ready := make(chan struct{}, contenders)
	start := make(chan struct{})
	results := make(chan error, contenders)
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			s := first
			if i%2 == 1 {
				s = second
			}
			results <- s.ReopenRun(ctx, before.ID, before.ReplayGeneration)
		}()
	}
	for range contenders {
		<-ready
	}
	close(start)
	select {
	case err := <-results:
		t.Fatalf("claim returned before held writer released: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	winners, refusals := 0, 0
	for range contenders {
		err := <-results
		switch {
		case err == nil:
			winners++
		case errors.Is(err, dispatch.ErrInvalidState):
			refusals++
		default:
			t.Fatalf("contender returned unexpected error: %v", err)
		}
	}
	if winners != 1 || refusals != contenders-1 {
		t.Fatalf("winners=%d refusals=%d", winners, refusals)
	}
	after, err := second.GetRun(t.Context(), before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != workflow.RunStateRunning || after.ReplayGeneration != before.ReplayGeneration+1 || after.CompletedAt != nil || after.Error != "" {
		t.Fatalf("invalid claimed run: %+v", after)
	}
	if !bytes.Equal(after.Input, before.Input) || !bytes.Equal(after.Output, before.Output) || after.ParentRunID == nil || *after.ParentRunID != *before.ParentRunID || after.Name != before.Name || after.Version != before.Version || after.ScopeAppID != before.ScopeAppID || after.ScopeOrgID != before.ScopeOrgID || !after.StartedAt.Equal(before.StartedAt) || !after.CreatedAt.Equal(before.CreatedAt) {
		t.Fatal("claim changed retained run fields")
	}
	after.State = workflow.RunStateFailed
	if err = second.UpdateRun(t.Context(), after); err != nil {
		t.Fatal(err)
	}
	if err = first.ReopenRun(t.Context(), before.ID, before.ReplayGeneration); !errors.Is(err, dispatch.ErrInvalidState) {
		t.Fatalf("stale generation admitted: %v", err)
	}
}

func TestReopenRunCancellationWithHeldWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cancel.db")
	first, writer := contentionHandle(t, path)
	before := contentionRun(t, first)
	second, _ := contentionHandle(t, path)
	tx := heldWriter(t, writer, before)
	defer func() { _ = tx.Rollback() }()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := second.ReopenRun(ctx, before.ID, before.ReplayGeneration); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller deadline lost while writer held: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	after, err := first.GetRun(t.Context(), before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != before.State || after.ReplayGeneration != before.ReplayGeneration {
		t.Fatal("canceled claim mutated run")
	}
}

func TestBusyTimeoutDSNAppliesToEachReservedConnection(t *testing.T) {
	_, db := contentionHandle(t, "file:"+filepath.Join(t.TempDir(), "profile.db")+"?_pragma=busy_timeout(5000)", driver.WithPoolSize(2))
	// Simultaneous transactions reserve different physical pooled connections.
	first, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Rollback() }()
	second, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Rollback() }()
	for _, tx := range []driver.Tx{first, second} {
		var timeout int
		if err = tx.QueryRow(t.Context(), "PRAGMA busy_timeout").Scan(&timeout); err != nil || timeout != 5000 {
			t.Fatalf("reserved connection timeout=%d error=%v", timeout, err)
		}
	}
}
