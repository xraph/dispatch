package sqlite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/sqlitedriver"
	_ "github.com/xraph/grove/drivers/sqlitedriver/sqlitemigrate" // registers the sqlite migrate executor

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	sqlitestore "github.com/xraph/dispatch/store/sqlite"
)

// resourceMigrationVersion is the version string of the migration under
// test, restated here so the test can delete its bookkeeping row.
const resourceMigrationVersion = "20260812130000"

// openMigratedWithDriver is openSqliteStore with the raw driver handed
// back too, so a test can reach past the store and mutate the schema
// into shapes no code path produces.
func openMigratedWithDriver(t *testing.T) (*sqlitestore.Store, driver.Driver, *grove.DB) {
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

	return s, drv, db
}

func mustExec(t *testing.T, drv driver.Driver, stmt string) {
	t.Helper()

	if _, err := drv.Exec(context.Background(), stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

func hasColumn(t *testing.T, drv driver.Driver, table, column string) bool {
	t.Helper()

	rows, err := drv.Query(context.Background(),
		`SELECT 1 FROM pragma_table_info(?) WHERE name = ?`, table, column)
	if err != nil {
		t.Fatalf("pragma_table_info: %v", err)
	}

	found := rows.Next()

	if err = rows.Close(); err != nil {
		t.Fatalf("close pragma rows: %v", err)
	}

	return found
}

// TestResourceMigrationSurvivesAPartialApplication is the proof that the
// resource migration can no longer wedge a SQLite deployment
// permanently.
//
// The failure it reproduces is not hypothetical and has no workaround.
// SQLite has no ADD COLUMN IF NOT EXISTS, and grove executes Up outside
// any transaction (migrate.Orchestrator.Migrate calls m.Up and only then
// RecordApplied). So a process killed — or a disk filled, or a context
// cancelled — partway through the ten ADD COLUMNs leaves some columns
// added and NO row in grove_migrations. The next start re-runs Up from
// the top, hits ALTER TABLE ADD COLUMN on a column that already exists,
// and fails with "duplicate column name". It then fails identically on
// every subsequent start, forever, on every pod, with no recovery short
// of an operator hand-writing DDL against a live database.
//
// The state below is exactly what a crash after the fourth statement
// leaves behind: the first four columns present, the remaining six
// absent, the index absent, and the migration unrecorded. A retry must
// complete it and the store must work afterwards.
//
// Mutation-verified: reverting Up to unguarded ALTER TABLE ADD COLUMN
// fails here with "duplicate column name: req_cpu_milli".
func TestResourceMigrationSurvivesAPartialApplication(t *testing.T) {
	s, drv, db := openMigratedWithDriver(t)
	ctx := context.Background()

	// Rewind to the halfway state a crash would have left.
	for _, col := range []string{
		"req_custom_keys", "resource_requests", "resource_limits",
		"resource_class", "input_bytes", "primary_input_hash",
	} {
		mustExec(t, drv, `ALTER TABLE dispatch_jobs DROP COLUMN `+col)
	}

	mustExec(t, drv, `DROP INDEX IF EXISTS idx_dispatch_jobs_dequeue_res`)
	mustExec(t, drv, `DELETE FROM grove_migrations WHERE version = '`+resourceMigrationVersion+`'`)

	for _, col := range []string{"req_cpu_milli", "req_gpu_milli"} {
		if !hasColumn(t, drv, "dispatch_jobs", col) {
			t.Fatalf("fixture is wrong: %s should still be present", col)
		}
	}

	if hasColumn(t, drv, "dispatch_jobs", "primary_input_hash") {
		t.Fatal("fixture is wrong: primary_input_hash should have been dropped")
	}

	// The retry every restarting pod performs.
	retried := sqlitestore.New(db)
	if err := retried.Migrate(ctx); err != nil {
		t.Fatalf("re-running a half-applied migration must succeed, got: %v\n"+
			"a SQLite deployment that failed partway through this migration would be "+
			"unrecoverable without hand-written DDL", err)
	}

	for _, col := range []string{
		"req_cpu_milli", "req_memory_bytes", "req_disk_bytes", "req_gpu_milli",
		"req_custom_keys", "resource_requests", "resource_limits",
		"resource_class", "input_bytes", "primary_input_hash",
	} {
		if !hasColumn(t, drv, "dispatch_jobs", col) {
			t.Errorf("column %s missing after the retry", col)
		}
	}

	// And the schema is usable, not merely present.
	j := &job.Job{
		Entity:           dispatch.NewEntity(),
		ID:               id.NewJobID(),
		Name:             "after-retry",
		Queue:            "default",
		State:            job.StatePending,
		Payload:          []byte(`{}`),
		Resources:        resource.Set{resource.Memory: 8 << 30, "fpga": 1},
		PrimaryInputHash: "blake3:abc",
	}

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob after the retry: %v", err)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob after the retry: %v", err)
	}

	if got.Resources[resource.Memory] != 8<<30 || got.Resources["fpga"] != 1 {
		t.Errorf("resources = %v, want the declaration intact", got.Resources)
	}
}

// TestResourceMigrationIsFullyIdempotent covers the other partial shape:
// a crash AFTER the last statement but BEFORE RecordApplied, which is a
// real window because the two are separate calls. Re-running Up with
// every column and the index already in place must be a no-op.
func TestResourceMigrationIsFullyIdempotent(t *testing.T) {
	_, drv, db := openMigratedWithDriver(t)
	ctx := context.Background()

	mustExec(t, drv, `DELETE FROM grove_migrations WHERE version = '`+resourceMigrationVersion+`'`)

	if err := sqlitestore.New(db).Migrate(ctx); err != nil {
		t.Fatalf("re-running a fully applied migration must be a no-op, got: %v", err)
	}
}

// TestResourceMigrationDropsTheRedundantDequeueIndex pins the other half
// of the change: idx_dispatch_jobs_dequeue is a strict key prefix of
// idx_dispatch_jobs_dequeue_res over the identical partial predicate, so
// shipping both means every enqueue pays two B-tree inserts and every
// claim two deletes, forever, for a plan SQLite would never choose.
func TestResourceMigrationDropsTheRedundantDequeueIndex(t *testing.T) {
	_, drv, _ := openMigratedWithDriver(t)

	rows, err := drv.Query(context.Background(),
		`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'dispatch_jobs'`)
	if err != nil {
		t.Fatalf("read sqlite_master: %v", err)
	}

	var names []string

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			t.Fatalf("scan index name: %v", err)
		}

		names = append(names, name)
	}

	if err = rows.Close(); err != nil {
		t.Fatalf("close sqlite_master rows: %v", err)
	}

	var sawSuperset bool

	for _, n := range names {
		if n == "idx_dispatch_jobs_dequeue" {
			t.Errorf("idx_dispatch_jobs_dequeue survived the resource migration; it is a key "+
				"prefix of idx_dispatch_jobs_dequeue_res, so both are maintained on every "+
				"write for no plan. Indexes present: %s", strings.Join(names, ", "))
		}

		if n == "idx_dispatch_jobs_dequeue_res" {
			sawSuperset = true
		}
	}

	if !sawSuperset {
		t.Fatalf("idx_dispatch_jobs_dequeue_res is missing; indexes present: %s",
			strings.Join(names, ", "))
	}
}
