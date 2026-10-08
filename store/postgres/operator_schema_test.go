//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/hook"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/postgres"
	"github.com/xraph/dispatch/workflow"
)

// dlqReplayMigrationVersion is the version string of dlq_replayed_job_id,
// restated so a test can delete its bookkeeping row and force a re-run.
const dlqReplayMigrationVersion = "20261009120000"

// operatorColumns are the columns dlq_replayed_job_id and
// workflow_run_version_parent add, as information_schema reports them.
var operatorColumns = []struct {
	table, column, dataType, nullable string
}{
	{"dispatch_dlq", "replayed_job_id", "text", "YES"},
	{"dispatch_workflow_runs", "parent_run_id", "text", "YES"},
	{"dispatch_workflow_runs", "version", "integer", "NO"},
	{"dispatch_workflow_runs", "replay_generation", "bigint", "NO"},
}

// operatorIndexes are the indexes the two migrations build, with the tail
// of the definition Postgres reports for each in pg_indexes.
var operatorIndexes = []listIndex{
	{"idx_dispatch_dlq_job", "dispatch_dlq", "USING btree (job_id, id)"},
	{
		"idx_dispatch_workflow_runs_parent", "dispatch_workflow_runs",
		"USING btree (parent_run_id, id) WHERE (parent_run_id IS NOT NULL)",
	},
}

// assertOperatorSchema checks every column the two migrations add, and
// every index they build is on the right table with the right columns
// and is valid. Valid matters for the same reason as on the list
// indexes: both are built CONCURRENTLY.
func assertOperatorSchema(t *testing.T, conn driver.DedicatedConn) {
	t.Helper()

	ctx := context.Background()

	for _, col := range operatorColumns {
		var dataType, nullable string

		err := conn.QueryRow(ctx, `
			SELECT data_type, is_nullable FROM information_schema.columns
			WHERE table_name = $1 AND column_name = $2`,
			col.table, col.column,
		).Scan(&dataType, &nullable)
		if err != nil {
			t.Errorf("%s.%s: not in information_schema: %v", col.table, col.column, err)

			continue
		}

		if dataType != col.dataType || nullable != col.nullable {
			t.Errorf("%s.%s: %s nullable=%s, want %s nullable=%s",
				col.table, col.column, dataType, nullable, col.dataType, col.nullable)
		}
	}

	for _, ix := range operatorIndexes {
		var table, def string

		err := conn.QueryRow(ctx,
			`SELECT tablename, indexdef FROM pg_indexes WHERE indexname = $1`, ix.name,
		).Scan(&table, &def)
		if err != nil {
			t.Errorf("%s: not in pg_indexes: %v", ix.name, err)

			continue
		}

		if table != ix.table || !strings.HasSuffix(def, ix.def) {
			t.Errorf("%s: on %s as %q, want on %s ending %q", ix.name, table, def, ix.table, ix.def)
		}

		if _, valid := indexIsValid(t, conn, ix.name); !valid {
			t.Errorf("%s: INVALID, so the planner ignores it", ix.name)
		}
	}
}

func dedicatedConn(t *testing.T, s *postgres.Store) driver.DedicatedConn {
	t.Helper()

	conn, err := pgdriver.Unwrap(s.DB()).AcquireConn(context.Background())
	if err != nil {
		t.Fatalf("acquire dedicated conn: %v", err)
	}

	t.Cleanup(conn.Release)

	return conn
}

func TestOperatorSchemaAfterMigrate(t *testing.T) {
	s := setupTestStore(t)

	assertOperatorSchema(t, dedicatedConn(t, s))
}

// TestOperatorSchemaSurvivesASecondMigrate runs the whole group again the
// way every pod start does. Every statement is IF NOT EXISTS and each
// index is dropped only when INVALID, so a second run must change nothing.
func TestOperatorSchemaSurvivesASecondMigrate(t *testing.T) {
	s := setupTestStore(t)

	remigrate(t, s)
	remigrate(t, s)

	assertOperatorSchema(t, dedicatedConn(t, s))
}

// TestDLQJobIndexConvergesFromAnInvalidIndex is why dlq_replayed_job_id
// drops an invalid leftover before building: CREATE INDEX CONCURRENTLY IF
// NOT EXISTS would see the unusable index, skip it, and report success.
func TestDLQJobIndexConvergesFromAnInvalidIndex(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	conn := dedicatedConn(t, s)

	// The catalog state a failed CONCURRENTLY build leaves behind.
	if _, err := conn.Exec(ctx, `
		UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = 'idx_dispatch_dlq_job'::regclass`); err != nil {
		t.Fatalf("mark the index invalid: %v", err)
	}

	if _, valid := indexIsValid(t, conn, "idx_dispatch_dlq_job"); valid {
		t.Fatal("fixture is wrong: the index should be INVALID")
	}

	if _, err := conn.Exec(ctx,
		`DELETE FROM grove_migrations WHERE version = $1`, dlqReplayMigrationVersion); err != nil {
		t.Fatalf("delete migration row: %v", err)
	}

	remigrate(t, s)

	if exists, valid := indexIsValid(t, conn, "idx_dispatch_dlq_job"); !exists || !valid {
		t.Errorf("after the retry idx_dispatch_dlq_job exists=%v valid=%v, want both", exists, valid)
	}
}

// TestGetDLQByJobIDReadsTheJobIndex checks the statement GetDLQByJobID
// sends is answered by walking idx_dispatch_dlq_job backwards: job_id is
// the index condition and the newest entry is the first row read.
func TestGetDLQByJobIDReadsTheJobIndex(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// grove runs post-query hooks only for a query that succeeded, and a
	// miss comes back as sql.ErrNoRows, so give the lookup a row to find.
	now := time.Now().UTC()
	e := &dlq.Entry{
		ID: id.NewDLQID(), JobID: id.NewJobID(), JobName: "plan", Queue: "plan",
		Payload: []byte(`{}`), Error: "boom", FailedAt: now, CreatedAt: now,
	}
	if err := s.PushDLQ(ctx, e); err != nil {
		t.Fatalf("PushDLQ: %v", err)
	}

	capture := &selectCapture{last: map[string]capturedSelect{}}
	s.DB().Hooks().AddHook(capture, hook.Scope{Operations: []hook.Operation{hook.OpSelect}})

	if _, err := s.GetDLQByJobID(ctx, e.JobID); err != nil {
		t.Fatalf("GetDLQByJobID: %v", err)
	}

	plan := explain(t, s, capture.take(t, "dispatch_dlq"))

	if !strings.Contains(plan, "Index Scan Backward using idx_dispatch_dlq_job ") {
		t.Errorf("plan does not walk idx_dispatch_dlq_job backwards:\n%s", plan)
	}
}

// TestRunVersionAndParentRoundTrip covers what the shared workflow suite
// cannot see missing. It compares a run with itself before and after a
// reopen, so a version that always read back as 0 would still pass it.
// Every read path goes through the same converter, but each is read here
// anyway, and after each write that rewrites or touches the row.
func TestRunVersionAndParentRoundTrip(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	parent := &workflow.Run{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewRunID(),
		Name:      "versioned-parent-" + id.NewRunID().String(),
		State:     workflow.RunStateRunning,
		StartedAt: time.Now().UTC(),
		Version:   3,
	}
	parentID := parent.ID
	child := &workflow.Run{
		Entity:      dispatch.NewEntity(),
		ID:          id.NewRunID(),
		Name:        parent.Name + "-child",
		State:       workflow.RunStateFailed,
		StartedAt:   time.Now().UTC(),
		Version:     3,
		ParentRunID: &parentID,
	}
	for _, r := range []*workflow.Run{parent, child} {
		if err := s.CreateRun(ctx, r); err != nil {
			t.Fatalf("CreateRun(%s): %v", r.Name, err)
		}
	}

	check := func(label string, got *workflow.Run) {
		t.Helper()

		if got.Version != 3 {
			t.Errorf("%s: Version = %d, want 3", label, got.Version)
		}
		if got.ParentRunID == nil || *got.ParentRunID != parent.ID {
			t.Errorf("%s: ParentRunID = %v, want %s", label, got.ParentRunID, parent.ID)
		}
	}
	read := func(stage string) {
		t.Helper()

		got, err := s.GetRun(ctx, child.ID)
		if err != nil {
			t.Fatalf("%s: GetRun: %v", stage, err)
		}
		check(stage+": GetRun", got)

		children, err := s.ListChildRuns(ctx, parent.ID)
		if err != nil {
			t.Fatalf("%s: ListChildRuns: %v", stage, err)
		}
		if len(children) != 1 {
			t.Fatalf("%s: ListChildRuns = %d runs, want 1", stage, len(children))
		}
		check(stage+": ListChildRuns", children[0])

		page, err := s.ListRunsPage(ctx, workflow.ListRunsPageOpts{NamePrefix: child.Name})
		if err != nil {
			t.Fatalf("%s: ListRunsPage: %v", stage, err)
		}
		if len(page.Runs) != 1 {
			t.Fatalf("%s: ListRunsPage = %d runs, want 1", stage, len(page.Runs))
		}
		check(stage+": ListRunsPage", page.Runs[0])

		listed, err := s.ListRuns(ctx, workflow.ListOpts{State: got.State})
		if err != nil {
			t.Fatalf("%s: ListRuns: %v", stage, err)
		}
		found := false
		for _, r := range listed {
			if r.ID == child.ID {
				found = true
				check(stage+": ListRuns", r)
			}
		}
		if !found {
			t.Errorf("%s: ListRuns(state %s) did not return the child", stage, got.State)
		}
	}

	read("after CreateRun")

	if err := s.ReopenRun(ctx, child.ID, child.ReplayGeneration); err != nil {
		t.Fatalf("ReopenRun: %v", err)
	}
	read("after ReopenRun")

	// UpdateRun writes the whole row, so it must write these two back too.
	updated, err := s.GetRun(ctx, child.ID)
	if err != nil {
		t.Fatalf("GetRun before UpdateRun: %v", err)
	}
	updated.State = workflow.RunStateCompleted
	if err = s.UpdateRun(ctx, updated); err != nil {
		t.Fatalf("UpdateRun: %v", err)
	}
	read("after UpdateRun")

	top, err := s.GetRun(ctx, parent.ID)
	if err != nil {
		t.Fatalf("GetRun(parent): %v", err)
	}
	if top.Version != 3 || top.ParentRunID != nil {
		t.Errorf("parent: Version = %d, ParentRunID = %v; want 3 and nil", top.Version, top.ParentRunID)
	}
}
