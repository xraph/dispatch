package sqlite_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// listIndex is one index migration list_order_indexes builds and the
// columns it must cover, in order.
type listIndex struct {
	name, table string
	columns     []string
}

var listIndexes = []listIndex{
	{"idx_dispatch_jobs_list_state", "dispatch_jobs", []string{"state", "id"}},
	{"idx_dispatch_jobs_list_queue", "dispatch_jobs", []string{"queue", "id"}},
	{"idx_dispatch_dlq_list_queue", "dispatch_dlq", []string{"queue", "id"}},
	{"idx_dispatch_workflow_runs_list_state", "dispatch_workflow_runs", []string{"state", "id"}},
	{"idx_dispatch_artifacts_list_scope", "dispatch_artifacts", []string{"scope_app_id", "id"}},
}

// queryStrings runs a query whose rows are all text and returns them,
// one slice per row.
func queryStrings(t *testing.T, drv driver.Driver, query string, args ...any) [][]string {
	t.Helper()

	rows, err := drv.Query(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}

	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns of %s: %v", query, err)
	}

	var out [][]string

	for rows.Next() {
		row := make([]any, len(cols))
		for i := range row {
			row[i] = new(any)
		}

		if err = rows.Scan(row...); err != nil {
			t.Fatalf("scan %s: %v", query, err)
		}

		text := make([]string, len(cols))
		for i, v := range row {
			switch x := (*v.(*any)).(type) {
			case string:
				text[i] = x
			case []byte:
				text[i] = string(x)
			}
		}

		out = append(out, text)
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("read %s: %v", query, err)
	}

	return out
}

// assertListIndexes checks each list index is on the right table and
// covers exactly its columns, in order. pragma_index_info lists an
// index's key columns by position.
func assertListIndexes(t *testing.T, drv driver.Driver) {
	t.Helper()

	for _, ix := range listIndexes {
		owner := queryStrings(t, drv,
			`SELECT tbl_name FROM sqlite_master WHERE type = 'index' AND name = ?`, ix.name)
		if len(owner) != 1 || owner[0][0] != ix.table {
			t.Errorf("%s: sqlite_master has %v, want one index on %s", ix.name, owner, ix.table)

			continue
		}

		var cols []string
		for _, r := range queryStrings(t, drv,
			`SELECT name FROM pragma_index_info(?) ORDER BY seqno`, ix.name) {
			cols = append(cols, r[0])
		}

		if strings.Join(cols, ",") != strings.Join(ix.columns, ",") {
			t.Errorf("%s: columns %v, want %v", ix.name, cols, ix.columns)
		}
	}
}

func TestListIndexesExistAfterMigrate(t *testing.T) {
	_, drv, _ := openMigratedWithDriver(t)

	assertListIndexes(t, drv)
}

// TestListIndexesSurviveASecondMigrate runs Migrate again on a migrated
// database, as every process start does. CREATE INDEX IF NOT EXISTS makes
// the rerun a no-op; it must not fail and must leave every index in place.
func TestListIndexesSurviveASecondMigrate(t *testing.T) {
	s, drv, _ := openMigratedWithDriver(t)

	for range 2 {
		if err := s.Migrate(context.Background()); err != nil {
			t.Fatalf("migrate again: %v", err)
		}
	}

	assertListIndexes(t, drv)
}

// selectCapture records the last SELECT grove built for each table, as
// sent: the SQL with its placeholders and the bound arguments. grove sets
// RawQuery and RawArgs before it runs post-query hooks.
type selectCapture struct {
	mu   sync.Mutex
	last map[string]capturedSelect
}

type capturedSelect struct {
	query string
	args  []any
}

func (c *selectCapture) AfterQuery(_ context.Context, qc *hook.QueryContext, _ any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.last[qc.Table] = capturedSelect{query: qc.RawQuery, args: qc.RawArgs}

	return nil
}

func (c *selectCapture) take(t *testing.T, table string) capturedSelect {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	got, ok := c.last[table]
	if !ok {
		t.Fatalf("no SELECT on %s was captured", table)
	}

	delete(c.last, table)

	return got
}

// TestFilteredListsReadInIndexOrder runs each paged list with one filter,
// captures the statement the store actually sent, and asks SQLite how it
// would run it. The plan must search the filter's list index and must not
// build a temporary B-tree for ORDER BY: the index already returns the
// matching rows in ID order, so a page stops after limit+1 rows instead
// of sorting every match.
//
// SQLite has no planner switches, and with no ANALYZE statistics it
// prefers an index that satisfies both the WHERE and the ORDER BY over one
// that needs a sort, so the plan is the same on an empty table as on a
// large one.
func TestFilteredListsReadInIndexOrder(t *testing.T) {
	s, drv, db := openMigratedWithDriver(t)
	ctx := context.Background()

	capture := &selectCapture{last: map[string]capturedSelect{}}
	db.Hooks().AddHook(capture, hook.Scope{Operations: []hook.Operation{hook.OpSelect}})

	cases := []struct {
		index string
		table string
		list  func() error
	}{
		{"idx_dispatch_jobs_list_state", "dispatch_jobs", func() error {
			_, err := s.ListJobs(ctx, job.ListJobsOpts{States: []job.State{job.StateFailed}})
			return err
		}},
		{"idx_dispatch_jobs_list_queue", "dispatch_jobs", func() error {
			_, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: "reports"})
			return err
		}},
		{"idx_dispatch_dlq_list_queue", "dispatch_dlq", func() error {
			_, err := s.ListDLQPage(ctx, dlq.PageOpts{Queue: "reports"})
			return err
		}},
		{"idx_dispatch_workflow_runs_list_state", "dispatch_workflow_runs", func() error {
			_, err := s.ListRunsPage(ctx, workflow.ListRunsPageOpts{State: workflow.RunStateFailed})
			return err
		}},
		{"idx_dispatch_artifacts_list_scope", "dispatch_artifacts", func() error {
			_, err := s.ListArtifactsPage(ctx, artifact.PageOpts{ScopeAppID: "app-1"})
			return err
		}},
	}

	for _, c := range cases {
		t.Run(c.index, func(t *testing.T) {
			if err := c.list(); err != nil {
				t.Fatalf("list: %v", err)
			}

			q := capture.take(t, c.table)

			var lines []string
			for _, r := range queryStrings(t, drv, `EXPLAIN QUERY PLAN `+q.query, q.args...) {
				lines = append(lines, r[len(r)-1])
			}

			plan := strings.Join(lines, "\n")

			if !strings.Contains(plan, "USING INDEX "+c.index+" ") {
				t.Errorf("plan does not search %s:\n%s", c.index, plan)
			}

			if strings.Contains(plan, "TEMP B-TREE") {
				t.Errorf("plan sorts instead of reading in index order:\n%s", plan)
			}
		})
	}
}
