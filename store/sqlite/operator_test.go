package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/storetest"
	"github.com/xraph/dispatch/workflow"
)

// Each conformance test opens a fresh migrated database per subtest
// through openSqliteStore (store/sqlite/reap_test.go:19), like the other
// suites in this package.

func TestDLQReplayConformance(t *testing.T) {
	storetest.RunDLQReplaySuite(t, func(t *testing.T) storetest.DLQReplayStore {
		t.Helper()

		return openSqliteStore(t)
	})
}

func TestCronConformance(t *testing.T) {
	storetest.RunCronSuite(t, func(t *testing.T) storetest.CronStore {
		t.Helper()

		return openSqliteStore(t)
	})
}

func TestWorkflowConformance(t *testing.T) {
	storetest.RunWorkflowSuite(t, func(t *testing.T) storetest.WorkflowStore {
		t.Helper()

		return openSqliteStore(t)
	})
}

// driverTimeLayout is how grove's sqlitedriver renders a bound time.Time:
// Go's default time.Time.String() form, not ISO-8601. Every time column
// this store compares is compared as text in this form.
const driverTimeLayout = "2006-01-02 15:04:05.999999999 -0700 MST"

// TestClaimReplayStoresReplayedAtInTheDriverTimeForm pins how ClaimReplay
// writes replayed_at. SQLite has no timestamp type, so a time column is
// whatever text the writer put there, and comparisons are string
// comparisons. ClaimReplay must bind a time.Time like ReplayDLQ and the
// insert path do. A hand-formatted value (RFC 3339, say) would read back
// fine through the model and still sort wrongly against every other
// timestamp the moment anything compares replayed_at.
func TestClaimReplayStoresReplayedAtInTheDriverTimeForm(t *testing.T) {
	s, drv, _ := openMigratedWithDriver(t)
	ctx := context.Background()
	now := time.Now().UTC()

	claimed := &dlq.Entry{
		ID: id.NewDLQID(), JobID: id.NewJobID(), JobName: "form", Queue: "form",
		Payload: []byte(`{}`), Error: "boom", FailedAt: now, CreatedAt: now,
	}
	marked := &dlq.Entry{
		ID: id.NewDLQID(), JobID: id.NewJobID(), JobName: "form", Queue: "form",
		Payload: []byte(`{}`), Error: "boom", FailedAt: now, CreatedAt: now,
	}
	for _, e := range []*dlq.Entry{claimed, marked} {
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatalf("PushDLQ: %v", err)
		}
	}

	if err := s.ClaimReplay(ctx, claimed.ID, id.NewJobID()); err != nil {
		t.Fatalf("ClaimReplay: %v", err)
	}
	if err := s.ReplayDLQ(ctx, marked.ID); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}

	for _, e := range []*dlq.Entry{claimed, marked} {
		rows := queryStrings(t, drv,
			`SELECT replayed_at, failed_at FROM dispatch_dlq WHERE id = ?`, e.ID.String())
		if len(rows) != 1 {
			t.Fatalf("%s: %d rows, want 1", e.ID, len(rows))
		}

		for i, col := range []string{"replayed_at", "failed_at"} {
			if _, err := time.Parse(driverTimeLayout, rows[0][i]); err != nil {
				t.Errorf("%s %s = %q, not in the driver's time form: %v", e.ID, col, rows[0][i], err)
			}
		}
	}
}

// TestRunVersionAndParentReadBackFromEveryPath pins the two run columns
// workflow_run_version_parent added. Before it, a run created on version 3
// read back as version 0, which the runner resolves to the latest
// registered version, so a resume or a replay-from-step ran different
// code from the run's own. RunWorkflowSuite covers the parent link
// through GetRun and ListChildRuns; this adds Version, the paged list, and
// the UpdateRun write MigrateRun relies on.
func TestRunVersionAndParentReadBackFromEveryPath(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	parent := &workflow.Run{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewRunID(),
		Name:      "lineage-parent",
		State:     workflow.RunStateRunning,
		StartedAt: now,
	}
	parentID := parent.ID
	child := &workflow.Run{
		Entity:      dispatch.NewEntity(),
		ID:          id.NewRunID(),
		Name:        "lineage-child",
		State:       workflow.RunStateRunning,
		StartedAt:   now,
		Version:     3,
		ParentRunID: &parentID,
	}
	for _, r := range []*workflow.Run{parent, child} {
		if err := s.CreateRun(ctx, r); err != nil {
			t.Fatalf("CreateRun(%s): %v", r.Name, err)
		}
	}

	check := func(label string, r *workflow.Run, version int) {
		t.Helper()

		if r.Version != version {
			t.Errorf("%s: Version = %d, want %d", label, r.Version, version)
		}
		if r.ParentRunID == nil || *r.ParentRunID != parentID {
			t.Errorf("%s: ParentRunID = %v, want %s", label, r.ParentRunID, parentID)
		}
	}

	got, err := s.GetRun(ctx, child.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	check("GetRun", got, 3)

	children, err := s.ListChildRuns(ctx, parentID)
	if err != nil {
		t.Fatalf("ListChildRuns: %v", err)
	}
	if len(children) != 1 {
		t.Fatalf("ListChildRuns = %d runs, want 1", len(children))
	}
	check("ListChildRuns", children[0], 3)

	page, err := s.ListRunsPage(ctx, workflow.ListRunsPageOpts{NamePrefix: "lineage-child"})
	if err != nil {
		t.Fatalf("ListRunsPage: %v", err)
	}
	if len(page.Runs) != 1 {
		t.Fatalf("ListRunsPage = %d runs, want 1", len(page.Runs))
	}
	check("ListRunsPage", page.Runs[0], 3)

	// MigrateRun moves a run to another version through UpdateRun.
	got.Version = 4
	if err = s.UpdateRun(ctx, got); err != nil {
		t.Fatalf("UpdateRun: %v", err)
	}
	got, err = s.GetRun(ctx, child.ID)
	if err != nil {
		t.Fatalf("GetRun after UpdateRun: %v", err)
	}
	check("GetRun after UpdateRun", got, 4)
}
