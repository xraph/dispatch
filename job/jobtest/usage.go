// Package jobtest provides shared conformance suites for the optional
// job store capabilities.
package jobtest

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// RunUsageRecorderSuite exercises the job.UsageRecorder contract.
// newRecorder must return a fresh, empty recorder on every call.
func RunUsageRecorderSuite(t *testing.T, newRecorder func() job.UsageRecorder) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(*testing.T, job.UsageRecorder)
	}{
		{"RecordAndList", testRecordAndList},
		{"RecordsFailedAttempts", testRecordsFailedAttempts},
		{"PreservesPredictionAndActual", testPreservesPredictionAndActual},
		{"FilterByName", testFilterByName},
		{"FilterBySince", testFilterBySince},
		{"NewestFirst", testNewestFirst},
		{"Paging", testPaging},
		{"Purge", testPurge},
		{"PurgeRespectsLimit", testPurgeRespectsLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, newRecorder())
		})
	}
}

func newUsage(name string, at time.Time) *job.Usage {
	return &job.Usage{
		ID:         id.NewUsageID(),
		JobID:      id.NewJobID(),
		Name:       name,
		Queue:      "default",
		Status:     job.StateCompleted,
		WallTime:   time.Second,
		RecordedAt: at,
	}
}

func testRecordAndList(t *testing.T, r job.UsageRecorder) {
	ctx := context.Background()
	u := newUsage("tessellate", time.Now().UTC())

	if err := r.RecordJobUsage(ctx, u); err != nil {
		t.Fatalf("RecordJobUsage: %v", err)
	}

	got, err := r.ListJobUsage(ctx, job.UsageListOpts{})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}

	if got[0].Name != "tessellate" || got[0].JobID != u.JobID {
		t.Fatalf("round trip mismatch: %+v", got[0])
	}

	if got[0].WallTime != time.Second {
		t.Fatalf("WallTime = %v, want 1s", got[0].WallTime)
	}
}

// testRecordsFailedAttempts pins the case that matters most for sizing:
// a job that died is the strongest evidence about what it needed.
func testRecordsFailedAttempts(t *testing.T, r job.UsageRecorder) {
	ctx := context.Background()

	u := newUsage("tessellate", time.Now().UTC())
	u.Status = job.StateFailed
	u.PeakRSS = 8 << 30

	if err := r.RecordJobUsage(ctx, u); err != nil {
		t.Fatalf("RecordJobUsage: %v", err)
	}

	got, err := r.ListJobUsage(ctx, job.UsageListOpts{})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("a failed attempt was not recorded")
	}

	if got[0].Status != job.StateFailed {
		t.Fatalf("Status = %q, want failed", got[0].Status)
	}

	if got[0].PeakRSS != 8<<30 {
		t.Fatalf("PeakRSS = %d, want %d", got[0].PeakRSS, int64(8)<<30)
	}
}

// testPreservesPredictionAndActual is the whole point of the record: an
// estimate with no measurement beside it cannot be calibrated.
func testPreservesPredictionAndActual(t *testing.T, r job.UsageRecorder) {
	ctx := context.Background()

	u := newUsage("render", time.Now().UTC())
	u.InputBytes = 2 << 30
	u.Resources = resource.MemoryBytes(4 << 30)
	u.PeakRSS = 3 << 30
	u.CPUTime = 90 * time.Second
	u.DiskWritten = 512 << 20
	u.Executor = "subprocess"

	if err := r.RecordJobUsage(ctx, u); err != nil {
		t.Fatalf("RecordJobUsage: %v", err)
	}

	got, err := r.ListJobUsage(ctx, job.UsageListOpts{})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	rec := got[0]

	if rec.InputBytes != 2<<30 {
		t.Fatalf("InputBytes = %d, want %d", rec.InputBytes, int64(2)<<30)
	}

	if rec.Resources.IsZero() {
		t.Fatal("the predicted resource set was not preserved")
	}

	if rec.PeakRSS != 3<<30 || rec.CPUTime != 90*time.Second || rec.DiskWritten != 512<<20 {
		t.Fatalf("measurements not preserved: %+v", rec)
	}

	if rec.Executor != "subprocess" {
		t.Fatalf("Executor = %q, want subprocess", rec.Executor)
	}
}

func testFilterByName(t *testing.T, r job.UsageRecorder) {
	ctx := context.Background()
	now := time.Now().UTC()

	for _, name := range []string{"a", "a", "b"} {
		if err := r.RecordJobUsage(ctx, newUsage(name, now)); err != nil {
			t.Fatalf("RecordJobUsage: %v", err)
		}
	}

	got, err := r.ListJobUsage(ctx, job.UsageListOpts{Name: "a"})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d records for name a, want 2", len(got))
	}
}

func testFilterBySince(t *testing.T, r job.UsageRecorder) {
	ctx := context.Background()
	now := time.Now().UTC()

	if err := r.RecordJobUsage(ctx, newUsage("a", now.Add(-48*time.Hour))); err != nil {
		t.Fatalf("RecordJobUsage old: %v", err)
	}

	if err := r.RecordJobUsage(ctx, newUsage("a", now)); err != nil {
		t.Fatalf("RecordJobUsage new: %v", err)
	}

	got, err := r.ListJobUsage(ctx, job.UsageListOpts{Since: now.Add(-time.Hour)})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d records since cutoff, want 1", len(got))
	}
}

func testNewestFirst(t *testing.T, r job.UsageRecorder) {
	ctx := context.Background()
	now := time.Now().UTC()

	old := newUsage("a", now.Add(-time.Hour))
	recent := newUsage("a", now)

	for _, u := range []*job.Usage{old, recent} {
		if err := r.RecordJobUsage(ctx, u); err != nil {
			t.Fatalf("RecordJobUsage: %v", err)
		}
	}

	got, err := r.ListJobUsage(ctx, job.UsageListOpts{})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}

	if got[0].ID != recent.ID {
		t.Fatal("ListJobUsage did not return the most recent record first")
	}
}

func testPaging(t *testing.T, r job.UsageRecorder) {
	ctx := context.Background()
	now := time.Now().UTC()

	for i := range 5 {
		if err := r.RecordJobUsage(ctx, newUsage("a", now.Add(-time.Duration(i)*time.Minute))); err != nil {
			t.Fatalf("RecordJobUsage %d: %v", i, err)
		}
	}

	first, err := r.ListJobUsage(ctx, job.UsageListOpts{Limit: 2})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(first) != 2 {
		t.Fatalf("limit 2 returned %d", len(first))
	}

	second, err := r.ListJobUsage(ctx, job.UsageListOpts{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatalf("ListJobUsage offset: %v", err)
	}

	if len(second) != 2 {
		t.Fatalf("limit 2 offset 2 returned %d", len(second))
	}

	if first[0].ID == second[0].ID {
		t.Fatal("offset did not advance the page")
	}
}

func testPurge(t *testing.T, r job.UsageRecorder) {
	ctx := context.Background()
	now := time.Now().UTC()

	if err := r.RecordJobUsage(ctx, newUsage("a", now.Add(-48*time.Hour))); err != nil {
		t.Fatalf("RecordJobUsage old: %v", err)
	}

	if err := r.RecordJobUsage(ctx, newUsage("a", now)); err != nil {
		t.Fatalf("RecordJobUsage new: %v", err)
	}

	removed, err := r.PurgeJobUsage(ctx, now.Add(-time.Hour), 0)
	if err != nil {
		t.Fatalf("PurgeJobUsage: %v", err)
	}

	if removed != 1 {
		t.Fatalf("PurgeJobUsage removed %d, want 1", removed)
	}

	got, err := r.ListJobUsage(ctx, job.UsageListOpts{})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("%d records survived the purge, want 1", len(got))
	}
}

func testPurgeRespectsLimit(t *testing.T, r job.UsageRecorder) {
	ctx := context.Background()
	now := time.Now().UTC()

	for range 5 {
		if err := r.RecordJobUsage(ctx, newUsage("a", now.Add(-48*time.Hour))); err != nil {
			t.Fatalf("RecordJobUsage: %v", err)
		}
	}

	removed, err := r.PurgeJobUsage(ctx, now, 2)
	if err != nil {
		t.Fatalf("PurgeJobUsage: %v", err)
	}

	if removed != 2 {
		t.Fatalf("PurgeJobUsage with limit 2 removed %d", removed)
	}
}
