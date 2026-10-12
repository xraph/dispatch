package api_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

func TestReplayDLQ(t *testing.T) {
	f := newFixture(t)
	failed, entry := failedWithEntry(t, f)

	rec := f.do(t, http.MethodPost, "/v1/dlq/"+entry.ID.String()+"/replay", "")
	wantStatus(t, rec, http.StatusCreated)

	j := decode[job.Job](t, rec)
	if j.ID == failed.ID || j.Name != failed.Name || j.State != job.StatePending {
		t.Errorf("replayed job = %s %q %s; want a new pending %q", j.ID, j.Name, j.State, failed.Name)
	}

	// Replaying the same entry again is refused: it already made a job.
	wantStatus(t, f.do(t, http.MethodPost, "/v1/dlq/"+entry.ID.String()+"/replay", ""), http.StatusConflict)
	wantStatus(t, f.do(t, http.MethodPost, "/v1/dlq/"+id.NewDLQID().String()+"/replay", ""), http.StatusNotFound)
	wantStatus(t, f.do(t, http.MethodPost, "/v1/dlq/nope/replay", ""), http.StatusBadRequest)
}

// bigFailedEntry gives f a dead letter entry for a job sized for 8 CPUs,
// as if the fleet had been bigger when it was enqueued.
func bigFailedEntry(t *testing.T, f *fixture) *dlq.Entry {
	t.Helper()
	ctx := context.Background()

	j := jobInState(t, f, job.StateFailed)
	j.Resources = resource.CPUs(8)
	if err := f.s.UpdateJob(ctx, j); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if err := f.eng.DLQService().Push(ctx, j, context.DeadlineExceeded); err != nil {
		t.Fatalf("Push: %v", err)
	}
	entry, err := f.s.GetDLQByJobID(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetDLQByJobID: %v", err)
	}

	return entry
}

func TestReplayDLQ_Unschedulable(t *testing.T) {
	f := newFixture(t, engine.WithWorkerCapacity(resource.CPUs(2)))
	entry := bigFailedEntry(t, f)

	wantStatus(t, f.do(t, http.MethodPost, "/v1/dlq/"+entry.ID.String()+"/replay", ""), http.StatusConflict)
}

func TestReplayAllDLQ(t *testing.T) {
	f := newFixture(t)
	failedWithEntry(t, f)
	failedWithEntry(t, f)
	failedWithEntry(t, f, job.WithQueue("mail"))

	rec := f.do(t, http.MethodPost, "/v1/dlq/replay-all?queue=mail", "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[api.ReplayAllDLQResponse](t, rec); got.Replayed != 1 || got.Errors != 0 {
		t.Errorf("queue=mail: %+v, want 1 replayed", got)
	}

	// No parameters and no body, as the route has always been called.
	rec = f.do(t, http.MethodPost, "/v1/dlq/replay-all", "")
	wantStatus(t, rec, http.StatusOK)
	got := decode[api.ReplayAllDLQResponse](t, rec)
	if got.Replayed != 2 || got.Conflicts != 0 || got.Errors != 0 {
		t.Errorf("replay-all: %+v, want 2 replayed and nothing else", got)
	}
	if got.ErrorMessages == nil || len(got.ErrorMessages) != 0 {
		t.Errorf("error_messages = %#v, want an empty list", got.ErrorMessages)
	}

	rec = f.do(t, http.MethodPost, "/v1/dlq/replay-all", "")
	wantStatus(t, rec, http.StatusOK)
	if again := decode[api.ReplayAllDLQResponse](t, rec); again.Replayed != 0 {
		t.Errorf("second replay-all replayed %d, want 0", again.Replayed)
	}
}

func TestReplayAllDLQ_LimitAndFailures(t *testing.T) {
	f := newFixture(t, engine.WithWorkerCapacity(resource.CPUs(2)))
	bigFailedEntry(t, f)
	// IDs sort by creation time; two milliseconds apart keeps "newest"
	// unambiguous.
	time.Sleep(2 * time.Millisecond)
	failedWithEntry(t, f)

	// The limit takes the newest entry, which is the schedulable one.
	rec := f.do(t, http.MethodPost, "/v1/dlq/replay-all?limit=1", "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[api.ReplayAllDLQResponse](t, rec); got.Replayed != 1 || got.Errors != 0 {
		t.Errorf("limit=1: %+v, want 1 replayed", got)
	}

	rec = f.do(t, http.MethodPost, "/v1/dlq/replay-all", "")
	wantStatus(t, rec, http.StatusOK)
	got := decode[api.ReplayAllDLQResponse](t, rec)
	if got.Replayed != 0 || got.Errors != 1 || len(got.ErrorMessages) != 1 {
		t.Errorf("unschedulable entry: %+v, want 1 error with its message", got)
	}

	for _, q := range []string{"limit=-1", "limit=1001", "limit=lots"} {
		wantStatus(t, f.do(t, http.MethodPost, "/v1/dlq/replay-all?"+q, ""), http.StatusBadRequest)
	}
}

// pushAged stores a dead letter entry that failed age ago.
func pushAged(t *testing.T, f *fixture, age time.Duration) {
	t.Helper()

	at := time.Now().UTC().Add(-age)
	e := &dlq.Entry{
		ID:        id.NewDLQID(),
		JobID:     id.NewJobID(),
		JobName:   "api-job",
		Queue:     "default",
		Payload:   []byte(`{}`),
		Error:     "boom",
		FailedAt:  at,
		CreatedAt: at,
	}
	if err := f.s.PushDLQ(context.Background(), e); err != nil {
		t.Fatalf("PushDLQ: %v", err)
	}
}

func dlqCount(t *testing.T, f *fixture) int64 {
	t.Helper()

	n, err := f.s.CountDLQ(context.Background())
	if err != nil {
		t.Fatalf("CountDLQ: %v", err)
	}

	return n
}

func TestPurgeDLQ_DefaultsToThirtyDays(t *testing.T) {
	f := newFixture(t)
	pushAged(t, f, 31*24*time.Hour)
	pushAged(t, f, 24*time.Hour)

	rec := f.do(t, http.MethodPost, "/v1/dlq/purge", "")
	wantStatus(t, rec, http.StatusOK)

	got := decode[api.PurgeDLQResponse](t, rec)
	if got.Purged != 1 || got.Matched != 1 || got.DryRun {
		t.Errorf("purge = %+v, want 1 purged and matched", got)
	}
	if age := time.Since(got.Before); age < 30*24*time.Hour-time.Minute || age > 30*24*time.Hour+time.Minute {
		t.Errorf("before = %v, want about 30 days ago", got.Before)
	}
	if n := dlqCount(t, f); n != 1 {
		t.Errorf("entries left = %d, want 1", n)
	}
}

func TestPurgeDLQ_Cutoffs(t *testing.T) {
	f := newFixture(t)
	pushAged(t, f, 48*time.Hour)
	pushAged(t, f, 24*time.Hour)
	pushAged(t, f, time.Hour)

	rec := f.do(t, http.MethodPost, "/v1/dlq/purge?dry_run=true&older_than=12h", "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[api.PurgeDLQResponse](t, rec); got.Matched != 2 || got.Purged != 0 || !got.DryRun {
		t.Errorf("dry run = %+v, want 2 matched, 0 purged", got)
	}
	if n := dlqCount(t, f); n != 3 {
		t.Fatalf("a dry run deleted entries: %d left, want 3", n)
	}

	before := time.Now().UTC().Add(-36 * time.Hour).Format(time.RFC3339)
	rec = f.do(t, http.MethodPost, "/v1/dlq/purge?"+url.Values{"before": {before}}.Encode(), "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[api.PurgeDLQResponse](t, rec); got.Purged != 1 {
		t.Errorf("before 36h ago: purged %d, want 1", got.Purged)
	}

	rec = f.do(t, http.MethodPost, "/v1/dlq/purge?older_than=12h", "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[api.PurgeDLQResponse](t, rec); got.Purged != 1 {
		t.Errorf("older than 12h: purged %d, want 1", got.Purged)
	}
	if n := dlqCount(t, f); n != 1 {
		t.Errorf("entries left = %d, want 1", n)
	}
}

func TestPurgeDLQ_BadCutoffs(t *testing.T) {
	f := newFixture(t)
	pushAged(t, f, 31*24*time.Hour)

	for _, q := range []string{
		"before=2026-01-01T00:00:00Z&older_than=1h",
		"before=yesterday",
		"before=0001-01-01T00:00:00Z",
		"older_than=soon",
		"older_than=0s",
		"older_than=-1h",
		"dry_run=maybe",
	} {
		wantStatus(t, f.do(t, http.MethodPost, "/v1/dlq/purge?"+q, ""), http.StatusBadRequest)
	}
	if n := dlqCount(t, f); n != 1 {
		t.Errorf("a refused purge deleted entries: %d left, want 1", n)
	}
}

func TestDeleteDLQ(t *testing.T) {
	f := newFixture(t)
	_, entry := failedWithEntry(t, f)
	path := "/v1/dlq/" + entry.ID.String()

	wantStatus(t, f.do(t, http.MethodDelete, path, ""), http.StatusNoContent)
	wantStatus(t, f.do(t, http.MethodGet, path, ""), http.StatusNotFound)
	wantStatus(t, f.do(t, http.MethodDelete, path, ""), http.StatusNotFound)
	wantStatus(t, f.do(t, http.MethodDelete, "/v1/dlq/nope", ""), http.StatusBadRequest)
}
