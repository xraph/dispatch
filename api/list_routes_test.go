package api_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// listBody checks a list route answered 200 with a JSON array, an empty
// list included (never null), and decodes it.
func listBody[T any](t *testing.T, rec *httptest.ResponseRecorder) []T {
	t.Helper()

	wantStatus(t, rec, http.StatusOK)
	if b := bytes.TrimSpace(rec.Body.Bytes()); len(b) == 0 || b[0] != '[' {
		t.Fatalf("body is not a JSON array: %s", rec.Body.String())
	}

	return decode[[]T](t, rec)
}

// idsOf lists the IDs of items, in order, as strings.
func idsOf[T any](items []T, idOf func(T) string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, idOf(item))
	}

	return out
}

// apart keeps creation times strictly ordered between fixtures; the list
// routes answer oldest first.
func apart() { time.Sleep(2 * time.Millisecond) }

func TestListJobs(t *testing.T) {
	f := newFixture(t)
	pending := jobInState(t, f, job.StatePending)
	apart()
	failed := jobInState(t, f, job.StateFailed, job.WithQueue("mail"))
	apart()
	done := jobInState(t, f, job.StateCompleted)

	jobID := func(j job.Job) string { return j.ID.String() }
	cases := []struct {
		query string
		want  []*job.Job
	}{
		{"", []*job.Job{pending, failed, done}},
		{"?state=failed", []*job.Job{failed}},
		{"?queue=mail", []*job.Job{failed}},
		{"?state=completed&queue=default", []*job.Job{done}},
		{"?state=pending&queue=mail", nil},
		{"?limit=1", []*job.Job{pending}},
		{"?limit=1&offset=1", []*job.Job{failed}},
		{"?offset=1", []*job.Job{failed, done}},
		{"?offset=9", nil},
	}
	for _, tc := range cases {
		t.Run("query "+tc.query, func(t *testing.T) {
			got := listBody[job.Job](t, f.do(t, http.MethodGet, "/v1/jobs"+tc.query, ""))
			want := idsOf(tc.want, func(j *job.Job) string { return j.ID.String() })
			if ids := idsOf(got, jobID); !slices.Equal(ids, want) {
				t.Errorf("jobs = %v, want %v", ids, want)
			}
		})
	}

	wantStatus(t, f.do(t, http.MethodGet, "/v1/jobs?state=lost", ""), http.StatusBadRequest)
}

func TestListDLQ(t *testing.T) {
	f := newFixture(t)
	_, first := failedWithEntry(t, f)
	apart()
	_, mail := failedWithEntry(t, f, job.WithQueue("mail"))
	apart()
	_, last := failedWithEntry(t, f)

	entryID := func(e dlq.Entry) string { return e.ID.String() }
	cases := []struct {
		query string
		want  []*dlq.Entry
	}{
		{"", []*dlq.Entry{first, mail, last}},
		{"?queue=mail", []*dlq.Entry{mail}},
		{"?queue=none", nil},
		{"?limit=2", []*dlq.Entry{first, mail}},
		{"?limit=1&offset=2", []*dlq.Entry{last}},
		{"?offset=9", nil},
	}
	for _, tc := range cases {
		t.Run("query "+tc.query, func(t *testing.T) {
			got := listBody[dlq.Entry](t, f.do(t, http.MethodGet, "/v1/dlq"+tc.query, ""))
			want := idsOf(tc.want, func(e *dlq.Entry) string { return e.ID.String() })
			if ids := idsOf(got, entryID); !slices.Equal(ids, want) {
				t.Errorf("entries = %v, want %v", ids, want)
			}
		})
	}
}

func TestListCrons(t *testing.T) {
	f := newFixture(t)
	a := addCron(t, f, "0 3 * * *", true)
	apart()
	b := addCron(t, f, "0 4 * * *", false)
	apart()
	c := addCron(t, f, "0 5 * * *", true)

	cronID := func(e cron.Entry) string { return e.ID.String() }
	cases := []struct {
		query string
		want  []*cron.Entry
	}{
		{"", []*cron.Entry{a, b, c}},
		{"?limit=2", []*cron.Entry{a, b}},
		{"?limit=2&offset=2", []*cron.Entry{c}},
		{"?offset=9", nil},
	}
	for _, tc := range cases {
		t.Run("query "+tc.query, func(t *testing.T) {
			got := listBody[cron.Entry](t, f.do(t, http.MethodGet, "/v1/crons"+tc.query, ""))
			want := idsOf(tc.want, func(e *cron.Entry) string { return e.ID.String() })
			if ids := idsOf(got, cronID); !slices.Equal(ids, want) {
				t.Errorf("crons = %v, want %v", ids, want)
			}
		})
	}
}

func TestListWorkflowRuns(t *testing.T) {
	rf := newReplayFixture(t)
	failed := rf.failedRun(t)
	apart()
	completed, err := engine.StartWorkflow(context.Background(), rf.eng, "api-replay", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	rf.waitState(t, completed.ID, workflow.RunStateCompleted)

	runID := func(r workflow.Run) string { return r.ID.String() }
	cases := []struct {
		query string
		want  []*workflow.Run
	}{
		{"", []*workflow.Run{failed, completed}},
		{"?state=failed", []*workflow.Run{failed}},
		{"?state=completed", []*workflow.Run{completed}},
		{"?state=running", nil},
		{"?limit=1", []*workflow.Run{failed}},
		{"?limit=1&offset=1", []*workflow.Run{completed}},
		{"?offset=9", nil},
	}
	for _, tc := range cases {
		t.Run("query "+tc.query, func(t *testing.T) {
			got := listBody[workflow.Run](t, rf.do(t, http.MethodGet, "/v1/workflows/runs"+tc.query, ""))
			want := idsOf(tc.want, func(r *workflow.Run) string { return r.ID.String() })
			if ids := idsOf(got, runID); !slices.Equal(ids, want) {
				t.Errorf("runs = %v, want %v", ids, want)
			}
		})
	}
}
