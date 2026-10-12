package api_test

import (
	"net/http"
	"testing"

	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// TestGetRoutesWriteOneBody reads one job, entry, cron and run. The
// handlers used to write the body themselves and also return it, so the
// router wrote it a second time and the response held two JSON documents.
func TestGetRoutesWriteOneBody(t *testing.T) {
	rf := newReplayFixture(t)
	j, entry := failedWithEntry(t, rf.fixture)
	e := addCron(t, rf.fixture, "0 3 * * *", true)
	run := rf.failedRun(t)

	rec := rf.do(t, http.MethodGet, "/v1/jobs/"+j.ID.String(), "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[job.Job](t, rec); got.ID != j.ID {
		t.Errorf("job = %s, want %s", got.ID, j.ID)
	}

	rec = rf.do(t, http.MethodGet, "/v1/dlq/"+entry.ID.String(), "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[dlq.Entry](t, rec); got.ID != entry.ID {
		t.Errorf("entry = %s, want %s", got.ID, entry.ID)
	}

	rec = rf.do(t, http.MethodGet, "/v1/crons/"+e.ID.String(), "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[cron.Entry](t, rec); got.ID != e.ID {
		t.Errorf("cron = %s, want %s", got.ID, e.ID)
	}

	rec = rf.do(t, http.MethodGet, "/v1/workflows/runs/"+run.ID.String(), "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[workflow.Run](t, rec); got.ID != run.ID {
		t.Errorf("run = %s, want %s", got.ID, run.ID)
	}
}
