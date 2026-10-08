package api_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

func TestCancelJob(t *testing.T) {
	cases := []struct {
		state job.State
		want  int
	}{
		{job.StatePending, http.StatusNoContent},
		{job.StateRetrying, http.StatusNoContent},
		{job.StateRunning, http.StatusNoContent},
		{job.StateCompleted, http.StatusConflict},
		{job.StateFailed, http.StatusConflict},
		{job.StateCancelled, http.StatusConflict},
	}
	for _, tc := range cases {
		t.Run(string(tc.state), func(t *testing.T) {
			f := newFixture(t)
			j := jobInState(t, f, tc.state)

			rec := f.do(t, http.MethodPost, "/v1/jobs/"+j.ID.String()+"/cancel", "")
			wantStatus(t, rec, tc.want)

			got := storedJob(t, f, j.ID)
			switch {
			case tc.want == http.StatusNoContent && got.State != job.StateCancelled:
				t.Errorf("state = %s, want cancelled", got.State)
			case tc.want == http.StatusConflict && got.State != tc.state:
				t.Errorf("a refused cancel moved the job from %s to %s", tc.state, got.State)
			}
		})
	}
}

func TestCancelJob_UnknownAndMalformed(t *testing.T) {
	f := newFixture(t)

	wantStatus(t, f.do(t, http.MethodPost, "/v1/jobs/"+id.NewJobID().String()+"/cancel", ""), http.StatusNotFound)
	wantStatus(t, f.do(t, http.MethodPost, "/v1/jobs/nope/cancel", ""), http.StatusBadRequest)
}

func TestRetryJob(t *testing.T) {
	f := newFixture(t)
	j, entry := failedWithEntry(t, f)

	wantStatus(t, f.do(t, http.MethodPost, "/v1/jobs/"+j.ID.String()+"/retry", ""), http.StatusNoContent)

	got := storedJob(t, f, j.ID)
	if got.State != job.StatePending || got.RetryCount != 0 || got.LastError != "" {
		t.Errorf("job = state %s, retries %d, error %q; want pending, 0, empty", got.State, got.RetryCount, got.LastError)
	}

	// The retry claims the job's dead letter entry, so it cannot also be
	// replayed into a second copy of the same work.
	claimed, err := f.s.GetDLQ(context.Background(), entry.ID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if claimed.ReplayedJobID == nil || *claimed.ReplayedJobID != j.ID {
		t.Errorf("entry ReplayedJobID = %v, want %s", claimed.ReplayedJobID, j.ID)
	}
	wantStatus(t, f.do(t, http.MethodPost, "/v1/dlq/"+entry.ID.String()+"/replay", ""), http.StatusConflict)
}

func TestRetryJob_Refusals(t *testing.T) {
	f := newFixture(t)

	t.Run("not failed", func(t *testing.T) {
		j := jobInState(t, f, job.StatePending)
		wantStatus(t, f.do(t, http.MethodPost, "/v1/jobs/"+j.ID.String()+"/retry", ""), http.StatusConflict)
	})

	t.Run("entry already replayed", func(t *testing.T) {
		j, entry := failedWithEntry(t, f)
		wantStatus(t, f.do(t, http.MethodPost, "/v1/dlq/"+entry.ID.String()+"/replay", ""), http.StatusCreated)

		wantStatus(t, f.do(t, http.MethodPost, "/v1/jobs/"+j.ID.String()+"/retry", ""), http.StatusConflict)
		if got := storedJob(t, f, j.ID); got.State != job.StateFailed {
			t.Errorf("a refused retry moved the job to %s", got.State)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		wantStatus(t, f.do(t, http.MethodPost, "/v1/jobs/"+id.NewJobID().String()+"/retry", ""), http.StatusNotFound)
	})
}
