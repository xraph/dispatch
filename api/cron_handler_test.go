package api_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// addCron stores an entry directly, so a test controls every field.
func addCron(t *testing.T, f *fixture, schedule string, enabled bool) *cron.Entry {
	t.Helper()

	next := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	e := &cron.Entry{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewCronID(),
		Name:      "nightly-" + id.NewCronID().String(),
		Schedule:  schedule,
		JobName:   "report",
		Queue:     "reports",
		Payload:   []byte(`{"kind":"sales"}`),
		NextRunAt: &next,
		Enabled:   enabled,
	}
	if err := f.s.RegisterCron(context.Background(), e); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}

	return e
}

func TestCronEnableDisable(t *testing.T) {
	f := newFixture(t)
	e := addCron(t, f, "0 3 * * *", true)
	path := "/v1/crons/" + e.ID.String()

	rec := f.do(t, http.MethodPost, path+"/disable", "")
	wantStatus(t, rec, http.StatusOK)
	if got := decode[cron.Entry](t, rec); got.ID != e.ID || got.Enabled {
		t.Errorf("disable answered %s enabled=%v, want %s disabled", got.ID, got.Enabled, e.ID)
	}

	rec = f.do(t, http.MethodPost, path+"/enable", "")
	wantStatus(t, rec, http.StatusOK)
	got := decode[cron.Entry](t, rec)
	if !got.Enabled || got.NextRunAt == nil || !got.NextRunAt.After(time.Now()) {
		t.Errorf("enable answered enabled=%v next=%v, want enabled with a future next run", got.Enabled, got.NextRunAt)
	}
}

func TestCronEnable_ScheduleThatNeverFires(t *testing.T) {
	f := newFixture(t)
	e := addCron(t, f, "0 0 30 2 *", false) // 30 February

	wantStatus(t, f.do(t, http.MethodPost, "/v1/crons/"+e.ID.String()+"/enable", ""), http.StatusConflict)
}

func TestCronDelete(t *testing.T) {
	f := newFixture(t)
	e := addCron(t, f, "0 3 * * *", true)
	path := "/v1/crons/" + e.ID.String()

	wantStatus(t, f.do(t, http.MethodDelete, path, ""), http.StatusNoContent)
	wantStatus(t, f.do(t, http.MethodGet, path, ""), http.StatusNotFound)
	wantStatus(t, f.do(t, http.MethodDelete, path, ""), http.StatusNotFound)
}

func TestCronTrigger(t *testing.T) {
	f := newFixture(t)
	e := addCron(t, f, "0 3 * * *", false)

	rec := f.do(t, http.MethodPost, "/v1/crons/"+e.ID.String()+"/trigger", "")
	wantStatus(t, rec, http.StatusCreated)

	j := decode[job.Job](t, rec)
	if j.Name != "report" || j.Queue != "reports" || string(j.Payload) != `{"kind":"sales"}` || j.State != job.StatePending {
		t.Errorf("triggered job = %q on %q payload %s state %s; want the entry's pending job", j.Name, j.Queue, j.Payload, j.State)
	}

	// Running it by hand leaves the schedule alone.
	stored, err := f.s.GetCron(context.Background(), e.ID)
	if err != nil {
		t.Fatalf("GetCron: %v", err)
	}
	if stored.NextRunAt == nil || !stored.NextRunAt.Equal(*e.NextRunAt) || stored.LastRunAt != nil {
		t.Errorf("schedule moved: next %v last %v, want next %v and no last run", stored.NextRunAt, stored.LastRunAt, e.NextRunAt)
	}
}

func TestCronOps_UnknownAndMalformed(t *testing.T) {
	f := newFixture(t)
	unknown := "/v1/crons/" + id.NewCronID().String()

	for _, op := range []string{"/enable", "/disable", "/trigger"} {
		wantStatus(t, f.do(t, http.MethodPost, unknown+op, ""), http.StatusNotFound)
		wantStatus(t, f.do(t, http.MethodPost, "/v1/crons/nope"+op, ""), http.StatusBadRequest)
	}
	wantStatus(t, f.do(t, http.MethodDelete, unknown, ""), http.StatusNotFound)
}
