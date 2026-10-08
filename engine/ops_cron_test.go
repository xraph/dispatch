package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// cronOpsRecorder records operator actions and cron fires.
type cronOpsRecorder struct {
	mu      sync.Mutex
	actions []ext.Action
	fired   []cronOpsFire
}

type cronOpsFire struct {
	name  string
	jobID id.JobID
	at    time.Time
}

func (r *cronOpsRecorder) Name() string { return "cron-ops-recorder" }

func (r *cronOpsRecorder) OnOperatorAction(_ context.Context, a ext.Action) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, a)
	return nil
}

func (r *cronOpsRecorder) OnCronFired(_ context.Context, name string, jobID id.JobID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fired = append(r.fired, cronOpsFire{name: name, jobID: jobID, at: time.Now()})
	return nil
}

func (r *cronOpsRecorder) getActions() []ext.Action {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ext.Action(nil), r.actions...)
}

func (r *cronOpsRecorder) getFired() []cronOpsFire {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cronOpsFire(nil), r.fired...)
}

// newCronOpsEngine builds an engine on a memory store with a 20ms cron
// tick and a cron cache that never refreshes on its own, so the scheduler
// sees an operator's change only through InvalidateCronCache.
func newCronOpsEngine(t *testing.T) (*engine.Engine, *memory.Store, *cronOpsRecorder) {
	t.Helper()
	s := memory.New()
	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithCronTickInterval(20*time.Millisecond),
		dispatch.WithCronRefreshInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}
	rec := &cronOpsRecorder{}
	eng, err := engine.Build(d, engine.WithExtension(rec))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}
	engine.Register(eng, job.NewDefinition("report", func(_ context.Context, _ struct{}) error {
		return nil
	}))
	return eng, s, rec
}

func startCronOpsEngine(t *testing.T, eng *engine.Engine) {
	t.Helper()
	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
}

// addCron stores an entry directly, so a test controls every field.
func addCron(t *testing.T, s *memory.Store, schedule string, enabled bool, nextRunAt time.Time) *cron.Entry {
	t.Helper()
	next := nextRunAt
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
	if err := s.RegisterCron(context.Background(), e); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}
	return e
}

func getCron(t *testing.T, s *memory.Store, cronID id.CronID) cron.Entry {
	t.Helper()
	e, err := s.GetCron(context.Background(), cronID)
	if err != nil {
		t.Fatalf("GetCron: %v", err)
	}
	return *e
}

func wantOneAction(t *testing.T, rec *cronOpsRecorder, kind ext.ActionKind, cronID id.CronID) ext.Action {
	t.Helper()
	actions := rec.getActions()
	if len(actions) != 1 {
		t.Fatalf("operator actions = %d (%v), want 1", len(actions), actions)
	}
	a := actions[0]
	if a.Kind != kind {
		t.Errorf("action kind = %q, want %q", a.Kind, kind)
	}
	if a.CronID.String() != cronID.String() {
		t.Errorf("action cron = %s, want %s", a.CronID, cronID)
	}
	if a.Actor != "user_7" {
		t.Errorf("action actor = %q, want user_7", a.Actor)
	}
	if a.At.IsZero() {
		t.Error("action At is zero")
	}
	return a
}

func TestEngine_DisableCron(t *testing.T) {
	eng, s, rec := newCronOpsEngine(t)
	next := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	e := addCron(t, s, "@every 1h", true, next)
	ctx := ext.WithActor(context.Background(), "user_7")

	got, err := eng.DisableCron(ctx, e.ID)
	if err != nil {
		t.Fatalf("DisableCron: %v", err)
	}
	if got.Enabled {
		t.Error("returned entry is still enabled")
	}
	stored := getCron(t, s, e.ID)
	if stored.Enabled {
		t.Error("stored entry is still enabled")
	}
	if stored.NextRunAt == nil || !stored.NextRunAt.Equal(next) {
		t.Errorf("NextRunAt = %v, want it left at %v", stored.NextRunAt, next)
	}
	wantOneAction(t, rec, ext.ActionCronDisabled, e.ID)
}

func TestEngine_EnableCronComputesNextFromNow(t *testing.T) {
	eng, s, rec := newCronOpsEngine(t)
	e := addCron(t, s, "@every 1h", false, time.Now().UTC().Add(-3*time.Hour))
	ctx := ext.WithActor(context.Background(), "user_7")

	before := time.Now().UTC()
	got, err := eng.EnableCron(ctx, e.ID)
	if err != nil {
		t.Fatalf("EnableCron: %v", err)
	}
	if !got.Enabled {
		t.Error("returned entry is not enabled")
	}
	stored := getCron(t, s, e.ID)
	if !stored.Enabled {
		t.Error("stored entry is not enabled")
	}
	if stored.NextRunAt == nil || stored.NextRunAt.Before(before.Add(59*time.Minute)) || stored.NextRunAt.After(before.Add(61*time.Minute)) {
		t.Errorf("NextRunAt = %v, want about an hour after %v", stored.NextRunAt, before)
	}
	wantOneAction(t, rec, ext.ActionCronEnabled, e.ID)
}

func TestEngine_EnableCronRefusesAScheduleThatNeverFires(t *testing.T) {
	eng, s, rec := newCronOpsEngine(t)
	e := addCron(t, s, "0 0 30 2 *", false, time.Now().UTC().Add(-time.Hour)) // 30 February

	_, err := eng.EnableCron(context.Background(), e.ID)
	if !errors.Is(err, dispatch.ErrInvalidState) {
		t.Fatalf("EnableCron error = %v, want ErrInvalidState", err)
	}
	if getCron(t, s, e.ID).Enabled {
		t.Error("entry was enabled anyway")
	}
	if n := len(rec.getActions()); n != 0 {
		t.Errorf("operator actions = %d, want 0 for a refused enable", n)
	}
}

// TestEngine_EnableCronDoesNotCatchUp enables an entry whose old next run
// is long past. The scheduler must not fire it straight away, and must
// still pick it up at the new next run without waiting out the hour-long
// cache refresh, which only EnableCron's cache invalidation can make
// happen.
func TestEngine_EnableCronDoesNotCatchUp(t *testing.T) {
	eng, s, rec := newCronOpsEngine(t)
	// @every rounds its next fire down to a whole second, so with 2s the
	// first fire after enabling lands between one and two seconds out.
	e := addCron(t, s, "@every 2s", false, time.Now().UTC().Add(-time.Hour))
	startCronOpsEngine(t, eng)
	time.Sleep(100 * time.Millisecond) // leadership and the first cron list

	enabledAt := time.Now()
	if _, err := eng.EnableCron(context.Background(), e.ID); err != nil {
		t.Fatalf("EnableCron: %v", err)
	}

	time.Sleep(800 * time.Millisecond)
	if fired := rec.getFired(); len(fired) != 0 {
		t.Fatalf("entry fired %v after enable, want no catch-up fire", fired[0].at.Sub(enabledAt))
	}

	deadline := time.After(4 * time.Second)
	for len(rec.getFired()) == 0 {
		select {
		case <-deadline:
			t.Fatal("entry never fired after enable; the scheduler cache was not invalidated")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestEngine_DisableCronStopsARunningSchedule is the end-to-end
// regression: once DisableCron returns, the scheduler fires the entry no
// more, and nothing it writes turns the entry back on.
func TestEngine_DisableCronStopsARunningSchedule(t *testing.T) {
	eng, s, rec := newCronOpsEngine(t)
	e := addCron(t, s, "@every 1s", true, time.Now().UTC().Add(-time.Second))
	startCronOpsEngine(t, eng)

	deadline := time.After(3 * time.Second)
	for len(rec.getFired()) == 0 {
		select {
		case <-deadline:
			t.Fatal("entry never fired")
		case <-time.After(20 * time.Millisecond):
		}
	}

	if _, err := eng.DisableCron(context.Background(), e.ID); err != nil {
		t.Fatalf("DisableCron: %v", err)
	}
	atDisable := len(rec.getFired())

	time.Sleep(1500 * time.Millisecond)

	if got := len(rec.getFired()); got != atDisable {
		t.Errorf("fires after DisableCron = %d, want 0", got-atDisable)
	}
	if getCron(t, s, e.ID).Enabled {
		t.Error("entry is enabled again")
	}
}

func TestEngine_DeleteCron(t *testing.T) {
	eng, s, rec := newCronOpsEngine(t)
	e := addCron(t, s, "@every 1h", true, time.Now().UTC().Add(time.Hour))
	ctx := ext.WithActor(context.Background(), "user_7")

	if err := eng.DeleteCron(ctx, e.ID); err != nil {
		t.Fatalf("DeleteCron: %v", err)
	}
	if _, err := s.GetCron(context.Background(), e.ID); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Fatalf("GetCron after delete: %v, want ErrCronNotFound", err)
	}
	wantOneAction(t, rec, ext.ActionCronDeleted, e.ID)

	if err := eng.DeleteCron(ctx, e.ID); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Errorf("second DeleteCron error = %v, want ErrCronNotFound", err)
	}
	if n := len(rec.getActions()); n != 1 {
		t.Errorf("operator actions = %d after a failed delete, want still 1", n)
	}
}

func TestEngine_TriggerCron(t *testing.T) {
	eng, s, rec := newCronOpsEngine(t)
	next := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	e := addCron(t, s, "@every 1h", true, next)
	lastRun := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if err := s.UpdateCronLastRun(context.Background(), e.ID, lastRun); err != nil {
		t.Fatalf("UpdateCronLastRun: %v", err)
	}
	ctx := ext.WithActor(context.Background(), "user_7")

	j, err := eng.TriggerCron(ctx, e.ID)
	if err != nil {
		t.Fatalf("TriggerCron: %v", err)
	}
	if j.Name != "report" || j.Queue != "reports" || string(j.Payload) != `{"kind":"sales"}` {
		t.Errorf("job = %s on %s with %s, want report on reports with the entry's payload", j.Name, j.Queue, j.Payload)
	}
	if j.State != job.StatePending {
		t.Errorf("job state = %s, want pending", j.State)
	}

	page, err := s.ListJobs(context.Background(), job.ListJobsOpts{})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].ID.String() != j.ID.String() {
		t.Fatalf("jobs in store = %d, want exactly the triggered one", len(page.Jobs))
	}

	stored := getCron(t, s, e.ID)
	if stored.NextRunAt == nil || !stored.NextRunAt.Equal(next) {
		t.Errorf("NextRunAt = %v, want it left at %v", stored.NextRunAt, next)
	}
	if stored.LastRunAt == nil || !stored.LastRunAt.Equal(lastRun) {
		t.Errorf("LastRunAt = %v, want it left at %v", stored.LastRunAt, lastRun)
	}

	fired := rec.getFired()
	if len(fired) != 1 || fired[0].name != e.Name || fired[0].jobID.String() != j.ID.String() {
		t.Errorf("cron fired hooks = %v, want one for %s with job %s", fired, e.Name, j.ID)
	}
	a := wantOneAction(t, rec, ext.ActionCronTriggered, e.ID)
	if a.NewJobID.String() != j.ID.String() {
		t.Errorf("action NewJobID = %s, want %s", a.NewJobID, j.ID)
	}
}

func TestEngine_CronOpsUnknownEntry(t *testing.T) {
	eng, _, rec := newCronOpsEngine(t)
	ctx := context.Background()
	unknown := id.NewCronID()

	if _, err := eng.EnableCron(ctx, unknown); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Errorf("EnableCron error = %v, want ErrCronNotFound", err)
	}
	if _, err := eng.DisableCron(ctx, unknown); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Errorf("DisableCron error = %v, want ErrCronNotFound", err)
	}
	if err := eng.DeleteCron(ctx, unknown); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Errorf("DeleteCron error = %v, want ErrCronNotFound", err)
	}
	if _, err := eng.TriggerCron(ctx, unknown); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Errorf("TriggerCron error = %v, want ErrCronNotFound", err)
	}
	if n := len(rec.getActions()); n != 0 {
		t.Errorf("operator actions = %d, want 0", n)
	}
}
