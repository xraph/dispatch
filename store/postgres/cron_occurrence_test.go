package postgres_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/postgres"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

func TestCronOccurrenceConcurrentWorkersAndRestart(t *testing.T) {
	dsn := startWakePostgres(t)
	a, b := openWakeStore(t, dsn), openWakeStore(t, dsn)
	due := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
	entry := &cron.Entry{Entity: dispatch.NewEntity(), ID: id.NewCronID(), Name: "once", JobName: "tick", Schedule: "@every 1m", NextRunAt: &due, Enabled: true, Payload: []byte(`{}`)}
	if err := a.RegisterCron(t.Context(), entry); err != nil {
		t.Fatal(err)
	}
	owner := id.NewWorkerID()
	if ok, err := a.AcquireCronLock(t.Context(), entry.ID, owner, time.Minute); err != nil || !ok {
		t.Fatalf("lock %v %v", ok, err)
	}
	fire := job.CronOccurrence{CronID: entry.ID, WorkerID: owner, Due: due, Next: due.Add(time.Minute), FiredAt: time.Now(), Schedule: entry.Schedule}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := a
			if i%2 == 1 {
				st = b
			}
			j := runningJob("tick", 0)
			j.State = job.StatePending
			err := st.EnqueueCronOccurrence(t.Context(), j, fire)
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, job.ErrCronOccurrenceLost) {
				t.Errorf("enqueue %v", err)
			}
		}(i)
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("%d enqueues", winners.Load())
	}
	restarted := openWakeStore(t, dsn)
	if err := restarted.EnqueueCronOccurrence(t.Context(), runningJob("duplicate", 0), fire); !errors.Is(err, job.ErrCronOccurrenceLost) {
		t.Fatalf("restart duplicate: %v", err)
	}
	got, err := a.GetCron(t.Context(), entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.NextRunAt.Equal(fire.Next) {
		t.Fatalf("next %v", got.NextRunAt)
	}
}

func TestCronOccurrenceInsertFailureRollsBackSchedule(t *testing.T) {
	st := openWakeStore(t, startWakePostgres(t))
	due := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
	e := &cron.Entry{Entity: dispatch.NewEntity(), ID: id.NewCronID(), Name: "rollback", JobName: "tick", Schedule: "@every 1m", NextRunAt: &due, Enabled: true, Payload: []byte(`{}`)}
	if err := st.RegisterCron(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	owner := id.NewWorkerID()
	if ok, err := st.AcquireCronLock(t.Context(), e.ID, owner, time.Minute); err != nil || !ok {
		t.Fatalf("lock %v %v", ok, err)
	}
	j := runningJob("existing", 0)
	if err := st.EnqueueJob(t.Context(), j); err != nil {
		t.Fatal(err)
	}
	fire := job.CronOccurrence{CronID: e.ID, WorkerID: owner, Due: due, Next: due.Add(time.Minute), FiredAt: time.Now(), Schedule: e.Schedule}
	if err := st.EnqueueCronOccurrence(t.Context(), j, fire); err == nil {
		t.Fatal("duplicate insert succeeded")
	}
	got, err := st.GetCron(t.Context(), e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.NextRunAt.Equal(due) {
		t.Fatal("failed insert advanced schedule")
	}
	j.ID = id.NewJobID()
	if err := st.EnqueueCronOccurrence(t.Context(), j, fire); err != nil {
		t.Fatal(err)
	}
}

// The scheduler must pass the occurrence through Engine.EnqueueRaw to the store.
func TestCronOccurrenceTwoEngines(t *testing.T) {
	dsn := startWakePostgres(t)
	a, b := openWakeStore(t, dsn), openWakeStore(t, dsn)
	due := time.Now().UTC().Add(-time.Second)
	entry := &cron.Entry{Entity: dispatch.NewEntity(), ID: id.NewCronID(), Name: "engines", JobName: "tick", Schedule: "@every 1m", NextRunAt: &due, Enabled: true, Payload: []byte(`{}`)}
	if err := a.RegisterCron(t.Context(), entry); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	for _, st := range []*postgres.Store{a, b} {
		d, err := dispatch.New(dispatch.WithStore(st), dispatch.WithPollInterval(10*time.Millisecond), dispatch.WithCronTickInterval(20*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		eng, err := engine.Build(d)
		if err != nil {
			t.Fatal(err)
		}
		engine.Register(eng, job.NewDefinition("tick", func(context.Context, struct{}) error { calls.Add(1); return nil }))
		if err = eng.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := eng.Stop(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls %d", calls.Load())
	}
	time.Sleep(100 * time.Millisecond)
	n, err := a.CountJobs(t.Context(), job.CountOpts{})
	if err != nil || n != 1 || calls.Load() != 1 {
		t.Fatalf("jobs %d calls %d: %v", n, calls.Load(), err)
	}
}
