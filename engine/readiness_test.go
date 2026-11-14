package engine_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

type readinessStore struct {
	*memory.Store
	failPing atomic.Bool
	failPoll atomic.Bool
}

func (s *readinessStore) Ping(context.Context) error {
	if s.failPing.Load() {
		return errors.New("database unavailable")
	}
	return nil
}
func (s *readinessStore) DequeueJobs(ctx context.Context, opts job.DequeueOpts) ([]*job.Job, error) {
	if s.failPoll.Load() {
		return nil, errors.New("poll unavailable")
	}
	return s.Store.DequeueJobs(ctx, opts)
}
func TestReadinessTracksStartupStoragePollingAndDrain(t *testing.T) {
	st := &readinessStore{Store: memory.New()}
	d, err := dispatch.New(dispatch.WithStore(st), dispatch.WithPollInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if eng.Ready(t.Context()) == nil {
		t.Fatal("ready before start")
	}
	if err = eng.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })
	waitFor(t, 3*time.Second, func() bool { return eng.Ready(t.Context()) == nil })
	st.failPing.Store(true)
	if eng.Ready(t.Context()) == nil {
		t.Fatal("ready with unreachable database")
	}
	st.failPing.Store(false)
	st.failPoll.Store(true)
	waitFor(t, 3*time.Second, func() bool { return eng.Ready(t.Context()) != nil })
	st.failPoll.Store(false)
	waitFor(t, 3*time.Second, func() bool { return eng.Ready(t.Context()) == nil })
	if err = eng.StopWorkers(t.Context()); err != nil {
		t.Fatal(err)
	}
	if eng.Ready(t.Context()) == nil {
		t.Fatal("ready while draining")
	}
}
