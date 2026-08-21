package redis_test

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	redisstore "github.com/xraph/dispatch/store/redis"
)

// This file pins the invariant that separates this backend from the other
// four: the queue sorted set is a CANDIDATE INDEX, and a job absent from it
// is never even considered, whatever its state says. Postgres, SQLite,
// Mongo and memory have no such structure — they re-derive candidacy per
// query from the row itself, so returning a job to pending is enough there
// and means nothing here.
//
// dequeue.go re-checks state and RunAt against the decoded entity, so the
// index does not decide ELIGIBILITY. It decides VISIBILITY, and it is
// maintained by only three writers: EnqueueJob and ReclaimExpiredLeases add,
// the claim removes. UpdateJob is the fourth writer every post-claim state
// transition flows through — Runner.scheduleRetry, Pool.requeueRateLimited,
// Pool.requeueUndispatched and Pool.reapStaleJobs — and it is the one this
// file exists for.
//
// The suites below assert on DequeueJobs rather than on the sorted set,
// because "the job runs again" is the property those four callers depend
// on. TestUpdateJob_MaintainsQueueIndex is the deliberate exception, and
// says why.

func requeueJob(name, queue string, runAt time.Time) *job.Job {
	return &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      queue,
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      runAt,
	}
}

// dueNow is a RunAt far enough in the past to be unambiguously due, since
// dequeue gates on run_at <= now.
func dueNow() time.Time { return time.Now().UTC().Add(-time.Second) }

// dequeueOnce claims from queue and reports whether jobID came back.
func dequeueOnce(t *testing.T, s *redisstore.Store, queue string, jobID id.JobID) bool {
	t.Helper()

	got, err := s.DequeueJobs(context.Background(), job.DequeueOpts{
		Queues: []string{queue},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("dequeue from %s: %v", queue, err)
	}

	for _, j := range got {
		if j.ID == jobID {
			return true
		}
	}

	return false
}

// TestUpdateJob_ReturnsRunnableJobToQueue covers every path that hands a
// claimed job back for another attempt. All four express themselves
// identically — set a runnable state on a job that has already been
// claimed, then UpdateJob — so all four stand or fall on this one rule.
//
// The retrying case is the load-bearing one: without it, no retry ever runs
// a second time on this backend.
func TestUpdateJob_ReturnsRunnableJobToQueue(t *testing.T) {
	s := openRedisStore(t, startRedis(t))
	ctx := context.Background()

	tests := []struct {
		name     string
		state    job.State
		queue    string
		runnable bool
	}{
		{name: "pending", state: job.StatePending, queue: "requeue-pending", runnable: true},
		{name: "retrying", state: job.StateRetrying, queue: "requeue-retrying", runnable: true},
		{name: "completed", state: job.StateCompleted, queue: "requeue-completed", runnable: false},
		{name: "failed", state: job.StateFailed, queue: "requeue-failed", runnable: false},
		{name: "cancelled", state: job.StateCancelled, queue: "requeue-cancelled", runnable: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := requeueJob(tt.name, tt.queue, dueNow())
			if err := s.EnqueueJob(ctx, j); err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			// The claim is what removes the job from the index, so it is a
			// precondition of the bug, not incidental setup.
			if !dequeueOnce(t, s, tt.queue, j.ID) {
				t.Fatalf("precondition: freshly enqueued job was not claimed")
			}

			j.State = tt.state
			j.RunAt = dueNow()
			if err := s.UpdateJob(ctx, j); err != nil {
				t.Fatalf("update to %s: %v", tt.state, err)
			}

			if got := dequeueOnce(t, s, tt.queue, j.ID); got != tt.runnable {
				t.Fatalf("after UpdateJob to %s: dequeued=%v, want %v", tt.state, got, tt.runnable)
			}
		})
	}
}

// TestUpdateJob_RequeuedRetryWaitsForBackoff guards the half of the fix
// that is easy to lose. Restoring a retry's visibility must not also make
// it due: scheduleRetry sets RunAt to now+backoff, and nothing in worker/
// re-checks RunAt after the claim, so the store is the only thing standing
// between a failing job and a hot retry loop.
//
// The two dequeues differ only in the job's RunAt, which is what makes this
// a test of the time gate rather than of the index.
func TestUpdateJob_RequeuedRetryWaitsForBackoff(t *testing.T) {
	const queue = "requeue-backoff"

	s := openRedisStore(t, startRedis(t))
	ctx := context.Background()

	j := requeueJob("backoff", queue, dueNow())
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !dequeueOnce(t, s, queue, j.ID) {
		t.Fatalf("precondition: freshly enqueued job was not claimed")
	}

	// Exactly what scheduleRetry writes: retrying, due after a backoff.
	j.State = job.StateRetrying
	j.RetryCount = 1
	j.RunAt = time.Now().UTC().Add(time.Hour)
	if err := s.UpdateJob(ctx, j); err != nil {
		t.Fatalf("update to retrying: %v", err)
	}

	if dequeueOnce(t, s, queue, j.ID) {
		t.Fatal("retry was claimed before its backoff elapsed")
	}

	// Standing in for the backoff elapsing, so the test needs no sleep.
	j.RunAt = dueNow()
	if err := s.UpdateJob(ctx, j); err != nil {
		t.Fatalf("update run_at: %v", err)
	}

	if !dequeueOnce(t, s, queue, j.ID) {
		t.Fatal("retry was not claimed after its backoff elapsed")
	}
}

// TestUpdateJob_MaintainsQueueIndex is the one white-box test here, because
// the property is itself white-box: a job that reaches a terminal state
// without ever being claimed keeps its index member, and nothing else ever
// removes it. dequeue.go's state gate means that member is harmless to
// CORRECTNESS, which is exactly why no behavioural assertion can see it —
// and why it would otherwise accumulate, one dead member per cancelled job,
// forever, inside the bounded window scanQueue reads from the head of the
// index.
//
// Asserting presence before asserting absence is deliberate: a test that
// only checked absence would pass just as happily against a mistyped key
// that never existed.
func TestUpdateJob_MaintainsQueueIndex(t *testing.T) {
	const (
		queue    = "index-hygiene"
		queueKey = "dispatch:queue:" + queue
	)

	connStr := startRedis(t)
	s := openRedisStore(t, connStr)
	ctx := context.Background()

	opt, err := goredis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })

	indexed := func() bool {
		t.Helper()

		members, mErr := rdb.ZRange(ctx, queueKey, 0, -1).Result()
		if mErr != nil {
			t.Fatalf("read queue index: %v", mErr)
		}

		return len(members) > 0
	}

	j := requeueJob("cancelled-before-claim", queue, dueNow())
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !indexed() {
		t.Fatalf("precondition: enqueued job is not in %s", queueKey)
	}

	j.State = job.StateCancelled
	if err := s.UpdateJob(ctx, j); err != nil {
		t.Fatalf("update to cancelled: %v", err)
	}

	if indexed() {
		t.Fatal("cancelled job still holds a member in the queue index")
	}
}
