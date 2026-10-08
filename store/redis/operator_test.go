//go:build integration

package redis_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	redisstore "github.com/xraph/dispatch/store/redis"
	"github.com/xraph/dispatch/store/storetest"
	"github.com/xraph/dispatch/workflow"
)

// The three operator suites share one container each, like the list and
// lease suites: every case isolates itself by its own queue, entry or
// run, so one store per case on the same Redis is what they expect.

func TestDLQReplayConformance(t *testing.T) {
	connStr := startRedis(t)

	storetest.RunDLQReplaySuite(t, func(t *testing.T) storetest.DLQReplayStore {
		t.Helper()

		return openRedisStore(t, connStr)
	})
}

func TestCronConformance(t *testing.T) {
	connStr := startRedis(t)

	storetest.RunCronSuite(t, func(t *testing.T) storetest.CronStore {
		t.Helper()

		return openRedisStore(t, connStr)
	})
}

func TestWorkflowConformance(t *testing.T) {
	connStr := startRedis(t)

	storetest.RunWorkflowSuite(t, func(t *testing.T) storetest.WorkflowStore {
		t.Helper()

		return openRedisStore(t, connStr)
	})
}

// operatorCron registers an enabled cron entry with a unique name.
func operatorCron(t *testing.T, s *redisstore.Store) *cron.Entry {
	t.Helper()

	next := time.Now().UTC().Add(time.Minute)
	cronID := id.NewCronID()
	e := &cron.Entry{
		Entity:    dispatch.NewEntity(),
		ID:        cronID,
		Name:      "redis-operator-" + cronID.String(),
		Schedule:  "* * * * *",
		JobName:   "redis-operator-job",
		Queue:     "redis-operator",
		Enabled:   true,
		NextRunAt: &next,
	}
	if err := s.RegisterCron(context.Background(), e); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}

	return e
}

// TestCronDisableSurvivesConcurrentFires is the race the conformance
// suite's DisableSurvivesAFire case cannot reach, because there the two
// writes take turns. Here a scheduler loop keeps acquiring the lock,
// recording a fire and the next run, and releasing, while an operator
// disables the entry in the middle of it. Every one of those scheduler
// writes rewrites the whole JSON blob, so if any of them is a plain read
// then SET, one that read the entry before the disable puts enabled back.
func TestCronDisableSurvivesConcurrentFires(t *testing.T) {
	s := openRedisStore(t, startRedis(t))
	ctx := context.Background()

	const rounds = 20
	for round := range rounds {
		entry := operatorCron(t, s)
		worker := id.NewWorkerID()

		var (
			stop  atomic.Bool
			fires atomic.Int64
			wg    sync.WaitGroup
			errMu sync.Mutex
			errs  []error
		)
		record := func(err error) {
			errMu.Lock()
			defer errMu.Unlock()
			errs = append(errs, err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, err := s.AcquireCronLock(ctx, entry.ID, worker, time.Minute); err != nil {
					record(err)
					return
				}
				at := time.Now().UTC()
				if err := s.UpdateCronLastRun(ctx, entry.ID, at); err != nil {
					record(err)
					return
				}
				if err := s.UpdateCronNextRun(ctx, entry.ID, at.Add(time.Minute)); err != nil {
					record(err)
					return
				}
				if err := s.ReleaseCronLock(ctx, entry.ID, worker); err != nil {
					record(err)
					return
				}
				fires.Add(1)
			}
		}()

		// Let the loop get going, disable in the middle of it, and let it
		// run on for a while after.
		for fires.Load() < 5 {
			runtime.Gosched()
		}
		if err := s.SetCronEnabled(ctx, entry.ID, false, nil); err != nil {
			t.Fatalf("round %d: SetCronEnabled(false): %v", round, err)
		}
		afterDisable := fires.Load()
		for fires.Load() < afterDisable+5 {
			runtime.Gosched()
		}
		stop.Store(true)
		wg.Wait()

		if len(errs) > 0 {
			t.Fatalf("round %d: scheduler loop: %v", round, errs)
		}

		got, err := s.GetCron(ctx, entry.ID)
		if err != nil {
			t.Fatalf("round %d: GetCron: %v", round, err)
		}
		if got.Enabled {
			t.Fatalf("round %d: the cron came back enabled after %d fires raced its disable", round, fires.Load())
		}
		if got.LastRunAt == nil {
			t.Fatalf("round %d: LastRunAt = nil, want the scheduler's fires recorded", round)
		}
	}
}

// TestAcquireCronLockExactlyOneHolder races workers for the lock on a
// fresh entry. The lock lives inside the cron JSON blob, so a lock taken
// as a read then a SET lets every worker that read the unlocked entry
// believe it holds the lock, and each of them fires the cron.
func TestAcquireCronLockExactlyOneHolder(t *testing.T) {
	s := openRedisStore(t, startRedis(t))
	ctx := context.Background()

	const workers = 16
	for round := range 10 {
		entry := operatorCron(t, s)

		var (
			wg    sync.WaitGroup
			ready atomic.Int64
			held  atomic.Int64
			errMu sync.Mutex
			errs  []error
		)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()

				ready.Add(1)
				for ready.Load() < workers {
					runtime.Gosched()
				}

				ok, err := s.AcquireCronLock(ctx, entry.ID, id.NewWorkerID(), time.Minute)
				if err != nil {
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
					return
				}
				if ok {
					held.Add(1)
				}
			}()
		}
		wg.Wait()

		if len(errs) > 0 {
			t.Fatalf("round %d: AcquireCronLock: %v", round, errs)
		}
		if n := held.Load(); n != 1 {
			t.Fatalf("round %d: %d of %d workers acquired the lock, want exactly 1", round, n, workers)
		}
	}
}

// TestClaimReplayKeepsLargeDurations guards the reason the claim builds
// its blob in Go. cjson turns an int64 past 2^53 into scientific
// notation on encode, which encoding/json then refuses to read, so a
// claim written by a Lua script that re-encodes the entry would make a
// dead letter with a 200 day timeout unreadable.
func TestClaimReplayKeepsLargeDurations(t *testing.T) {
	s := openRedisStore(t, startRedis(t))
	ctx := context.Background()

	const bigDuration = 200 * 24 * time.Hour
	now := time.Now().UTC()
	e := &dlq.Entry{
		ID:         id.NewDLQID(),
		JobID:      id.NewJobID(),
		JobName:    "large-duration",
		Queue:      "redis-operator",
		Payload:    []byte(`{}`),
		Error:      "boom",
		FailedAt:   now,
		CreatedAt:  now,
		Timeout:    bigDuration,
		LeaseTTL:   bigDuration,
		InputBytes: 1 << 60,
	}
	if err := s.PushDLQ(ctx, e); err != nil {
		t.Fatalf("PushDLQ: %v", err)
	}

	newJob := id.NewJobID()
	if err := s.ClaimReplay(ctx, e.ID, newJob); err != nil {
		t.Fatalf("ClaimReplay: %v", err)
	}

	got, err := s.GetDLQ(ctx, e.ID)
	if err != nil {
		t.Fatalf("GetDLQ after claim: %v", err)
	}
	if got.Timeout != bigDuration || got.LeaseTTL != bigDuration || got.InputBytes != 1<<60 {
		t.Fatalf("after claim: timeout %v, lease ttl %v, input bytes %d; want %v, %v, %d",
			got.Timeout, got.LeaseTTL, got.InputBytes, bigDuration, bigDuration, int64(1<<60))
	}
	if got.ReplayedJobID == nil || *got.ReplayedJobID != newJob {
		t.Fatalf("ReplayedJobID = %v, want %s", got.ReplayedJobID, newJob)
	}
}

// A Redis written by the release before this one has dead letters but no
// per-job index. GetDLQByJobID must find them anyway, and must build the
// index as it goes so the next lookup does not scan again.
func TestGetDLQByJobID_backfillsIndexForPreexistingRows(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)

	jobID := id.NewJobID()
	now := time.Now().UTC()
	older := &dlq.Entry{ID: id.NewDLQID(), JobID: jobID, JobName: "legacy", Queue: "legacy", FailedAt: now, CreatedAt: now}
	newer := &dlq.Entry{ID: id.NewDLQID(), JobID: jobID, JobName: "legacy", Queue: "legacy", FailedAt: now, CreatedAt: now}
	for _, e := range []*dlq.Entry{older, newer} {
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatalf("PushDLQ: %v", err)
		}
	}

	// What the previous release left behind: no per-job sets at all.
	for _, key := range []string{"dispatch:dlq_by_job:" + jobID.String(), "dispatch:dlq_job_indexed"} {
		if err := kvStore.Delete(ctx, key); err != nil {
			t.Fatalf("delete %s: %v", key, err)
		}
	}

	got, err := s.GetDLQByJobID(ctx, jobID)
	if err != nil {
		t.Fatalf("GetDLQByJobID on a pre-index Redis: %v", err)
	}
	if got.ID != newer.ID {
		t.Fatalf("GetDLQByJobID = %s, want the newer entry %s", got.ID, newer.ID)
	}

	members, err := kvStore.SMembers(ctx, "dispatch:dlq_by_job:"+jobID.String())
	if err != nil {
		t.Fatalf("SMembers per-job index: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("per-job index after the lookup holds %v, want both entries", members)
	}
}

// Redis used to drop a run's version along with its parent. The runner
// resumes a run on run.Version and replay-from-step checks it, so a
// dropped version silently moves a run onto the latest definition. The
// workflow suite reads the version back from the store on both sides of
// a reopen, so it cannot tell a dropped version from a kept one; this
// compares with what was written, after the create and after a reopen.
func TestRunVersionAndParentRoundTrip(t *testing.T) {
	s := openRedisStore(t, startRedis(t))
	ctx := context.Background()

	parentID := id.NewRunID()
	r := &workflow.Run{
		Entity:      dispatch.NewEntity(),
		ID:          id.NewRunID(),
		Name:        "versioned",
		State:       workflow.RunStateFailed,
		StartedAt:   time.Now().UTC(),
		Version:     3,
		ParentRunID: &parentID,
	}
	if err := s.CreateRun(ctx, r); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	check := func(label string) {
		t.Helper()

		got, err := s.GetRun(ctx, r.ID)
		if err != nil {
			t.Fatalf("%s: GetRun: %v", label, err)
		}
		if got.Version != 3 {
			t.Errorf("%s: Version = %d, want 3", label, got.Version)
		}
		if got.ParentRunID == nil || *got.ParentRunID != parentID {
			t.Errorf("%s: ParentRunID = %v, want %s", label, got.ParentRunID, parentID)
		}
	}

	check("after CreateRun")

	if err := s.ReopenRun(ctx, r.ID); err != nil {
		t.Fatalf("ReopenRun: %v", err)
	}
	check("after ReopenRun")

	if !errors.Is(s.ReopenRun(ctx, r.ID), dispatch.ErrInvalidState) {
		t.Fatal("a second ReopenRun on the now running run did not wrap ErrInvalidState")
	}
}
