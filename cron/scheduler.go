package cron

import (
	"context"
	"log/slog"
	"sync"
	"time"

	cronlib "github.com/robfig/cron/v3"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// EnqueueFunc is the callback the scheduler uses to enqueue jobs.
// This breaks the import cycle: the engine provides the implementation.
type EnqueueFunc func(ctx context.Context, name string, payload []byte, opts ...job.Option) (id.JobID, error)

// Emitter emits cron lifecycle events.
// ext.Registry satisfies this interface via EmitCronFired.
type Emitter interface {
	EmitCronFired(ctx context.Context, entryName string, jobID id.JobID)
}

// SchedulerOption configures a Scheduler.
type SchedulerOption func(*Scheduler)

// WithTickInterval sets how often the scheduler checks for due entries.
func WithTickInterval(d time.Duration) SchedulerOption {
	return func(s *Scheduler) { s.tickInterval = d }
}

// WithLockTTL sets the TTL for per-entry distributed locks.
func WithLockTTL(d time.Duration) SchedulerOption {
	return func(s *Scheduler) { s.lockTTL = d }
}

// WithLeaderTTL sets the TTL for leader election.
func WithLeaderTTL(d time.Duration) SchedulerOption {
	return func(s *Scheduler) { s.leaderTTL = d }
}

// cronParser supports standard 5-field cron and descriptors like "@every 30s".
var cronParser = cronlib.NewParser(
	cronlib.Minute | cronlib.Hour | cronlib.Dom | cronlib.Month | cronlib.Dow | cronlib.Descriptor,
)

// ParseSchedule parses a cron expression and returns the schedule.
// Exported for use by engine.RegisterCron.
func ParseSchedule(expr string) (cronlib.Schedule, error) {
	return cronParser.Parse(expr)
}

// Scheduler runs cron entries on a tick loop. Only the cluster
// leader executes cron ticks to prevent double-firing.
type Scheduler struct {
	cronStore    Store
	clusterStore cluster.Store
	enqueue      EnqueueFunc
	emitter      Emitter
	workerID     id.WorkerID
	logger       *slog.Logger

	tickInterval time.Duration
	lockTTL      time.Duration
	leaderTTL    time.Duration

	// parsedSchedules caches parsed cron expressions.
	parsedMu sync.RWMutex
	parsed   map[string]cronlib.Schedule

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewScheduler creates a Scheduler.
func NewScheduler(
	cronStore Store,
	clusterStore cluster.Store,
	enqueue EnqueueFunc,
	emitter Emitter,
	workerID id.WorkerID,
	logger *slog.Logger,
	opts ...SchedulerOption,
) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Scheduler{
		cronStore:    cronStore,
		clusterStore: clusterStore,
		enqueue:      enqueue,
		emitter:      emitter,
		workerID:     workerID,
		logger:       logger,
		tickInterval: 1 * time.Second,
		lockTTL:      30 * time.Second,
		leaderTTL:    15 * time.Second,
		parsed:       make(map[string]cronlib.Schedule),
		stopCh:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start launches the leader election and cron tick goroutines.
func (s *Scheduler) Start(_ context.Context) error {
	s.wg.Add(2)
	go s.leaderLoop()
	go s.tickLoop()
	s.logger.Info("cron scheduler started",
		slog.String("worker_id", s.workerID.String()),
		slog.Duration("tick_interval", s.tickInterval),
	)
	return nil
}

// Stop signals the scheduler to stop and waits for goroutines to finish.
func (s *Scheduler) Stop(_ context.Context) error {
	close(s.stopCh)
	s.wg.Wait()
	s.logger.Info("cron scheduler stopped")
	return nil
}

// leaderLoop continuously attempts to acquire or renew leadership.
func (s *Scheduler) leaderLoop() {
	defer s.wg.Done()

	renewInterval := s.leaderTTL / 2
	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()

	// Try once immediately at start.
	s.tryLeadership()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.tryLeadership()
		}
	}
}

func (s *Scheduler) tryLeadership() {
	ctx := context.Background()

	// Try to renew first (cheap if already leader).
	renewed, err := s.clusterStore.RenewLeadership(ctx, s.workerID, s.leaderTTL)
	if err != nil {
		s.logger.Warn("leadership renew error", slog.String("error", err.Error()))
		return
	}
	if renewed {
		return
	}

	// Not leader yet; try to acquire.
	acquired, err := s.clusterStore.AcquireLeadership(ctx, s.workerID, s.leaderTTL)
	if err != nil {
		s.logger.Warn("leadership acquire error", slog.String("error", err.Error()))
		return
	}
	if acquired {
		s.logger.Info("acquired cron leadership", slog.String("worker_id", s.workerID.String()))
	}
}

// tickLoop fires on each tick interval and processes due cron entries.
func (s *Scheduler) tickLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.tick()
		}
	}
}

func (s *Scheduler) tick() {
	ctx := context.Background()

	// Check if we are the leader.
	leader, err := s.clusterStore.GetLeader(ctx)
	if err != nil {
		s.logger.Warn("get leader error", slog.String("error", err.Error()))
		return
	}
	if leader == nil || leader.ID.String() != s.workerID.String() {
		return // Not the leader; skip.
	}

	// List all cron entries.
	entries, err := s.cronStore.ListCrons(ctx)
	if err != nil {
		s.logger.Error("list crons error", slog.String("error", err.Error()))
		return
	}

	now := time.Now().UTC()
	for _, entry := range entries {
		if !entry.Enabled {
			continue
		}
		if entry.NextRunAt == nil || entry.NextRunAt.After(now) {
			continue
		}
		s.fireEntry(ctx, entry, now)
	}
}

func (s *Scheduler) fireEntry(ctx context.Context, entry *Entry, now time.Time) {
	// Acquire per-entry lock.
	acquired, err := s.cronStore.AcquireCronLock(ctx, entry.ID, s.workerID, s.lockTTL)
	if err != nil {
		s.logger.Error("acquire cron lock error",
			slog.String("cron_id", entry.ID.String()),
			slog.String("error", err.Error()),
		)
		return
	}
	if !acquired {
		return // Another worker got it.
	}

	// Enqueue the job with optional queue override.
	var enqOpts []job.Option
	if entry.Queue != "" {
		enqOpts = append(enqOpts, job.WithQueue(entry.Queue))
	}
	jobID, enqErr := s.enqueue(ctx, entry.JobName, entry.Payload, enqOpts...)
	if enqErr != nil {
		s.logger.Error("cron enqueue error",
			slog.String("cron_name", entry.Name),
			slog.String("job_name", entry.JobName),
			slog.String("error", enqErr.Error()),
		)
		if relErr := s.cronStore.ReleaseCronLock(ctx, entry.ID, s.workerID); relErr != nil {
			s.logger.Error("release cron lock error",
				slog.String("cron_id", entry.ID.String()),
				slog.String("error", relErr.Error()),
			)
		}
		return
	}

	// Update LastRunAt.
	if updateErr := s.cronStore.UpdateCronLastRun(ctx, entry.ID, now); updateErr != nil {
		s.logger.Error("update cron last run error",
			slog.String("cron_id", entry.ID.String()),
			slog.String("error", updateErr.Error()),
		)
	}

	// Compute and persist NextRunAt.
	sched, parseErr := s.getOrParseSchedule(entry.Schedule)
	if parseErr != nil {
		s.logger.Error("parse cron schedule error",
			slog.String("cron_name", entry.Name),
			slog.String("schedule", entry.Schedule),
			slog.String("error", parseErr.Error()),
		)
	} else {
		next := sched.Next(now)
		entry.NextRunAt = &next
		if updateErr := s.cronStore.UpdateCronEntry(ctx, entry); updateErr != nil {
			s.logger.Error("update cron next run error",
				slog.String("cron_id", entry.ID.String()),
				slog.String("error", updateErr.Error()),
			)
		}
	}

	// Release lock.
	if relErr := s.cronStore.ReleaseCronLock(ctx, entry.ID, s.workerID); relErr != nil {
		s.logger.Error("release cron lock error",
			slog.String("cron_id", entry.ID.String()),
			slog.String("error", relErr.Error()),
		)
	}

	// Emit hook.
	if s.emitter != nil {
		s.emitter.EmitCronFired(ctx, entry.Name, jobID)
	}

	s.logger.Info("cron fired",
		slog.String("cron_name", entry.Name),
		slog.String("job_name", entry.JobName),
		slog.String("job_id", jobID.String()),
	)
}

// getOrParseSchedule caches parsed cron expressions.
func (s *Scheduler) getOrParseSchedule(expr string) (cronlib.Schedule, error) {
	s.parsedMu.RLock()
	sched, ok := s.parsed[expr]
	s.parsedMu.RUnlock()
	if ok {
		return sched, nil
	}

	sched, err := ParseSchedule(expr)
	if err != nil {
		return nil, err
	}

	s.parsedMu.Lock()
	s.parsed[expr] = sched
	s.parsedMu.Unlock()
	return sched, nil
}
