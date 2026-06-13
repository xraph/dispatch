package cron

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	cronlib "github.com/robfig/cron/v3"

	log "github.com/xraph/go-utils/log"

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

// defaultStoreCallTimeout caps how long a single store roundtrip
// (GetLeader, RenewLeadership, ListCrons, AcquireCronLock, …) is
// allowed to run before the scheduler abandons it. Without this, a
// stalled driver session could pin a connection from the shared
// pool until either side reset it — and with a 1s tick interval the
// scheduler would queue up faster than the pool could free
// connections.
const defaultStoreCallTimeout = 5 * time.Second

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

// WithCronRefreshInterval sets how often the leader re-lists cron entries
// from the store. Between refreshes ticks run against an in-memory cache;
// InvalidateCronCache forces an early refresh. Default 30s.
func WithCronRefreshInterval(d time.Duration) SchedulerOption {
	return func(s *Scheduler) { s.cronRefreshInterval = d }
}

// WithSchedulerStoreCallTimeout caps a single store roundtrip. Pass a
// positive duration to override defaultStoreCallTimeout, zero to keep
// the default, or negative to disable the timeout (test-only).
func WithSchedulerStoreCallTimeout(d time.Duration) SchedulerOption {
	return func(s *Scheduler) { s.storeCallTimeout = d }
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
	logger       log.Logger

	tickInterval        time.Duration
	lockTTL             time.Duration
	leaderTTL           time.Duration
	cronRefreshInterval time.Duration
	storeCallTimeout    time.Duration

	// parsedSchedules caches parsed cron expressions.
	parsedMu sync.RWMutex
	parsed   map[string]cronlib.Schedule

	// Leadership cache. tick consults this instead of issuing a GetLeader
	// read every tickInterval; leaderLoop maintains it on each
	// renew/acquire cycle. leaderUntil mirrors the store lease so a
	// renewal outage demotes us locally when the lease would expire.
	leaderMu    sync.Mutex
	isLeader    bool
	leaderUntil time.Time

	// Cron entry cache, owned by the tickLoop goroutine. cronDirty is the
	// cross-goroutine invalidation signal (set by InvalidateCronCache and
	// on leadership changes).
	cronDirty    atomic.Bool
	cronCache    []*Entry
	lastCronList time.Time

	stopCh     chan struct{}
	cancelCtx  context.Context
	cancelFunc context.CancelFunc
	stopOnce   sync.Once
	wg         sync.WaitGroup
}

// NewScheduler creates a Scheduler.
func NewScheduler(
	cronStore Store,
	clusterStore cluster.Store,
	enqueue EnqueueFunc,
	emitter Emitter,
	workerID id.WorkerID,
	logger log.Logger,
	opts ...SchedulerOption,
) *Scheduler {
	if logger == nil {
		logger = log.NewNoopLogger()
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
		// 60s lease, renewed at 30s: one durable write per 30s per
		// cluster instead of one per 7.5s. Cron failover after a leader
		// crash takes up to a minute, which is acceptable for schedules
		// with minute-level granularity.
		leaderTTL:           60 * time.Second,
		cronRefreshInterval: 30 * time.Second,
		parsed:              make(map[string]cronlib.Schedule),
		stopCh:              make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start launches the leader election and cron tick goroutines.
func (s *Scheduler) Start(_ context.Context) error {
	s.cancelCtx, s.cancelFunc = context.WithCancel(context.Background())
	s.wg.Add(2)
	go s.leaderLoop()
	go s.tickLoop()
	s.logger.Info("cron scheduler started",
		log.String("worker_id", s.workerID.String()),
		log.Duration("tick_interval", s.tickInterval),
	)
	return nil
}

// Stop signals the scheduler to stop and waits for goroutines to finish.
// It is safe to call Stop multiple times; only the first call closes the
// stop channel and waits for goroutines.
func (s *Scheduler) Stop(_ context.Context) error {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		if s.cancelFunc != nil {
			s.cancelFunc()
		}
		s.wg.Wait()
		s.logger.Info("cron scheduler stopped")
	})
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

// callCtx wraps s.cancelCtx with the configured per-call timeout so a
// stalled driver session can't hold a pool connection beyond the
// scheduler's own tick budget. Mirrors worker.Pool.callCtx.
func (s *Scheduler) callCtx() (context.Context, context.CancelFunc) {
	if s.storeCallTimeout < 0 {
		return s.cancelCtx, func() {}
	}
	d := s.storeCallTimeout
	if d == 0 {
		d = defaultStoreCallTimeout
	}
	return context.WithTimeout(s.cancelCtx, d)
}

// amLeader reports whether this instance holds a still-valid leadership
// lease according to the local cache. tick relies on this instead of a
// GetLeader read per tick; the lease expiry bounds how long a stale local
// view can outlive lost leadership.
func (s *Scheduler) amLeader() bool {
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()
	return s.isLeader && time.Now().UTC().Before(s.leaderUntil)
}

func (s *Scheduler) setLeader(leading bool, until time.Time) {
	s.leaderMu.Lock()
	wasLeader := s.isLeader
	s.isLeader = leading
	s.leaderUntil = until
	s.leaderMu.Unlock()

	if leading && !wasLeader {
		// Fresh leadership: re-list entries before the first tick fires.
		s.cronDirty.Store(true)
		s.logger.Info("acquired cron leadership", log.String("worker_id", s.workerID.String()))
	}
}

// tryLeadership maintains the lease with at most one write per cycle.
// Leaders renew; followers only read, and attempt the acquisition write
// solely when the store reports no live leader. The old renew-then-acquire
// sequence issued two write attempts per cycle from every idle follower.
func (s *Scheduler) tryLeadership() {
	s.leaderMu.Lock()
	wasLeader := s.isLeader
	s.leaderMu.Unlock()

	if wasLeader {
		now := time.Now().UTC()
		renewCtx, renewCancel := s.callCtx()
		renewed, err := s.clusterStore.RenewLeadership(renewCtx, s.workerID, s.leaderTTL)
		renewCancel()
		if err != nil {
			// Keep the cached lease: its expiry demotes us if renewal
			// keeps failing, without flapping on one transient error.
			s.logger.Warn("leadership renew error", log.String("error", err.Error()))
			return
		}
		if renewed {
			s.setLeader(true, now.Add(s.leaderTTL))
			return
		}
		s.setLeader(false, time.Time{})
		// Lease lost; fall through to the follower path.
	}

	// Follower: a read decides whether an acquisition write is worthwhile.
	getCtx, getCancel := s.callCtx()
	leader, err := s.clusterStore.GetLeader(getCtx)
	getCancel()
	if err != nil {
		s.logger.Warn("get leader error", log.String("error", err.Error()))
		return
	}

	now := time.Now().UTC()
	switch {
	case leader != nil && leader.ID.String() == s.workerID.String():
		// The store already holds our lease (e.g. process restart):
		// re-adopt it via renewal.
		renewCtx, renewCancel := s.callCtx()
		renewed, renewErr := s.clusterStore.RenewLeadership(renewCtx, s.workerID, s.leaderTTL)
		renewCancel()
		if renewErr != nil {
			s.logger.Warn("leadership renew error", log.String("error", renewErr.Error()))
			return
		}
		if renewed {
			s.setLeader(true, now.Add(s.leaderTTL))
		}
	case leader != nil:
		return // Live leader elsewhere; no write attempts.
	default:
		acqCtx, acqCancel := s.callCtx()
		acquired, acqErr := s.clusterStore.AcquireLeadership(acqCtx, s.workerID, s.leaderTTL)
		acqCancel()
		if acqErr != nil {
			s.logger.Warn("leadership acquire error", log.String("error", acqErr.Error()))
			return
		}
		if acquired {
			s.setLeader(true, now.Add(s.leaderTTL))
		}
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

// InvalidateCronCache forces the scheduler to re-list cron entries from the
// store on the next tick. Call it after registering, updating, or removing
// entries so changes take effect before the next scheduled refresh.
func (s *Scheduler) InvalidateCronCache() {
	s.cronDirty.Store(true)
}

// cronEntries returns the cached cron entries, re-listing from the store
// when the cache is stale (cronRefreshInterval elapsed) or was invalidated.
// Only the tickLoop goroutine touches the cache fields.
func (s *Scheduler) cronEntries() []*Entry {
	fresh := time.Since(s.lastCronList) < s.cronRefreshInterval
	if fresh && !s.cronDirty.Load() {
		return s.cronCache
	}

	listCtx, listCancel := s.callCtx()
	entries, err := s.cronStore.ListCrons(listCtx)
	listCancel()
	if err != nil {
		s.logger.Error("list crons error", log.String("error", err.Error()))
		return s.cronCache // Stale entries beat skipping the tick.
	}

	s.cronCache = entries
	s.lastCronList = time.Now()
	s.cronDirty.Store(false)
	return entries
}

func (s *Scheduler) tick() {
	// Consult the in-memory lease instead of issuing a GetLeader read on
	// every tick; leaderLoop keeps it current at renewInterval cadence.
	if !s.amLeader() {
		return
	}

	now := time.Now().UTC()
	for _, entry := range s.cronEntries() {
		if !entry.Enabled {
			continue
		}
		if entry.NextRunAt == nil || entry.NextRunAt.After(now) {
			continue
		}
		s.fireEntry(s.cancelCtx, entry, now)
	}
}

func (s *Scheduler) fireEntry(ctx context.Context, entry *Entry, now time.Time) {
	// Acquire per-entry lock under a bounded subcontext.
	acqCtx, acqCancel := s.callCtx()
	acquired, err := s.cronStore.AcquireCronLock(acqCtx, entry.ID, s.workerID, s.lockTTL)
	acqCancel()
	if err != nil {
		s.logger.Error("acquire cron lock error",
			log.String("cron_id", entry.ID.String()),
			log.String("error", err.Error()),
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
	enqCtx, enqCancel := s.callCtx()
	jobID, enqErr := s.enqueue(enqCtx, entry.JobName, entry.Payload, enqOpts...)
	enqCancel()
	if enqErr != nil {
		s.logger.Error("cron enqueue error",
			log.String("cron_name", entry.Name),
			log.String("job_name", entry.JobName),
			log.String("error", enqErr.Error()),
		)
		relCtx, relCancel := s.callCtx()
		relErr := s.cronStore.ReleaseCronLock(relCtx, entry.ID, s.workerID)
		relCancel()
		if relErr != nil {
			s.logger.Error("release cron lock error",
				log.String("cron_id", entry.ID.String()),
				log.String("error", relErr.Error()),
			)
		}
		return
	}

	// Update LastRunAt.
	lrCtx, lrCancel := s.callCtx()
	updateErr := s.cronStore.UpdateCronLastRun(lrCtx, entry.ID, now)
	lrCancel()
	if updateErr != nil {
		s.logger.Error("update cron last run error",
			log.String("cron_id", entry.ID.String()),
			log.String("error", updateErr.Error()),
		)
	}

	// Compute and persist NextRunAt.
	sched, parseErr := s.getOrParseSchedule(entry.Schedule)
	if parseErr != nil {
		s.logger.Error("parse cron schedule error",
			log.String("cron_name", entry.Name),
			log.String("schedule", entry.Schedule),
			log.String("error", parseErr.Error()),
		)
	} else {
		next := sched.Next(now)
		entry.NextRunAt = &next
		nrCtx, nrCancel := s.callCtx()
		updateErr := s.cronStore.UpdateCronEntry(nrCtx, entry)
		nrCancel()
		if updateErr != nil {
			s.logger.Error("update cron next run error",
				log.String("cron_id", entry.ID.String()),
				log.String("error", updateErr.Error()),
			)
		}
	}

	// Release lock.
	relCtx, relCancel := s.callCtx()
	relErr := s.cronStore.ReleaseCronLock(relCtx, entry.ID, s.workerID)
	relCancel()
	if relErr != nil {
		s.logger.Error("release cron lock error",
			log.String("cron_id", entry.ID.String()),
			log.String("error", relErr.Error()),
		)
	}

	// Emit hook.
	if s.emitter != nil {
		s.emitter.EmitCronFired(ctx, entry.Name, jobID)
	}

	s.logger.Info("cron fired",
		log.String("cron_name", entry.Name),
		log.String("job_name", entry.JobName),
		log.String("job_id", jobID.String()),
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
