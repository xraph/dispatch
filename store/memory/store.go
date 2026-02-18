package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Ensure Store implements store.Store at compile time.
// We can't import store here (import cycle), so we verify each subsystem.
var (
	_ job.Store      = (*Store)(nil)
	_ workflow.Store = (*Store)(nil)
	_ cron.Store     = (*Store)(nil)
	_ dlq.Store      = (*Store)(nil)
	_ event.Store    = (*Store)(nil)
	_ cluster.Store  = (*Store)(nil)
)

// Store is a fully in-memory implementation of store.Store.
// Safe for concurrent access. Intended for unit testing and development.
type Store struct {
	mu sync.RWMutex

	jobs        map[string]*job.Job
	runs        map[string]*workflow.Run
	checkpoints map[string]*workflow.Checkpoint // key: "runID:stepName"
	crons       map[string]*cron.Entry
	dlqs        map[string]*dlq.Entry
	events      map[string]*event.Event
	workers     map[string]*cluster.Worker

	// leader tracks the current cluster leader worker ID string.
	leader      string
	leaderUntil time.Time
}

// New returns a new empty Store.
func New() *Store {
	return &Store{
		jobs:        make(map[string]*job.Job),
		runs:        make(map[string]*workflow.Run),
		checkpoints: make(map[string]*workflow.Checkpoint),
		crons:       make(map[string]*cron.Entry),
		dlqs:        make(map[string]*dlq.Entry),
		events:      make(map[string]*event.Event),
		workers:     make(map[string]*cluster.Worker),
	}
}

// ──────────────────────────────────────────────────
// Lifecycle — Migrate / Ping / Close
// ──────────────────────────────────────────────────

// Migrate is a no-op for the memory store.
func (m *Store) Migrate(_ context.Context) error { return nil }

// Ping always succeeds for the memory store.
func (m *Store) Ping(_ context.Context) error { return nil }

// Close is a no-op for the memory store.
func (m *Store) Close() error { return nil }

// ──────────────────────────────────────────────────
// Job Store
// ──────────────────────────────────────────────────

// EnqueueJob persists a new job in pending state.
func (m *Store) EnqueueJob(_ context.Context, j *job.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := j.ID.String()
	if _, exists := m.jobs[key]; exists {
		return dispatch.ErrJobAlreadyExists
	}
	cp := *j
	m.jobs[key] = &cp
	return nil
}

// DequeueJobs atomically claims up to limit pending jobs from the given
// queues, sets them to running, and returns them.
func (m *Store) DequeueJobs(_ context.Context, queues []string, limit int) ([]*job.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	queueSet := make(map[string]struct{}, len(queues))
	for _, q := range queues {
		queueSet[q] = struct{}{}
	}

	now := time.Now().UTC()

	// Collect candidates.
	candidates := make([]*job.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if j.State != job.StatePending && j.State != job.StateRetrying {
			continue
		}
		if !j.RunAt.IsZero() && j.RunAt.After(now) {
			continue
		}
		if len(queueSet) > 0 {
			if _, ok := queueSet[j.Queue]; !ok {
				continue
			}
		}
		candidates = append(candidates, j)
	}

	// Sort: priority DESC, RunAt ASC.
	sort.Slice(candidates, func(i, k int) bool {
		if candidates[i].Priority != candidates[k].Priority {
			return candidates[i].Priority > candidates[k].Priority
		}
		return candidates[i].RunAt.Before(candidates[k].RunAt)
	})

	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}

	result := make([]*job.Job, len(candidates))
	for i, j := range candidates {
		j.State = job.StateRunning
		n := now
		j.StartedAt = &n
		// Return a copy so callers can mutate without racing with the store.
		cp := *j
		result[i] = &cp
	}

	return result, nil
}

// GetJob retrieves a job by ID.
func (m *Store) GetJob(_ context.Context, jobID id.JobID) (*job.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	j, ok := m.jobs[jobID.String()]
	if !ok {
		return nil, dispatch.ErrJobNotFound
	}
	cp := *j
	return &cp, nil
}

// UpdateJob persists changes to an existing job.
func (m *Store) UpdateJob(_ context.Context, j *job.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := j.ID.String()
	if _, ok := m.jobs[key]; !ok {
		return dispatch.ErrJobNotFound
	}
	cp := *j
	cp.UpdatedAt = time.Now().UTC()
	m.jobs[key] = &cp
	return nil
}

// DeleteJob removes a job by ID.
func (m *Store) DeleteJob(_ context.Context, jobID id.JobID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := jobID.String()
	if _, ok := m.jobs[key]; !ok {
		return dispatch.ErrJobNotFound
	}
	delete(m.jobs, key)
	return nil
}

// ListJobsByState returns jobs matching the given state.
func (m *Store) ListJobsByState(_ context.Context, state job.State, opts job.ListOpts) ([]*job.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*job.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if j.State != state {
			continue
		}
		if opts.Queue != "" && j.Queue != opts.Queue {
			continue
		}
		cp := *j
		result = append(result, &cp)
	}

	// Sort by CreatedAt for deterministic output.
	sort.Slice(result, func(i, k int) bool {
		return result[i].CreatedAt.Before(result[k].CreatedAt)
	})

	// Apply offset / limit.
	if opts.Offset > 0 {
		if opts.Offset >= len(result) {
			return nil, nil
		}
		result = result[opts.Offset:]
	}
	if opts.Limit > 0 && len(result) > opts.Limit {
		result = result[:opts.Limit]
	}

	return result, nil
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (m *Store) HeartbeatJob(_ context.Context, jobID id.JobID, _ id.WorkerID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[jobID.String()]
	if !ok {
		return dispatch.ErrJobNotFound
	}
	now := time.Now().UTC()
	j.HeartbeatAt = &now
	return nil
}

// ReapStaleJobs returns running jobs whose last heartbeat is older than
// the given threshold.
func (m *Store) ReapStaleJobs(_ context.Context, threshold time.Duration) ([]*job.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cutoff := time.Now().UTC().Add(-threshold)
	var stale []*job.Job
	for _, j := range m.jobs {
		if j.State != job.StateRunning {
			continue
		}
		if j.HeartbeatAt != nil && j.HeartbeatAt.Before(cutoff) {
			cp := *j
			stale = append(stale, &cp)
		}
	}
	return stale, nil
}

// CountJobs returns the number of jobs matching the given options.
func (m *Store) CountJobs(_ context.Context, opts job.CountOpts) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int64
	for _, j := range m.jobs {
		if opts.Queue != "" && j.Queue != opts.Queue {
			continue
		}
		if opts.State != "" && j.State != opts.State {
			continue
		}
		count++
	}
	return count, nil
}

// ──────────────────────────────────────────────────
// Workflow Store
// ──────────────────────────────────────────────────

// CreateRun persists a new workflow run.
func (m *Store) CreateRun(_ context.Context, run *workflow.Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := run.ID.String()
	if _, exists := m.runs[key]; exists {
		return dispatch.ErrJobAlreadyExists // reuse for "already exists"
	}
	m.runs[key] = run
	return nil
}

// GetRun retrieves a workflow run by ID.
func (m *Store) GetRun(_ context.Context, runID id.RunID) (*workflow.Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	r, ok := m.runs[runID.String()]
	if !ok {
		return nil, dispatch.ErrRunNotFound
	}
	return r, nil
}

// UpdateRun persists changes to an existing workflow run.
func (m *Store) UpdateRun(_ context.Context, run *workflow.Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := run.ID.String()
	if _, ok := m.runs[key]; !ok {
		return dispatch.ErrRunNotFound
	}
	run.UpdatedAt = time.Now().UTC()
	m.runs[key] = run
	return nil
}

// ListRuns returns workflow runs matching the given options.
func (m *Store) ListRuns(_ context.Context, opts workflow.ListOpts) ([]*workflow.Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*workflow.Run, 0, len(m.runs))
	for _, r := range m.runs {
		if opts.State != "" && r.State != opts.State {
			continue
		}
		result = append(result, r)
	}

	sort.Slice(result, func(i, k int) bool {
		return result[i].CreatedAt.Before(result[k].CreatedAt)
	})

	if opts.Offset > 0 {
		if opts.Offset >= len(result) {
			return nil, nil
		}
		result = result[opts.Offset:]
	}
	if opts.Limit > 0 && len(result) > opts.Limit {
		result = result[:opts.Limit]
	}

	return result, nil
}

// checkpointKey builds a composite map key for a checkpoint.
func checkpointKey(runID id.RunID, stepName string) string {
	return runID.String() + ":" + stepName
}

// SaveCheckpoint persists checkpoint data for a workflow step.
func (m *Store) SaveCheckpoint(_ context.Context, runID id.RunID, stepName string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := checkpointKey(runID, stepName)
	m.checkpoints[key] = &workflow.Checkpoint{
		ID:        id.NewCheckpointID(),
		RunID:     runID,
		StepName:  stepName,
		Data:      data,
		CreatedAt: time.Now().UTC(),
	}
	return nil
}

// GetCheckpoint retrieves checkpoint data for a specific workflow step.
func (m *Store) GetCheckpoint(_ context.Context, runID id.RunID, stepName string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cp, ok := m.checkpoints[checkpointKey(runID, stepName)]
	if !ok {
		return nil, nil // no checkpoint is not an error
	}
	return cp.Data, nil
}

// ListCheckpoints returns all checkpoints for a workflow run.
func (m *Store) ListCheckpoints(_ context.Context, runID id.RunID) ([]*workflow.Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	prefix := runID.String() + ":"
	var result []*workflow.Checkpoint
	for k, cp := range m.checkpoints {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			result = append(result, cp)
		}
	}

	sort.Slice(result, func(i, k int) bool {
		return result[i].CreatedAt.Before(result[k].CreatedAt)
	})

	return result, nil
}

// ──────────────────────────────────────────────────
// Cron Store
// ──────────────────────────────────────────────────

// RegisterCron persists a new cron entry. Returns an error if the name
// already exists.
func (m *Store) RegisterCron(_ context.Context, entry *cron.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for duplicate name.
	for _, e := range m.crons {
		if e.Name == entry.Name {
			return dispatch.ErrDuplicateCron
		}
	}

	m.crons[entry.ID.String()] = entry
	return nil
}

// GetCron retrieves a cron entry by ID.
func (m *Store) GetCron(_ context.Context, entryID id.CronID) (*cron.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, ok := m.crons[entryID.String()]
	if !ok {
		return nil, dispatch.ErrCronNotFound
	}
	return e, nil
}

// ListCrons returns all cron entries.
func (m *Store) ListCrons(_ context.Context) ([]*cron.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*cron.Entry, 0, len(m.crons))
	for _, e := range m.crons {
		result = append(result, e)
	}

	sort.Slice(result, func(i, k int) bool {
		return result[i].CreatedAt.Before(result[k].CreatedAt)
	})

	return result, nil
}

// AcquireCronLock attempts to acquire a distributed lock for a cron entry.
func (m *Store) AcquireCronLock(_ context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.crons[entryID.String()]
	if !ok {
		return false, dispatch.ErrCronNotFound
	}

	now := time.Now().UTC()

	// If already locked by someone else and lock hasn't expired, fail.
	if e.LockedBy != "" && e.LockedUntil != nil && e.LockedUntil.After(now) {
		if e.LockedBy != workerID.String() {
			return false, nil
		}
	}

	e.LockedBy = workerID.String()
	until := now.Add(ttl)
	e.LockedUntil = &until
	return true, nil
}

// ReleaseCronLock releases the distributed lock for a cron entry.
func (m *Store) ReleaseCronLock(_ context.Context, entryID id.CronID, workerID id.WorkerID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.crons[entryID.String()]
	if !ok {
		return dispatch.ErrCronNotFound
	}

	if e.LockedBy != workerID.String() {
		return nil // not holding the lock; no-op
	}

	e.LockedBy = ""
	e.LockedUntil = nil
	return nil
}

// UpdateCronLastRun records when a cron entry last fired.
func (m *Store) UpdateCronLastRun(_ context.Context, entryID id.CronID, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.crons[entryID.String()]
	if !ok {
		return dispatch.ErrCronNotFound
	}
	e.LastRunAt = &at
	e.UpdatedAt = time.Now().UTC()
	return nil
}

// UpdateCronEntry updates a cron entry (Enabled, NextRunAt, etc.).
func (m *Store) UpdateCronEntry(_ context.Context, entry *cron.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := entry.ID.String()
	if _, ok := m.crons[key]; !ok {
		return dispatch.ErrCronNotFound
	}
	entry.UpdatedAt = time.Now().UTC()
	m.crons[key] = entry
	return nil
}

// DeleteCron removes a cron entry by ID.
func (m *Store) DeleteCron(_ context.Context, entryID id.CronID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := entryID.String()
	if _, ok := m.crons[key]; !ok {
		return dispatch.ErrCronNotFound
	}
	delete(m.crons, key)
	return nil
}

// ──────────────────────────────────────────────────
// DLQ Store
// ──────────────────────────────────────────────────

// PushDLQ adds a failed job entry to the dead letter queue.
func (m *Store) PushDLQ(_ context.Context, entry *dlq.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dlqs[entry.ID.String()] = entry
	return nil
}

// ListDLQ returns DLQ entries matching the given options.
func (m *Store) ListDLQ(_ context.Context, opts dlq.ListOpts) ([]*dlq.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*dlq.Entry, 0, len(m.dlqs))
	for _, e := range m.dlqs {
		if opts.Queue != "" && e.Queue != opts.Queue {
			continue
		}
		result = append(result, e)
	}

	sort.Slice(result, func(i, k int) bool {
		return result[i].FailedAt.Before(result[k].FailedAt)
	})

	if opts.Offset > 0 {
		if opts.Offset >= len(result) {
			return nil, nil
		}
		result = result[opts.Offset:]
	}
	if opts.Limit > 0 && len(result) > opts.Limit {
		result = result[:opts.Limit]
	}

	return result, nil
}

// GetDLQ retrieves a DLQ entry by ID.
func (m *Store) GetDLQ(_ context.Context, entryID id.DLQID) (*dlq.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, ok := m.dlqs[entryID.String()]
	if !ok {
		return nil, dispatch.ErrDLQNotFound
	}
	return e, nil
}

// ReplayDLQ marks a DLQ entry as replayed.
func (m *Store) ReplayDLQ(_ context.Context, entryID id.DLQID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.dlqs[entryID.String()]
	if !ok {
		return dispatch.ErrDLQNotFound
	}
	now := time.Now().UTC()
	e.ReplayedAt = &now
	return nil
}

// PurgeDLQ removes DLQ entries with FailedAt before the given time.
func (m *Store) PurgeDLQ(_ context.Context, before time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var count int64
	for key, e := range m.dlqs {
		if e.FailedAt.Before(before) {
			delete(m.dlqs, key)
			count++
		}
	}
	return count, nil
}

// CountDLQ returns the total number of entries in the dead letter queue.
func (m *Store) CountDLQ(_ context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return int64(len(m.dlqs)), nil
}

// ──────────────────────────────────────────────────
// Event Store
// ──────────────────────────────────────────────────

// PublishEvent persists a new event.
func (m *Store) PublishEvent(_ context.Context, evt *event.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.events[evt.ID.String()] = evt
	return nil
}

// SubscribeEvent waits for an unacked event matching the given name.
// Poll-based: loops with 10ms sleep until an event is available or timeout.
func (m *Store) SubscribeEvent(ctx context.Context, name string, timeout time.Duration) (*event.Event, error) {
	deadline := time.Now().Add(timeout)

	for {
		// Check context cancellation.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if time.Now().After(deadline) {
			return nil, nil
		}

		m.mu.RLock()
		for _, evt := range m.events {
			if evt.Name == name && !evt.Acked {
				m.mu.RUnlock()
				return evt, nil
			}
		}
		m.mu.RUnlock()

		// Brief sleep to avoid busy-waiting.
		time.Sleep(10 * time.Millisecond)
	}
}

// AckEvent acknowledges an event, marking it as consumed.
func (m *Store) AckEvent(_ context.Context, eventID id.EventID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	evt, ok := m.events[eventID.String()]
	if !ok {
		return dispatch.ErrEventNotFound
	}
	evt.Acked = true
	return nil
}

// ──────────────────────────────────────────────────
// Cluster Store
// ──────────────────────────────────────────────────

// RegisterWorker adds a new worker to the cluster registry.
func (m *Store) RegisterWorker(_ context.Context, w *cluster.Worker) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers[w.ID.String()] = w
	return nil
}

// DeregisterWorker removes a worker from the cluster registry.
func (m *Store) DeregisterWorker(_ context.Context, workerID id.WorkerID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := workerID.String()
	if _, ok := m.workers[key]; !ok {
		return dispatch.ErrWorkerNotFound
	}
	delete(m.workers, key)
	return nil
}

// HeartbeatWorker updates the last-seen timestamp for a worker.
func (m *Store) HeartbeatWorker(_ context.Context, workerID id.WorkerID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	w, ok := m.workers[workerID.String()]
	if !ok {
		return dispatch.ErrWorkerNotFound
	}
	w.LastSeen = time.Now().UTC()
	return nil
}

// ListWorkers returns all registered workers.
func (m *Store) ListWorkers(_ context.Context) ([]*cluster.Worker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*cluster.Worker, 0, len(m.workers))
	for _, w := range m.workers {
		result = append(result, w)
	}

	sort.Slice(result, func(i, k int) bool {
		return result[i].CreatedAt.Before(result[k].CreatedAt)
	})

	return result, nil
}

// ReapDeadWorkers returns workers whose last-seen timestamp is older than
// the given threshold.
func (m *Store) ReapDeadWorkers(_ context.Context, threshold time.Duration) ([]*cluster.Worker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cutoff := time.Now().UTC().Add(-threshold)
	var dead []*cluster.Worker
	for _, w := range m.workers {
		if w.LastSeen.Before(cutoff) {
			dead = append(dead, w)
		}
	}
	return dead, nil
}

// AcquireLeadership attempts to become the cluster leader.
func (m *Store) AcquireLeadership(_ context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	wKey := workerID.String()

	// If there's already a leader whose TTL hasn't expired and it's not us, fail.
	if m.leader != "" && m.leaderUntil.After(now) && m.leader != wKey {
		return false, nil
	}

	// Acquire (or re-acquire) leadership.
	m.leader = wKey
	m.leaderUntil = now.Add(ttl)

	// Update worker record.
	if w, ok := m.workers[wKey]; ok {
		w.IsLeader = true
		until := m.leaderUntil
		w.LeaderUntil = &until
	}

	return true, nil
}

// RenewLeadership extends the leader's hold.
func (m *Store) RenewLeadership(_ context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wKey := workerID.String()
	if m.leader != wKey {
		return false, nil
	}

	m.leaderUntil = time.Now().UTC().Add(ttl)

	if w, ok := m.workers[wKey]; ok {
		until := m.leaderUntil
		w.LeaderUntil = &until
	}

	return true, nil
}

// GetLeader returns the current cluster leader, or nil if there is no leader.
func (m *Store) GetLeader(_ context.Context) (*cluster.Worker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.leader == "" || m.leaderUntil.Before(time.Now().UTC()) {
		return nil, nil
	}

	w, ok := m.workers[m.leader]
	if !ok {
		return nil, nil
	}
	return w, nil
}
