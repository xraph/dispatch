package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// ── JSON model for KV storage ──

type jobEntity struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Queue       string     `json:"queue"`
	Payload     []byte     `json:"payload"`
	State       string     `json:"state"`
	Priority    int        `json:"priority"`
	MaxRetries  int        `json:"max_retries"`
	RetryCount  int        `json:"retry_count"`
	LastError   string     `json:"last_error"`
	ScopeAppID  string     `json:"scope_app_id"`
	ScopeOrgID  string     `json:"scope_org_id"`
	WorkerID    string     `json:"worker_id"`
	RunAt       time.Time  `json:"run_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	HeartbeatAt *time.Time `json:"heartbeat_at,omitempty"`
	Timeout     int64      `json:"timeout"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func toJobEntity(j *job.Job) *jobEntity {
	return &jobEntity{
		ID:          j.ID.String(),
		Name:        j.Name,
		Queue:       j.Queue,
		Payload:     j.Payload,
		State:       string(j.State),
		Priority:    j.Priority,
		MaxRetries:  j.MaxRetries,
		RetryCount:  j.RetryCount,
		LastError:   j.LastError,
		ScopeAppID:  j.ScopeAppID,
		ScopeOrgID:  j.ScopeOrgID,
		WorkerID:    j.WorkerID.String(),
		RunAt:       j.RunAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		HeartbeatAt: j.HeartbeatAt,
		Timeout:     j.Timeout.Nanoseconds(),
		CreatedAt:   j.CreatedAt,
		UpdatedAt:   j.UpdatedAt,
	}
}

func fromJobEntity(e *jobEntity) (*job.Job, error) {
	parsedID, err := id.ParseJobID(e.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse job id: %w", err)
	}

	j := &job.Job{
		Entity: dispatch.Entity{
			CreatedAt: e.CreatedAt,
			UpdatedAt: e.UpdatedAt,
		},
		ID:          parsedID,
		Name:        e.Name,
		Queue:       e.Queue,
		Payload:     e.Payload,
		State:       job.State(e.State),
		Priority:    e.Priority,
		MaxRetries:  e.MaxRetries,
		RetryCount:  e.RetryCount,
		LastError:   e.LastError,
		ScopeAppID:  e.ScopeAppID,
		ScopeOrgID:  e.ScopeOrgID,
		RunAt:       e.RunAt,
		StartedAt:   e.StartedAt,
		CompletedAt: e.CompletedAt,
		HeartbeatAt: e.HeartbeatAt,
		Timeout:     time.Duration(e.Timeout),
	}

	if e.WorkerID != "" {
		parsedWorker, wErr := id.ParseWorkerID(e.WorkerID)
		if wErr == nil {
			j.WorkerID = parsedWorker
		}
	}

	return j, nil
}

// EnqueueJob stores the job as a JSON entity and adds it to the queue's Sorted Set.
func (s *Store) EnqueueJob(ctx context.Context, j *job.Job) error {
	jID := j.ID.String()
	key := jobKey(jID)

	// Check for duplicate.
	exists, err := s.entityExists(ctx, key)
	if err != nil {
		return fmt.Errorf("dispatch/redis: enqueue check exists: %w", err)
	}
	if exists {
		return dispatch.ErrJobAlreadyExists
	}

	e := toJobEntity(j)
	if setErr := s.setEntity(ctx, key, e); setErr != nil {
		return fmt.Errorf("dispatch/redis: enqueue set entity: %w", setErr)
	}

	pipe := s.rdb.TxPipeline()
	pipe.SAdd(ctx, jobIDsKey, jID)

	// Add to queue sorted set: score = priority (negated for DESC) + time component.
	score := jobScore(j.Priority, j.RunAt)
	pipe.ZAdd(ctx, queueKey(j.Queue), goredis.Z{Score: score, Member: jID})

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: enqueue job indexes: %w", err)
	}
	return nil
}

// DequeueJobs atomically pops up to limit jobs from the given queues.
func (s *Store) DequeueJobs(ctx context.Context, queues []string, limit int) ([]*job.Job, error) {
	t := now()
	var jobs []*job.Job

	for _, q := range queues {
		if len(jobs) >= limit {
			break
		}
		remaining := limit - len(jobs)
		qk := queueKey(q)

		// Pop from sorted set (lowest score = highest priority + earliest RunAt).
		members, err := s.rdb.ZPopMin(ctx, qk, int64(remaining)).Result()
		if err != nil {
			return nil, fmt.Errorf("dispatch/redis: dequeue zpopmin: %w", err)
		}

		for _, z := range members {
			jID, ok := z.Member.(string)
			if !ok {
				continue
			}

			key := jobKey(jID)
			var e jobEntity
			if getErr := s.getEntity(ctx, key, &e); getErr != nil {
				continue // skip missing
			}

			// Update state to running.
			e.State = string(job.StateRunning)
			e.StartedAt = &t
			e.UpdatedAt = t
			if setErr := s.setEntity(ctx, key, &e); setErr != nil {
				return nil, fmt.Errorf("dispatch/redis: dequeue update: %w", setErr)
			}

			j, convErr := fromJobEntity(&e)
			if convErr != nil {
				return nil, convErr
			}
			jobs = append(jobs, j)
		}
	}
	return jobs, nil
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	var e jobEntity
	if err := s.getEntity(ctx, jobKey(jobID.String()), &e); err != nil {
		if isNotFound(err) {
			return nil, dispatch.ErrJobNotFound
		}
		return nil, fmt.Errorf("dispatch/redis: get job: %w", err)
	}
	return fromJobEntity(&e)
}

// UpdateJob persists changes to an existing job.
func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	jID := j.ID.String()
	key := jobKey(jID)

	exists, err := s.entityExists(ctx, key)
	if err != nil {
		return fmt.Errorf("dispatch/redis: update job exists: %w", err)
	}
	if !exists {
		return dispatch.ErrJobNotFound
	}

	e := toJobEntity(j)
	e.UpdatedAt = now()
	return s.setEntity(ctx, key, e)
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, jobID id.JobID) error {
	jID := jobID.String()
	key := jobKey(jID)

	// Get queue name before deleting to remove from sorted set.
	var e jobEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return dispatch.ErrJobNotFound
		}
		return fmt.Errorf("dispatch/redis: delete job get: %w", err)
	}

	// Delete entity via raw Redis DEL (KV store may not have Delete).
	pipe := s.rdb.TxPipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, jobIDsKey, jID)
	pipe.ZRem(ctx, queueKey(e.Queue), jID)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: delete job: %w", err)
	}
	return nil
}

// ListJobsByState returns jobs matching the given state.
func (s *Store) ListJobsByState(ctx context.Context, state job.State, opts job.ListOpts) ([]*job.Job, error) {
	ids, err := s.rdb.SMembers(ctx, jobIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list jobs smembers: %w", err)
	}

	jobs := make([]*job.Job, 0, len(ids))
	for _, jID := range ids {
		var e jobEntity
		if getErr := s.getEntity(ctx, jobKey(jID), &e); getErr != nil {
			continue // skip missing
		}
		if job.State(e.State) != state {
			continue
		}
		if opts.Queue != "" && e.Queue != opts.Queue {
			continue
		}
		j, convErr := fromJobEntity(&e)
		if convErr != nil {
			continue
		}
		jobs = append(jobs, j)
	}

	return applyPagination(jobs, opts.Offset, opts.Limit), nil
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (s *Store) HeartbeatJob(ctx context.Context, jobID id.JobID, _ id.WorkerID) error {
	key := jobKey(jobID.String())
	var e jobEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return dispatch.ErrJobNotFound
		}
		return fmt.Errorf("dispatch/redis: heartbeat get: %w", err)
	}

	t := now()
	e.HeartbeatAt = &t
	e.UpdatedAt = t
	return s.setEntity(ctx, key, &e)
}

// ReapStaleJobs returns running jobs whose last heartbeat is older than the threshold.
func (s *Store) ReapStaleJobs(ctx context.Context, threshold time.Duration) ([]*job.Job, error) {
	cutoff := now().Add(-threshold)

	ids, err := s.rdb.SMembers(ctx, jobIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: reap smembers: %w", err)
	}

	var stale []*job.Job
	for _, jID := range ids {
		var e jobEntity
		if getErr := s.getEntity(ctx, jobKey(jID), &e); getErr != nil {
			continue
		}
		if job.State(e.State) != job.StateRunning {
			continue
		}
		if e.HeartbeatAt != nil && e.HeartbeatAt.Before(cutoff) {
			j, convErr := fromJobEntity(&e)
			if convErr != nil {
				continue
			}
			stale = append(stale, j)
		}
	}
	return stale, nil
}

// CountJobs returns the number of jobs matching the given options.
func (s *Store) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	ids, err := s.rdb.SMembers(ctx, jobIDsKey).Result()
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: count smembers: %w", err)
	}

	var count int64
	for _, jID := range ids {
		raw, getErr := s.kv.GetRaw(ctx, jobKey(jID))
		if getErr != nil {
			continue
		}
		// Quick check state/queue from JSON without full decode.
		var partial struct {
			State string `json:"state"`
			Queue string `json:"queue"`
		}
		if json.Unmarshal(raw, &partial) != nil {
			continue
		}
		if opts.State != "" && job.State(partial.State) != opts.State {
			continue
		}
		if opts.Queue != "" && partial.Queue != opts.Queue {
			continue
		}
		count++
	}
	return count, nil
}

// ── unused but kept for reference ──

var _ = strconv.Itoa // suppress unused import if needed
