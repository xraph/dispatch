package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// EnqueueJob stores the job as a Hash and adds it to the queue's Sorted Set.
func (s *Store) EnqueueJob(ctx context.Context, j *job.Job) error {
	jID := j.ID.String()
	key := jobKey(jID)

	// Check for duplicate.
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: enqueue check exists: %w", err)
	}
	if exists > 0 {
		return dispatch.ErrJobAlreadyExists
	}

	fields := jobToMap(j)

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	pipe.SAdd(ctx, jobIDsKey, jID)

	// Add to queue sorted set: score = priority (negated for DESC) + time component.
	score := jobScore(j.Priority, j.RunAt)
	pipe.ZAdd(ctx, queueKey(j.Queue), goredis.Z{Score: score, Member: jID})

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: enqueue job: %w", err)
	}
	return nil
}

// DequeueJobs atomically pops up to limit jobs from the given queues.
func (s *Store) DequeueJobs(ctx context.Context, queues []string, limit int) ([]*job.Job, error) {
	now := time.Now().UTC()
	var jobs []*job.Job

	for _, q := range queues {
		if len(jobs) >= limit {
			break
		}
		remaining := limit - len(jobs)
		qk := queueKey(q)

		// Pop from sorted set (lowest score = highest priority + earliest RunAt).
		members, err := s.client.ZPopMin(ctx, qk, int64(remaining)).Result()
		if err != nil {
			return nil, fmt.Errorf("dispatch/redis: dequeue zpopmin: %w", err)
		}

		for _, z := range members {
			jID, ok := z.Member.(string)
			if !ok {
				continue
			}

			key := jobKey(jID)
			// Update state to running.
			pipe := s.client.TxPipeline()
			pipe.HSet(ctx, key,
				"state", string(job.StateRunning),
				"started_at", now.Format(time.RFC3339Nano),
				"updated_at", now.Format(time.RFC3339Nano),
			)
			if _, pErr := pipe.Exec(ctx); pErr != nil {
				return nil, fmt.Errorf("dispatch/redis: dequeue update: %w", pErr)
			}

			j, getErr := s.getJobByKey(ctx, key)
			if getErr != nil {
				return nil, getErr
			}
			jobs = append(jobs, j)
		}
	}
	return jobs, nil
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	j, err := s.getJobByKey(ctx, jobKey(jobID.String()))
	if err != nil {
		return nil, err
	}
	return j, nil
}

// UpdateJob persists changes to an existing job.
func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	jID := j.ID.String()
	key := jobKey(jID)

	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: update job exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrJobNotFound
	}

	fields := jobToMap(j)
	fields["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)

	_, err = s.client.HSet(ctx, key, fields).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: update job: %w", err)
	}
	return nil
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, jobID id.JobID) error {
	jID := jobID.String()
	key := jobKey(jID)

	// Get queue name before deleting to remove from sorted set.
	q, err := s.client.HGet(ctx, key, "queue").Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return dispatch.ErrJobNotFound
		}
		return fmt.Errorf("dispatch/redis: delete job get queue: %w", err)
	}

	pipe := s.client.TxPipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, jobIDsKey, jID)
	pipe.ZRem(ctx, queueKey(q), jID)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: delete job: %w", err)
	}
	return nil
}

// ListJobsByState returns jobs matching the given state.
func (s *Store) ListJobsByState(ctx context.Context, state job.State, opts job.ListOpts) ([]*job.Job, error) {
	ids, err := s.client.SMembers(ctx, jobIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list jobs smembers: %w", err)
	}

	jobs := make([]*job.Job, 0, len(ids))
	for _, jID := range ids {
		j, getErr := s.getJobByKey(ctx, jobKey(jID))
		if getErr != nil {
			continue // skip missing
		}
		if j.State != state {
			continue
		}
		if opts.Queue != "" && j.Queue != opts.Queue {
			continue
		}
		jobs = append(jobs, j)
	}

	// Apply offset/limit.
	if opts.Offset > 0 && opts.Offset < len(jobs) {
		jobs = jobs[opts.Offset:]
	} else if opts.Offset >= len(jobs) {
		return nil, nil
	}
	if opts.Limit > 0 && opts.Limit < len(jobs) {
		jobs = jobs[:opts.Limit]
	}
	return jobs, nil
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (s *Store) HeartbeatJob(ctx context.Context, jobID id.JobID, workerID id.WorkerID) error {
	key := jobKey(jobID.String())
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: heartbeat exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrJobNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.client.HSet(ctx, key,
		"heartbeat_at", now,
		"worker_id", workerID.String(),
		"updated_at", now,
	).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: heartbeat job: %w", err)
	}
	return nil
}

// ReapStaleJobs returns running jobs whose last heartbeat is older than the threshold.
func (s *Store) ReapStaleJobs(ctx context.Context, threshold time.Duration) ([]*job.Job, error) {
	cutoff := time.Now().UTC().Add(-threshold)

	ids, err := s.client.SMembers(ctx, jobIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: reap smembers: %w", err)
	}

	var stale []*job.Job
	for _, jID := range ids {
		j, getErr := s.getJobByKey(ctx, jobKey(jID))
		if getErr != nil {
			continue
		}
		if j.State != job.StateRunning {
			continue
		}
		if j.HeartbeatAt != nil && j.HeartbeatAt.Before(cutoff) {
			stale = append(stale, j)
		}
	}
	return stale, nil
}

// CountJobs returns the number of jobs matching the given options.
func (s *Store) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	ids, err := s.client.SMembers(ctx, jobIDsKey).Result()
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: count smembers: %w", err)
	}

	var count int64
	for _, jID := range ids {
		j, getErr := s.getJobByKey(ctx, jobKey(jID))
		if getErr != nil {
			continue
		}
		if opts.State != "" && j.State != opts.State {
			continue
		}
		if opts.Queue != "" && j.Queue != opts.Queue {
			continue
		}
		count++
	}
	return count, nil
}

// ── helpers ──

// jobScore computes a sorted-set score from priority and run_at.
// Lower score = dequeued first.
// We negate priority so higher priority = lower score.
func jobScore(priority int, runAt time.Time) float64 {
	// Use negative priority so higher priority jobs sort first.
	// Add a fractional time component for FIFO within same priority.
	return float64(-priority) + float64(runAt.UnixMilli())/1e15
}

func jobToMap(j *job.Job) map[string]interface{} {
	m := map[string]interface{}{
		"id":          j.ID.String(),
		"name":        j.Name,
		"queue":       j.Queue,
		"payload":     string(j.Payload),
		"state":       string(j.State),
		"priority":    strconv.Itoa(j.Priority),
		"max_retries": strconv.Itoa(j.MaxRetries),
		"retry_count": strconv.Itoa(j.RetryCount),
		"last_error":  j.LastError,
		"scope_app":   j.ScopeAppID,
		"scope_org":   j.ScopeOrgID,
		"worker_id":   j.WorkerID.String(),
		"run_at":      j.RunAt.Format(time.RFC3339Nano),
		"timeout":     strconv.FormatInt(int64(j.Timeout), 10),
		"created_at":  j.CreatedAt.Format(time.RFC3339Nano),
		"updated_at":  j.UpdatedAt.Format(time.RFC3339Nano),
	}
	if j.StartedAt != nil {
		m["started_at"] = j.StartedAt.Format(time.RFC3339Nano)
	}
	if j.CompletedAt != nil {
		m["completed_at"] = j.CompletedAt.Format(time.RFC3339Nano)
	}
	if j.HeartbeatAt != nil {
		m["heartbeat_at"] = j.HeartbeatAt.Format(time.RFC3339Nano)
	}
	return m
}

func (s *Store) getJobByKey(ctx context.Context, key string) (*job.Job, error) {
	vals, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: get job: %w", err)
	}
	if len(vals) == 0 {
		return nil, dispatch.ErrJobNotFound
	}
	return mapToJob(vals)
}

func mapToJob(m map[string]string) (*job.Job, error) {
	jID, err := id.ParseJobID(m["id"])
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse job id: %w", err)
	}

	priority, _ := strconv.Atoi(m["priority"])           //nolint:errcheck // best-effort parse from trusted Redis data
	maxRetries, _ := strconv.Atoi(m["max_retries"])      //nolint:errcheck // best-effort parse from trusted Redis data
	retryCount, _ := strconv.Atoi(m["retry_count"])      //nolint:errcheck // best-effort parse from trusted Redis data
	timeout, _ := strconv.ParseInt(m["timeout"], 10, 64) //nolint:errcheck // best-effort parse from trusted Redis data

	runAt, _ := time.Parse(time.RFC3339Nano, m["run_at"])         //nolint:errcheck // best-effort parse from trusted Redis data
	createdAt, _ := time.Parse(time.RFC3339Nano, m["created_at"]) //nolint:errcheck // best-effort parse from trusted Redis data
	updatedAt, _ := time.Parse(time.RFC3339Nano, m["updated_at"]) //nolint:errcheck // best-effort parse from trusted Redis data

	j := &job.Job{
		Entity: dispatch.Entity{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		},
		ID:         jID,
		Name:       m["name"],
		Queue:      m["queue"],
		Payload:    []byte(m["payload"]),
		State:      job.State(m["state"]),
		Priority:   priority,
		MaxRetries: maxRetries,
		RetryCount: retryCount,
		LastError:  m["last_error"],
		ScopeAppID: m["scope_app"],
		ScopeOrgID: m["scope_org"],
		RunAt:      runAt,
		Timeout:    time.Duration(timeout),
	}

	if wid := m["worker_id"]; wid != "" {
		j.WorkerID, _ = id.ParseWorkerID(wid) //nolint:errcheck // best-effort parse from trusted Redis data
	}
	if v := m["started_at"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v) //nolint:errcheck // best-effort parse from trusted Redis data
		j.StartedAt = &t
	}
	if v := m["completed_at"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v) //nolint:errcheck // best-effort parse from trusted Redis data
		j.CompletedAt = &t
	}
	if v := m["heartbeat_at"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v) //nolint:errcheck // best-effort parse from trusted Redis data
		j.HeartbeatAt = &t
	}

	return j, nil
}

// marshalJSON is a helper to marshal to JSON string.
func marshalJSON(v interface{}) string {
	b, _ := json.Marshal(v) //nolint:errcheck // marshal should not fail for basic types
	return string(b)
}

// unmarshalStrings parses a JSON array of strings.
func unmarshalStrings(s string) []string {
	if s == "" || s == "null" {
		return nil
	}
	var out []string
	_ = json.Unmarshal([]byte(s), &out) //nolint:errcheck // best-effort parse from trusted Redis data
	return out
}

// unmarshalMap parses a JSON map.
func unmarshalMap(s string) map[string]string {
	if s == "" || s == "null" {
		return nil
	}
	out := make(map[string]string)
	_ = json.Unmarshal([]byte(s), &out) //nolint:errcheck // best-effort parse from trusted Redis data
	return out
}
