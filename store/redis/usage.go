package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// usageEntity is the JSON shape stored under a usage key.
type usageEntity struct {
	ID          string    `json:"id"`
	JobID       string    `json:"job_id"`
	Name        string    `json:"name"`
	Queue       string    `json:"queue"`
	Attempt     int       `json:"attempt"`
	Status      string    `json:"status"`
	InputBytes  int64     `json:"input_bytes"`
	Resources   []byte    `json:"resources,omitempty"`
	WallTimeNS  int64     `json:"wall_time_ns"`
	CPUTimeNS   int64     `json:"cpu_time_ns"`
	PeakRSS     int64     `json:"peak_rss"`
	DiskWritten int64     `json:"disk_written"`
	Executor    string    `json:"executor,omitempty"`
	ScopeAppID  string    `json:"scope_app_id,omitempty"`
	ScopeOrgID  string    `json:"scope_org_id,omitempty"`
	WorkerID    string    `json:"worker_id,omitempty"`
	RecordedAt  time.Time `json:"recorded_at"`
}

func toUsageEntity(u *job.Usage) (*usageEntity, error) {
	res, err := resource.EncodeSet(u.Resources)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: encode usage resources: %w", err)
	}

	return &usageEntity{
		ID:          u.ID.String(),
		JobID:       u.JobID.String(),
		Name:        u.Name,
		Queue:       u.Queue,
		Attempt:     u.Attempt,
		Status:      string(u.Status),
		InputBytes:  u.InputBytes,
		Resources:   res,
		WallTimeNS:  int64(u.WallTime),
		CPUTimeNS:   int64(u.CPUTime),
		PeakRSS:     u.PeakRSS,
		DiskWritten: u.DiskWritten,
		Executor:    u.Executor,
		ScopeAppID:  u.ScopeAppID,
		ScopeOrgID:  u.ScopeOrgID,
		WorkerID:    u.WorkerID.String(),
		RecordedAt:  u.RecordedAt,
	}, nil
}

func fromUsageEntity(e *usageEntity) (*job.Usage, error) {
	uid, err := id.ParseUsageID(e.ID)
	if err != nil {
		return nil, err
	}

	jid, err := id.ParseJobID(e.JobID)
	if err != nil {
		return nil, err
	}

	res, err := resource.DecodeSet(e.Resources)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: decode usage resources: %w", err)
	}

	u := &job.Usage{
		ID:          uid,
		JobID:       jid,
		Name:        e.Name,
		Queue:       e.Queue,
		Attempt:     e.Attempt,
		Status:      job.State(e.Status),
		InputBytes:  e.InputBytes,
		Resources:   res,
		WallTime:    time.Duration(e.WallTimeNS),
		CPUTime:     time.Duration(e.CPUTimeNS),
		PeakRSS:     e.PeakRSS,
		DiskWritten: e.DiskWritten,
		Executor:    e.Executor,
		ScopeAppID:  e.ScopeAppID,
		ScopeOrgID:  e.ScopeOrgID,
		RecordedAt:  e.RecordedAt,
	}

	if e.WorkerID != "" {
		wid, werr := id.ParseWorkerID(e.WorkerID)
		if werr != nil {
			return nil, werr
		}

		u.WorkerID = wid
	}

	return u, nil
}

// RecordJobUsage persists one attempt's measurements.
//
// Redis has no query engine, so the ordering and the two filters the
// contract requires have to come from indexes written here: a sorted set
// scored by record time for the global view and the retention sweep, and
// one per definition for the read an estimator actually makes.
func (s *Store) RecordJobUsage(ctx context.Context, u *job.Usage) error {
	e, err := toUsageEntity(u)
	if err != nil {
		return err
	}

	key := u.ID.String()

	if serr := s.setEntity(ctx, s.keys.usage(key), e); serr != nil {
		return fmt.Errorf("dispatch/redis: record job usage: %w", serr)
	}

	score := float64(u.RecordedAt.UnixNano())
	member := goredis.Z{Score: score, Member: key}

	pipe := s.rdb.TxPipeline()
	pipe.ZAdd(ctx, s.keys.usageIndex(), member)
	pipe.ZAdd(ctx, s.keys.usageName(u.Name), member)

	if _, perr := pipe.Exec(ctx); perr != nil {
		return fmt.Errorf("dispatch/redis: index job usage: %w", perr)
	}

	return nil
}

// ListJobUsage returns recorded attempts, most recent first.
func (s *Store) ListJobUsage(ctx context.Context, opts job.UsageListOpts) ([]*job.Usage, error) {
	index := s.keys.usageIndex()
	if opts.Name != "" {
		index = s.keys.usageName(opts.Name)
	}

	minScore := "-inf"
	if !opts.Since.IsZero() {
		minScore = strconv.FormatInt(opts.Since.UnixNano(), 10)
	}

	// Newest first, which ZRevRangeByScore gives directly.
	ids, err := s.rdb.ZRevRangeByScore(ctx, index, &goredis.ZRangeBy{
		Min: minScore,
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list job usage: %w", err)
	}

	if opts.Offset > 0 {
		if opts.Offset >= len(ids) {
			return nil, nil
		}

		ids = ids[opts.Offset:]
	}

	if opts.Limit > 0 && opts.Limit < len(ids) {
		ids = ids[:opts.Limit]
	}

	out := make([]*job.Usage, 0, len(ids))

	for _, got := range ids {
		var e usageEntity

		if gerr := s.getEntity(ctx, s.keys.usage(got), &e); gerr != nil {
			if isNotFound(gerr) {
				// The index outlived the record. Nothing reads usage for
				// correctness, so skip rather than fail the whole query.
				continue
			}

			return nil, fmt.Errorf("dispatch/redis: get job usage: %w", gerr)
		}

		u, cerr := fromUsageEntity(&e)
		if cerr != nil {
			return nil, cerr
		}

		out = append(out, u)
	}

	return out, nil
}

// PurgeJobUsage deletes records older than before, up to limit rows.
func (s *Store) PurgeJobUsage(ctx context.Context, before time.Time, limit int) (int64, error) {
	ids, err := s.rdb.ZRangeByScore(ctx, s.keys.usageIndex(), &goredis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(before.UnixNano(), 10),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: purge job usage: %w", err)
	}

	if limit > 0 && limit < len(ids) {
		ids = ids[:limit]
	}

	if len(ids) == 0 {
		return 0, nil
	}

	var removed int64

	for _, got := range ids {
		// The per-definition index is keyed by name, which lives on the
		// record, so the record has to be read before it is dropped.
		name := ""

		var e usageEntity
		if gerr := s.getEntity(ctx, s.keys.usage(got), &e); gerr == nil {
			name = e.Name
		}

		pipe := s.rdb.TxPipeline()
		pipe.Del(ctx, s.keys.usage(got))
		pipe.ZRem(ctx, s.keys.usageIndex(), got)

		if name != "" {
			pipe.ZRem(ctx, s.keys.usageName(name), got)
		}

		if _, perr := pipe.Exec(ctx); perr != nil {
			return removed, fmt.Errorf("dispatch/redis: purge job usage: %w", perr)
		}

		removed++
	}

	return removed, nil
}
