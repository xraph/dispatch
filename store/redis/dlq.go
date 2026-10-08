package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
)

// ── JSON model for KV storage ──

type dlqEntity struct {
	ID         string     `json:"id"`
	JobID      string     `json:"job_id"`
	JobName    string     `json:"job_name"`
	Queue      string     `json:"queue"`
	Payload    []byte     `json:"payload"`
	Error      string     `json:"error"`
	RetryCount int        `json:"retry_count"`
	MaxRetries int        `json:"max_retries"`
	ScopeAppID string     `json:"scope_app_id"`
	ScopeOrgID string     `json:"scope_org_id"`
	FailedAt   time.Time  `json:"failed_at"`
	ReplayedAt *time.Time `json:"replayed_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`

	// ReplayedJobID is set by ClaimReplay together with ReplayedAt, and
	// empty on an unreplayed entry or one the older ReplayDLQ marked.
	ReplayedJobID string `json:"replayed_job_id,omitempty"`

	// Carried so Replay can rebuild a job that behaves like the failed
	// one; see the dlq.Entry doc. resource.Set is a map[string]int64 and
	// marshals natively, like every other field here.
	Priority         int           `json:"priority,omitempty"`
	Timeout          time.Duration `json:"timeout,omitempty"`
	LeaseTTL         time.Duration `json:"lease_ttl,omitempty"`
	ArtifactBindings []byte        `json:"artifact_bindings,omitempty"`
	Resources        resource.Set  `json:"resources,omitempty"`
	ResourceLimits   resource.Set  `json:"resource_limits,omitempty"`
	ResourceClass    string        `json:"resource_class,omitempty"`
	InputBytes       int64         `json:"input_bytes,omitempty"`
	PrimaryInputHash string        `json:"primary_input_hash,omitempty"`
}

func toDLQEntity(e *dlq.Entry) *dlqEntity {
	var replayedJobID string
	if e.ReplayedJobID != nil {
		replayedJobID = e.ReplayedJobID.String()
	}

	return &dlqEntity{
		ID:         e.ID.String(),
		JobID:      e.JobID.String(),
		JobName:    e.JobName,
		Queue:      e.Queue,
		Payload:    e.Payload,
		Error:      e.Error,
		RetryCount: e.RetryCount,
		MaxRetries: e.MaxRetries,
		ScopeAppID: e.ScopeAppID,
		ScopeOrgID: e.ScopeOrgID,
		FailedAt:   e.FailedAt,
		ReplayedAt: e.ReplayedAt,
		CreatedAt:  e.CreatedAt,

		ReplayedJobID: replayedJobID,

		Priority:         e.Priority,
		Timeout:          e.Timeout,
		LeaseTTL:         e.LeaseTTL,
		ArtifactBindings: e.ArtifactBindings,
		Resources:        e.Resources,
		ResourceLimits:   e.ResourceLimits,
		ResourceClass:    e.ResourceClass,
		InputBytes:       e.InputBytes,
		PrimaryInputHash: e.PrimaryInputHash,
	}
}

func fromDLQEntity(e *dlqEntity) (*dlq.Entry, error) {
	parsedID, err := id.ParseDLQID(e.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse dlq id: %w", err)
	}

	parsedJobID, _ := id.ParseJobID(e.JobID) //nolint:errcheck // best-effort

	var replayedJobID *id.JobID
	if e.ReplayedJobID != "" {
		parsed, parseErr := id.ParseJobID(e.ReplayedJobID)
		if parseErr != nil {
			return nil, fmt.Errorf("dispatch/redis: parse dlq replayed job id: %w", parseErr)
		}
		replayedJobID = &parsed
	}

	return &dlq.Entry{
		ID:         parsedID,
		JobID:      parsedJobID,
		JobName:    e.JobName,
		Queue:      e.Queue,
		Payload:    e.Payload,
		Error:      e.Error,
		RetryCount: e.RetryCount,
		MaxRetries: e.MaxRetries,
		ScopeAppID: e.ScopeAppID,
		ScopeOrgID: e.ScopeOrgID,
		FailedAt:   e.FailedAt,
		ReplayedAt: e.ReplayedAt,
		CreatedAt:  e.CreatedAt,

		ReplayedJobID: replayedJobID,

		Priority:         e.Priority,
		Timeout:          e.Timeout,
		LeaseTTL:         e.LeaseTTL,
		ArtifactBindings: e.ArtifactBindings,
		Resources:        e.Resources,
		ResourceLimits:   e.ResourceLimits,
		ResourceClass:    e.ResourceClass,
		InputBytes:       e.InputBytes,
		PrimaryInputHash: e.PrimaryInputHash,
	}, nil
}

// pushDLQScript writes a new DLQ entry and every index that points at it,
// or nothing at all when the entry ID is already taken.
//
// KEYS[1] the entry key, KEYS[2] dlqIDs, KEYS[3] the DLQ created-order
// index, KEYS[4] the job's dlqByJob set, KEYS[5] dlqJobIndexed.
// ARGV[1] the entry blob, ARGV[2] the entry ID, ARGV[3] its created score.
// Returns 1 when written, 0 when the entry already existed.
var pushDLQScript = goredis.NewScript(`
if not redis.call('SET', KEYS[1], ARGV[1], 'NX') then
  return 0
end
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
redis.call('SADD', KEYS[2], ARGV[2])
redis.call('SADD', KEYS[4], ARGV[2])
redis.call('SADD', KEYS[5], ARGV[2])
return 1
`)

// PushDLQ adds a failed job entry to the dead letter queue. An ID that is
// already there is refused with dispatch.ErrDLQAlreadyExists rather than
// overwritten, which would silently drop a replay claim. The SET NX and
// the index writes run in one script, so a refused push leaves no index
// member behind and an accepted one is never visible half-indexed.
func (s *Store) PushDLQ(ctx context.Context, entry *dlq.Entry) error {
	eID := entry.ID.String()

	blob, err := json.Marshal(toDLQEntity(entry))
	if err != nil {
		return fmt.Errorf("dispatch/redis: push dlq marshal: %w", err)
	}

	res, err := pushDLQScript.Run(ctx, s.rdb,
		[]string{
			s.keys.dlq(eID),
			s.keys.dlqIDs(),
			s.keys.byCreated(entityDLQ),
			s.keys.dlqByJob(entry.JobID.String()),
			s.keys.dlqJobIndexed(),
		},
		blob,
		eID,
		strconv.FormatFloat(createdScore(entry.ID), 'f', -1, 64),
	).Int64()
	if err != nil {
		return fmt.Errorf("dispatch/redis: push dlq: %w", err)
	}
	if res == 0 {
		return dispatch.ErrDLQAlreadyExists
	}

	return nil
}

// ListDLQ returns DLQ entries matching the given options.
func (s *Store) ListDLQ(ctx context.Context, opts dlq.ListOpts) ([]*dlq.Entry, error) {
	ids, err := s.rdb.SMembers(ctx, s.keys.dlqIDs()).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list dlq: %w", err)
	}

	entries := make([]*dlq.Entry, 0, len(ids))
	for _, eID := range ids {
		var e dlqEntity
		if getErr := s.getEntity(ctx, s.keys.dlq(eID), &e); getErr != nil {
			continue
		}
		if opts.Queue != "" && e.Queue != opts.Queue {
			continue
		}
		entry, convErr := fromDLQEntity(&e)
		if convErr != nil {
			continue
		}
		entries = append(entries, entry)
	}

	return applyPagination(entries, opts.Offset, opts.Limit), nil
}

// GetDLQ retrieves a DLQ entry by ID.
func (s *Store) GetDLQ(ctx context.Context, entryID id.DLQID) (*dlq.Entry, error) {
	var e dlqEntity
	if err := s.getEntity(ctx, s.keys.dlq(entryID.String()), &e); err != nil {
		if isNotFound(err) {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf("dispatch/redis: get dlq: %w", err)
	}
	return fromDLQEntity(&e)
}

// ReplayDLQ marks a DLQ entry as replayed. It goes through the same
// compare-and-set as ClaimReplay, so it can never overwrite a claim's
// replayed_job_id with a copy of the entry read before the claim.
func (s *Store) ReplayDLQ(ctx context.Context, entryID id.DLQID) error {
	return updateEntity(ctx, s, s.keys.dlq(entryID.String()), dispatch.ErrDLQNotFound,
		func(e *dlqEntity) error {
			t := now()
			e.ReplayedAt = &t

			return nil
		})
}

// PurgeDLQ removes DLQ entries with FailedAt before the given time.
func (s *Store) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	ids, err := s.rdb.SMembers(ctx, s.keys.dlqIDs()).Result()
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: purge dlq smembers: %w", err)
	}

	var purged int64
	for _, eID := range ids {
		key := s.keys.dlq(eID)
		var e dlqEntity
		if getErr := s.getEntity(ctx, key, &e); getErr != nil {
			continue
		}

		if e.FailedAt.Before(before) {
			pipe := s.rdb.TxPipeline()
			pipe.Del(ctx, key)
			pipe.SRem(ctx, s.keys.dlqIDs(), eID)
			pipe.ZRem(ctx, s.keys.byCreated(entityDLQ), eID)
			pipe.SRem(ctx, s.keys.dlqByJob(e.JobID), eID)
			pipe.SRem(ctx, s.keys.dlqJobIndexed(), eID)
			if _, pErr := pipe.Exec(ctx); pErr != nil {
				return purged, fmt.Errorf("dispatch/redis: purge dlq del: %w", pErr)
			}
			purged++
		}
	}
	return purged, nil
}

// CountDLQ returns the total number of entries in the dead letter queue.
func (s *Store) CountDLQ(ctx context.Context) (int64, error) {
	count, err := s.rdb.SCard(ctx, s.keys.dlqIDs()).Result()
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: count dlq: %w", err)
	}
	return count, nil
}
