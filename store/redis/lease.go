package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Both scripts below decode the stored job blob with cjson, mutate a
// handful of fields, and re-encode the whole thing. That round trip is a
// documented cjson hazard in general: Lua tables can't distinguish an
// empty JSON array from an empty JSON object, absent keys and JSON null
// aren't always preserved the way they went in, and every JSON number
// becomes a Lua (double-precision) number, which can silently lose
// precision for large integers.
//
// None of that is reachable for jobEntity as it stands. There are no
// slice- or map-typed fields in the JSON this store persists for a job —
// Payload is a []byte, which encoding/json always renders as a base64
// string, not an array — so the empty-array/empty-object ambiguity has
// nothing to attach to. Every *time.Time field is a string (RFC3339Nano)
// once marshaled, not a number, so no precision is at risk there either.
// omitempty fields (StartedAt, CompletedAt, HeartbeatAt, LeaseExpiresAt)
// are absent, not null, when unset; cjson.decode leaves an absent JSON
// key absent from the Lua table, and encoding a table that never had the
// key set re-omits it — absence round-trips as absence, matching Go's
// omitempty semantics on the way back through fromJobEntity. The one
// caveat worth naming: Timeout and LeaseTTL are int64 nanosecond
// durations, and Lua's float64 numbers stop representing integers
// exactly past 2^53 (~104 days in nanoseconds). A job timeout or lease
// TTL longer than that would round on every renewal or reclaim that
// touches it. That's an accepted, narrow limitation — every realistic
// timeout and lease TTL in this system is minutes to hours — not a
// silent risk to the fields these scripts actually exist to protect.
//
// The alternative considered was having Go serialize the full updated
// entity via encoding/json and have Lua only check-then-blind-SET that
// pre-built blob, skipping cjson entirely. That was rejected: it trades
// this narrow, bounded risk for a much wider one. Go's read and the
// script's write would be two separate round trips apart, and anything
// that writes this job's entity in between — a heartbeat, a plain
// UpdateJob call — without going through this store's lease-aware paths
// would be silently discarded by the blind SET, because neither of those
// paths touches lease_epoch and so wouldn't be caught by the epoch check
// the script still has to do. Keeping the decode-mutate-encode shape
// means the GET inside the script is the freshest possible read of the
// row, taken atomically with the SET that follows it, so there is no
// window for a concurrent writer to lose a field this way at all.

// renewLeaseScript extends a lease only when the caller still holds it.
//
// The rest of this store reads a job, mutates it in Go, and writes it
// back. That is fine for last-write-wins fields and useless for an epoch
// check: two callers can both read epoch 3 and both write "renewed" —
// there is no compare in a plain SET. Lua runs atomically inside Redis,
// so the compare and the set cannot be interleaved by anything, including
// another renewal, a reclaim, or a plain UpdateJob. That is the only
// reason the fencing guarantee holds here at all.
//
// This script decodes the stored blob, checks three fields, mutates
// three fields, and re-encodes the whole thing (see the file-level
// comment above for why that round trip through cjson is safe for this
// schema). KEYS[1] job key. ARGV[1] worker id, ARGV[2] expected epoch,
// ARGV[3] lease_expires_at (RFC3339Nano, unquoted), ARGV[4] now
// (RFC3339Nano, unquoted), used for both heartbeat_at and updated_at.
// Returns 1 on renewal, 0 when the lease is no longer held.
var renewLeaseScript = goredis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
local j = cjson.decode(raw)
if j.state ~= 'running' then
  return 0
end
if j.worker_id ~= ARGV[1] then
  return 0
end
if tostring(j.lease_epoch) ~= ARGV[2] then
  return 0
end
j.lease_expires_at = ARGV[3]
j.heartbeat_at = ARGV[4]
j.updated_at = ARGV[4]
redis.call('SET', KEYS[1], cjson.encode(j))
return 1
`)

// reclaimScript resets one job to pending only if it is still running at
// the expected epoch.
//
// Reclamation does not need to re-derive "is the lease expired" inside
// Lua: that decision was already made correctly in Go, using real
// time.Time comparison (job.Lease.IsExpired), before this script was
// ever called. Doing an equivalent comparison here in Lua would mean
// comparing two RFC3339Nano strings with '>' — fragile, since Go trims
// trailing zeros from the fractional seconds and a naive assumption
// that these strings sort chronologically is exactly the kind of thing
// that looks right in every manual test and breaks on one timestamp in
// a billion. This script instead re-verifies only equality: still
// running, still at the epoch Go observed. That is enough to make the
// claim exclusive — if another caller (or a fresh grant) already moved
// the job, the epoch or state check fails and this caller loses,
// cleanly, without ever comparing a timestamp.
//
// KEYS[1] job key. ARGV[1] expected epoch, ARGV[2] now (RFC3339Nano,
// unquoted), used for run_at and updated_at.
// Returns 1 when this caller took the job, 0 when someone else did (or
// the job moved out of running between Go's read and this script).
var reclaimScript = goredis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
local j = cjson.decode(raw)
if j.state ~= 'running' then
  return 0
end
if tostring(j.lease_epoch) ~= ARGV[1] then
  return 0
end
j.state = 'pending'
j.run_at = ARGV[2]
j.updated_at = ARGV[2]
j.worker_id = ''
j.started_at = nil
j.heartbeat_at = nil
j.lease_expires_at = nil
j.lease_epoch = j.lease_epoch + 1
j.evict_count = (j.evict_count or 0) + 1
redis.call('SET', KEYS[1], cjson.encode(j))
return 1
`)

// DequeueLeased claims up to limit ready jobs and grants each a lease.
//
// This stays a plain read-modify-write, unlike renewal and reclaim.
// ZPopMin already removed the job from the queue's sorted set before this
// function ever reads the entity, so no other worker can reach it by any
// path this store exposes — there is nothing left to race against, and
// no epoch compare is needed to make the grant safe.
func (s *Store) DequeueLeased(
	ctx context.Context,
	queues []string,
	limit int,
	workerID id.WorkerID,
	leaseUntil time.Time,
) ([]*job.Job, error) {
	t := now()
	until := leaseUntil.UTC()
	// max(limit, 0): a non-positive limit must not panic make() with a
	// negative capacity. The loop below already returns nothing for
	// limit <= 0 (len(jobs) >= limit is true from the first iteration),
	// matching DequeueJobs' existing behavior for the same input.
	jobs := make([]*job.Job, 0, max(limit, 0))

	for _, q := range queues {
		if len(jobs) >= limit {
			break
		}
		remaining := limit - len(jobs)

		members, err := s.rdb.ZPopMin(ctx, queueKey(q), int64(remaining)).Result()
		if err != nil {
			return nil, fmt.Errorf("dispatch/redis: dequeue leased zpopmin: %w", err)
		}

		for _, z := range members {
			jID, ok := z.Member.(string)
			if !ok {
				continue
			}

			key := jobKey(jID)
			var e jobEntity
			if getErr := s.getEntity(ctx, key, &e); getErr != nil {
				continue // popped from the queue but the entity is gone; skip it
			}

			e.State = string(job.StateRunning)
			e.StartedAt = &t
			e.WorkerID = workerID.String()
			e.LeaseEpoch++
			e.LeaseExpiresAt = &until
			e.UpdatedAt = t

			if setErr := s.setEntity(ctx, key, &e); setErr != nil {
				return nil, fmt.Errorf("dispatch/redis: dequeue leased update: %w", setErr)
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

// RenewLease extends the lease only if the caller still holds it.
func (s *Store) RenewLease(
	ctx context.Context,
	jobID id.JobID,
	workerID id.WorkerID,
	epoch int,
	leaseUntil time.Time,
) error {
	t := now()

	res, err := renewLeaseScript.Run(ctx, s.rdb,
		[]string{jobKey(jobID.String())},
		workerID.String(),
		epoch,
		redisTime(leaseUntil),
		redisTime(t),
	).Int64()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("dispatch/redis: renew lease: %w", err)
	}
	if res != 1 {
		return job.ErrLeaseLost
	}

	return nil
}

// ReclaimExpiredLeases returns expired-lease jobs to pending, fencing
// their previous holders.
//
// Reclamation walks the job-id set rather than a sorted index, matching
// ReapStaleJobs — there is no secondary index of running-with-expired-
// lease jobs in this backend. Each candidate is filtered here in Go
// using real time.Time comparison, then claimed through reclaimScript,
// keyed on the epoch this call observed, so two pools scanning
// concurrently cannot both take it: whichever script call runs second
// sees an epoch (or state) that no longer matches and backs off.
func (s *Store) ReclaimExpiredLeases(ctx context.Context, limit int) ([]*job.Job, error) {
	t := now()

	ids, err := s.rdb.SMembers(ctx, jobIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: reclaim smembers: %w", err)
	}

	// limit <= 0 means unlimited here (mirrors the memory backend), so the
	// break below only fires for a positive limit — but the capacity must
	// still never go negative, hence max(limit, 0).
	reclaimed := make([]*job.Job, 0, max(limit, 0))
	for _, jID := range ids {
		if limit > 0 && len(reclaimed) >= limit {
			break
		}

		var e jobEntity
		if getErr := s.getEntity(ctx, jobKey(jID), &e); getErr != nil {
			continue // gone by the time we looked
		}
		if job.State(e.State) != job.StateRunning {
			continue
		}
		lease := job.Lease{Epoch: e.LeaseEpoch}
		if e.LeaseExpiresAt != nil {
			lease.ExpiresAt = *e.LeaseExpiresAt
		}
		if !lease.IsExpired(t) {
			continue
		}

		after, claimed, claimErr := s.claimExpired(ctx, jID, e.LeaseEpoch, t)
		if claimErr != nil {
			return nil, claimErr
		}
		if !claimed {
			continue // another pool got there first
		}

		// The claim reset the job's entity to pending, but ZPopMin already
		// removed it from queueKey at grant time — it never went back on
		// its own. Without this it becomes invisible to every future
		// dequeue: reset to pending but unreachable, forever. ZADD on a
		// member already present just updates its score, so this is safe
		// even for a job that was enqueued straight into running (as the
		// conformance suite's RunningJob helper does) and was therefore
		// never popped in the first place.
		zErr := s.rdb.ZAdd(ctx, queueKey(after.Queue),
			goredis.Z{Score: jobScore(after.Priority, after.RunAt), Member: jID}).Err()
		if zErr != nil {
			return nil, fmt.Errorf("dispatch/redis: reclaim requeue: %w", zErr)
		}

		j, convErr := fromJobEntity(after)
		if convErr != nil {
			continue
		}
		reclaimed = append(reclaimed, j)
	}

	return reclaimed, nil
}

// claimExpired atomically resets one expired job to pending, reporting
// whether this caller was the one that took it. On success it returns
// the entity as it now stands in the store, read fresh after the claim
// rather than reconstructed from the pre-claim read, so callers never
// see a copy that is stale in any field the claim did not touch.
func (s *Store) claimExpired(ctx context.Context, jID string, epoch int, t time.Time) (*jobEntity, bool, error) {
	res, err := reclaimScript.Run(ctx, s.rdb,
		[]string{jobKey(jID)},
		epoch,
		redisTime(t),
	).Int64()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return nil, false, fmt.Errorf("dispatch/redis: reclaim claim: %w", err)
	}
	if res != 1 {
		return nil, false, nil
	}

	var after jobEntity
	if getErr := s.getEntity(ctx, jobKey(jID), &after); getErr != nil {
		return nil, false, fmt.Errorf("dispatch/redis: reclaim reread: %w", getErr)
	}

	return &after, true, nil
}

// redisTime renders a timestamp exactly the way encoding/json renders a
// time.Time field: RFC3339Nano, UTC, trailing fractional zeros trimmed.
// Lua writes this string as the field's raw value (json.Marshal quotes
// it; Lua's cjson.encode will add the quotes for us), so a value written
// by a script round-trips through fromJobEntity identically to a value
// written by setEntity.
func redisTime(t time.Time) string {
	b, err := json.Marshal(t.UTC())
	if err != nil {
		// time.Time.MarshalJSON only fails for years outside [0,9999],
		// which cannot occur for a lease deadline computed from time.Now.
		return t.UTC().Format(time.RFC3339Nano)
	}

	// json.Marshal quotes the string; Lua wants the raw value.
	return string(b[1 : len(b)-1])
}
