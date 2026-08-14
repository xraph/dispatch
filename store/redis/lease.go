package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Both scripts below decode the stored job blob with cjson, but only to
// CHECK three scalar fields (state, worker_id, lease_epoch) — never to
// mutate and re-encode it. That split exists because of a bug found in
// review: an earlier version of this file had each script decode the
// whole blob, mutate a few fields in Lua, and cjson.encode the result
// back. That looked safe on paper — jobEntity has no slice/map fields, so
// the classic cjson empty-array/empty-object ambiguity never applied, and
// every *time.Time field is a string, not a number — and it was still
// wrong. cjson represents every JSON number as a Lua double, and Redis's
// cjson renders a double that large in scientific notation on encode
// (`9999999999999999` came back as `1e+16`). encoding/json then refuses
// to parse that back into an int64 at all on the next read — not a
// rounding error, a hard unmarshal failure — for Timeout or LeaseTTL past
// 2^53ns (~104 days), on every renewal or reclaim that touched the row,
// whether or not it cared about those fields. See
// TestLeaseLargeDurationRoundTrip for the reproduction, and note that in
// ReclaimExpiredLeases specifically, that unmarshal error aborted the
// whole scan, abandoning every other expired job the call had already
// found before it ever reached the queue re-add.
//
// The fix: Go now owns all serialization. RenewLease and
// ReclaimExpiredLeases each read the current entity, compute the fully
// updated entity in Go, and json.Marshal it themselves — the same path
// every other write in this store already uses. The script's job shrinks
// to being the compare-and-set: decode just enough to check
// state/worker_id/lease_epoch against what the caller expects, and if
// they match, SET the pre-built blob Go handed it. cjson.encode is never
// called by either script now, so no field can be reshaped by it — the
// whole corruption class is closed, not just the two fields that
// happened to trip it first.
//
// That fix has its own tradeoff, and it is deliberate, not overlooked.
// Between Go's read and the script's SET there is a real window — a full
// round trip — during which some other writer could change a field on
// this same job that the lease check doesn't cover. UpdateJob is the
// concrete example: it doesn't touch lease_epoch, so the epoch check
// inside these scripts would still pass, and the blind SET would
// overwrite whatever UpdateJob just wrote with Go's now-stale copy of
// that field. The epoch check makes the *lease* compare-and-set atomic;
// it does not make every write to the row serialize with every other
// write.
//
// This paragraph used to claim that window was "currently unreachable,
// not closed," because nothing called UpdateJob concurrently with
// RenewLease or ReclaimExpiredLeases on the same job. That premise was
// wrong, and worker/runner.go's terminal writes are the disproof: a
// worker whose lease had already been reclaimed — a live holder was
// mid-attempt, renewing on schedule — still called the unfenced UpdateJob
// from handleSuccess, scheduleRetry, or sendToDLQ, using a claim-time job
// snapshot. It won the race, rolled lease_epoch backwards, and marked the
// job completed while the reclaiming worker was still executing it. The
// "lease-aware pool loop" this comment said would be later work is
// exactly what RenewLease-on-heartbeat already was; the missing piece was
// never the pool loop, it was a fenced write for the runner to call.
//
// UpdateLeasedJob, below, is that write. It reuses renewLeaseScript
// itself — the fence it needs (still running, still this worker, still
// this epoch) is identical to RenewLease's — so the runner's terminal
// writes now go through the same compare-and-set family as renewal and
// can no longer race it on the same job. That closes the reachable
// instance of this window, but not the general case: UpdateJob remains
// unfenced by design (see job.LeaseStore.UpdateLeasedJob — the reaper's
// legacy path, rate-limit and shutdown requeues must stay unfenced, or
// reclaiming a dead worker's job would deadlock on that worker's own
// epoch), so a caller that reaches UpdateJob directly for a job whose
// lease has moved on — bypassing the pool and UpdateLeasedJob entirely —
// can still race these scripts. A full-blob compare-and-swap (checking
// the entire previous blob byte-for-byte, not just three fields, before
// the SET) would close that general case too, but is still rejected for
// the reason it always was: it would make renewal fail on any unrelated
// concurrent write, including perfectly legitimate ones, and a spurious
// ErrLeaseLost is exactly what makes a pool cancel a healthy running job.

// renewLeaseScript extends a lease only when the caller still holds it.
//
// KEYS[1] job key. ARGV[1] worker id, ARGV[2] expected epoch, ARGV[3] the
// complete updated entity, pre-serialized by Go (see the file comment
// above for why Lua never re-serializes it itself).
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
redis.call('SET', KEYS[1], ARGV[3])
return 1
`)

// reclaimScript resets one job to pending only if it is still running at
// the expected epoch.
//
// Reclamation does not need to re-derive "is the lease expired" inside
// Lua: that decision was already made correctly in Go, using real
// time.Time comparison (job.Lease.IsExpired), before this script was ever
// called. This script re-verifies only equality — still running, still at
// the epoch Go observed — which is enough to make the claim exclusive: if
// another caller (or a fresh grant) already moved the job, the epoch or
// state check fails and this caller loses, cleanly.
//
// KEYS[1] job key. ARGV[1] expected epoch, ARGV[2] the complete
// pending-state entity, pre-serialized by Go with lease_epoch already
// incremented and evict_count already incremented (see the file comment
// above for why Lua never mutates or re-serializes it itself).
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
redis.call('SET', KEYS[1], ARGV[2])
return 1
`)

// The grant is not in this file: it travels on job.DequeueOpts and is
// applied by claimCandidates, in the same read-modify-write that writes
// the claimed job as running, so a leased claim carries the fit predicate
// and the ordering like any other. See store/redis/dequeue.go for why
// that write needs no compare-and-set of its own while the two below do.

// RenewLease extends the lease only if the caller still holds it.
//
// Go reads the current entity, mutates only the lease/heartbeat fields,
// and serializes the whole thing with encoding/json — the same path
// every other write in this store uses. The script's only job is the
// compare-and-set: verify state/worker_id/lease_epoch still match what
// this read saw, and if so, SET the blob Go built. See the file comment
// above for the ABA tradeoff this introduces and why it's accepted.
func (s *Store) RenewLease(
	ctx context.Context,
	jobID id.JobID,
	workerID id.WorkerID,
	epoch int,
	leaseUntil time.Time,
) error {
	key := jobKey(jobID.String())

	var e jobEntity
	if getErr := s.getEntity(ctx, key, &e); getErr != nil {
		if isNotFound(getErr) {
			return job.ErrLeaseLost
		}
		return fmt.Errorf("dispatch/redis: renew lease get: %w", getErr)
	}

	t := now()
	until := leaseUntil.UTC()
	e.LeaseExpiresAt = &until
	e.HeartbeatAt = &t
	e.UpdatedAt = t

	blob, marshalErr := json.Marshal(&e)
	if marshalErr != nil {
		return fmt.Errorf("dispatch/redis: renew lease marshal: %w", marshalErr)
	}

	res, err := renewLeaseScript.Run(ctx, s.rdb,
		[]string{key},
		workerID.String(),
		epoch,
		blob,
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
// lease jobs in this backend. Each candidate is filtered here in Go using
// real time.Time comparison, the pending-state entity is computed in Go,
// and claimExpired does the compare-and-set: keyed on the epoch this call
// observed, so two pools scanning concurrently cannot both take the same
// job — whichever script call runs second sees an epoch (or state) that
// no longer matches and backs off.
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

		expectedEpoch := e.LeaseEpoch

		// The pending-state entity, computed entirely in Go. Lua only
		// checks state/lease_epoch against expectedEpoch and blind-SETs
		// this blob — see the file comment above for why.
		after := e
		after.State = string(job.StatePending)
		after.RunAt = t
		after.UpdatedAt = t
		after.WorkerID = ""
		after.StartedAt = nil
		after.HeartbeatAt = nil
		after.LeaseExpiresAt = nil
		after.LeaseEpoch = expectedEpoch + 1
		after.EvictCount++

		blob, marshalErr := json.Marshal(&after)
		if marshalErr != nil {
			return nil, fmt.Errorf("dispatch/redis: reclaim marshal: %w", marshalErr)
		}

		claimed, claimErr := s.claimExpired(ctx, jID, expectedEpoch, blob)
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

		j, convErr := fromJobEntity(&after)
		if convErr != nil {
			continue
		}
		reclaimed = append(reclaimed, j)
	}

	return reclaimed, nil
}

// claimExpired atomically SETs one expired job's pre-built pending-state
// blob, but only if it is still running at the expected epoch, reporting
// whether this caller was the one that took it.
//
// There is deliberately no re-read after the claim. Go already knows
// exactly what the row now says, because Go built the blob it just wrote.
// An earlier version of this function re-read the entity after a
// successful claim — which meant decoding whatever cjson.encode had just
// produced, and that was precisely the step that turned a large Timeout
// or LeaseTTL into a scientific-notation string encoding/json couldn't
// parse. Because that failure happened inside ReclaimExpiredLeases' loop,
// it aborted the whole scan and abandoned every other expired job already
// found. Go no longer needs to ask Redis what the row says; it already
// knows, because it wrote it.
func (s *Store) claimExpired(ctx context.Context, jID string, epoch int, blob []byte) (bool, error) {
	res, err := reclaimScript.Run(ctx, s.rdb,
		[]string{jobKey(jID)},
		epoch,
		blob,
	).Int64()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return false, fmt.Errorf("dispatch/redis: reclaim claim: %w", err)
	}

	return res == 1, nil
}

// UpdateLeasedJob persists j only while the caller still holds the
// lease.
//
// This is the read-modify-write the column-exclusion shape forces: a
// whole-entity SET can't express "every field except these four," so Go
// reads the current entity, overlays only j's business fields onto it —
// never a pre-serialized blob built from j alone — and hands the result
// to renewLeaseScript for the same compare-and-set RenewLease uses. The
// fence the two need is identical (still running, still this worker,
// still this epoch), so reusing the script means the runner's terminal
// writes now go through the exact same compare-and-set family as
// renewal and cannot race it on this job. See the file comment above.
//
// lease_epoch, lease_expires_at, worker_id, and heartbeat_at are copied
// from the entity Go just read, never from j: j is the caller's stale
// snapshot, and every renewal since it was taken has pushed the real
// expiry forward. Writing j's copy of any of those back would roll the
// current holder's lease backwards even though the script's epoch check
// passes — see job.LeaseStore.UpdateLeasedJob.
func (s *Store) UpdateLeasedJob(ctx context.Context, j *job.Job, workerID id.WorkerID, epoch int) error {
	key := jobKey(j.ID.String())

	var cur jobEntity
	if getErr := s.getEntity(ctx, key, &cur); getErr != nil {
		if isNotFound(getErr) {
			return dispatch.ErrJobNotFound
		}
		return fmt.Errorf("dispatch/redis: update leased job get: %w", getErr)
	}

	next, err := toJobEntity(j)
	if err != nil {
		return err
	}

	next.LeaseEpoch = cur.LeaseEpoch
	next.LeaseExpiresAt = cur.LeaseExpiresAt
	next.WorkerID = cur.WorkerID
	next.HeartbeatAt = cur.HeartbeatAt
	next.UpdatedAt = now()

	blob, marshalErr := json.Marshal(next)
	if marshalErr != nil {
		return fmt.Errorf("dispatch/redis: update leased job marshal: %w", marshalErr)
	}

	res, err := renewLeaseScript.Run(ctx, s.rdb,
		[]string{key},
		workerID.String(),
		epoch,
		blob,
	).Int64()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("dispatch/redis: update leased job: %w", err)
	}
	if res == 1 {
		return nil
	}

	// The read above found the row, so a failed compare-and-set here
	// means the lease moved on between that read and the script running
	// — not that the row is gone. dispatch.ErrJobNotFound is reserved
	// for the case caught above, where the row was already missing.
	return job.ErrLeaseLost
}
