package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// The compile-time check that this store provides the lease capability
// lives in store.go alongside the other interface assertions.
//
// The grant itself is not in this file: it travels on job.DequeueOpts and
// is applied by claimOne, inside the same FindOneAndUpdate that claims the
// document, so a leased claim carries the fit predicate and the ordering
// like any other. This file holds only what a lease needs afterwards.

// RenewLease extends the lease only if the caller still holds it.
func (s *Store) RenewLease(
	ctx context.Context,
	jobID id.JobID,
	workerID id.WorkerID,
	epoch int,
	leaseUntil time.Time,
) error {
	t := now()
	col := s.mdb.Collection(colJobs)

	filter := bson.M{
		"_id":         jobID.String(),
		"state":       string(job.StateRunning),
		"worker_id":   workerID.String(),
		"lease_epoch": epoch,
	}
	update := bson.M{"$set": bson.M{
		"lease_expires_at": leaseUntil.UTC(),
		"heartbeat_at":     t,
		"updated_at":       t,
	}}

	var matched int64
	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		r, updErr := col.UpdateOne(ctx, filter, update)
		if updErr != nil {
			return updErr
		}
		matched = r.MatchedCount

		return nil
	})
	if err != nil {
		return fmt.Errorf("dispatch/mongo: renew lease: %w", err)
	}
	if matched == 0 {
		return job.ErrLeaseLost
	}

	return nil
}

// ReclaimExpiredLeases returns expired-lease jobs to pending, fencing
// their previous holders.
//
// Each job is claimed by its own conditional FindOneAndUpdate keyed on the
// epoch it was seen at, so two pools reclaiming concurrently cannot both
// take the same job — the loser's filter no longer matches.
func (s *Store) ReclaimExpiredLeases(ctx context.Context, limit int) ([]*job.Job, error) {
	if limit <= 0 {
		return nil, nil
	}

	t := now()
	col := s.mdb.Collection(colJobs)

	// The first branch is the actual rule: a lease was granted and has
	// lapsed. The other two are the narrow exception for a running job
	// carrying no lease at all, gated on silence rather than on the null
	// expiry alone — see job.UnleasedReclaimGrace for why a null expiry
	// does not by itself mean the job was abandoned, and why a row with
	// neither timestamp is deliberately left alone.
	//
	// Testing lease_expires_at against null rather than $exists is
	// load-bearing: this collection holds both shapes for the same absent
	// value, because grove's insert path writes an explicit null while the
	// driver's own encoder honors omitempty and drops the key (see the
	// comment on jobModel.ResourceRequests). Plain null equality is the one
	// test that matches both.
	silent := t.Add(-job.UnleasedReclaimGrace)
	filter := bson.M{
		"state": string(job.StateRunning),
		"$or": bson.A{
			bson.M{"lease_expires_at": bson.M{"$ne": nil, "$lte": t}},
			bson.M{
				"lease_expires_at": nil,
				"heartbeat_at":     bson.M{"$ne": nil, "$lte": silent},
			},
			bson.M{
				"lease_expires_at": nil,
				"heartbeat_at":     nil,
				"started_at":       bson.M{"$ne": nil, "$lte": silent},
			},
		},
	}
	update := bson.M{
		"$set": bson.M{
			"state":            string(job.StatePending),
			"run_at":           t,
			"worker_id":        "",
			"started_at":       nil,
			"heartbeat_at":     nil,
			"lease_expires_at": nil,
			"updated_at":       t,
		},
		"$inc": bson.M{
			"lease_epoch": 1,
			"evict_count": 1,
		},
	}
	// Mongo sorts null and missing before every date, so the unleased rows
	// matched by the exception above are taken first. That is the right
	// order (they have been stranded longest) and it cannot starve the
	// leased ones, because each claim moves the row to pending and it stops
	// matching the filter.
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetSort(bson.D{{Key: "lease_expires_at", Value: 1}})

	jobs := make([]*job.Job, 0, limit)
	for len(jobs) < limit {
		var m jobModel
		err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
			return col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&m)
		})
		if err != nil {
			if isNoDocuments(err) {
				break
			}

			return nil, fmt.Errorf("dispatch/mongo: reclaim expired leases: %w", err)
		}

		j, convErr := fromJobModel(&m)
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: reclaim convert: %w", convErr)
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}

// UpdateLeasedJob persists j only while the caller still holds the
// lease.
//
// Unlike UpdateJob's ReplaceOne, this is an UpdateOne with the fence
// predicate IN THE FILTER and a $set of named business fields — no
// read-modify-write needed, and the document is never replaced wholesale.
// worker_id, lease_epoch, lease_expires_at, and heartbeat_at are never
// assigned in $set: j is the caller's stale snapshot, so writing its copy
// of lease_expires_at back would roll the current holder's expiry
// backwards even though the filter's epoch check passed.
//
// resource_requests and resource_limits mirror toJobModel's zero-Set
// handling: a zero Set is $unset rather than $set to an empty document,
// matching the "absent key, not an empty subdocument" contract UpdateJob
// keeps via bson "omitempty" on ReplaceOne.
func (s *Store) UpdateLeasedJob(ctx context.Context, j *job.Job, workerID id.WorkerID, epoch int) error {
	m := toJobModel(j)
	m.UpdatedAt = now()

	col := s.mdb.Collection(colJobs)

	filter := bson.M{
		"_id":         m.ID,
		"state":       string(job.StateRunning),
		"worker_id":   workerID.String(),
		"lease_epoch": epoch,
	}

	set := bson.M{
		"name":               m.Name,
		"queue":              m.Queue,
		"payload":            m.Payload,
		"state":              m.State,
		"priority":           m.Priority,
		"max_retries":        m.MaxRetries,
		"retry_count":        m.RetryCount,
		"last_error":         m.LastError,
		"scope_app_id":       m.ScopeAppID,
		"scope_org_id":       m.ScopeOrgID,
		"run_at":             m.RunAt,
		"started_at":         m.StartedAt,
		"completed_at":       m.CompletedAt,
		"timeout":            m.Timeout,
		"created_at":         m.CreatedAt,
		"updated_at":         m.UpdatedAt,
		"lease_ttl":          m.LeaseTTL,
		"evict_count":        m.EvictCount,
		"req_cpu_milli":      m.ReqCPUMilli,
		"req_memory_bytes":   m.ReqMemoryBytes,
		"req_disk_bytes":     m.ReqDiskBytes,
		"req_gpu_milli":      m.ReqGPUMilli,
		"req_custom_keys":    m.ReqCustomKeys,
		"resource_class":     m.ResourceClass,
		"input_bytes":        m.InputBytes,
		"primary_input_hash": m.PrimaryInputHash,
	}

	unset := bson.M{}
	if m.ResourceRequests.IsZero() {
		unset["resource_requests"] = ""
	} else {
		set["resource_requests"] = m.ResourceRequests
	}
	if m.ResourceLimits.IsZero() {
		unset["resource_limits"] = ""
	} else {
		set["resource_limits"] = m.ResourceLimits
	}

	update := bson.M{"$set": set}
	if len(unset) > 0 {
		update["$unset"] = unset
	}

	// No withRetry here, deliberately, unlike RenewLease above (and
	// UpdateJob never uses it either). RenewLease's filter is safe to
	// retry because its own $set never touches the fields the filter
	// tests — state, worker_id, lease_epoch — so re-matching after an
	// already-applied write reapplies the same lease_expires_at and
	// heartbeat_at values, which is idempotent by construction. This
	// filter is not: it requires state:"running" while $set moves state
	// to whatever terminal (or retrying) value the caller asked for, so a
	// retry after the first attempt actually landed would find zero
	// matching documents, fall through to the existence check below, and
	// return ErrLeaseLost for a write that already succeeded — worst case
	// on sendToDLQ, where the row is genuinely marked failed but the
	// runner, believing the lease was lost, returns before ever reaching
	// dlqService.Push. Matching UpdateJob's no-retry choice here is what
	// keeps this method's error exactly as trustworthy as the write it
	// reports on.
	r, err := col.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: update leased job: %w", err)
	}
	if r.MatchedCount > 0 {
		return nil
	}

	// Zero matches means either the fence predicate failed (the lease
	// moved on) or the document is gone. Only the latter is
	// ErrJobNotFound; the former is ErrLeaseLost, the entire point of
	// this method.
	count, countErr := col.CountDocuments(ctx, bson.M{"_id": m.ID})
	if countErr != nil {
		return fmt.Errorf("dispatch/mongo: update leased job existence check: %w", countErr)
	}
	if count == 0 {
		return dispatch.ErrJobNotFound
	}

	return job.ErrLeaseLost
}
