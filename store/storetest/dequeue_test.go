package storetest_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

// TestSuiteAgainstReference runs the conformance suite against the
// reference store below.
//
// The suite ships before any backend implements the widened signature, so
// without this it would be an untested specification. The reference is
// deliberately the most literal implementation of the contract there is —
// it filters with job.DequeueOpts.Allows and orders with
// job.DequeueOpts.Less — which makes this test a check that the suite's
// cases are mutually consistent and that the exported predicate helpers
// actually satisfy them.
func TestSuiteAgainstReference(t *testing.T) {
	storetest.RunDequeueSuite(t, func(_ *testing.T) job.Store {
		return newReferenceStore()
	})
}

// referenceStore is a minimal in-process job.Store. It exists only to
// exercise the suite; the real backends live under store/.
type referenceStore struct {
	mu   sync.Mutex
	jobs map[id.JobID]*job.Job
}

func newReferenceStore() *referenceStore {
	return &referenceStore{jobs: make(map[id.JobID]*job.Job)}
}

func cloneRefJob(j *job.Job) *job.Job {
	out := *j
	out.Resources = j.Resources.Clone()
	out.ResourceLimits = j.ResourceLimits.Clone()

	return &out
}

func (r *referenceStore) EnqueueJob(_ context.Context, j *job.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.jobs[j.ID]; exists {
		return dispatch.ErrJobAlreadyExists
	}

	r.jobs[j.ID] = cloneRefJob(j)

	return nil
}

// DequeueJobs selects, orders, limits, and only then claims — the order
// the contract requires.
func (r *referenceStore) DequeueJobs(_ context.Context, opts job.DequeueOpts) ([]*job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	queues := make(map[string]struct{}, len(opts.Queues))
	for _, q := range opts.Queues {
		queues[q] = struct{}{}
	}

	now := time.Now().UTC()

	candidates := make([]*job.Job, 0, len(r.jobs))

	for _, j := range r.jobs {
		if j.State != job.StatePending && j.State != job.StateRetrying {
			continue
		}

		if !j.RunAt.IsZero() && j.RunAt.After(now) {
			continue
		}

		if len(queues) > 0 {
			if _, ok := queues[j.Queue]; !ok {
				continue
			}
		}

		if !opts.Allows(j) {
			continue
		}

		candidates = append(candidates, j)
	}

	sort.SliceStable(candidates, func(a, b int) bool {
		return opts.Less(candidates[a], candidates[b])
	})

	if opts.Limit > 0 && len(candidates) > opts.Limit {
		candidates = candidates[:opts.Limit]
	}

	claimed := make([]*job.Job, 0, len(candidates))

	for _, j := range candidates {
		started := now
		j.State = job.StateRunning
		j.StartedAt = &started

		claimed = append(claimed, cloneRefJob(j))
	}

	return claimed, nil
}

func (r *referenceStore) GetJob(_ context.Context, jobID id.JobID) (*job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	j, ok := r.jobs[jobID]
	if !ok {
		return nil, dispatch.ErrJobNotFound
	}

	return cloneRefJob(j), nil
}

func (r *referenceStore) UpdateJob(_ context.Context, j *job.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.jobs[j.ID]; !ok {
		return dispatch.ErrJobNotFound
	}

	r.jobs[j.ID] = cloneRefJob(j)

	return nil
}

func (r *referenceStore) DeleteJob(_ context.Context, jobID id.JobID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.jobs, jobID)

	return nil
}

func (r *referenceStore) ListJobsByState(
	_ context.Context, state job.State, _ job.ListOpts,
) ([]*job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]*job.Job, 0, len(r.jobs))

	for _, j := range r.jobs {
		if j.State == state {
			out = append(out, cloneRefJob(j))
		}
	}

	return out, nil
}

func (r *referenceStore) HeartbeatJob(_ context.Context, jobID id.JobID, _ id.WorkerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	j, ok := r.jobs[jobID]
	if !ok {
		return dispatch.ErrJobNotFound
	}

	beat := time.Now().UTC()
	j.HeartbeatAt = &beat

	return nil
}

func (r *referenceStore) ReapStaleJobs(_ context.Context, _ time.Duration) ([]*job.Job, error) {
	return nil, nil
}

func (r *referenceStore) CountJobs(_ context.Context, _ job.CountOpts) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return int64(len(r.jobs)), nil
}
