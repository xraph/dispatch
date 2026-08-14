package worker

import (
	"context"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// leaseFenceKey is the unexported context key leaseFence values are
// stored under. An unexported type keyed to this package is what stops
// a caller outside worker from colliding with — or forging — the fence.
type leaseFenceKey struct{}

// leaseFence carries what a fenced terminal write needs: the store's
// lease capability, this worker's own ID, and the epoch it was granted
// at claim time.
//
// The epoch travels here rather than being re-read from the job at
// write time on purpose. inflight.leaseEpoch is a process-local copy
// taken once, at the moment DequeueJobs granted it; the job value a
// handler returns is that same claim-time snapshot passed by reference
// through the whole attempt, so reading j.LeaseEpoch here would just be
// a slower way to reach the identical number. What it must NOT become is
// a value re-read from the store: that would defeat the fence by asking
// the row what epoch to check itself against.
type leaseFence struct {
	store    job.LeaseStore
	workerID id.WorkerID
	epoch    int
}

// withLeaseFence attaches f to ctx for the duration of one job attempt.
//
// The pool calls this once, in runJob, before handing ctx to Execute —
// never inside Runner itself, which has no notion of "the pool's
// tracked epoch" and must not grow one. A Runner used directly, without
// a Pool (see NewExecutor, and every runner_test.go case that calls
// Execute against a bare context), simply never sees a fence and keeps
// today's unfenced UpdateJob — see leaseFenceFromContext.
func withLeaseFence(ctx context.Context, f leaseFence) context.Context {
	return context.WithValue(ctx, leaseFenceKey{}, f)
}

// leaseFenceFromContext returns the fence attached by withLeaseFence, if
// ctx carries one.
//
// ok is false whenever no fence was attached — a store that does not
// implement job.LeaseStore, or a Runner driven directly without a Pool
// — and every caller here treats that identically to "use the unfenced
// path," which is the backward-compatibility guarantee: a store that
// implements only job.Store must keep behaving exactly as it did before
// this method existed.
func leaseFenceFromContext(ctx context.Context) (leaseFence, bool) {
	f, ok := ctx.Value(leaseFenceKey{}).(leaseFence)

	return f, ok
}
