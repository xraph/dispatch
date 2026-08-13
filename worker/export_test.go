package worker

import (
	"context"
	"time"

	"github.com/xraph/dispatch/job"
)

// LeaseTTLFor exposes the unexported TTL resolution to worker_test.
func (p *Pool) LeaseTTLFor(j *job.Job) time.Duration { return p.leaseTTLFor(j) }

// ReapInterval exposes the resolved scan cadence to worker_test.
func (p *Pool) ReapInterval() time.Duration { return p.resolvedReapInterval() }

// ReclaimOnce runs a single reclamation pass against ctx.
//
// reapStaleJobs reaches the store through callCtx, which derives from
// cancelCtx — normally set by Start. This wires it directly so a test can
// drive one pass without running the pool's goroutines.
func (p *Pool) ReclaimOnce(ctx context.Context) {
	p.cancelCtx, p.cancelFunc = context.WithCancel(ctx)
	defer p.cancelFunc()
	p.reapStaleJobs()
}

// TrackJob creates a cancellable context for jobID and records it via the
// pool's own trackJob, exactly as runJob does for a real attempt. It
// returns the context so a test can observe whether — and via
// context.Cause, why — a later heartbeat/renewal pass cancelled it,
// without running the pool's goroutines or the executor.
func (p *Pool) TrackJob(jobID string, leaseEpoch int, leaseTTL time.Duration) context.Context {
	ctx, cancel := context.WithCancelCause(context.Background())
	p.trackJob(jobID, cancel, nil, leaseEpoch, leaseTTL)

	return ctx
}

// HeartbeatOnce runs a single heartbeat/renewal pass against ctx.
//
// sendHeartbeats reaches the store through callCtx, which derives from
// cancelCtx — normally set by Start. This wires it directly so a test can
// drive one pass without running the pool's goroutines, mirroring
// ReclaimOnce above.
func (p *Pool) HeartbeatOnce(ctx context.Context) {
	p.cancelCtx, p.cancelFunc = context.WithCancel(ctx)
	defer p.cancelFunc()
	p.sendHeartbeats()
}
