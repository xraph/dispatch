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
