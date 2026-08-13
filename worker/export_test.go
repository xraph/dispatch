package worker

import (
	"time"

	"github.com/xraph/dispatch/job"
)

// LeaseTTLFor exposes the unexported TTL resolution to worker_test.
func (p *Pool) LeaseTTLFor(j *job.Job) time.Duration { return p.leaseTTLFor(j) }
