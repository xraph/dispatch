package worker

import (
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/job"
)

// Ready reports whether the worker is accepting work and its last poll succeeded.
func (p *Pool) Ready() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.running {
		return errors.New("worker pool is stopped")
	}
	if !p.polled {
		return errors.New("worker pool has not polled yet")
	}
	if p.pollErr != nil {
		return fmt.Errorf("worker poll failed: %w", p.pollErr)
	}
	return nil
}

func (p *Pool) armLease(id string, until time.Time) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	rec := p.activeJobs[id]
	if rec == nil {
		return
	}
	rec.leaseUntil = until
	rec.leaseTimer = time.AfterFunc(time.Until(until), func() { p.expireLease(id, rec) })
}

func (p *Pool) renewLocalLease(id string, epoch int, until time.Time) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	rec := p.activeJobs[id]
	if rec == nil || rec.leaseEpoch != epoch || rec.leaseTimer == nil || !time.Now().Before(rec.leaseUntil) {
		return
	}
	rec.leaseUntil = until
	rec.leaseTimer.Reset(time.Until(until))
}

func (p *Pool) expireLease(id string, rec *inflight) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	if p.activeJobs[id] != rec {
		return
	}
	if remaining := time.Until(rec.leaseUntil); remaining > 0 {
		rec.leaseTimer.Reset(remaining)
		return
	}
	rec.cancel(job.ErrLeaseLost)
}
