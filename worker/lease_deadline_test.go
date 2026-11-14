package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

type unavailableRenewal struct{ *memory.Store }

func (*unavailableRenewal) RenewLease(context.Context, id.JobID, id.WorkerID, int, time.Time) error {
	return context.DeadlineExceeded
}

func TestLeaseDeadlineCancelsOnUnconfirmedRenewal(t *testing.T) {
	p := NewPool(&unavailableRenewal{memory.New()}, nil, nil, log.NewNoopLogger())
	p.cancelCtx = t.Context()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	key := id.NewJobID().String()
	p.trackJob(key, cancel, nil, 1, time.Second, nil)
	p.armLease(key, time.Now().Add(100*time.Millisecond))
	defer p.untrackJob(key)
	p.sendHeartbeats()
	if ctx.Err() != nil {
		t.Fatal("transient failure cancelled before expiry")
	}
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("unconfirmed lease remained active")
	}
	if !errors.Is(context.Cause(ctx), job.ErrLeaseLost) {
		t.Fatal(context.Cause(ctx))
	}
	p.renewLocalLease(key, 1, time.Now().Add(time.Hour))
	if !errors.Is(context.Cause(ctx), job.ErrLeaseLost) {
		t.Fatal("late renewal revived lost lease")
	}
}

func TestLeaseDeadlineRenewalExtendsOnlyCurrentEpoch(t *testing.T) {
	p := NewPool(memory.New(), nil, nil, log.NewNoopLogger())
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	key := id.NewJobID().String()
	p.trackJob(key, cancel, nil, 2, time.Second, nil)
	initial := time.Now().Add(time.Second)
	p.armLease(key, initial)
	defer p.untrackJob(key)
	p.renewLocalLease(key, 1, initial.Add(time.Hour))
	p.activeMu.Lock()
	got := p.activeJobs[key].leaseUntil
	p.activeMu.Unlock()
	if !got.Equal(initial) {
		t.Fatal("stale epoch extended lease")
	}
	renewed := initial.Add(time.Second)
	p.renewLocalLease(key, 2, renewed)
	p.activeMu.Lock()
	got = p.activeJobs[key].leaseUntil
	p.activeMu.Unlock()
	if !got.Equal(renewed) || ctx.Err() != nil {
		t.Fatal("current lease failed to renew")
	}
}
