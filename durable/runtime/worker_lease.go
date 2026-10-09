package runtime

import (
	"context"
	"time"

	"github.com/xraph/dispatch/durable"
)

// taskLease serializes worker renewal with an asynchronous ownership transfer.
// A detached lease stops renewal without cancelling the handler's context.
type taskLease struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	gate     chan struct{}
	done     chan struct{}
	detached bool // protected by gate
	// reconcile runs under gate before normal renewal. False suspends renewal
	// while a sent ownership transfer has an unknown outcome.
	reconcile func(context.Context) (bool, error)
}

func newTaskLease(ctx context.Context, w *Worker, task durable.Task) *taskLease {
	bound, cancel := context.WithCancelCause(ctx)
	lease := &taskLease{ctx: bound, cancel: cancel, gate: make(chan struct{}, 1), done: make(chan struct{})}
	lease.gate <- struct{}{}
	go lease.renew(w, task)
	return lease
}

func (l *taskLease) renew(w *Worker, task durable.Task) {
	defer close(l.done)
	for wait(l.ctx, w.renewInterval(task)) == nil {
		if err := takeGate(l.ctx, l.gate); err != nil {
			return
		}
		if l.detached {
			l.gate <- struct{}{}
			return
		}
		renew := true
		var err error
		if l.reconcile != nil {
			renew, err = l.reconcile(l.ctx)
		}
		if err == nil && renew {
			_, err = storeCall(l.ctx, w, func(ctx context.Context) (time.Time, error) {
				return w.store.RenewTask(ctx, task.Key, task.Token(), w.options.LeaseDuration)
			})
		}
		if err != nil {
			l.cancel(err)
		}
		stop := l.detached || err != nil
		l.gate <- struct{}{}
		if stop {
			return
		}
	}
}

func (l *taskLease) close() {
	l.cancel(nil)
	<-l.done
}

func takeGate(ctx context.Context, gate chan struct{}) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-gate:
		if ctx.Err() != nil {
			gate <- struct{}{}
			return context.Cause(ctx)
		}
		return nil
	}
}
