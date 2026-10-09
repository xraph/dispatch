package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/xraph/dispatch/durable"
)

type heartbeatSession struct {
	worker      *Worker
	task        durable.Task
	ctx         context.Context
	cancel      context.CancelCauseFunc
	gate        chan struct{}
	sequence    int64
	pending     *durable.HeartbeatRequest
	pendingSent bool
	detached    bool // protected by gate
}

func newHeartbeatSession(ctx context.Context, worker *Worker, task durable.Task) *heartbeatSession {
	bound, cancel := context.WithCancelCause(ctx)
	session := &heartbeatSession{worker: worker, task: task, ctx: bound, cancel: cancel, gate: make(chan struct{}, 1)}
	session.gate <- struct{}{}
	return session
}

func (s *heartbeatSession) close() {
	s.cancel(nil)
	// An in-flight call must settle before the final persisted checkpoint is read.
	<-s.gate
	s.gate <- struct{}{}
}

func (s *heartbeatSession) record(ctx context.Context, details []byte) error {
	if len(details) > 1<<20 {
		return fmt.Errorf("%w: heartbeat progress exceeds 1 MiB", durable.ErrInvalid)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	case <-s.gate:
	}
	defer func() { s.gate <- struct{}{} }()
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if s.ctx.Err() != nil {
		return context.Cause(s.ctx)
	}
	if s.detached {
		return durable.ErrLeaseLost
	}
	callCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.ctx, func() { cancel(context.Cause(s.ctx)) })
	defer func() { stop(); cancel(nil) }()
	if s.pending != nil {
		same := bytes.Equal(s.pending.Progress, details)
		if err := s.persist(callCtx); err != nil {
			return err
		}
		if same {
			return nil
		}
	}
	if s.sequence == math.MaxInt64 {
		return fmt.Errorf("%w: heartbeat sequence exhausted", durable.ErrInvalid)
	}
	s.pending = &durable.HeartbeatRequest{Key: s.task.Key, RequestID: fmt.Sprintf("heartbeat:%s:%d:%d", s.task.ID, s.task.Epoch, s.sequence+1),
		Token: s.task.Token(), Sequence: s.sequence + 1, Progress: bytes.Clone(details), LeaseDuration: s.worker.options.LeaseDuration}
	return s.persist(callCtx)
}

func (s *heartbeatSession) persist(ctx context.Context) error {
	var last error
	for attempt := range 3 {
		if ctx.Err() != nil {
			if !s.pendingSent {
				s.pending = nil
			}
			return context.Cause(ctx)
		}
		s.pendingSent = true
		_, last = storeCall(ctx, s.worker, func(callCtx context.Context) (durable.Receipt, error) {
			return s.worker.store.RecordHeartbeat(callCtx, *s.pending)
		})
		if last == nil {
			s.sequence, s.pending = s.pending.Sequence, nil
			s.pendingSent = false
			return nil
		}
		if normalContention(last) || errors.Is(last, durable.ErrInvalid) || errors.Is(last, durable.ErrRequestConflict) || errors.Is(last, durable.ErrNotFound) {
			s.cancel(last)
			return last
		}
		if attempt < 2 {
			if err := wait(ctx, time.Duration(attempt+1)*10*time.Millisecond); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("persist activity heartbeat after retries: %w", last)
}
