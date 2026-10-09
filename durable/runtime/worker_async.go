package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

type handoffSession struct {
	worker     *Worker
	lease      *taskLease
	heartbeats *heartbeatSession
	task       durable.Task
	payload    taskPayload
	buildID    string
	attempt    int64
	secret     string
	pending    *durable.CommitRequest
	handle     AsyncActivityHandle
}

func (s *handoffSession) deferCompletion(ctx context.Context) (AsyncActivityHandle, error) {
	callCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.heartbeats.ctx, func() { cancel(context.Cause(s.heartbeats.ctx)) })
	defer func() { stop(); cancel(nil) }()
	// Renewal and handoff share ownership first; ordinary heartbeats take only
	// the progress gate. Holding both keeps the returned checkpoint stable.
	if err := takeGate(callCtx, s.lease.gate); err != nil {
		return AsyncActivityHandle{}, err
	}
	defer func() { s.lease.gate <- struct{}{} }()
	if err := takeGate(callCtx, s.heartbeats.gate); err != nil {
		return AsyncActivityHandle{}, err
	}
	defer func() { s.heartbeats.gate <- struct{}{} }()
	if s.heartbeats.ctx.Err() != nil {
		return AsyncActivityHandle{}, context.Cause(s.heartbeats.ctx)
	}
	if s.heartbeats.detached {
		return s.handle, nil
	}
	for range 16 {
		if s.pending == nil {
			if err := s.prepare(callCtx); err != nil {
				return AsyncActivityHandle{}, err
			}
		}
		_, err := s.worker.persistReceipt(callCtx, *s.pending)
		if err == nil {
			s.heartbeats.detached, s.lease.detached = true, true
			s.pending = nil
			return s.handle, nil
		}
		if errors.Is(err, durable.ErrRevisionConflict) || errors.Is(err, durable.ErrTaskConflict) {
			s.pending = nil
			continue
		}
		// An unknown outcome retains this exact transaction and its handle.
		// A later explicit call resolves that request before reading newer state.
		return AsyncActivityHandle{}, err
	}
	return AsyncActivityHandle{}, durable.ErrRevisionConflict
}

func (s *handoffSession) prepare(ctx context.Context) error {
	if s.heartbeats.pending != nil {
		if err := s.heartbeats.persist(ctx); err != nil {
			return err
		}
	}
	execution, history, err := s.worker.effectSnapshot(ctx, s.task, s.payload.Command)
	if err != nil {
		return err
	}
	prior := history.attempts[s.payload.Command.ID]
	if prior.failed || prior.handoff != nil || prior.value.Attempt != s.attempt || prior.value.Epoch != s.task.Epoch {
		return s.worker.effectConflict(ctx, s.task, "handoff does not match active attempt")
	}
	checkpoint, condition, err := s.worker.activityCheckpoint(ctx, s.task, prior)
	if err != nil {
		return err
	}
	if checkpoint == nil || condition == nil {
		return fmt.Errorf("%w: handoff requires active progress tracking", durable.ErrInvalid)
	}
	prior.value.Heartbeat = checkpoint
	deadline, _, err := activityDeadline(s.payload.Command, history.scheduled[s.payload.Command.ID], prior)
	if err != nil {
		return err
	}
	if deadline.IsZero() {
		return fmt.Errorf("%w: asynchronous handoff requires a finite activity deadline", durable.ErrInvalid)
	}
	if s.secret == "" {
		secret := make([]byte, 32)
		if _, err = rand.Read(secret); err != nil {
			return err
		}
		s.secret = hex.EncodeToString(secret)
	}
	hash, err := durable.HashAsyncSecret(s.secret)
	if err != nil {
		return err
	}
	event, err := json.Marshal(ActivityHandoff{Version: 1, CommandID: s.payload.Command.ID, Attempt: s.attempt, Epoch: s.task.Epoch, Heartbeat: checkpoint})
	if err != nil {
		return err
	}
	request := taskRequest(s.task, execution.Revision)
	request.RequestID = fmt.Sprintf("handoff:%s:%d", s.task.ID, s.task.Epoch)
	request.Events = []durable.EventInput{{Type: EventActivityDeferred, Payload: event}}
	request.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskAwait, AsyncKeyHash: hash}
	request.Conditions = []durable.TaskCondition{*condition}
	token := s.task.Token()
	token.LeaseKind = durable.LeaseAsync
	s.handle = AsyncActivityHandle{Version: 1, Key: s.task.Key, BuildID: s.buildID, Token: token, Secret: s.secret, InitialHeartbeatSequence: checkpoint.Sequence}
	s.pending = &request
	return nil
}
