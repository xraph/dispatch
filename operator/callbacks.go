package operator

import (
	"context"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
)

// CallbackHandle is credential material, supplied only by the callback caller.
// The HTTP wire uses decimal strings for every 64-bit counter.
type CallbackHandle struct {
	Version                  int           `json:"version"`
	Key                      durable.Key   `json:"key"`
	BuildID                  string        `json:"build_id"`
	Token                    CallbackToken `json:"token"`
	Secret                   string        `json:"secret"`
	InitialHeartbeatSequence int64         `json:"initial_heartbeat_sequence,string"`
}
type CallbackToken struct {
	TaskID    string                `json:"task_id"`
	Owner     string                `json:"owner"`
	Epoch     int64                 `json:"epoch,string"`
	LeaseKind durable.TaskLeaseKind `json:"lease_kind"`
}

func (h CallbackHandle) String() string   { return "CallbackHandle{redacted}" }
func (h CallbackHandle) GoString() string { return h.String() }
func (h CallbackHandle) runtime() drt.AsyncActivityHandle {
	return drt.AsyncActivityHandle{Version: h.Version, Key: h.Key, BuildID: h.BuildID, Secret: h.Secret, InitialHeartbeatSequence: h.InitialHeartbeatSequence, Token: durable.TaskToken{TaskID: h.Token.TaskID, Owner: h.Token.Owner, Epoch: h.Token.Epoch, LeaseKind: h.Token.LeaseKind}}
}

type CompletionInput struct {
	Handle    CallbackHandle        `json:"handle"`
	RequestID string                `json:"request_id"`
	Output    []byte                `json:"output,omitempty"`
	Failure   *drt.ApplicationError `json:"failure,omitempty"`
}
type HeartbeatInput struct {
	Handle    CallbackHandle `json:"handle"`
	RequestID string         `json:"request_id"`
	Sequence  int64          `json:"sequence,string"`
	Details   []byte         `json:"details,omitempty"`
}

func (s *Service) callback(ctx context.Context, p security.Principal, action string, h CallbackHandle, requestID string) (*drt.Worker, error) {
	if p.Validate() != nil {
		return nil, s.denied(ctx, p, action, security.ErrUnauthenticated)
	}
	switch p.Kind {
	case "service", "api_key", "service_acct":
	default:
		return nil, s.denied(ctx, p, action, security.ErrForbidden)
	}
	if h.runtime().Validate() != nil || !validCommandID(requestID) {
		return nil, durable.ErrInvalid
	}
	e, err := s.commandRun(ctx, p, action, h.Key, h.BuildID)
	if err != nil {
		return nil, err
	}
	return s.worker(e.Namespace, e.BuildID)
}
func (s *Service) Complete(ctx context.Context, p security.Principal, in CompletionInput) (Acceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	if len(in.Output) > 1<<20 {
		return Acceptance{}, durable.ErrInvalid
	}
	w, err := s.callback(ctx, p, CompleteActivity, in.Handle, in.RequestID)
	if err != nil {
		return Acceptance{}, err
	}
	receipt, err := w.CompleteAsyncActivity(commandContext(ctx, p, in.RequestID), drt.AsyncCompletionRequest{Handle: in.Handle.runtime(), RequestID: in.RequestID, Output: in.Output, Failure: in.Failure})
	if err != nil {
		return Acceptance{}, commandError(err)
	}
	return accepted(in.Handle.Key, in.RequestID, "accepted", receipt), nil
}
func (s *Service) Heartbeat(ctx context.Context, p security.Principal, in HeartbeatInput) (Acceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	if len(in.Details) > 1<<20 {
		return Acceptance{}, durable.ErrInvalid
	}
	w, err := s.callback(ctx, p, HeartbeatActivity, in.Handle, in.RequestID)
	if err != nil {
		return Acceptance{}, err
	}
	receipt, err := w.HeartbeatAsyncActivity(commandContext(ctx, p, in.RequestID), drt.AsyncHeartbeatRequest{Handle: in.Handle.runtime(), RequestID: in.RequestID, Sequence: in.Sequence, Details: in.Details})
	if err != nil {
		return Acceptance{}, commandError(err)
	}
	return accepted(in.Handle.Key, in.RequestID, "accepted", receipt), nil
}
