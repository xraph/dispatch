package operator

import (
	"context"
	"errors"
	"strconv"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
)

const maxStartInputBytes = 1 << 20

var (
	ErrBuildMismatch       = errors.New("dispatch: execution build mismatch")
	ErrRuntimeUnavailable  = errors.New("dispatch: compatible runtime unavailable")
	ErrHistoryIncompatible = errors.New("dispatch: execution history incompatible")
)

// StartInput keeps byte payloads intact. Remote starts use the runtime defaults
// for execution timeouts and retries; internal worker APIs retain richer options.
type StartInput struct {
	durable.Key
	RequestID    string `json:"request_id"`
	WorkflowType string `json:"workflow_type"`
	BuildID      string `json:"build_id"`
	Queue        string `json:"queue"`
	Input        []byte `json:"input,omitempty"`
}

func (i StartInput) request() durable.StartRequest {
	return durable.StartRequest{Key: i.Key, RequestID: i.RequestID, WorkflowType: i.WorkflowType, BuildID: i.BuildID, Queue: i.Queue, Input: i.Input}
}

type Acceptance struct {
	durable.Key
	RequestID     string `json:"request_id"`
	Revision      string `json:"revision"`
	FirstSequence string `json:"first_sequence"`
	LastSequence  string `json:"last_sequence"`
	Status        string `json:"status"`
	Started       bool   `json:"started,omitempty"`
}

func accepted(key durable.Key, requestID, status string, receipt durable.Receipt) Acceptance {
	return Acceptance{Key: key, RequestID: requestID, Status: status, Revision: strconv.FormatInt(receipt.Revision, 10), FirstSequence: strconv.FormatInt(receipt.FirstSequence, 10), LastSequence: strconv.FormatInt(receipt.LastSequence, 10)}
}
func commandError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, durable.ErrBuildMismatch) {
		return ErrBuildMismatch
	}
	for _, known := range []error{ErrBuildMismatch, ErrRuntimeUnavailable, ErrHistoryIncompatible, durable.ErrRequestConflict, durable.ErrRevisionConflict, durable.ErrClosed, durable.ErrExists, durable.ErrLeaseLost, durable.ErrTaskDeadline, durable.ErrExecutionDeadline, drt.ErrQueryMutation, drt.ErrQueryNotFound} {
		if errors.Is(err, known) {
			return known
		}
	}
	if errors.Is(err, drt.ErrHistory) || errors.Is(err, drt.ErrNondeterministic) {
		return ErrHistoryIncompatible
	}
	if errors.Is(err, drt.ErrHandlerNotFound) {
		return ErrRuntimeUnavailable
	}
	return safeError(err)
}
func (s *Service) worker(namespace, build string) (*drt.Worker, error) {
	if s.runtime == nil {
		return nil, ErrRuntimeUnavailable
	}
	w, err := s.runtime(namespace, build)
	if err != nil || !w.ServesBuild(namespace, build) {
		return nil, ErrRuntimeUnavailable
	}
	return w, nil
}
func commandContext(ctx context.Context, p security.Principal, requestID string) context.Context {
	metadata := durable.AuditMetadataFromContext(ctx)
	actor := security.Metadata(p)
	metadata.ActorKind, metadata.ActorID = actor.ActorKind, actor.ActorID
	metadata.RequestID = requestID
	// No provider policy decision or correlation identity is invented here.
	return durable.WithAuditMetadata(ctx, metadata)
}
func (s *Service) commandRun(ctx context.Context, p security.Principal, action string, key durable.Key, build string) (durable.Execution, error) {
	if p.Validate() != nil {
		return durable.Execution{}, s.denied(ctx, p, action, security.ErrUnauthenticated)
	}
	if key.Validate() != nil {
		return durable.Execution{}, durable.ErrInvalid
	}
	// Resolve immutable facts before evaluating run policy. Nothing is disclosed
	// until the persisted ownership and exact execution have passed authorization.
	e, err := s.store.GetExecution(ctx, key)
	if err != nil {
		if checkErr := s.check(ctx, p, action, key); checkErr != nil {
			return durable.Execution{}, checkErr
		}
		return durable.Execution{}, safeError(err)
	}
	if err := s.checkFacts(ctx, p, action, e.Key, e.WorkflowType, e.BuildID); err != nil {
		return durable.Execution{}, err
	}
	if build != e.BuildID {
		return durable.Execution{}, ErrBuildMismatch
	}
	return e, nil
}
func validCommandID(id string) bool { return durable.DeliveryIdentifier(id) }

func (s *Service) Start(ctx context.Context, p security.Principal, in StartInput) (Acceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	r := in.request()
	if r.Validate() != nil || !validCommandID(r.RequestID) || len(r.Input) > maxStartInputBytes {
		return Acceptance{}, durable.ErrInvalid
	}
	if err := s.checkFacts(ctx, p, StartWorkflow, r.Key, r.WorkflowType, r.BuildID); err != nil {
		return Acceptance{}, err
	}
	// Existing starts must be authorized from their persisted facts on replay.
	if e, err := s.store.GetExecution(ctx, r.Key); err == nil {
		if checkErr := s.checkFacts(ctx, p, StartWorkflow, e.Key, e.WorkflowType, e.BuildID); checkErr != nil {
			return Acceptance{}, checkErr
		}
	} else if !errors.Is(err, durable.ErrNotFound) {
		return Acceptance{}, safeError(err)
	}
	w, err := s.worker(r.Namespace, r.BuildID)
	if err != nil {
		return Acceptance{}, err
	}
	receipt, err := w.StartExecution(commandContext(ctx, p, r.RequestID), r)
	if err != nil {
		return Acceptance{}, commandError(err)
	}
	out := accepted(r.Key, r.RequestID, "accepted", receipt)
	out.Started = true
	return out, nil
}
func (s *Service) Signal(ctx context.Context, p security.Principal, r durable.SignalRequest) (Acceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	if r.Validate() != nil || r.Key.Validate() != nil || !validCommandID(r.RequestID) {
		return Acceptance{}, durable.ErrInvalid
	}
	e, err := s.commandRun(ctx, p, SignalWorkflow, r.Key, r.BuildID)
	if err != nil {
		return Acceptance{}, err
	}
	w, err := s.worker(e.Namespace, e.BuildID)
	if err != nil {
		return Acceptance{}, err
	}
	receipt, err := w.SignalExecution(commandContext(ctx, p, r.RequestID), r)
	if err != nil {
		return Acceptance{}, commandError(err)
	}
	return accepted(receipt.Key, r.RequestID, "accepted", receipt.Receipt), nil
}
func (s *Service) Cancel(ctx context.Context, p security.Principal, r durable.CancelExecutionRequest) (Acceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	if r.Validate() != nil || r.Key.Validate() != nil || !validCommandID(r.RequestID) {
		return Acceptance{}, durable.ErrInvalid
	}
	e, err := s.commandRun(ctx, p, CancelWorkflow, r.Key, r.BuildID)
	if err != nil {
		return Acceptance{}, err
	}
	w, err := s.worker(e.Namespace, e.BuildID)
	if err != nil {
		return Acceptance{}, err
	}
	receipt, err := w.RequestCancelExecution(commandContext(ctx, p, r.RequestID), r)
	if err != nil {
		return Acceptance{}, commandError(err)
	}
	return accepted(receipt.Key, r.RequestID, "cancellation_requested", receipt.Receipt), nil
}
