package operator

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

// QueryReservationInput identifies the immutable accepted reservation. The
// version is the original pre-reservation version, not the current binding.
type QueryReservationInput struct {
	QueryRuntimeInput
	ReservationRequestID string `json:"reservation_request_id"`
	ReservationVersion   string `json:"reservation_version"`
}

type QueryRemovalInput struct {
	QueryReservationInput
	RequestID string `json:"request_id"`
}

type QueryRemovalFence struct {
	QueryRuntimeInput
	InstanceID         string    `json:"instance_id"`
	CandidateVersion   string    `json:"candidate_version"`
	SurvivorRuntimeID  string    `json:"survivor_runtime_id,omitempty"`
	SurvivorInstanceID string    `json:"survivor_instance_id,omitempty"`
	SurvivorVersion    string    `json:"survivor_version"`
	BuildEpoch         string    `json:"build_epoch"`
	BuildVersion       string    `json:"build_version"`
	BuildState         string    `json:"build_state"`
	RemovalEpoch       string    `json:"removal_epoch"`
	RequestID          string    `json:"request_id"`
	Digest             string    `json:"digest"`
	AcceptedAt         time.Time `json:"accepted_at"`
	ValidUntil         time.Time `json:"valid_until"`
}

func queryFence(f durable.QueryRemovalFence) QueryRemovalFence {
	digest, _ := durable.Fingerprint("query_runtime.removal_fence.v1", f)
	return QueryRemovalFence{
		QueryRuntimeInput: QueryRuntimeInput{BuildInput: BuildInput{Namespace: f.Candidate.Namespace, BuildID: f.Candidate.BuildID}, RuntimeID: f.Candidate.RuntimeID},
		InstanceID:        f.Candidate.InstanceID, CandidateVersion: strconv.FormatInt(f.CandidateStateVersion, 10),
		SurvivorRuntimeID: f.Survivor.RuntimeID, SurvivorInstanceID: f.Survivor.InstanceID, SurvivorVersion: strconv.FormatInt(f.SurvivorStateVersion, 10),
		BuildEpoch: strconv.FormatInt(f.BuildEpoch, 10), BuildVersion: strconv.FormatInt(f.BuildVersion, 10), BuildState: f.BuildState,
		RemovalEpoch: strconv.FormatInt(f.RemovalEpoch, 10), RequestID: f.RequestID, Digest: digest, AcceptedAt: f.AcceptedAt, ValidUntil: f.ValidUntil,
	}
}

type QueryRemovalAcceptance struct {
	QueryRuntimeAcceptance
	Fence QueryRemovalFence `json:"fence"`
}

func removalAcceptance(r durable.LifecycleReceipt) QueryRemovalAcceptance {
	out := QueryRemovalAcceptance{QueryRuntimeAcceptance: queryAcceptance(r)}
	if r.QueryRuntime != nil {
		out.Fence = queryFence(r.QueryRuntime.Removal)
	}
	return out
}

func (s *Service) queryRequestLookup(ctx context.Context, p security.Principal, action string, target durable.QueryRuntimeTarget, op durable.LifecycleOperation, id string, request any) (durable.LifecycleReceipt, error) {
	digest, err := durable.Fingerprint(string(op), request)
	if err != nil {
		return durable.LifecycleReceipt{}, durable.ErrInvalid
	}
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: op, RequestID: id, RequestDigest: digest}
	return s.lookupQueryReceipt(ctx, p, action, target, lookup)
}

func (s *Service) reservation(ctx context.Context, p security.Principal, action string, in QueryReservationInput) (durable.QueryRemovalFence, error) {
	target, err := s.queryTarget(in.QueryRuntimeInput)
	if err != nil {
		return durable.QueryRemovalFence{}, err
	}
	version, err := lifecycleVersion(in.ReservationVersion, false)
	if err != nil || !durable.DeliveryIdentifier(in.ReservationRequestID) {
		return durable.QueryRemovalFence{}, durable.ErrInvalid
	}
	request := durable.BeginQueryRemovalRequest{QueryRuntimeTarget: target, RequestID: in.ReservationRequestID, ExpectedVersion: version}
	receipt, err := s.queryRequestLookup(ctx, p, action, target, durable.OperationBeginQueryRemoval, in.ReservationRequestID, request)
	if err != nil {
		return durable.QueryRemovalFence{}, s.queryFailure(ctx, p, action, target, err)
	}
	return receipt.QueryRuntime.Removal, nil
}

func (s *Service) BeginQueryRuntimeRemoval(ctx context.Context, p security.Principal, in QueryRuntimeCommand) (QueryRemovalAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	target, err := s.queryTarget(in.QueryRuntimeInput)
	if err != nil {
		return QueryRemovalAcceptance{}, err
	}
	version, err := lifecycleVersion(in.ExpectedVersion, false)
	if err != nil || !durable.DeliveryIdentifier(in.RequestID) {
		return QueryRemovalAcceptance{}, durable.ErrInvalid
	}
	if p.Validate() != nil {
		return QueryRemovalAcceptance{}, s.denied(ctx, p, RemoveQueryRuntime, security.ErrUnauthenticated)
	}
	request := durable.BeginQueryRemovalRequest{QueryRuntimeTarget: target, RequestID: in.RequestID, ExpectedVersion: version}
	saved, err := s.queryRequestLookup(ctx, p, RemoveQueryRuntime, target, durable.OperationBeginQueryRemoval, in.RequestID, request)
	if !errors.Is(err, durable.ErrNotFound) {
		return removalAcceptance(saved), err
	}
	binding, err := s.queryBinding(ctx, target)
	if err != nil {
		return QueryRemovalAcceptance{}, s.queryFailure(ctx, p, RemoveQueryRuntime, target, err)
	}
	if authErr := s.authorizeQuery(ctx, p, RemoveQueryRuntime, binding.QueryRuntimeIdentity); authErr != nil {
		return QueryRemovalAcceptance{}, authErr
	}
	store, err := s.queryStore()
	if err != nil {
		return QueryRemovalAcceptance{}, err
	}
	receipt, err := store.BeginQueryRuntimeRemoval(commandContext(ctx, p, in.RequestID), request)
	receipt, err = checkedQueryReceipt(receipt, err, target, durable.OperationBeginQueryRemoval, in.RequestID, request)
	return removalAcceptance(receipt), commandError(err)
}

type QueryRemovalCheck struct {
	Fence     QueryRemovalFence `json:"fence"`
	CheckedAt time.Time         `json:"checked_at"`
}

func (s *Service) CheckQueryRuntimeRemoval(ctx context.Context, p security.Principal, in QueryReservationInput) (QueryRemovalCheck, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	fence, err := s.reservation(ctx, p, RemoveQueryRuntime, in)
	if err != nil {
		return QueryRemovalCheck{}, err
	}
	store, err := s.queryStore()
	if err != nil {
		return QueryRemovalCheck{}, err
	}
	facts, err := store.CheckQueryRuntimeRemoval(ctx, fence)
	if err != nil {
		return QueryRemovalCheck{}, commandError(err)
	}
	if facts.Fence != fence {
		return QueryRemovalCheck{}, security.ErrUnavailable
	}
	if err = s.audit.RecordDurableRead(ctx, p, RemoveQueryRuntime, "allowed", fence.Candidate.Namespace); err != nil {
		return QueryRemovalCheck{}, security.ErrUnavailable
	}
	return QueryRemovalCheck{Fence: queryFence(facts.Fence), CheckedAt: facts.CheckedAt}, nil
}

func (s *Service) FinishQueryRuntimeRemoval(ctx context.Context, p security.Principal, in QueryRemovalInput) (QueryRuntimeAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	if !durable.DeliveryIdentifier(in.RequestID) {
		return QueryRuntimeAcceptance{}, durable.ErrInvalid
	}
	fence, err := s.reservation(ctx, p, RemoveQueryRuntime, in.QueryReservationInput)
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	if authErr := s.authorizeQuery(ctx, p, FinishQueryRuntime, fence.Candidate); authErr != nil {
		return QueryRuntimeAcceptance{}, authErr
	}
	request := durable.FinishQueryRemovalRequest{Fence: fence, RequestID: in.RequestID}
	saved, err := s.queryRequestLookup(ctx, p, FinishQueryRuntime, fence.Candidate.QueryRuntimeTarget, durable.OperationFinishQueryRemoval, in.RequestID, request)
	if !errors.Is(err, durable.ErrNotFound) {
		return queryAcceptance(saved), err
	}
	verifier, ok := s.queryHost.(QueryRemovalCompletionVerifier)
	if !ok {
		return QueryRuntimeAcceptance{}, ErrRuntimeUnavailable
	}
	if err = verifier.VerifyRemoved(ctx, fence); err != nil {
		return QueryRuntimeAcceptance{}, commandError(err)
	}
	store, err := s.queryStore()
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	receipt, err := store.FinishQueryRuntimeRemoval(commandContext(ctx, p, in.RequestID), request)
	receipt, err = checkedQueryReceipt(receipt, err, fence.Candidate.QueryRuntimeTarget, durable.OperationFinishQueryRemoval, in.RequestID, request)
	return queryAcceptance(receipt), commandError(err)
}

func (s *Service) AbortQueryRuntimeRemoval(ctx context.Context, p security.Principal, in QueryRemovalInput) (QueryRuntimeAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	target, err := s.queryTarget(in.QueryRuntimeInput)
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	if !durable.DeliveryIdentifier(in.RequestID) {
		return QueryRuntimeAcceptance{}, durable.ErrInvalid
	}
	if p.Validate() != nil {
		return QueryRuntimeAcceptance{}, s.denied(ctx, p, AbortQueryRuntime, security.ErrUnauthenticated)
	}
	digest, err := durable.Fingerprint("operator.query_runtime.abort.v1", in)
	if err != nil {
		return QueryRuntimeAcceptance{}, durable.ErrInvalid
	}
	saved, err := s.queryLookup(ctx, p, AbortQueryRuntime, target, durable.OperationAbortQueryRemoval, in.RequestID, digest)
	if !errors.Is(err, durable.ErrNotFound) {
		return queryAcceptance(saved), err
	}
	fence, err := s.reservation(ctx, p, AbortQueryRuntime, in.QueryReservationInput)
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	binding, err := s.queryBinding(ctx, target)
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	if binding.State != durable.QueryRuntimeRemoving || binding.Removal != fence || binding.QueryRuntimeIdentity != fence.Candidate {
		return QueryRuntimeAcceptance{}, durable.ErrQueryFence
	}
	verifier, ok := s.queryHost.(durable.QueryRemovalAbortVerifier)
	if !ok {
		return QueryRuntimeAcceptance{}, ErrRuntimeUnavailable
	}
	settlement, proof, err := verifier.VerifyAbort(ctx, binding, fence)
	if err != nil {
		s.observeQueryRejection(ctx, target, in.RequestID, durable.OperationAbortQueryRemoval, err)
		return QueryRuntimeAcceptance{}, commandError(err)
	}
	store, err := s.queryStore()
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	request := durable.AbortQueryRemovalRequest{VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: target, RequestID: in.RequestID, ExpectedVersion: fence.CandidateStateVersion, CommandDigest: digest, Verification: proof}, Fence: fence, Settlement: settlement}
	receipt, err := store.AbortQueryRuntimeRemoval(commandContext(ctx, p, in.RequestID), request)
	s.observeQueryRejection(ctx, target, in.RequestID, durable.OperationAbortQueryRemoval, err)
	receipt, err = checkedQueryReceipt(receipt, err, target, durable.OperationAbortQueryRemoval, in.RequestID, request)
	return queryAcceptance(receipt), commandError(err)
}
