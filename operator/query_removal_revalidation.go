package operator

import (
	"context"
	"errors"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

const (
	RevalidateQueryRuntime = "dispatch.query_runtime.revalidate"
	ReadQueryRevalidation  = "dispatch.query_runtime.revalidation.read"
	CommitQueryRemoval     = "dispatch.query_runtime.commit"
	ReadQueryCommit        = "dispatch.query_runtime.commit.read"
	CloseQueryGuard        = "dispatch.query_runtime.guard.close"
	ReadQueryGuard         = "dispatch.query_runtime.guard.read"
)

// QueryGuardScope names one external operation. The verifier authenticates its contents.
type QueryGuardScope struct {
	AuthorityID             string `json:"authority_id"`
	OperationScopeID        string `json:"operation_scope_id"`
	AttemptID               string `json:"attempt_id"`
	ProviderOperationID     string `json:"provider_operation_id"`
	CoverageDigest          string `json:"coverage_digest"`
	ExcludedInstancesDigest string `json:"excluded_instances_digest"`
	ProducerID              string `json:"producer_id"`
	ProfileID               string `json:"profile_id"`
	ProfileVersion          string `json:"profile_version"`
	VerifierID              string `json:"verifier_id"`
}

func (s QueryGuardScope) native() (durable.QueryRemovalExternalScope, error) {
	version, err := lifecycleVersion(s.ProfileVersion, false)
	if err != nil {
		return durable.QueryRemovalExternalScope{}, err
	}
	out := durable.QueryRemovalExternalScope{AuthorityID: s.AuthorityID, OperationScopeID: s.OperationScopeID, AttemptID: s.AttemptID, ProviderOperationID: s.ProviderOperationID, CoverageDigest: s.CoverageDigest, ExcludedInstancesDigest: s.ExcludedInstancesDigest, ProducerID: s.ProducerID, ProfileID: s.ProfileID, ProfileVersion: version, VerifierID: s.VerifierID}
	return out, out.Validate()
}

// QueryRevalidationInput carries opaque references, never trusted evidence.
type QueryRevalidationInput struct {
	QueryReservationInput
	RequestID             string          `json:"request_id"`
	Scope                 QueryGuardScope `json:"scope"`
	ExpectedCurrentDigest string          `json:"expected_current_digest"`
	EvidenceReference     string          `json:"evidence_reference"`
	SurvivorRuntimeID     string          `json:"survivor_runtime_id"`
	SurvivorVersion       string          `json:"survivor_version"`
}

// QueryGuardInput binds a command to the original reservation and guard root.
type QueryGuardInput struct {
	QueryReservationInput
	GuardID        string `json:"guard_id"`
	RootDigest     string `json:"root_digest"`
	OriginalDigest string `json:"original_digest"`
}

// QueryCommitInput names an authenticated permission and the current proof.
type QueryCommitInput struct {
	QueryGuardInput
	RequestID             string `json:"request_id"`
	PermissionReference   string `json:"permission_reference"`
	ExpectedCurrentDigest string `json:"expected_current_digest"`
	CurrentRequestID      string `json:"current_request_id"`
}

// QueryGuardCloseInput names authoritative permanent cutoff and terminal coverage.
type QueryGuardCloseInput struct {
	QueryGuardInput
	RequestID        string `json:"request_id"`
	ClosureReference string `json:"closure_reference"`
}

// QueryGuardReadInput recovers one exact immutable command.
type QueryGuardReadInput struct {
	QueryReservationInput
	RequestID     string `json:"request_id"`
	CommandDigest string `json:"command_digest"`
}

// QueryGuardReceiptInput selects a finite saved guard command kind.
type QueryGuardReceiptInput struct {
	QueryGuardReadInput
	Kind string `json:"kind"`
}

// QueryGuardAcceptance is historical and never grants provider delegation.
type QueryGuardAcceptance struct {
	Kind                  string    `json:"kind"`
	GuardID               string    `json:"guard_id"`
	RootDigest            string    `json:"root_digest"`
	RequestID             string    `json:"request_id"`
	CommandDigest         string    `json:"command_digest"`
	ReceiptDigest         string    `json:"receipt_digest"`
	PermissionID          string    `json:"permission_id"`
	InvocationID          string    `json:"invocation_id"`
	PermissionProofDigest string    `json:"permission_proof_digest"`
	CurrentProofDigest    string    `json:"current_proof_digest"`
	CutoffDigest          string    `json:"cutoff_digest"`
	Disposition           string    `json:"disposition"`
	AcceptedAt            time.Time `json:"accepted_at"`
}

func queryCommitAcceptance(r durable.QueryRemovalCommitReceipt) QueryGuardAcceptance {
	return QueryGuardAcceptance{Kind: "commit", GuardID: r.Request.Guard.GuardID, RootDigest: r.Request.Guard.RootDigest, RequestID: r.Request.RequestID, CommandDigest: r.Request.CommandDigest, ReceiptDigest: r.ReceiptDigest, PermissionID: r.Request.Permission.PermissionID, InvocationID: r.Request.Permission.CommandID, PermissionProofDigest: r.Request.Permission.RevalidationDigest, CurrentProofDigest: r.Request.ExpectedCurrentDigest, AcceptedAt: r.AcceptedAt}
}
func queryCloseAcceptance(r durable.QueryRemovalGuardCloseReceipt) QueryGuardAcceptance {
	return QueryGuardAcceptance{Kind: "close", GuardID: r.Request.Guard.GuardID, RootDigest: r.Request.Guard.RootDigest, RequestID: r.Request.RequestID, CommandDigest: r.Request.CommandDigest, ReceiptDigest: r.ReceiptDigest, CutoffDigest: r.Request.Closure.CutoffDigest, Disposition: r.Request.Closure.Disposition, AcceptedAt: r.AcceptedAt}
}

// QueryCommitStatus reports current guard observations beside historical acceptance.
type QueryCommitStatus struct {
	Receipt            QueryGuardAcceptance `json:"receipt"`
	State              string               `json:"state"`
	CurrentRequestID   string               `json:"current_request_id"`
	CurrentProofDigest string               `json:"current_proof_digest"`
	SourceObservedAt   time.Time            `json:"source_observed_at"`
	ObservedAt         time.Time            `json:"observed_at"`
}

func (s *Service) guardStore() (durable.QueryRemovalRevalidationStore, error) {
	store, ok := s.store.(durable.QueryRemovalRevalidationStore)
	if !ok {
		return nil, security.ErrUnavailable
	}
	return store, nil
}
func (s *Service) guardOriginal(ctx context.Context, p security.Principal, action string, in QueryReservationInput) (durable.QueryRemovalOriginal, error) {
	if p.Validate() != nil {
		return durable.QueryRemovalOriginal{}, s.denied(ctx, p, action, security.ErrUnauthenticated)
	}
	fence, err := s.reservation(ctx, p, action, in)
	if err != nil {
		return durable.QueryRemovalOriginal{}, err
	}
	version, err := lifecycleVersion(in.ReservationVersion, false)
	if err != nil {
		return durable.QueryRemovalOriginal{}, err
	}
	request := durable.BeginQueryRemovalRequest{QueryRuntimeTarget: fence.Candidate.QueryRuntimeTarget, RequestID: in.ReservationRequestID, ExpectedVersion: version}
	digest, err := durable.Fingerprint(string(durable.OperationBeginQueryRemoval), request)
	if err != nil {
		return durable.QueryRemovalOriginal{}, err
	}
	fd, err := durable.Fingerprint("query_runtime.removal_fence.v1", fence)
	if err != nil {
		return durable.QueryRemovalOriginal{}, err
	}
	original := durable.QueryRemovalOriginal{QueryRuntimeTarget: fence.Candidate.QueryRuntimeTarget, BeginRequestID: in.ReservationRequestID, BeginRequestDigest: digest, FenceDigest: fd, CandidateStateVersion: fence.CandidateStateVersion, RemovalEpoch: fence.RemovalEpoch}
	return original, original.Validate()
}
func guardReference(in QueryGuardInput, original durable.QueryRemovalOriginal) (durable.QueryRemovalGuardReference, error) {
	digest, err := durable.Fingerprint("dispatch.query-removal.original.v1", original)
	if err != nil || digest != in.OriginalDigest {
		return durable.QueryRemovalGuardReference{}, durable.ErrInvalid
	}
	r := durable.QueryRemovalGuardReference{QueryRuntimeTarget: original.QueryRuntimeTarget, OriginalDigest: in.OriginalDigest, GuardID: in.GuardID, RootDigest: in.RootDigest}
	return r, r.Validate()
}
func (s *Service) guardReadAudit(ctx context.Context, p security.Principal, action, namespace string) error {
	if err := s.audit.RecordDurableRead(ctx, p, action, "allowed", namespace); err != nil {
		return security.ErrUnavailable
	}
	return nil
}

// RevalidateQueryRuntimeRemoval recovers exact commands before obtaining new proof.
func (s *Service) RevalidateQueryRuntimeRemoval(ctx context.Context, p security.Principal, in QueryRevalidationInput) (durable.QueryRemovalRevalidationReceipt, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	original, err := s.guardOriginal(ctx, p, RevalidateQueryRuntime, in.QueryReservationInput)
	if err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, err
	}
	store, err := s.guardStore()
	if err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, err
	}
	digest, err := durable.Fingerprint("operator.query_runtime.revalidate.v1", in)
	if err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, err
	}
	lookup := durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: original.QueryRuntimeTarget, RequestID: in.RequestID, CommandDigest: digest}
	saved, err := store.LookupQueryRemovalRevalidation(ctx, lookup)
	if !errors.Is(err, durable.ErrNotFound) {
		if err != nil {
			return durable.QueryRemovalRevalidationReceipt{}, commandError(err)
		}
		if saved.Match(lookup) != nil {
			return durable.QueryRemovalRevalidationReceipt{}, security.ErrUnavailable
		}
		return saved.Compact()
	}
	if s.queryRemovalGuardVerifier == nil {
		return durable.QueryRemovalRevalidationReceipt{}, ErrRuntimeUnavailable
	}
	scope, err := in.Scope.native()
	if err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, err
	}
	version, err := lifecycleVersion(in.SurvivorVersion, true)
	if err != nil || !durable.QueryRemovalToken(in.EvidenceReference) {
		return durable.QueryRemovalRevalidationReceipt{}, durable.ErrInvalid
	}
	var survivor durable.QueryRuntimeTarget
	if in.SurvivorRuntimeID != "" {
		survivor = durable.QueryRuntimeTarget{BuildTarget: original.BuildTarget, RuntimeID: in.SurvivorRuntimeID}
	}
	evidence, err := s.queryRemovalGuardVerifier.VerifyRevalidation(ctx, durable.QueryRemovalGuardProbe{Original: original, ExternalScope: scope, EvidenceReference: in.EvidenceReference, Survivor: survivor, SurvivorVersion: version})
	if err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, commandError(err)
	}
	r := durable.QueryRemovalRevalidationRequest{Original: original, ExternalScope: scope, RequestID: in.RequestID, CommandDigest: digest, ExpectedCurrentDigest: in.ExpectedCurrentDigest, Survivor: survivor, SurvivorVersion: version, Evidence: evidence}
	result, err := store.RevalidateQueryRuntimeRemoval(commandContext(ctx, p, in.RequestID), r)
	return result, commandError(err)
}

// ReadQueryRemovalRevalidation returns only the compact authenticated receipt.
func (s *Service) ReadQueryRemovalRevalidation(ctx context.Context, p security.Principal, in QueryGuardReadInput) (durable.QueryRemovalRevalidationReceipt, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	original, err := s.guardOriginal(ctx, p, ReadQueryRevalidation, in.QueryReservationInput)
	if err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, err
	}
	store, err := s.guardStore()
	if err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, err
	}
	q := durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: original.QueryRuntimeTarget, RequestID: in.RequestID, CommandDigest: in.CommandDigest}
	m, err := store.LookupQueryRemovalRevalidation(ctx, q)
	if err != nil {
		return durable.QueryRemovalRevalidationReceipt{}, commandError(err)
	}
	if m.Match(q) != nil {
		return durable.QueryRemovalRevalidationReceipt{}, security.ErrUnavailable
	}
	if auditErr := s.guardReadAudit(ctx, p, ReadQueryRevalidation, original.Namespace); auditErr != nil {
		return durable.QueryRemovalRevalidationReceipt{}, auditErr
	}
	return m.Compact()
}

// BindQueryRemovalCommit authenticates a saved external permission before first bind.
func (s *Service) BindQueryRemovalCommit(ctx context.Context, p security.Principal, in QueryCommitInput) (QueryGuardAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	original, err := s.guardOriginal(ctx, p, CommitQueryRemoval, in.QueryReservationInput)
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	ref, err := guardReference(in.QueryGuardInput, original)
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	store, err := s.guardStore()
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	digest, err := durable.Fingerprint("operator.query_runtime.commit.v1", in)
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	q := durable.QueryRemovalGuardReceiptLookup{Kind: "commit", QueryRemovalRevalidationLookup: durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: original.QueryRuntimeTarget, RequestID: in.RequestID, CommandDigest: digest}}
	saved, err := store.LookupQueryRemovalGuardReceipt(ctx, q)
	if !errors.Is(err, durable.ErrNotFound) {
		if err != nil {
			return QueryGuardAcceptance{}, commandError(err)
		}
		if saved.Commit == nil || saved.Close != nil || saved.Commit.Match(q.QueryRemovalRevalidationLookup) != nil {
			return QueryGuardAcceptance{}, security.ErrUnavailable
		}
		return queryCommitAcceptance(*saved.Commit), nil
	}
	if s.queryRemovalGuardVerifier == nil {
		return QueryGuardAcceptance{}, ErrRuntimeUnavailable
	}
	if !durable.QueryRemovalToken(in.PermissionReference) {
		return QueryGuardAcceptance{}, durable.ErrInvalid
	}
	permission, err := s.queryRemovalGuardVerifier.VerifyPermission(ctx, ref, in.PermissionReference)
	if err != nil {
		return QueryGuardAcceptance{}, commandError(err)
	}
	r := durable.QueryRemovalCommitRequest{Guard: ref, RequestID: in.RequestID, CommandDigest: digest, ExpectedCurrentDigest: in.ExpectedCurrentDigest, CurrentRequestID: in.CurrentRequestID, Permission: permission}
	result, err := store.BindQueryRemovalCommit(commandContext(ctx, p, in.RequestID), r)
	if err != nil {
		return QueryGuardAcceptance{}, commandError(err)
	}
	if result.Match(q.QueryRemovalRevalidationLookup) != nil {
		return QueryGuardAcceptance{}, security.ErrUnavailable
	}
	return queryCommitAcceptance(result), nil
}

// CloseQueryRemovalGuard requires authenticated permanent cutoff and complete coverage.
func (s *Service) CloseQueryRemovalGuard(ctx context.Context, p security.Principal, in QueryGuardCloseInput) (QueryGuardAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	original, err := s.guardOriginal(ctx, p, CloseQueryGuard, in.QueryReservationInput)
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	ref, err := guardReference(in.QueryGuardInput, original)
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	store, err := s.guardStore()
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	digest, err := durable.Fingerprint("operator.query_runtime.close.v1", in)
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	q := durable.QueryRemovalGuardReceiptLookup{Kind: "close", QueryRemovalRevalidationLookup: durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: original.QueryRuntimeTarget, RequestID: in.RequestID, CommandDigest: digest}}
	saved, err := store.LookupQueryRemovalGuardReceipt(ctx, q)
	if !errors.Is(err, durable.ErrNotFound) {
		if err != nil {
			return QueryGuardAcceptance{}, commandError(err)
		}
		if saved.Close == nil || saved.Commit != nil || saved.Close.Match(q.QueryRemovalRevalidationLookup) != nil {
			return QueryGuardAcceptance{}, security.ErrUnavailable
		}
		return queryCloseAcceptance(*saved.Close), nil
	}
	if s.queryRemovalGuardVerifier == nil {
		return QueryGuardAcceptance{}, ErrRuntimeUnavailable
	}
	if !durable.QueryRemovalToken(in.ClosureReference) {
		return QueryGuardAcceptance{}, durable.ErrInvalid
	}
	closure, err := s.queryRemovalGuardVerifier.VerifyClosure(ctx, ref, in.ClosureReference)
	if err != nil {
		return QueryGuardAcceptance{}, commandError(err)
	}
	r := durable.QueryRemovalGuardCloseRequest{Guard: ref, RequestID: in.RequestID, CommandDigest: digest, Closure: closure}
	result, err := store.CloseQueryRemovalGuard(commandContext(ctx, p, in.RequestID), r)
	if err != nil {
		return QueryGuardAcceptance{}, commandError(err)
	}
	if result.Match(q.QueryRemovalRevalidationLookup) != nil {
		return QueryGuardAcceptance{}, security.ErrUnavailable
	}
	return queryCloseAcceptance(result), nil
}

// ReadQueryRemovalGuardReceipt recovers one receipt without invoking the verifier.
func (s *Service) ReadQueryRemovalGuardReceipt(ctx context.Context, p security.Principal, in QueryGuardReceiptInput) (QueryGuardAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	original, err := s.guardOriginal(ctx, p, ReadQueryGuard, in.QueryReservationInput)
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	store, err := s.guardStore()
	if err != nil {
		return QueryGuardAcceptance{}, err
	}
	q := durable.QueryRemovalGuardReceiptLookup{Kind: in.Kind, QueryRemovalRevalidationLookup: durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: original.QueryRuntimeTarget, RequestID: in.RequestID, CommandDigest: in.CommandDigest}}
	r, err := store.LookupQueryRemovalGuardReceipt(ctx, q)
	if err != nil {
		return QueryGuardAcceptance{}, commandError(err)
	}
	var result QueryGuardAcceptance
	switch {
	case in.Kind == "commit" && r.Commit != nil && r.Close == nil && r.Commit.Match(q.QueryRemovalRevalidationLookup) == nil:
		result = queryCommitAcceptance(*r.Commit)
	case in.Kind == "close" && r.Close != nil && r.Commit == nil && r.Close.Match(q.QueryRemovalRevalidationLookup) == nil:
		result = queryCloseAcceptance(*r.Close)
	default:
		return result, security.ErrUnavailable
	}
	return result, s.guardReadAudit(ctx, p, ReadQueryGuard, original.Namespace)
}

// CheckQueryRemovalCommit reads active or closed state without granting a send.
func (s *Service) CheckQueryRemovalCommit(ctx context.Context, p security.Principal, in QueryGuardReadInput) (QueryCommitStatus, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	original, err := s.guardOriginal(ctx, p, ReadQueryCommit, in.QueryReservationInput)
	if err != nil {
		return QueryCommitStatus{}, err
	}
	store, err := s.guardStore()
	if err != nil {
		return QueryCommitStatus{}, err
	}
	q := durable.QueryRemovalCommitLookup{QueryRemovalRevalidationLookup: durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: original.QueryRuntimeTarget, RequestID: in.RequestID, CommandDigest: in.CommandDigest}}
	r, err := store.CheckQueryRemovalCommit(ctx, q)
	if err != nil {
		return QueryCommitStatus{}, commandError(err)
	}
	if r.Receipt.Match(q.QueryRemovalRevalidationLookup) != nil || r.Guard.GuardID != r.Receipt.Request.Guard.GuardID || r.Current.GuardID != r.Guard.GuardID {
		return QueryCommitStatus{}, security.ErrUnavailable
	}
	out := QueryCommitStatus{Receipt: queryCommitAcceptance(r.Receipt), State: r.Guard.State, CurrentRequestID: r.Current.RequestID, CurrentProofDigest: r.Current.MaterialDigest, SourceObservedAt: r.Current.ObservedAt, ObservedAt: r.ObservedAt}
	return out, s.guardReadAudit(ctx, p, ReadQueryCommit, original.Namespace)
}
