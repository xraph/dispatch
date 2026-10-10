package durable

import (
	"context"
	"errors"
	"math"
	"time"
)

var ErrQueryRetention = errors.New("durable: verified query retention unavailable")
var ErrQueryFence = errors.New("durable: query removal fence changed or expired")

const (
	QueryRetentionSchemaVersion                      = 1
	QueryRuntimeActive                               = "active"
	QueryRuntimeRemoving                             = "removing"
	QueryRuntimeRemoved                              = "removed"
	OperationRegisterQueryRuntime LifecycleOperation = "query_runtime.register"
	OperationVerifyQueryRuntime   LifecycleOperation = "query_runtime.verify"
	OperationBeginQueryRemoval    LifecycleOperation = "query_runtime.remove"
	OperationFinishQueryRemoval   LifecycleOperation = "query_runtime.finish"
	OperationAbortQueryRemoval    LifecycleOperation = "query_runtime.abort"
)

// BuildQueryIdentity is supplied by trusted artifact enrollment, never inferred
// from a build label. A zero value means historical identity remains unknown.
type BuildQueryIdentity struct {
	ArtifactDigest           string
	ConfigurationDigest      string
	ConfigurationVersion     string
	EnrollmentEvidenceDigest string
	ProbePolicyID            string
	ProbePolicyVersion       int64
	VerifierID               string
	MaximumProofValidity     time.Duration
}

func (i BuildQueryIdentity) Validate() error {
	if !validHex256(i.ArtifactDigest) || !validHex256(i.ConfigurationDigest) || !validHex256(i.EnrollmentEvidenceDigest) || !DeliveryIdentifier(i.ConfigurationVersion) || !DeliveryIdentifier(i.ProbePolicyID) || i.ProbePolicyVersion < 1 || !DeliveryIdentifier(i.VerifierID) || i.MaximumProofValidity <= 0 {
		return ErrInvalid
	}
	return nil
}

type QueryRuntimeTarget struct {
	BuildTarget
	RuntimeID string
}

func (t QueryRuntimeTarget) Validate() error {
	if t.BuildTarget.Validate() != nil || !DeliveryIdentifier(t.RuntimeID) {
		return ErrInvalid
	}
	return nil
}

type QueryRuntimeIdentity struct {
	QueryRuntimeTarget
	InstanceID      string
	IdentityVersion int64
	BuildIdentity   BuildQueryIdentity
}

func (i QueryRuntimeIdentity) Validate() error {
	if i.QueryRuntimeTarget.Validate() != nil || !DeliveryIdentifier(i.InstanceID) || i.IdentityVersion != 1 || i.BuildIdentity.Validate() != nil {
		return ErrInvalid
	}
	return nil
}

type QueryRuntimeVerification struct {
	Identity           QueryRuntimeIdentity
	ProofID            string
	VerifierID         string
	EvidenceDigest     string
	ProbePolicyID      string
	ProbePolicyVersion int64
	VerifiedAt         time.Time
	ValidUntil         time.Time
	AcceptedAt         time.Time
}

func (p QueryRuntimeVerification) Validate(identity QueryRuntimeIdentity, now time.Time) error {
	if p.Identity != identity || identity.Validate() != nil || !DeliveryIdentifier(p.ProofID) || !validHex256(p.EvidenceDigest) || p.VerifierID != identity.BuildIdentity.VerifierID || p.ProbePolicyID != identity.BuildIdentity.ProbePolicyID || p.ProbePolicyVersion != identity.BuildIdentity.ProbePolicyVersion || p.VerifiedAt.IsZero() || p.VerifiedAt.After(now) || !p.ValidUntil.After(now) || !p.ValidUntil.After(p.VerifiedAt) || p.ValidUntil.Sub(p.VerifiedAt) > identity.BuildIdentity.MaximumProofValidity {
		return ErrQueryRetention
	}
	return nil
}

type QueryRemovalFence struct {
	Candidate             QueryRuntimeIdentity
	CandidateStateVersion int64
	Survivor              QueryRuntimeIdentity
	SurvivorStateVersion  int64
	SurvivorProof         QueryRuntimeVerification
	BuildEpoch            int64
	BuildVersion          int64
	BuildState            string
	RemovalEpoch          int64
	RequestID             string
	AcceptedAt            time.Time
	ValidUntil            time.Time
}
type QueryRuntimeBinding struct {
	QueryRuntimeIdentity
	State        string
	Version      int64
	RemovalEpoch int64
	CreatedAt    time.Time
	ChangedAt    time.Time
	Verification QueryRuntimeVerification
	Removal      QueryRemovalFence
}

func (b QueryRuntimeBinding) Validate() error {
	if b.QueryRuntimeIdentity.Validate() != nil || b.Version < 1 || b.RemovalEpoch < 0 || b.CreatedAt.IsZero() || b.ChangedAt.IsZero() || (b.State != QueryRuntimeActive && b.State != QueryRuntimeRemoving && b.State != QueryRuntimeRemoved) {
		return ErrInvalid
	}
	return nil
}

type QueryRetentionFacts struct {
	BuildTarget
	RetainedExecutions int64
	ActiveBindings     int64
	VerifiedBindings   int64
	ObservedAt         time.Time
}
type QueryRuntimeList struct {
	NamespaceTarget
	InstanceID string
	BuildID    string
	After      string
	Limit      int
}

func (r QueryRuntimeList) Validate() error {
	if r.NamespaceTarget.Validate() != nil || (r.InstanceID != "" && !DeliveryIdentifier(r.InstanceID)) || (r.BuildID != "" && ValidateBuildID(r.BuildID) != nil) || (r.After != "" && !DeliveryIdentifier(r.After)) || r.Limit < 1 || r.Limit > MaxReadPage {
		return ErrInvalid
	}
	return nil
}

type QueryRuntimePage struct {
	Items      []QueryRuntimeBinding
	Next       string
	ObservedAt time.Time
}
type RegisterQueryRuntimeRequest struct {
	Identity      QueryRuntimeIdentity
	RequestID     string
	CommandDigest string
}

func (r RegisterQueryRuntimeRequest) Validate() error {
	if r.Identity.Validate() != nil || !DeliveryIdentifier(r.RequestID) || (r.CommandDigest != "" && !validHex256(r.CommandDigest)) {
		return ErrInvalid
	}
	return nil
}

type VerifyQueryRuntimeRequest struct {
	QueryRuntimeTarget
	RequestID       string
	CommandDigest   string
	ExpectedVersion int64
	Verification    QueryRuntimeVerification
}

func (r VerifyQueryRuntimeRequest) Validate() error {
	if r.QueryRuntimeTarget.Validate() != nil || !DeliveryIdentifier(r.RequestID) || (r.CommandDigest != "" && !validHex256(r.CommandDigest)) || r.ExpectedVersion < 1 || r.Verification.Identity.QueryRuntimeTarget != r.QueryRuntimeTarget {
		return ErrInvalid
	}
	return nil
}

type BeginQueryRemovalRequest struct {
	QueryRuntimeTarget
	RequestID       string
	ExpectedVersion int64
}

func (r BeginQueryRemovalRequest) Validate() error {
	if r.QueryRuntimeTarget.Validate() != nil || !DeliveryIdentifier(r.RequestID) || r.ExpectedVersion < 1 {
		return ErrInvalid
	}
	return nil
}

type FinishQueryRemovalRequest struct {
	Fence     QueryRemovalFence
	RequestID string
}

func (r FinishQueryRemovalRequest) Validate() error {
	if r.Fence.Candidate.Validate() != nil || !DeliveryIdentifier(r.RequestID) || r.Fence.CandidateStateVersion < 1 || r.Fence.RemovalEpoch < 1 || !DeliveryIdentifier(r.Fence.RequestID) {
		return ErrInvalid
	}
	return nil
}

// QueryRemovalDisposition describes a settled destructive operation. Unknown or
// in-flight deletion is deliberately absent from this closed set.
type QueryRemovalDisposition string

const (
	QueryRemovalNotIssued              QueryRemovalDisposition = "not_issued"
	QueryRemovalRejected               QueryRemovalDisposition = "rejected"
	QueryRemovalCancelled              QueryRemovalDisposition = "cancelled"
	QueryRemovalFenced                 QueryRemovalDisposition = "fenced"
	QueryRemovalSettledWithoutDeletion QueryRemovalDisposition = "settled_without_deletion"
)

// QueryRemovalSettlement comes only from the trusted removal controller. Its
// operation identity is allocated before dispatch and need not be a provider ID.
// NotIssued requires atomic revocation of future issuance. Cancelled requires
// definitive operation settlement. Fenced requires prevention of issuance or a
// provider-enforced fence that prevents an outstanding deletion from completing.
// A lease expiry, cancelled client context or healthy probe is not settlement.
type QueryRemovalSettlement struct {
	FenceDigest         string
	VerifierID          string
	EvidenceDigest      string
	Disposition         QueryRemovalDisposition
	ExternalOperationID string
	SettledAt           time.Time
}

func (s QueryRemovalSettlement) Validate(f QueryRemovalFence, now time.Time) error {
	digest, err := Fingerprint("query_runtime.removal_fence.v1", f)
	if err != nil || s.FenceDigest != digest || s.VerifierID != f.Candidate.BuildIdentity.VerifierID || !validHex256(s.EvidenceDigest) || !DeliveryIdentifier(s.ExternalOperationID) || s.SettledAt.Before(f.AcceptedAt) || s.SettledAt.After(now) {
		return ErrQueryFence
	}
	switch s.Disposition {
	case QueryRemovalNotIssued, QueryRemovalRejected, QueryRemovalCancelled, QueryRemovalFenced, QueryRemovalSettledWithoutDeletion:
		return nil
	default:
		return ErrQueryFence
	}
}

type QueryRemovalAbortEvidence struct {
	Fence      QueryRemovalFence
	Settlement QueryRemovalSettlement
}
type AbortQueryRemovalRequest struct {
	VerifyQueryRuntimeRequest
	Fence      QueryRemovalFence
	Settlement QueryRemovalSettlement
}

func (r AbortQueryRemovalRequest) Validate() error {
	if r.VerifyQueryRuntimeRequest.Validate() != nil || r.Fence.Candidate.QueryRuntimeTarget != r.QueryRuntimeTarget || r.Fence.CandidateStateVersion != r.ExpectedVersion || r.Fence.RemovalEpoch < 1 {
		return ErrInvalid
	}
	return nil
}

// QueryRemovalAbortVerifier is a trusted host boundary, never a remote request
// adapter. VerifyAbort settles this exact reservation in the host's operation
// ledger, then captures fresh existence and query evidence for its original
// identity. Issued deletion with an unknown outcome must return an error.
// Recover an accepted command receipt before invoking it again.
type QueryRemovalAbortVerifier interface {
	VerifyAbort(context.Context, QueryRuntimeBinding, QueryRemovalFence) (QueryRemovalSettlement, QueryRuntimeVerification, error)
}
type QueryRemovalFacts struct {
	Fence     QueryRemovalFence
	CheckedAt time.Time
}
type QueryRuntimeStore interface {
	RegisterQueryRuntime(context.Context, RegisterQueryRuntimeRequest) (LifecycleReceipt, error)
	RecordQueryRuntimeVerification(context.Context, VerifyQueryRuntimeRequest) (LifecycleReceipt, error)
	InspectQueryRetention(context.Context, BuildTarget) (QueryRetentionFacts, error)
	ListQueryRuntimes(context.Context, QueryRuntimeList) (QueryRuntimePage, error)
	BeginQueryRuntimeRemoval(context.Context, BeginQueryRemovalRequest) (LifecycleReceipt, error)
	CheckQueryRuntimeRemoval(context.Context, QueryRemovalFence) (QueryRemovalFacts, error)
	FinishQueryRuntimeRemoval(context.Context, FinishQueryRemovalRequest) (LifecycleReceipt, error)
	AbortQueryRuntimeRemoval(context.Context, AbortQueryRemovalRequest) (LifecycleReceipt, error)
}

// VerifyQueryBinding conditionally records trusted host evidence after locks.
func VerifyQueryBinding(b QueryRuntimeBinding, r VerifyQueryRuntimeRequest, now time.Time) (QueryRuntimeBinding, error) {
	if b.QueryRuntimeTarget != r.QueryRuntimeTarget || b.Version != r.ExpectedVersion {
		return b, ErrRevisionConflict
	}
	if b.State != QueryRuntimeActive {
		return b, ErrRequestConflict
	}
	if b.Version == math.MaxInt64 {
		return b, ErrInvalid
	}
	if err := r.Verification.Validate(b.QueryRuntimeIdentity, now); err != nil {
		return b, err
	}
	b.Version++
	b.ChangedAt = now
	b.Verification = r.Verification
	b.Verification.AcceptedAt = now
	return b, nil
}

// AbortQueryRemoval requires settled removal and fresh query proof under the
// same namespace coordination that excludes competing removal reservations.
func AbortQueryRemoval(b QueryRuntimeBinding, r AbortQueryRemovalRequest, now time.Time) (QueryRuntimeBinding, error) {
	if b.State != QueryRuntimeRemoving || b.QueryRuntimeIdentity != r.Fence.Candidate || b.Removal != r.Fence || b.Version != r.ExpectedVersion || b.Version != r.Fence.CandidateStateVersion || b.RemovalEpoch != r.Fence.RemovalEpoch {
		return b, ErrQueryFence
	}
	if err := r.Settlement.Validate(r.Fence, now); err != nil {
		return b, err
	}
	if r.Verification.VerifiedAt.Before(r.Settlement.SettledAt) {
		return b, ErrQueryRetention
	}
	if b.Version == math.MaxInt64 {
		return b, ErrInvalid
	}
	if err := r.Verification.Validate(b.QueryRuntimeIdentity, now); err != nil {
		return b, err
	}
	b.Version++
	b.ChangedAt = now
	b.Verification = r.Verification
	b.Verification.AcceptedAt = now
	b.State = QueryRuntimeActive
	b.Removal = QueryRemovalFence{}
	return b, nil
}
