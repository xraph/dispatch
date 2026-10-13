package durable

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

// QueryRemovalGuardSchemaVersion identifies the complete native guard capability.
const QueryRemovalGuardSchemaVersion = 1

// QueryRemovalMaterialLimit bounds the complete persisted material, including evidence.
const QueryRemovalMaterialLimit = 524288

// QueryRemovalCommitReceiptLimit includes the request and authenticated receipt wrapper.
const QueryRemovalCommitReceiptLimit = 17408

// ErrQueryRemovalGuard means a protected association or current proof cannot change.
var ErrQueryRemovalGuard = errors.New("durable: query removal guard refused")

// QueryRemovalOriginal identifies an immutable accepted Begin reservation.
type QueryRemovalOriginal struct {
	QueryRuntimeTarget
	BeginRequestID        string
	BeginRequestDigest    string
	FenceDigest           string
	CandidateStateVersion int64
	RemovalEpoch          int64
}

// Validate checks the original reference without replacing its retained fence.
func (r QueryRemovalOriginal) Validate() error {
	if r.QueryRuntimeTarget.Validate() != nil || !DeliveryIdentifier(r.BeginRequestID) || !validHex256(r.BeginRequestDigest) || !validHex256(r.FenceDigest) || r.CandidateStateVersion < 2 || r.RemovalEpoch < 1 {
		return ErrInvalid
	}
	return nil
}

// QueryRemovalExternalScope identifies one immutable external operation and producer.
type QueryRemovalExternalScope struct {
	AuthorityID             string
	OperationScopeID        string
	AttemptID               string
	ProviderOperationID     string
	CoverageDigest          string
	ExcludedInstancesDigest string
	ProducerID              string
	ProfileID               string
	ProfileVersion          int64
	VerifierID              string
}

// Validate admits bounded canonical native tokens and fixed digests.
func (s QueryRemovalExternalScope) Validate() error {
	for _, id := range []string{s.AuthorityID, s.OperationScopeID, s.AttemptID, s.ProviderOperationID, s.ProducerID, s.ProfileID, s.VerifierID} {
		if !QueryRemovalToken(id) {
			return ErrInvalid
		}
	}
	if s.ProfileVersion < 1 || !validHex256(s.CoverageDigest) || !validHex256(s.ExcludedInstancesDigest) {
		return ErrInvalid
	}
	return nil
}

// QueryRemovalToken preserves a canonical ASCII representation for new opaque IDs.
func QueryRemovalToken(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune("._:-", c) {
			return false
		}
	}
	return true
}

// QueryRemovalProcess binds the original startup, registration and drain tuple.
type QueryRemovalProcess struct {
	HostID           string
	ProcessID        string
	DescriptorID     string
	DescriptorDigest string
	RegistrationID   string
	DrainRuntimeID   string
	DrainOperationID string
	DrainDeadline    time.Time
	Role             string
	Admission        string
	Claims           string
	DrainOutcome     string
	ObservedAt       time.Time
	ValidUntil       time.Time
}

// QueryRemovalPositiveEffect contains authenticated positive facts for one incarnation.
type QueryRemovalPositiveEffect struct {
	PermissionID      string
	PermissionDigest  string
	ResultID          string
	ResultDigest      string
	CommandID         string
	ResourceID        string
	IncarnationID     string
	AssociationDigest string
	PreEffectDigest   string
	PreEffectAt       time.Time
	AcceptedAt        time.Time
	CompletedAt       time.Time
}

// QueryRemovalEvidence is returned only by a configured trusted host verifier.
// Each attestation has its own independent 4096-byte allowance.
type QueryRemovalEvidence struct {
	BuildFinalizationRequestID     string
	BuildFinalizationRequestDigest string
	Disposition                    string
	CandidateProof                 QueryRuntimeVerification
	SurvivorProof                  QueryRuntimeVerification
	Process                        QueryRemovalProcess
	Positive                       QueryRemovalPositiveEffect
	ExcludedInstances              []string
	ObservedAt                     time.Time
	ValidUntil                     time.Time
	CandidateAttestation           string
	SurvivorAttestation            string
	ProcessAttestation             string
	ScopeAttestation               string
	PermissionAttestation          string
	ResultAttestation              string
	PreEffectAttestation           string
	AssociationAttestation         string
}

// QueryRemovalRevalidationRequest accepts fresh evidence for the same reservation.
type QueryRemovalRevalidationRequest struct {
	Original              QueryRemovalOriginal
	ExternalScope         QueryRemovalExternalScope
	RequestID             string
	CommandDigest         string
	ExpectedCurrentDigest string
	Survivor              QueryRuntimeTarget
	SurvivorVersion       int64
	Evidence              QueryRemovalEvidence
}

// Clone prevents caller mutations from changing admitted evidence.
func (r QueryRemovalRevalidationRequest) Clone() QueryRemovalRevalidationRequest {
	r.Evidence.ExcludedInstances = slices.Clone(r.Evidence.ExcludedInstances)
	e := &r.Evidence
	e.ObservedAt = Timestamp(e.ObservedAt)
	e.ValidUntil = Timestamp(e.ValidUntil)
	e.Process.ObservedAt = Timestamp(e.Process.ObservedAt)
	e.Process.ValidUntil = Timestamp(e.Process.ValidUntil)
	e.Process.DrainDeadline = Timestamp(e.Process.DrainDeadline)
	e.Positive.PreEffectAt = Timestamp(e.Positive.PreEffectAt)
	e.Positive.AcceptedAt = Timestamp(e.Positive.AcceptedAt)
	e.Positive.CompletedAt = Timestamp(e.Positive.CompletedAt)
	for _, p := range []*QueryRuntimeVerification{&e.CandidateProof, &e.SurvivorProof} {
		p.VerifiedAt = Timestamp(p.VerifiedAt)
		p.ValidUntil = Timestamp(p.ValidUntil)
		p.AcceptedAt = Timestamp(p.AcceptedAt)
	}
	return r
}

// Validate checks bounded input before any authority write.
func (r QueryRemovalRevalidationRequest) Validate() error {
	if r.Original.Validate() != nil || r.ExternalScope.Validate() != nil || !QueryRemovalToken(r.RequestID) || !validHex256(r.CommandDigest) || r.ExpectedCurrentDigest != "" && !validHex256(r.ExpectedCurrentDigest) {
		return ErrInvalid
	}
	if r.Survivor.RuntimeID != "" && (r.Survivor.Validate() != nil || r.SurvivorVersion < 1) {
		return ErrInvalid
	}
	e := r.Evidence
	if (e.BuildFinalizationRequestID != "" || e.BuildFinalizationRequestDigest != "") && (!DeliveryIdentifier(e.BuildFinalizationRequestID) || !validHex256(e.BuildFinalizationRequestDigest)) {
		return ErrInvalid
	}
	if e.Disposition != "live" && e.Disposition != "effect_complete" {
		return ErrInvalid
	}
	for _, proof := range []string{e.CandidateAttestation, e.SurvivorAttestation, e.ProcessAttestation, e.ScopeAttestation, e.PermissionAttestation, e.ResultAttestation, e.PreEffectAttestation, e.AssociationAttestation} {
		if len(proof) > 4096 {
			return ErrInvalid
		}
	}
	if e.CandidateAttestation == "" || e.ProcessAttestation == "" || e.ScopeAttestation == "" || len(e.ExcludedInstances) == 0 || len(e.ExcludedInstances) > 64 {
		return ErrInvalid
	}
	for i, id := range e.ExcludedInstances {
		if !DeliveryIdentifier(id) || i > 0 && e.ExcludedInstances[i-1] >= id {
			return ErrInvalid
		}
	}
	digest, err := Fingerprint("dispatch.query-removal.exclusions.v1", e.ExcludedInstances)
	if err != nil || digest != r.ExternalScope.ExcludedInstancesDigest {
		return ErrInvalid
	}
	return QueryRemovalSize(r, QueryRemovalMaterialLimit)
}

// QueryRemovalSize enforces actual compact JSON bytes, including HTML escaping.
func QueryRemovalSize(v any, limit int) error {
	b, err := json.Marshal(v)
	if err != nil || len(b) > limit {
		return ErrInvalid
	}
	return nil
}

// QueryRemovalGuard is a compact immutable root with a monotone terminal state.
// Full identities are retained in the exact initial material row.
type QueryRemovalGuard struct {
	GuardID             string
	RootDigest          string
	OriginalDigest      string
	ExternalScopeDigest string
	CandidateDigest     string
	ProcessDigest       string
	InitialRequestID    string
	State               string
	CloseRequestID      string
	CloseDigest         string
}

// QueryRemovalCurrent is an exact pointer, never permission by itself.
type QueryRemovalCurrent struct {
	GuardID        string
	RootDigest     string
	RequestID      string
	MaterialDigest string
	ObservedAt     time.Time
}

// QueryRemovalRevalidationMaterial retains the full authenticated acceptance facts.
type QueryRemovalRevalidationMaterial struct {
	Request            QueryRemovalRevalidationRequest
	RequestDigest      string
	GuardID            string
	RootDigest         string
	Candidate          QueryRuntimeIdentity
	CandidateState     string
	CandidateVersion   int64
	RemovalEpoch       int64
	Build              BuildAdmission
	RetainedExecutions int64
	Survivor           QueryRuntimeIdentity
	SurvivorVersion    int64
	AcceptedAt         time.Time
	ValidUntil         time.Time
	DeliveryID         string
	MaterialDigest     string
}

// QueryRemovalRevalidationReceipt is the bounded exported commitment to material.
type QueryRemovalRevalidationReceipt struct {
	Schema              string    `json:"schema"`
	GuardID             string    `json:"guard_id"`
	RootDigest          string    `json:"root_digest"`
	RequestID           string    `json:"request_id"`
	RequestDigest       string    `json:"request_digest"`
	MaterialDigest      string    `json:"material_digest"`
	OriginalDigest      string    `json:"original_digest"`
	ExternalScopeDigest string    `json:"external_scope_digest"`
	CandidateDigest     string    `json:"candidate_digest"`
	BuildDigest         string    `json:"build_digest"`
	SurvivorDigest      string    `json:"survivor_digest"`
	ProcessDigest       string    `json:"process_digest"`
	ExclusionDigest     string    `json:"exclusion_digest"`
	CoverageDigest      string    `json:"coverage_digest"`
	Disposition         string    `json:"disposition"`
	AcceptedAt          time.Time `json:"accepted_at"`
	ValidUntil          time.Time `json:"valid_until"`
}

// QueryRemovalRevalidationLookup recovers one immutable request, never fresh authority.
type QueryRemovalRevalidationLookup struct {
	QueryRuntimeTarget
	RequestID     string
	CommandDigest string
	RequestDigest string
}

// Validate checks the exact lookup association.
func (q QueryRemovalRevalidationLookup) Validate() error {
	if q.QueryRuntimeTarget.Validate() != nil || !QueryRemovalToken(q.RequestID) || (q.CommandDigest == "" && q.RequestDigest == "") || q.CommandDigest != "" && !validHex256(q.CommandDigest) || q.RequestDigest != "" && !validHex256(q.RequestDigest) {
		return ErrInvalid
	}
	return nil
}

// QueryRemovalGuardReference binds every command to its immutable original and root.
type QueryRemovalGuardReference struct {
	QueryRuntimeTarget
	OriginalDigest string
	GuardID        string
	RootDigest     string
}

// QueryRemovalPermission is authenticated external acceptance of one exact invocation.
type QueryRemovalPermission struct {
	ExternalScope         QueryRemovalExternalScope
	PermissionID          string
	PermissionDigest      string
	CommandID             string
	StepDigest            string
	ResourceDigest        string
	IncarnationDigest     string
	RevalidationRequestID string
	RevalidationDigest    string
	AcceptedAt            time.Time
	EvidenceDigest        string
	Attestation           string
}

// QueryRemovalCommitRequest requires fresh current evidence for first acceptance.
type QueryRemovalCommitRequest struct {
	Guard                 QueryRemovalGuardReference
	RequestID             string
	CommandDigest         string
	ExpectedCurrentDigest string
	CurrentRequestID      string
	Permission            QueryRemovalPermission
}

// QueryRemovalCommitReceipt is a historical, nonissuing invocation association.
type QueryRemovalCommitReceipt struct {
	Request       QueryRemovalCommitRequest
	RequestDigest string
	ReceiptDigest string
	AcceptedAt    time.Time
	DeliveryID    string
}

// QueryRemovalClosure proves permanent issuance cutoff and complete terminal coverage.
type QueryRemovalClosure struct {
	ExternalScope   QueryRemovalExternalScope
	CutoffReceiptID string
	CutoffDigest    string
	CoverageDigest  string
	Disposition     string
	TerminalDigest  string
	CutoffAt        time.Time
	TerminalAt      time.Time
	EvidenceDigest  string
	Attestation     string
}

// QueryRemovalGuardCloseRequest permanently closes an exact guard.
type QueryRemovalGuardCloseRequest struct {
	Guard         QueryRemovalGuardReference
	RequestID     string
	CommandDigest string
	Closure       QueryRemovalClosure
}

// QueryRemovalGuardCloseReceipt retains closure despite expiry and later native mutations.
type QueryRemovalGuardCloseReceipt struct {
	Request       QueryRemovalGuardCloseRequest
	RequestDigest string
	ReceiptDigest string
	AcceptedAt    time.Time
	DeliveryID    string
}

// QueryRemovalGuardReceiptLookup reads exactly one finite command kind.
type QueryRemovalGuardReceiptLookup struct {
	QueryRemovalRevalidationLookup
	Kind string
}

// QueryRemovalGuardReceipt contains exactly one command receipt.
type QueryRemovalGuardReceipt struct {
	Commit *QueryRemovalCommitReceipt
	Close  *QueryRemovalGuardCloseReceipt
}

// QueryRemovalCommitLookup identifies a saved commitment and its protected state.
type QueryRemovalCommitLookup struct{ QueryRemovalRevalidationLookup }

// QueryRemovalCommitStatus reports observations without granting delegation.
type QueryRemovalCommitStatus struct {
	Receipt    QueryRemovalCommitReceipt
	Guard      QueryRemovalGuard
	Current    QueryRemovalCurrent
	ObservedAt time.Time
}

// QueryRemovalRevalidationStore is optional. Hosts without it must refuse this profile.
type QueryRemovalRevalidationStore interface {
	RevalidateQueryRuntimeRemoval(context.Context, QueryRemovalRevalidationRequest) (QueryRemovalRevalidationReceipt, error)
	LookupQueryRemovalRevalidation(context.Context, QueryRemovalRevalidationLookup) (QueryRemovalRevalidationMaterial, error)
	BindQueryRemovalCommit(context.Context, QueryRemovalCommitRequest) (QueryRemovalCommitReceipt, error)
	CheckQueryRemovalCommit(context.Context, QueryRemovalCommitLookup) (QueryRemovalCommitStatus, error)
	CloseQueryRemovalGuard(context.Context, QueryRemovalGuardCloseRequest) (QueryRemovalGuardCloseReceipt, error)
	LookupQueryRemovalGuardReceipt(context.Context, QueryRemovalGuardReceiptLookup) (QueryRemovalGuardReceipt, error)
}

// QueryRemovalGuardProbe carries opaque host record references, never remote evidence.
type QueryRemovalGuardProbe struct {
	Original          QueryRemovalOriginal
	ExternalScope     QueryRemovalExternalScope
	EvidenceReference string
	Survivor          QueryRuntimeTarget
	SurvivorVersion   int64
}

// QueryRemovalGuardVerifier authenticates retained records outside native locks.
// Implementations must reject unsupported profiles and incomplete effect coverage.
type QueryRemovalGuardVerifier interface {
	VerifyRevalidation(context.Context, QueryRemovalGuardProbe) (QueryRemovalEvidence, error)
	VerifyPermission(context.Context, QueryRemovalGuardReference, string) (QueryRemovalPermission, error)
	VerifyClosure(context.Context, QueryRemovalGuardReference, string) (QueryRemovalClosure, error)
}
