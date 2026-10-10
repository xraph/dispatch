package durable

import (
	"context"
	"errors"
	"time"
)

const RetirementWriterProtocol = 1
const RetirementSchemaVersion = 1

var ErrWriterCompatibility = errors.New("durable: incompatible retirement writer")
var ErrLifecycleBusy = errors.New("durable: namespace coordination busy")

// NamespaceTarget binds a lifecycle operation to its persisted installation.
type NamespaceTarget struct {
	InstallationID string
	Namespace      string
}

func (t NamespaceTarget) Validate() error {
	if !DeliveryIdentifier(t.InstallationID) || !DeliveryIdentifier(t.Namespace) {
		return ErrInvalid
	}
	return nil
}

type BuildTarget struct {
	NamespaceTarget
	BuildID string
}

func (t BuildTarget) Validate() error {
	if t.NamespaceTarget.Validate() != nil || ValidateBuildID(t.BuildID) != nil {
		return ErrInvalid
	}
	return nil
}

type CompatibilityFacts struct {
	// QueryRetentionSchemaVersion describes current store capability only. It is
	// not proof that a host artifact or controller is query-retention qualified.
	QueryRetentionSchemaVersion int `json:",omitempty"`
	NamespaceTarget
	Enrolled       bool
	SchemaVersion  int
	WriterProtocol int
	Version        int64
	EnrolledAt     time.Time
	ObservedAt     time.Time
}

type RetirementEnrollmentRequest struct {
	NamespaceTarget
	RequestID      string
	SchemaVersion  int
	WriterProtocol int
}

func (r RetirementEnrollmentRequest) Validate() error {
	if r.NamespaceTarget.Validate() != nil || !DeliveryIdentifier(r.RequestID) {
		return ErrInvalid
	}
	if r.SchemaVersion != RetirementSchemaVersion || r.WriterProtocol != RetirementWriterProtocol {
		return ErrWriterCompatibility
	}
	return nil
}

type RetirementEnrollment struct {
	Compatibility         CompatibilityFacts
	HistoricalBuildCount  int64
	HistoricalBuildDigest string
}

type LifecycleOperation string

const OperationEnrollRetirement LifecycleOperation = "namespace.retirement.enroll"

// LifecycleReceipt is the immutable accepted response, including derived facts.
// A receipt proves persistence, not a later process or infrastructure effect.
type LifecycleReceipt struct {
	NamespaceTarget
	Operation       LifecycleOperation
	RequestID       string
	RequestDigest   string
	CommandDigest   string
	ResponseVersion int
	AcceptedAt      time.Time
	DeliveryID      string
	Enrollment      *RetirementEnrollment
	QueryRuntime    *QueryRuntimeBinding
	QueryAbort      *QueryRemovalAbortEvidence `json:",omitempty"`
	Build           *BuildAdmission
}

type LifecycleReceiptLookup struct {
	NamespaceTarget
	Operation     LifecycleOperation
	RequestID     string
	RequestDigest string
	CommandDigest string
}

func (r LifecycleReceiptLookup) Validate() error {
	if r.NamespaceTarget.Validate() != nil || !DeliveryIdentifier(r.RequestID) || !validLifecycleOperation(r.Operation) {
		return ErrInvalid
	}
	if r.RequestDigest == "" && r.CommandDigest == "" {
		return ErrInvalid
	}
	return nil
}

// RetirementEnrollmentStore is optional. Base Store implementations remain
// usable without enrolling a namespace in persisted deployment retirement.
type RetirementEnrollmentStore interface {
	InspectCompatibility(context.Context, NamespaceTarget) (CompatibilityFacts, error)
	EnrollRetirement(context.Context, RetirementEnrollmentRequest) (LifecycleReceipt, error)
	LookupLifecycleReceipt(context.Context, LifecycleReceiptLookup) (LifecycleReceipt, error)
}

// LifecycleSourceID keeps namespace commands outside execution receipt domains.
func LifecycleSourceID(namespace string, operation LifecycleOperation, requestID string) string {
	return ReceiptSourceID("dispatch.lifecycle.v1", namespace, string(operation), requestID)
}

func LifecycleDeliverySource(ctx context.Context, receipt LifecycleReceipt) DeliverySource {
	sourceID := LifecycleSourceID(receipt.Namespace, receipt.Operation, receipt.RequestID)
	return DeliverySource{Key: Key{Namespace: receipt.Namespace}, Kind: "security", ID: sourceID, OccurredAt: receipt.AcceptedAt, Action: LifecycleAction(receipt.Operation), Outcome: "accepted", Target: sourceID, Metadata: AuditMetadataFromContext(ctx)}
}

func (r LifecycleReceipt) Clone() LifecycleReceipt {
	if r.QueryAbort != nil {
		v := *r.QueryAbort
		r.QueryAbort = &v
	}
	if r.QueryRuntime != nil {
		v := *r.QueryRuntime
		r.QueryRuntime = &v
	}
	if r.Build != nil {
		v := *r.Build
		r.Build = &v
	}
	if r.Enrollment != nil {
		v := *r.Enrollment
		r.Enrollment = &v
	}
	return r
}

// Match validates stored evidence before allowing an exact accepted replay.
func (r LifecycleReceipt) Match(q LifecycleReceiptLookup) error {
	if r.NamespaceTarget != q.NamespaceTarget || r.Operation != q.Operation || r.RequestID != q.RequestID || r.ResponseVersion != 1 || r.AcceptedAt.IsZero() || r.DeliveryID == "" || !r.validResult() {
		return ErrInvalid
	}
	if q.RequestDigest != "" && q.RequestDigest != r.RequestDigest || q.CommandDigest != "" && q.CommandDigest != r.CommandDigest {
		return ErrRequestConflict
	}
	return nil
}

// BuildAdmission records admission and optional trusted query identity.
// Historical enrollment leaves QueryIdentity empty until explicit evidence arrives.
type BuildAdmission struct {
	QueryIdentity BuildQueryIdentity
	BuildTarget
	State       string
	Epoch       int64
	Version     int64
	CutoffEpoch int64
	ChangedAt   time.Time
}

func validLifecycleOperation(operation LifecycleOperation) bool {
	switch operation {
	case OperationRegisterQueryRuntime, OperationVerifyQueryRuntime, OperationBeginQueryRemoval, OperationFinishQueryRemoval, OperationAbortQueryRemoval, OperationEnrollRetirement, OperationRegisterBuild, OperationBeginRetirement, OperationFinalizeRetirement, OperationAbortRetirement:
		return true
	}
	return false
}
func (r LifecycleReceipt) validResult() bool {
	if r.QueryRuntime != nil || r.QueryAbort != nil || queryLifecycleOperation(r.Operation) {
		return r.validQueryResult()
	}
	if r.Operation == OperationEnrollRetirement {
		return r.Enrollment != nil && r.Build == nil && r.Enrollment.Compatibility.NamespaceTarget == r.NamespaceTarget && r.Enrollment.Compatibility.Enrolled && r.Enrollment.Compatibility.WriterProtocol == RetirementWriterProtocol && r.Enrollment.Compatibility.SchemaVersion == RetirementSchemaVersion && r.Enrollment.HistoricalBuildCount >= 0 && len(r.Enrollment.HistoricalBuildDigest) == 64
	}
	return (r.Operation == OperationRegisterBuild || r.Operation == OperationBeginRetirement || r.Operation == OperationFinalizeRetirement || r.Operation == OperationAbortRetirement) && r.Build != nil && r.Enrollment == nil && r.Build.Validate() == nil && r.Build.NamespaceTarget == r.NamespaceTarget && r.Build.Epoch > 0 && r.Build.Version > 0 && (r.Build.State == BuildAccepting || r.Build.State == BuildRetiring || r.Build.State == BuildRetired)
}

// LifecycleAction is the closed authorization and Chronicle action mapping.
func LifecycleAction(operation LifecycleOperation) string {
	switch operation {
	case OperationRegisterQueryRuntime:
		return "dispatch.query_runtime.register"
	case OperationVerifyQueryRuntime:
		return "dispatch.query_runtime.verify"
	case OperationBeginQueryRemoval:
		return "dispatch.query_runtime.remove"
	case OperationFinishQueryRemoval:
		return "dispatch.query_runtime.finish"
	case OperationAbortQueryRemoval:
		return "dispatch.query_runtime.abort"
	case OperationEnrollRetirement:
		return "dispatch.retirement.enroll"
	case OperationRegisterBuild:
		return "dispatch.build.register"
	case OperationBeginRetirement:
		return "dispatch.build.retire"
	case OperationFinalizeRetirement:
		return "dispatch.build.finalize"
	case OperationAbortRetirement:
		return "dispatch.build.resume"
	default:
		return ""
	}
}

// ValidateBuildID retains the execution contract's existing identifier limits.
func ValidateBuildID(build string) error {
	if !identifier(build) {
		return ErrInvalid
	}
	return nil
}
