package durable

import (
	"errors"
	"time"
)

// QueryRejectionDiagnostic is private instrumentation, never a command response.
// It contains values only, without query payloads or raw database messages.
type QueryRejectionDiagnostic struct {
	Stage           string             `json:"stage"`
	Reason          string             `json:"reason"`
	SQLState        string             `json:"sqlstate,omitempty"`
	Target          QueryRuntimeTarget `json:"target"`
	RequestID       string             `json:"request_id,omitempty"`
	Operation       LifecycleOperation `json:"operation,omitempty"`
	BindingState    string             `json:"binding_state,omitempty"`
	BindingVersion  int64              `json:"binding_version,omitempty"`
	VerifiedAt      time.Time          `json:"verified_at"`
	ValidUntil      time.Time          `json:"valid_until"`
	ObservedAt      time.Time          `json:"observed_at"`
	SettledAt       time.Time          `json:"settled_at"`
	IdentityMatches bool               `json:"identity_matches,omitempty"`
	Removed         bool               `json:"removed,omitempty"`
	AdmissionClosed bool               `json:"admission_closed,omitempty"`
	InFlight        int64              `json:"in_flight,omitempty"`
	UnknownClaims   int64              `json:"unknown_claims,omitempty"`
	ExpectedDigest  string             `json:"expected_digest,omitempty"`
	ActualDigest    string             `json:"actual_digest,omitempty"`
}

type QueryRejectionError struct {
	cause      error
	diagnostic QueryRejectionDiagnostic
}

func (e *QueryRejectionError) Error() string {
	for _, known := range []error{ErrQueryRetention, ErrRevisionConflict, ErrRequestConflict, ErrInvalid} {
		if errors.Is(e.cause, known) {
			return known.Error()
		}
	}
	return "durable: query verification rejected"
}
func (e *QueryRejectionError) Unwrap() error { return e.cause }

// NewQueryRejection preserves the original error class and bounds all metadata.
func NewQueryRejection(cause error, d QueryRejectionDiagnostic) error {
	if cause == nil {
		return nil
	}
	switch d.Stage {
	case "host_identity", "host_drain", "host_probe", "host_clock", "proof", "binding", "settlement", "postgres_guard":
	default:
		d.Stage = "unknown"
	}
	switch d.Reason {
	case "identity", "removed", "admission", "in_flight", "unknown_claims", "digest", "query_read", "clock_read", "clock_sample", "proof_id", "evidence_digest", "verifier", "policy", "verified_zero", "verified_future", "expired", "nonpositive_validity", "overlong_validity", "proof_before_settlement", "version", "state", "version_exhausted", "immutable_binding", "binding_row", "database_guard":
	default:
		d.Reason = "unknown"
	}
	if d.SQLState != "DL004" {
		d.SQLState = ""
	}
	if d.Target.Validate() != nil {
		d.Target = QueryRuntimeTarget{}
	}
	if !DeliveryIdentifier(d.RequestID) {
		d.RequestID = ""
	}
	if !queryLifecycleOperation(d.Operation) {
		d.Operation = ""
	}
	switch d.BindingState {
	case QueryRuntimeActive, QueryRuntimeRemoving, QueryRuntimeRemoved:
	default:
		d.BindingState = ""
	}
	if !validHex256(d.ExpectedDigest) {
		d.ExpectedDigest = ""
	}
	if !validHex256(d.ActualDigest) {
		d.ActualDigest = ""
	}
	return &QueryRejectionError{cause: cause, diagnostic: d}
}

// QueryRejectionDetails returns a value copy suitable for trusted instrumentation.
func QueryRejectionDetails(err error) (QueryRejectionDiagnostic, bool) {
	var rejected *QueryRejectionError
	if !errors.As(err, &rejected) {
		return QueryRejectionDiagnostic{}, false
	}
	return rejected.diagnostic, true
}
func proofRejection(reason string, p QueryRuntimeVerification, now time.Time) error {
	return NewQueryRejection(ErrQueryRetention, QueryRejectionDiagnostic{Stage: "proof", Reason: reason, VerifiedAt: p.VerifiedAt, ValidUntil: p.ValidUntil, ObservedAt: now})
}

func bindingRejection(cause error, reason string, b QueryRuntimeBinding, now time.Time) error {
	return NewQueryRejection(cause, QueryRejectionDiagnostic{Stage: "binding", Reason: reason, Target: b.QueryRuntimeTarget, BindingState: b.State, BindingVersion: b.Version, ObservedAt: now})
}
func queryBindingDiagnostic(err error, b QueryRuntimeBinding) error {
	d, ok := QueryRejectionDetails(err)
	if !ok {
		return err
	}
	d.Target, d.BindingState, d.BindingVersion = b.QueryRuntimeTarget, b.State, b.Version
	return NewQueryRejection(err, d)
}
