package durable

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"
)

type Destination string

const (
	DestinationChronicle Destination = "chronicle"
	DestinationRelay     Destination = "relay"
)

// AuditMetadata contains trusted, verified facts only. Scope is always supplied
// by the persisted namespace catalog. The carrier is not an authentication API.
type AuditMetadata struct {
	ActorKind     string
	ActorID       string
	RequestID     string
	CorrelationID string
	DecisionID    string
	PolicyVersion string
	ReasonCode    string
}

type auditContextKey struct{}

func WithAuditMetadata(ctx context.Context, m AuditMetadata) context.Context {
	return context.WithValue(ctx, auditContextKey{}, m)
}
func AuditMetadataFromContext(ctx context.Context) AuditMetadata {
	if m, ok := ctx.Value(auditContextKey{}).(AuditMetadata); ok {
		return m
	}
	return AuditMetadata{ActorKind: "system", ActorID: "dispatch"}
}
func (m AuditMetadata) Validate() error {
	switch m.ActorKind {
	case "system", "worker", "user", "service", "api_key", "service_acct":
		if !DeliveryIdentifier(m.ActorID) {
			return ErrInvalid
		}
	case "anonymous":
		if m.ActorID != "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	for _, v := range []string{m.RequestID, m.CorrelationID, m.DecisionID, m.PolicyVersion, m.ReasonCode} {
		if v != "" && !DeliveryIdentifier(v) {
			return ErrInvalid
		}
	}
	return nil
}

// Delivery contains no execution payload, progress, secret or arbitrary map.
type Delivery struct {
	ID             string
	Destination    Destination
	SchemaVersion  int
	InstallationID string
	Namespace      string
	AppID          string
	TenantID       string
	WorkflowID     string
	RunID          string
	SourceKind     string
	SourceID       string
	Sequence       int64
	OccurredAt     time.Time
	Action         string
	Outcome        string
	Target         string
	Metadata       AuditMetadata
	Fingerprint    string
}

// DeliverySource is supplied by a store mutation, or by a captured security
// attempt. SourceKind separates event, receipt and security identity domains.
type DeliverySource struct {
	Key        Key
	Kind       string
	ID         string
	Sequence   int64
	OccurredAt time.Time
	Action     string
	Outcome    string
	Target     string
	Metadata   AuditMetadata
}

func NewDelivery(n NamespaceRecord, destination Destination, s DeliverySource) (Delivery, error) {
	if err := n.Validate(); err != nil {
		return Delivery{}, err
	}
	if destination != DestinationChronicle && destination != DestinationRelay {
		return Delivery{}, ErrInvalid
	}
	if s.Key.Namespace != n.Namespace || s.OccurredAt.IsZero() || !DeliveryIdentifier(s.Kind) || !DeliveryIdentifier(s.ID) || !DeliveryIdentifier(s.Action) || !DeliveryIdentifier(s.Outcome) || s.Sequence < 0 {
		return Delivery{}, ErrInvalid
	}
	for _, v := range []string{s.Key.WorkflowID, s.Key.RunID, s.Target} {
		if v != "" && !DeliveryIdentifier(v) {
			return Delivery{}, ErrInvalid
		}
	}
	if err := s.Metadata.Validate(); err != nil {
		return Delivery{}, err
	}
	d := Delivery{Destination: destination, SchemaVersion: n.SchemaVersion, InstallationID: n.InstallationID, Namespace: n.Namespace, AppID: n.AppID, TenantID: n.TenantID, WorkflowID: s.Key.WorkflowID, RunID: s.Key.RunID, SourceKind: s.Kind, SourceID: s.ID, Sequence: s.Sequence, OccurredAt: s.OccurredAt.UTC().Truncate(time.Microsecond), Action: s.Action, Outcome: s.Outcome, Target: s.Target, Metadata: s.Metadata}
	var err error
	d.ID, err = deliveryHash([]string{d.InstallationID, string(d.Destination), strconv.Itoa(d.SchemaVersion), d.Namespace, d.WorkflowID, d.RunID, d.SourceKind, d.SourceID})
	if err != nil {
		return Delivery{}, err
	}
	d.Fingerprint, err = deliveryHash(d)
	return d, err
}
func deliveryHash(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func (d Delivery) Verify() error {
	fingerprint := d.Fingerprint
	d.Fingerprint = ""
	computed, err := deliveryHash(d)
	if err != nil {
		return err
	}
	if fingerprint != computed {
		return ErrRequestConflict
	}
	return nil
}
func EventDeliverySource(ctx context.Context, key Key, event Event) DeliverySource {
	return DeliverySource{Key: key, Kind: "event", ID: strconv.FormatInt(event.Sequence, 10), Sequence: event.Sequence, OccurredAt: event.Time, Action: event.Type, Outcome: "accepted", Metadata: AuditMetadataFromContext(ctx)}
}

// ReceiptSourceID is unambiguous even when a child delivery ID contains a colon.
func ReceiptSourceID(parts ...string) string {
	value := ""
	for _, part := range parts {
		value += strconv.Itoa(len(part)) + ":" + part
	}
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])
}

type SecurityAudit struct {
	InstallationID string
	Namespace      string
	AttemptID      string
	OccurredAt     time.Time
	Action         string
	Outcome        string
	Target         string
	Metadata       AuditMetadata
}

// CaptureSecurityAudit allocates immutable server identity and time once. Retry
// the returned value unchanged after an unknown persistence outcome.
func CaptureSecurityAudit(installation, namespace, action, outcome, target string, metadata AuditMetadata) (SecurityAudit, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return SecurityAudit{}, err
	}
	return SecurityAudit{InstallationID: installation, Namespace: namespace, AttemptID: hex.EncodeToString(b), OccurredAt: time.Now().UTC().Truncate(time.Microsecond), Action: action, Outcome: outcome, Target: target, Metadata: metadata}, nil
}
func (a SecurityAudit) Delivery(n NamespaceRecord) (Delivery, error) {
	if a.InstallationID != n.InstallationID || a.Namespace != n.Namespace || !n.RequireAudit || len(a.AttemptID) != 64 {
		return Delivery{}, ErrInvalid
	}
	if _, err := hex.DecodeString(a.AttemptID); err != nil {
		return Delivery{}, ErrInvalid
	}
	return NewDelivery(n, DestinationChronicle, DeliverySource{Key: Key{Namespace: a.Namespace}, Kind: "security", ID: a.AttemptID, OccurredAt: a.OccurredAt, Action: a.Action, Outcome: a.Outcome, Target: a.Target, Metadata: a.Metadata})
}

type DeliveryScope struct {
	InstallationID string
	Destination    Destination
}

func (s DeliveryScope) Validate() error {
	if !DeliveryIdentifier(s.InstallationID) || s.Destination != DestinationChronicle && s.Destination != DestinationRelay {
		return ErrInvalid
	}
	return nil
}

type DeliveryClaim struct {
	DeliveryScope
	Owner         string
	Limit         int
	LeaseDuration time.Duration
}

func (r DeliveryClaim) Validate() error {
	if r.DeliveryScope.Validate() != nil || !DeliveryIdentifier(r.Owner) || r.Limit < 1 || r.Limit > MaxDeliveryBatch || !validDeliveryLease(r.LeaseDuration) {
		return ErrInvalid
	}
	return nil
}
func validDeliveryLease(d time.Duration) bool { return d >= time.Millisecond && d <= 5*time.Minute }

type DeliveryToken struct {
	DeliveryScope
	ID    string
	Owner string
	Epoch int64
}

func (t DeliveryToken) Validate() error {
	if t.DeliveryScope.Validate() != nil || !DeliveryIdentifier(t.ID) || !DeliveryIdentifier(t.Owner) || t.Epoch < 1 {
		return ErrInvalid
	}
	return nil
}

type SinkReceipt struct {
	MappingVersion  int
	SinkFingerprint string
	Evidence        string
	ID              string
	DeliveryID      string
	Destination     Destination
	SchemaVersion   int
	Fingerprint     string
}

func (r SinkReceipt) Verify(d Delivery) error {
	if !DeliveryIdentifier(r.ID) || r.DeliveryID != d.ID || r.Destination != d.Destination || r.SchemaVersion != d.SchemaVersion || r.Fingerprint != d.Fingerprint {
		return ErrRequestConflict
	}
	return nil
}

type DeliveryRecord struct {
	Delivery      Delivery
	Owner         string
	Epoch         int64
	LeaseUntil    time.Time
	Attempts      int64
	NextAttemptAt time.Time
	AcceptedAt    time.Time
	DeliveredAt   time.Time
	Receipt       SinkReceipt
	ErrorCategory string
}

func (r DeliveryRecord) Token() DeliveryToken {
	return DeliveryToken{DeliveryScope: DeliveryScope{InstallationID: r.Delivery.InstallationID, Destination: r.Delivery.Destination}, ID: r.Delivery.ID, Owner: r.Owner, Epoch: r.Epoch}
}

type DeliveryRetry struct {
	Token    DeliveryToken
	Delay    time.Duration
	Category string
}

func (r DeliveryRetry) Validate() error {
	if r.Token.Validate() != nil || r.Delay < 0 || r.Delay > 24*time.Hour {
		return ErrInvalid
	}
	switch r.Category {
	case "unavailable", "timeout", "rejected", "invalid_receipt", "canceled":
		return nil
	default:
		return ErrInvalid
	}
}

type DeliveryStatusRequest struct {
	DeliveryScope
	After string
	Limit int
}

func (r DeliveryStatusRequest) Validate() error {
	if r.DeliveryScope.Validate() != nil || r.Limit < 1 || r.Limit > MaxDeliveryBatch || r.After != "" && !DeliveryIdentifier(r.After) {
		return ErrInvalid
	}
	return nil
}

type DeliveryStatus struct {
	Blocked          int64
	Pending          int64
	OldestAcceptedAt time.Time
	Records          []DeliveryRecord
}

type OutboxStore interface {
	AppendSecurityAudit(context.Context, SecurityAudit) (Delivery, error)
	ClaimDeliveries(context.Context, DeliveryClaim) ([]DeliveryRecord, error)
	RenewDelivery(context.Context, DeliveryToken, time.Duration) (time.Time, error)
	AcknowledgeDelivery(context.Context, DeliveryToken, SinkReceipt) error
	RetryDelivery(context.Context, DeliveryRetry) error
	BlockDelivery(context.Context, DeliveryToken) error
	DeliveryStatus(context.Context, DeliveryStatusRequest) (DeliveryStatus, error)
}

// ValidateDeliveryRenewal bounds lease extensions consistently across backends.
func ValidateDeliveryRenewal(t DeliveryToken, d time.Duration) error {
	if t.Validate() != nil || !validDeliveryLease(d) {
		return ErrInvalid
	}
	return nil
}

// Blocked reports a confirmed sink content conflict. Repair requires a separate
// protected operation; automatic claims never clear this disposition.
func (r DeliveryRecord) Blocked() bool { return r.ErrorCategory == "conflict" }

// DeliveryPublisherProtocol is independent of the audit writer protocol. Version
// 1 understands permanent conflict disposition and verifies sink receipts.
const DeliveryPublisherProtocol = 1

// DeliveryCompatibilityStore rejects unsupported publisher/schema combinations
// before a publisher starts. Database fences also reject pre-protocol artifacts.
type DeliveryCompatibilityStore interface {
	CheckDeliveryCompatibility(context.Context, int) error
}
