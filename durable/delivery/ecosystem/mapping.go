// Package ecosystem maps Dispatch envelopes to reliable ecosystem acceptance APIs.
package ecosystem

import (
	"encoding/json"
	"strconv"

	ca "github.com/xraph/chronicle/acceptance"
	"github.com/xraph/chronicle/audit"
	ra "github.com/xraph/relay/acceptance"

	"github.com/xraph/dispatch/durable"
)

const MappingVersion = 1
const RelayEventType = "dispatch.delivery.v1"

// Binding comes from the host installation registry, never delivery content.
// Each adapter serves one immutable namespace ownership record.
type Binding struct {
	Producer       string
	InstallationID string
	Namespace      string
	AppID          string
	OrgID          string
	TenantID       string
}

func (b Binding) Validate() error {
	for _, v := range []string{b.Producer, b.InstallationID, b.Namespace, b.AppID, b.TenantID} {
		if !durable.DeliveryIdentifier(v) {
			return durable.ErrInvalid
		}
	}
	if b.OrgID != "" && !durable.DeliveryIdentifier(b.OrgID) {
		return durable.ErrInvalid
	}
	return nil
}
func (b Binding) check(d durable.Delivery, destination durable.Destination) error {
	if b.Validate() != nil || d.SchemaVersion != MappingVersion || d.InstallationID != b.InstallationID || d.Namespace != b.Namespace || d.AppID != b.AppID || d.TenantID != b.TenantID || d.Destination != destination {
		return durable.ErrInvalid
	}
	return d.Verify()
}

func ChronicleRequest(b Binding, d durable.Delivery) (ca.Request, error) {
	if err := b.check(d, durable.DestinationChronicle); err != nil {
		return ca.Request{}, err
	}
	metadata := map[string]any{"mapping_version": MappingVersion, "namespace": d.Namespace, "workflow_id": d.WorkflowID, "run_id": d.RunID, "source_kind": d.SourceKind, "source_id": d.SourceID, "source_sequence": json.Number(strconv.FormatInt(d.Sequence, 10)), "actor_kind": d.Metadata.ActorKind, "actor_id": d.Metadata.ActorID, "correlation_id": d.Metadata.CorrelationID, "decision_id": d.Metadata.DecisionID, "policy_version": d.Metadata.PolicyVersion}
	event := &audit.Event{Timestamp: d.OccurredAt, AppID: d.AppID, TenantID: d.TenantID, RequestID: d.Metadata.RequestID, Action: d.Action, Resource: "dispatch.execution", Category: "dispatch", ResourceID: d.Target, Metadata: metadata, Outcome: d.Outcome, Severity: audit.SeverityInfo, Reason: d.Metadata.ReasonCode}
	if d.SourceKind == "security" {
		event.Resource = "dispatch.installation"
	}
	if d.Metadata.ActorKind == "user" {
		event.UserID = d.Metadata.ActorID
	}
	request := ca.Request{Producer: b.Producer, Installation: b.InstallationID, SourceKey: d.ID, SourceFingerprint: d.Fingerprint, OrgID: b.OrgID, Event: event}
	normalized, _, err := ca.Normalize(request)
	return normalized, err
}
func RelayRequest(b Binding, d durable.Delivery) (ra.Request, error) {
	if err := b.check(d, durable.DestinationRelay); err != nil {
		return ra.Request{}, err
	}
	if d.SourceKind != "event" {
		return ra.Request{}, durable.ErrInvalid
	}
	data, err := json.Marshal(d)
	if err != nil {
		return ra.Request{}, err
	}
	return (ra.Request{Producer: b.Producer, InstallationID: b.InstallationID, SourceKey: d.ID, SourceFingerprint: d.Fingerprint, AppID: b.AppID, OrgID: b.OrgID, TenantID: b.TenantID, Type: RelayEventType, Data: data}).Normalize()
}
