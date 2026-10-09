package sinkhost

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strconv"

	ca "github.com/xraph/chronicle/acceptance"
	ra "github.com/xraph/relay/acceptance"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

func decode(r *http.Request, value any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 65537))
	if err != nil || len(raw) > 65536 {
		return errors.New("invalid body")
	}
	_, err = ra.CanonicalJSON(raw)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}

// VerifyChronicle recomputes the source envelope and the complete versioned sink
// mapping. A credential cannot supply an unrelated source or semantic hash.
func VerifyChronicle(b ecosystem.Binding, r ca.Request) error {
	if r.Event == nil {
		return durable.ErrInvalid
	}
	e := r.Event
	m := e.Metadata
	field := func(key string) string {
		v, ok := m[key].(string)
		if !ok {
			return ""
		}
		return v
	}
	n, ok := m["source_sequence"].(json.Number)
	if !ok {
		return durable.ErrInvalid
	}
	seq, err := strconv.ParseInt(n.String(), 10, 64)
	if err != nil {
		return err
	}
	d, err := durable.NewDelivery(durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: b.InstallationID, Namespace: b.Namespace, AppID: b.AppID, TenantID: b.TenantID, SchemaVersion: 1}}, durable.DestinationChronicle, durable.DeliverySource{Key: durable.Key{Namespace: field("namespace"), WorkflowID: field("workflow_id"), RunID: field("run_id")}, Kind: field("source_kind"), ID: field("source_id"), Sequence: seq, OccurredAt: e.Timestamp, Action: e.Action, Outcome: e.Outcome, Target: e.ResourceID, Metadata: durable.AuditMetadata{ActorKind: field("actor_kind"), ActorID: field("actor_id"), RequestID: e.RequestID, CorrelationID: field("correlation_id"), DecisionID: field("decision_id"), PolicyVersion: field("policy_version"), ReasonCode: e.Reason}})
	if err != nil || d.ID != r.SourceKey || d.Fingerprint != r.SourceFingerprint {
		return durable.ErrInvalid
	}
	expected, err := ecosystem.ChronicleRequest(b, d)
	if err != nil {
		return err
	}
	want, err := ca.Fingerprint(expected)
	if err != nil {
		return err
	}
	got, err := ca.Fingerprint(r)
	if err != nil || got != want {
		return durable.ErrInvalid
	}
	return nil
}
func VerifyRelay(b ecosystem.Binding, r ra.Request) error {
	canonical, err := ra.CanonicalJSON(r.Data)
	if err != nil {
		return err
	}
	// Relay canonicalizes exact integers with trailing zeroes to exponent form.
	// Decode Sequence through an exact rational before narrowing to int64.
	var wire struct {
		durable.Delivery
		Sequence json.Number
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if decodeErr := decoder.Decode(&wire); decodeErr != nil {
		return decodeErr
	}
	number, ok := new(big.Rat).SetString(wire.Sequence.String())
	if !ok || !number.IsInt() || !number.Num().IsInt64() {
		return durable.ErrInvalid
	}
	d := wire.Delivery
	d.Sequence = number.Num().Int64()
	source, err := durable.NewDelivery(durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: b.InstallationID, Namespace: b.Namespace, AppID: b.AppID, TenantID: b.TenantID, SchemaVersion: 1}}, durable.DestinationRelay, durable.DeliverySource{Key: durable.Key{Namespace: d.Namespace, WorkflowID: d.WorkflowID, RunID: d.RunID}, Kind: d.SourceKind, ID: d.SourceID, Sequence: d.Sequence, OccurredAt: d.OccurredAt, Action: d.Action, Outcome: d.Outcome, Target: d.Target, Metadata: d.Metadata})
	if err != nil || source != d {
		return durable.ErrInvalid
	}
	expected, err := ecosystem.RelayRequest(b, d)
	if err != nil {
		return err
	}
	want, err := ra.Fingerprint(expected)
	if err != nil {
		return err
	}
	got, err := ra.Fingerprint(r)
	if err != nil || got != want {
		return durable.ErrInvalid
	}
	return nil
}
