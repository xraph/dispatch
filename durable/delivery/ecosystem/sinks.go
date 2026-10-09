package ecosystem

import (
	"context"
	"encoding/json"
	"errors"

	ca "github.com/xraph/chronicle/acceptance"
	ci "github.com/xraph/chronicle/id"
	ra "github.com/xraph/relay/acceptance"
	ri "github.com/xraph/relay/id"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
)

type ChronicleClient interface {
	RecordOnce(context.Context, ca.Request) (*ca.Receipt, error)
}
type RelayClient interface {
	SendReliable(context.Context, ra.Request) (*ra.Receipt, error)
}

type Chronicle struct {
	Binding Binding
	Client  ChronicleClient
}
type Relay struct {
	Binding Binding
	Client  RelayClient
}

func (s Chronicle) Accept(ctx context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
	req, err := ChronicleRequest(s.Binding, d)
	if err != nil {
		return durable.SinkReceipt{}, err
	}
	if s.Client == nil {
		return durable.SinkReceipt{}, durable.ErrInvalid
	}
	fp, err := ca.Fingerprint(req)
	if err != nil {
		return durable.SinkReceipt{}, err
	}
	r, err := s.Client.RecordOnce(ctx, req)
	if errors.Is(err, ca.ErrConflict) {
		return durable.SinkReceipt{}, delivery.ErrConfirmedConflict
	}
	if err != nil {
		return durable.SinkReceipt{}, err
	}
	if r == nil || r.Producer != req.Producer || r.Installation != req.Installation || r.SourceKey != req.SourceKey || r.SourceFingerprint != req.SourceFingerprint || r.AppID != d.AppID || r.OrgID != req.OrgID || r.TenantID != d.TenantID || r.Fingerprint != fp || r.EventID.Prefix() != ci.PrefixAudit || r.StreamID.Prefix() != ci.PrefixStream || r.Sequence == 0 || r.Hash == "" || r.HashScheme == "" {
		return durable.SinkReceipt{}, durable.ErrRequestConflict
	}
	return receipt(d, r.EventID.String(), fp, r)
}
func (s Relay) Accept(ctx context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
	req, err := RelayRequest(s.Binding, d)
	if err != nil {
		return durable.SinkReceipt{}, err
	}
	if s.Client == nil {
		return durable.SinkReceipt{}, durable.ErrInvalid
	}
	fp, err := ra.Fingerprint(req)
	if err != nil {
		return durable.SinkReceipt{}, err
	}
	r, err := s.Client.SendReliable(ctx, req)
	if errors.Is(err, ra.ErrConflict) {
		return durable.SinkReceipt{}, delivery.ErrConfirmedConflict
	}
	if err != nil {
		return durable.SinkReceipt{}, err
	}
	if r == nil || r.Verify(req) != nil || r.Fingerprint != fp || r.EventID.Prefix() != ri.PrefixEvent || r.AcceptedAt.IsZero() || len(r.Recipients) > ra.MaxRecipients {
		return durable.SinkReceipt{}, durable.ErrRequestConflict
	}
	endpoints := map[string]bool{}
	deliveries := map[string]bool{}
	for _, recipient := range r.Recipients {
		endpointID, deliveryID := recipient.EndpointID.String(), recipient.DeliveryID.String()
		if recipient.EndpointID.Prefix() != ri.PrefixEndpoint || recipient.DeliveryID.Prefix() != ri.PrefixDelivery || endpoints[endpointID] || deliveries[deliveryID] {
			return durable.SinkReceipt{}, durable.ErrRequestConflict
		}
		endpoints[endpointID] = true
		deliveries[deliveryID] = true
	}
	return receipt(d, r.EventID.String(), r.Fingerprint, r)
}
func receipt(d durable.Delivery, id, fp string, evidence any) (durable.SinkReceipt, error) {
	raw, err := json.Marshal(evidence)
	if err != nil {
		return durable.SinkReceipt{}, err
	}
	if len(raw) > MaxResponseBytes {
		return durable.SinkReceipt{}, durable.ErrInvalid
	}
	return durable.SinkReceipt{ID: id, DeliveryID: d.ID, Destination: d.Destination, SchemaVersion: d.SchemaVersion, Fingerprint: d.Fingerprint, MappingVersion: MappingVersion, SinkFingerprint: fp, Evidence: string(raw)}, nil
}
