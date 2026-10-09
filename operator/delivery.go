package operator

import (
	"context"
	"strconv"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

type DeliveryInput struct {
	durable.Key
	Destination durable.Destination `json:"destination"`
	Cursor      string              `json:"cursor,omitempty"`
	Limit       int                 `json:"limit,omitempty"`
}
type Delivery struct {
	ID             string     `json:"id"`
	State          string     `json:"state"`
	Attempts       string     `json:"attempts"`
	AcceptedAt     time.Time  `json:"accepted_at"`
	SinkAcceptedAt *time.Time `json:"sink_accepted_at"`
	NextAttemptAt  *time.Time `json:"next_attempt_at"`
}
type Deliveries struct {
	Page[Delivery]
	Pending           string `json:"pending"`
	Blocked           string `json:"blocked"`
	RemoteDelivery    string `json:"remote_delivery"`
	ExternalAnchoring string `json:"external_anchoring"`
}

func (s *Service) Deliveries(ctx context.Context, p security.Principal, in DeliveryInput) (Deliveries, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	out := Deliveries{Page: observed([]Delivery{}), RemoteDelivery: "unavailable", ExternalAnchoring: "unavailable"}
	action := ReadAudit
	switch in.Destination {
	case durable.DestinationChronicle:
	case durable.DestinationRelay:
		action = ReadHooks
	default:
		return out, durable.ErrInvalid
	}
	if err := s.check(ctx, p, action, in.Key); err != nil {
		return out, err
	}
	limit, err := pageLimit(in.Limit)
	if err != nil {
		return out, err
	}
	in.Limit = limit
	token := in.Cursor
	in.Cursor = ""
	bind := binding(p, s.installation, action, in)
	state, err := s.cursors.open(token, bind)
	if err != nil {
		return out, err
	}
	// The storage query requires the authorized namespace before counts and pages.
	status, err := s.reads.ReadDeliveryStatus(ctx, durable.ScopedDeliveryStatus{Key: in.Key, DeliveryStatusRequest: durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: s.installation, Destination: in.Destination}, After: state.Position, Limit: limit}})
	if err != nil {
		return out, safeError(err)
	}
	out.Pending = strconv.FormatInt(status.Pending, 10)
	out.Blocked = strconv.FormatInt(status.Blocked, 10)
	for _, d := range status.Records {
		state := "pending"
		if d.Blocked() {
			state = "blocked"
		}
		if !d.DeliveredAt.IsZero() {
			state = "sink_accepted"
		}
		out.Items = append(out.Items, Delivery{ID: d.Delivery.ID, State: state, Attempts: strconv.FormatInt(d.Attempts, 10), AcceptedAt: d.AcceptedAt, SinkAcceptedAt: optionalTime(d.DeliveredAt), NextAttemptAt: optionalTime(d.NextAttemptAt)})
	}
	out.Complete = len(status.Records) < limit
	if !out.Complete {
		out.Cursor, err = s.cursors.seal(cursorState{Position: status.Records[len(status.Records)-1].Delivery.ID}, bind)
	}
	return out, err
}
