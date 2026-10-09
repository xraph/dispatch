package postgres

import (
	"context"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

func prepareEventIntents(ctx context.Context, tx driver.Tx, key durable.Key, event durable.Event) error {
	n, ok, err := lockedAuditNamespace(ctx, tx, key.Namespace)
	if err != nil || !ok {
		return err
	}
	source := durable.EventDeliverySource(ctx, key, event)
	for _, destination := range []durable.Destination{durable.DestinationChronicle, durable.DestinationRelay} {
		if destination == durable.DestinationChronicle && !n.RequireAudit || destination == durable.DestinationRelay && !n.RequireHooks {
			continue
		}
		d, createErr := durable.NewDelivery(n, destination, source)
		if createErr != nil {
			return createErr
		}
		if err := insertDelivery(ctx, tx, d); err != nil {
			return err
		}
	}
	return nil
}
func prepareReceiptIntent(ctx context.Context, tx driver.Tx, key durable.Key, kind, id, outcome, action string) error {
	n, ok, err := lockedAuditNamespace(ctx, tx, key.Namespace)
	if err != nil || !ok || !n.RequireAudit {
		return err
	}
	if kind != "child_receipt" {
		id = durable.ReceiptSourceID(id)
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return err
	}
	d, err := durable.NewDelivery(n, durable.DestinationChronicle, durable.DeliverySource{Key: key, Kind: kind, ID: id, OccurredAt: now, Action: action, Outcome: outcome, Metadata: durable.AuditMetadataFromContext(ctx)})
	if err != nil {
		return err
	}
	return insertDelivery(ctx, tx, d)
}
