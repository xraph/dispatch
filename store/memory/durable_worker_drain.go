package memory

import (
	"context"
	"time"

	"github.com/xraph/dispatch/durable"
)

var _ durable.WorkerDrainStore = (*Store)(nil)

func (m *Store) RequestWorkerDrain(ctx context.Context, r durable.WorkerDrainRequest) (durable.LifecycleReceipt, error) {
	if err := r.Validate(); err != nil {
		return durable.LifecycleReceipt{}, err
	}
	digest, err := durable.Fingerprint(string(durable.OperationRequestWorkerDrain), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.LifecycleReceipt, error) {
		key := lifecycleReceiptKey{r.Namespace, durable.OperationRequestWorkerDrain, r.RequestID}
		lookup := durable.LifecycleReceiptLookup{NamespaceTarget: r.NamespaceTarget, Operation: key.operation, RequestID: r.RequestID, RequestDigest: digest}
		if saved, ok := c.lifecycleReceipts[key]; ok {
			return saved.Clone(), saved.Match(lookup)
		}
		if checkErr := c.checkLifecycleTarget(r.NamespaceTarget); checkErr != nil {
			return durable.LifecycleReceipt{}, checkErr
		}
		if _, ok := c.buildAdmissions[r.BuildTarget]; !ok {
			return durable.LifecycleReceipt{}, durable.ErrNotFound
		}
		now := durable.Timestamp(time.Now())
		if !r.Deadline.After(now) {
			return durable.LifecycleReceipt{}, durable.ErrInvalid
		}
		receipt := durable.LifecycleReceipt{NamespaceTarget: r.NamespaceTarget, Operation: key.operation, RequestID: r.RequestID, RequestDigest: digest, CommandDigest: r.CommandDigest, ResponseVersion: 1, AcceptedAt: now, WorkerDrain: &r}
		delivery, deliveryErr := durable.NewDelivery(c.namespaces[r.Namespace], durable.DestinationChronicle, durable.LifecycleDeliverySource(ctx, receipt))
		if deliveryErr != nil {
			return durable.LifecycleReceipt{}, deliveryErr
		}
		receipt.DeliveryID = delivery.ID
		c.lifecycleReceipts[key] = receipt
		return receipt.Clone(), nil
	})
}
