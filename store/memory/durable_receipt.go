package memory

import (
	"context"

	"github.com/xraph/dispatch/durable"
)

// LookupReceipt returns the original result only for the exact accepted intent.
func (m *Store) LookupReceipt(ctx context.Context, r durable.ReceiptRequest) (durable.Receipt, bool, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.Receipt{}, false, err
	}
	record, ok := m.executions[r.Key]
	if !ok {
		return durable.Receipt{}, false, durable.ErrNotFound
	}
	receipt, ok := record.receipts[r.RequestID]
	if !ok {
		return durable.Receipt{}, false, nil
	}
	if err := durable.CheckReceiptIntent(receipt.intent, r.IntentDigest); err != nil {
		return durable.Receipt{}, false, err
	}
	return receipt.value, true, nil
}
