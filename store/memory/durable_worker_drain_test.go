package memory

import (
	"errors"
	"maps"
	"testing"

	"github.com/xraph/dispatch/durable"

	"github.com/xraph/dispatch/durable/durabletest"
)

func TestWorkerDrainReceipt(t *testing.T) { durabletest.RunWorkerDrainReceipt(t, New()) }

func TestWorkerDrainIntentRollback(t *testing.T) {
	s := New()
	durabletest.RunWorkerDrainIntentRollback(t, s, func(t *testing.T, _ durable.NamespaceTarget) any {
		t.Helper()
		s.mu.RLock()
		defer s.mu.RUnlock()
		return []any{maps.Clone(s.lifecycleReceipts), maps.Clone(s.outbox), maps.Clone(s.buildAdmissions)}
	}, func(t *testing.T) func() {
		t.Helper()
		s.outboxPrepare = func(d durable.Delivery) error {
			if d.Action == durable.LifecycleAction(durable.OperationRequestWorkerDrain) {
				return errors.New("injected drain intent failure")
			}
			return nil
		}
		return func() { s.outboxPrepare = nil }
	})
}
