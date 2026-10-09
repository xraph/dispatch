//go:build integration

package postgres_test

import (
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestDurableWorkflowRetryChains(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	durabletest.RunWorkflowRetryChains(t, s, func() durable.Store {
		s = reopenAsyncStore(t, s, dsn)
		return s
	})
}

func TestDurableRunChainHistoryBoundary(t *testing.T) {
	durabletest.RunRunChainHistoryBoundary(t, setupTestStore(t))
}
func TestDurableWorkflowRetryCapacity(t *testing.T) {
	durabletest.RunWorkflowRetryCapacity(t, setupTestStore(t))
}
