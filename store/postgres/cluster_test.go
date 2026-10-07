//go:build integration

package postgres_test

import (
	"testing"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/store/storetest"
)

func TestClusterSuite(t *testing.T) {
	// One container for every case: the suite isolates by worker ID.
	shared := setupTestStore(t)

	storetest.RunClusterSuite(t, func(t *testing.T) cluster.Store {
		t.Helper()

		return shared
	})
}
