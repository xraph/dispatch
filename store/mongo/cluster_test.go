package mongo_test

import (
	"context"
	"testing"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/store/storetest"
)

func TestClusterSuite(t *testing.T) {
	// One container for every case: the suite isolates by worker ID.
	// Migrate so the worker indexes, the partial unique leader index
	// among them, are in force the way they are in production.
	shared := openStore(t, startMongo(t))
	if err := shared.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	storetest.RunClusterSuite(t, func(t *testing.T) cluster.Store {
		t.Helper()

		return shared
	})
}
