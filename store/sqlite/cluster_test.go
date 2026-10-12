package sqlite_test

import (
	"testing"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/store/storetest"
)

func TestClusterSuite(t *testing.T) {
	// openSqliteStore (reap_test.go) opens a migrated store in a per-test
	// temp directory, so every case gets its own database.
	storetest.RunClusterSuite(t, func(t *testing.T) cluster.Store {
		t.Helper()

		return openSqliteStore(t)
	})
}
