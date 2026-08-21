package sqlite_test

import (
	"testing"

	"github.com/xraph/dispatch/store/storetest"
)

func TestLeaseConformance(t *testing.T) {
	// openSqliteStore already opens a migrated store on a per-test temp
	// directory (store/sqlite/reap_test.go:19), so every subtest gets its
	// own database for free.
	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return openSqliteStore(t)
	})
}

func TestDLQConformance(t *testing.T) {
	storetest.RunDLQSuite(t, func(t *testing.T) storetest.DLQStore {
		t.Helper()

		return openSqliteStore(t)
	})
}
