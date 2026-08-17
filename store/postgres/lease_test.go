package postgres_test

import (
	"testing"

	"github.com/xraph/dispatch/store/storetest"
)

func TestLeaseConformance(t *testing.T) {
	dsn := startWakePostgres(t)

	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return openWakeStore(t, dsn)
	})
}

func TestDLQConformance(t *testing.T) {
	// One container for the suite, like TestLeaseConformance above; the
	// cases work only on entries they created, so a shared store is fine.
	dsn := startWakePostgres(t)

	storetest.RunDLQSuite(t, func(t *testing.T) storetest.DLQStore {
		t.Helper()

		return openWakeStore(t, dsn)
	})
}
