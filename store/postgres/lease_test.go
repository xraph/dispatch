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
