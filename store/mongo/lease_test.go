package mongo_test

import (
	"testing"

	"github.com/xraph/dispatch/store/storetest"
)

func TestLeaseConformance(t *testing.T) {
	// One container for the whole suite — startMongo spins a testcontainer
	// and doing that eleven times would dominate the runtime. The suite is
	// written to tolerate a shared store.
	uri := startMongo(t)

	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return openStore(t, uri)
	})
}
