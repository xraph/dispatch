package memory_test

import (
	"testing"

	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/store/storetest"
)

func TestLeaseConformance(t *testing.T) {
	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return memory.New()
	})
}
