package redis_test

import (
	"testing"

	"github.com/xraph/dispatch/store/storetest"
)

func TestLeaseConformance(t *testing.T) {
	// One container, shared keyspace — do not use openReapRedis here, which
	// calls startRedis on every invocation and would spin twelve containers.
	connStr := startRedis(t)

	storetest.RunLeaseSuite(t, func(t *testing.T) storetest.LeaseStore {
		t.Helper()

		return openRedisStore(t, connStr)
	})
}
