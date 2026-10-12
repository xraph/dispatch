//go:build integration

package contract

import "testing"

func TestDLQDomainOtherBackends(t *testing.T) {
	t.Run("postgres", func(t *testing.T) { runDLQDomain(t, postgresContractStore(t)) })
	t.Run("redis", func(t *testing.T) { runDLQDomain(t, redisContractStore(t)) })
	t.Run("mongo", func(t *testing.T) { runDLQDomain(t, mongoContractStore(t)) })
}
