//go:build integration

package contract

import "testing"

func TestOperationalDomainOtherBackends(t *testing.T) {
	t.Run("postgres", func(t *testing.T) { runOperationalDomain(t, postgresContractStore(t)) })
	t.Run("redis", func(t *testing.T) { runOperationalDomain(t, redisContractStore(t)) })
	t.Run("mongo", func(t *testing.T) { runOperationalDomain(t, mongoContractStore(t)) })
}
