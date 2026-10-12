//go:build integration

package contract

import "testing"

func TestArtifactDomainOtherBackends(t *testing.T) {
	t.Run("postgres", func(t *testing.T) { runArtifactDomain(t, postgresContractStore(t)) })
	t.Run("redis", func(t *testing.T) { runArtifactDomain(t, redisContractStore(t)) })
	t.Run("mongo", func(t *testing.T) { runArtifactDomain(t, mongoContractStore(t)) })
}
