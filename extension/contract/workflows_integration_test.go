//go:build integration

package contract

import "testing"

func TestWorkflowDomainOtherBackends(t *testing.T) {
	t.Run("postgres", func(t *testing.T) { runWorkflowDomain(t, postgresContractStore(t)) })
	t.Run("redis", func(t *testing.T) { runWorkflowDomain(t, redisContractStore(t)) })
	t.Run("mongo", func(t *testing.T) { runWorkflowDomain(t, mongoContractStore(t)) })
}
