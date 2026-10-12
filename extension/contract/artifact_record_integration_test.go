//go:build integration

package contract

import "testing"

func TestArtifactRecordReadsOtherBackends(t *testing.T) {
	t.Run("postgres", func(t *testing.T) { runArtifactRecordReads(t, postgresContractStore(t)) })
	t.Run("redis", func(t *testing.T) { runArtifactRecordReads(t, redisContractStore(t)) })
	t.Run("mongo", func(t *testing.T) { runArtifactRecordReads(t, mongoContractStore(t)) })
}
