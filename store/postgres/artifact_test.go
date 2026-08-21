//go:build integration

package postgres_test

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

// TestArtifactStoreConformance runs the shared artifact.Store suite
// against Postgres.
//
// Each subtest gets its own container. That is slow, but the suite
// asserts absolute row counts, so it needs a genuinely empty store, and
// this path only runs under the integration build tag.
func TestArtifactStoreConformance(t *testing.T) {
	artifacttest.RunStoreSuite(t, func(t *testing.T) artifact.Store {
		return setupTestStore(t)
	})
}
