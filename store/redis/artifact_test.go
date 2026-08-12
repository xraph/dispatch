//go:build integration

package redis_test

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

// TestArtifactStoreConformance runs the shared artifact.Store suite
// against Redis.
func TestArtifactStoreConformance(t *testing.T) {
	artifacttest.RunStoreSuite(t, func(t *testing.T) artifact.Store {
		return setupTestStore(t)
	})
}
