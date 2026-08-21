package sqlite_test

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

// TestArtifactStoreConformance runs the shared artifact.Store suite
// against SQLite. Each subtest gets its own in-memory database because
// the suite asserts absolute row counts.
func TestArtifactStoreConformance(t *testing.T) {
	artifacttest.RunStoreSuite(t, func(t *testing.T) artifact.Store {
		return openSqliteStore(t)
	})
}
