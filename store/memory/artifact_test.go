package memory_test

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/store/memory"
)

func TestArtifactStoreConformance(t *testing.T) {
	artifacttest.RunStoreSuite(t, func() artifact.Store { return memory.New() })
}
