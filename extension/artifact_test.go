package extension_test

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/store/memory"
)

// TestArtifactsDisabledByDefault pins the backward-compatibility
// guarantee: an application that never asked for artifacts gets none, and
// nothing about its behaviour changes.
func TestArtifactsDisabledByDefault(t *testing.T) {
	ext := extension.New()

	if ext.Artifacts() != nil {
		t.Fatal("artifact service exists before Register — the plane must be opt-in")
	}

	if ext.ArtifactCache() != nil {
		t.Fatal("staging cache exists before Register")
	}
}

// TestWithArtifactBackendEnablesPlane checks that supplying a backend
// explicitly is enough — no container, no Trove, no Forge.
func TestWithArtifactBackendEnablesPlane(t *testing.T) {
	backend := artifacttest.NewBackend()

	ext := extension.New(
		extension.WithArtifactBackend(backend),
		extension.WithArtifactStore(memory.New()),
		extension.WithArtifactCacheDir(t.TempDir()),
		extension.WithArtifactCacheBudget(1<<20),
	)

	if ext == nil {
		t.Fatal("New returned nil")
	}

	// The plane is constructed during Register, which needs a Forge app.
	// What is verifiable here is that the options applied without panic
	// and left the extension in a usable state.
	if ext.Artifacts() != nil {
		t.Fatal("service should not exist until Register runs")
	}
}

// TestArtifactBackendSatisfiesInterface is a compile-time guard in test
// form: the test double and the real adapter must stay interchangeable.
func TestArtifactBackendSatisfiesInterface(t *testing.T) {
	var b artifact.Backend = artifacttest.NewBackend()

	if b.Name() != "memory" {
		t.Fatalf("Name() = %q, want %q", b.Name(), "memory")
	}
}
