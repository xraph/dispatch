package memory

import (
	"context"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

var _ artifact.RecordReader = (*Store)(nil)

// GetArtifactRecord returns detached metadata even after soft deletion.
func (s *Store) GetArtifactRecord(_ context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.artifacts[artifactID.String()]
	if !ok {
		return nil, artifact.ErrNotFound
	}
	return a.Clone(), nil
}
