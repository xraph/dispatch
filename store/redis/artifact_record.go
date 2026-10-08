package redis

import (
	"context"
	"fmt"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

var _ artifact.RecordReader = (*Store)(nil)

// GetArtifactRecord returns metadata even after soft deletion.
func (s *Store) GetArtifactRecord(ctx context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	return s.loadArtifact(ctx, artifactID.String())
}
func validateArtifactIdentity(a *artifact.Artifact, keyID string) error {
	if a.ID.IsNil() || a.ID.String() != keyID {
		return fmt.Errorf("dispatch/redis: artifact key and record identity differ")
	}
	return nil
}
