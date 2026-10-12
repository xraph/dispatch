package sqlite

import (
	"context"
	"fmt"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

var _ artifact.RecordReader = (*Store)(nil)

// GetArtifactRecord returns metadata even after soft deletion.
func (s *Store) GetArtifactRecord(ctx context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	m := new(artifactModel)
	err := s.sdb.NewSelect(m).Where("id = ?", artifactID.String()).Limit(1).Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, artifact.ErrNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get artifact record: %w", err)
	}
	return fromArtifactModel(m)
}
