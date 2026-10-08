package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

var _ artifact.RecordReader = (*Store)(nil)

// GetArtifactRecord returns metadata even after soft deletion.
func (s *Store) GetArtifactRecord(ctx context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	var m artifactModel
	err := s.mdb.Collection(colArtifacts).FindOne(ctx, bson.M{"_id": artifactID.String()}).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, artifact.ErrNotFound
		}
		return nil, fmt.Errorf("dispatch/mongo: get artifact record: %w", err)
	}
	return fromArtifactModel(&m)
}
