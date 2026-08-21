package mongo

import (
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// ── Artifact model ────────────────────────────────────────────────

type artifactModel struct {
	grove.BaseModel `grove:"table:dispatch_artifacts"`

	ID          string     `grove:"id,pk"        bson:"_id"`
	Backend     string     `bson:"backend"`
	Bucket      string     `bson:"bucket"`
	Key         string     `bson:"key"`
	Size        int64      `bson:"size"`
	ContentHash string     `bson:"content_hash,omitempty"`
	ContentType string     `bson:"content_type,omitempty"`
	Lifecycle   string     `bson:"lifecycle"`
	ScopeAppID  string     `bson:"scope_app_id,omitempty"`
	ScopeOrgID  string     `bson:"scope_org_id,omitempty"`
	ExpiresAt   *time.Time `bson:"expires_at,omitempty"`
	CreatedAt   time.Time  `bson:"created_at"`
	DeletedAt   *time.Time `bson:"deleted_at,omitempty"`
}

func toArtifactModel(a *artifact.Artifact) *artifactModel {
	return &artifactModel{
		ID:          a.ID.String(),
		Backend:     a.Backend,
		Bucket:      a.Bucket,
		Key:         a.Key,
		Size:        a.Size,
		ContentHash: a.ContentHash,
		ContentType: a.ContentType,
		Lifecycle:   string(a.Lifecycle),
		ScopeAppID:  a.ScopeAppID,
		ScopeOrgID:  a.ScopeOrgID,
		ExpiresAt:   a.ExpiresAt,
		CreatedAt:   a.CreatedAt,
		DeletedAt:   a.DeletedAt,
	}
}

func fromArtifactModel(m *artifactModel) (*artifact.Artifact, error) {
	aid, err := id.ParseArtifactID(m.ID)
	if err != nil {
		return nil, err
	}

	return &artifact.Artifact{
		ID:          aid,
		Backend:     m.Backend,
		Bucket:      m.Bucket,
		Key:         m.Key,
		Size:        m.Size,
		ContentHash: m.ContentHash,
		ContentType: m.ContentType,
		Lifecycle:   artifact.Lifecycle(m.Lifecycle),
		ScopeAppID:  m.ScopeAppID,
		ScopeOrgID:  m.ScopeOrgID,
		ExpiresAt:   m.ExpiresAt,
		CreatedAt:   m.CreatedAt,
		DeletedAt:   m.DeletedAt,
	}, nil
}

func fromArtifactModels(models []artifactModel) ([]*artifact.Artifact, error) {
	out := make([]*artifact.Artifact, 0, len(models))

	for i := range models {
		a, err := fromArtifactModel(&models[i])
		if err != nil {
			return nil, err
		}

		out = append(out, a)
	}

	return out, nil
}

// ── Artifact link model ───────────────────────────────────────────

type artifactLinkModel struct {
	grove.BaseModel `grove:"table:dispatch_artifact_links"`

	ArtifactID string    `grove:"artifact_id,pk" bson:"artifact_id"`
	OwnerKind  string    `bson:"owner_kind"`
	OwnerID    string    `bson:"owner_id"`
	Name       string    `bson:"name"`
	Attempt    int       `bson:"attempt"`
	Role       string    `bson:"role"`
	CreatedAt  time.Time `bson:"created_at"`
}

func toLinkModel(l *artifact.Link) *artifactLinkModel {
	return &artifactLinkModel{
		ArtifactID: l.ArtifactID.String(),
		OwnerKind:  string(l.OwnerKind),
		OwnerID:    l.OwnerID,
		Name:       l.Name,
		Attempt:    l.Attempt,
		Role:       string(l.Role),
		CreatedAt:  l.CreatedAt,
	}
}

func fromLinkModel(m *artifactLinkModel) (*artifact.Link, error) {
	aid, err := id.ParseArtifactID(m.ArtifactID)
	if err != nil {
		return nil, err
	}

	return &artifact.Link{
		ArtifactID: aid,
		OwnerKind:  artifact.OwnerKind(m.OwnerKind),
		OwnerID:    m.OwnerID,
		Role:       artifact.Role(m.Role),
		Name:       m.Name,
		Attempt:    m.Attempt,
		CreatedAt:  m.CreatedAt,
	}, nil
}
