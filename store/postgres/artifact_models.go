package postgres

import (
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// ── Artifact model ────────────────────────────────────────────────

type artifactModel struct {
	grove.BaseModel `grove:"table:dispatch_artifacts"`

	ID          string     `grove:"id,pk"`
	Backend     string     `grove:"backend,notnull"`
	Bucket      string     `grove:"bucket,notnull"`
	Key         string     `grove:"key,notnull"`
	Size        int64      `grove:"size,notnull,default:0"`
	ContentHash string     `grove:"content_hash"`
	ContentType string     `grove:"content_type"`
	Lifecycle   string     `grove:"lifecycle,notnull"`
	ScopeAppID  string     `grove:"scope_app_id"`
	ScopeOrgID  string     `grove:"scope_org_id"`
	ExpiresAt   *time.Time `grove:"expires_at"`
	CreatedAt   time.Time  `grove:"created_at,notnull,default:current_timestamp"`
	DeletedAt   *time.Time `grove:"deleted_at"`
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

	ArtifactID string    `grove:"artifact_id,pk"`
	OwnerKind  string    `grove:"owner_kind,pk"`
	OwnerID    string    `grove:"owner_id,pk"`
	Name       string    `grove:"name,pk"`
	Attempt    int       `grove:"attempt,pk"`
	Role       string    `grove:"role,notnull"`
	CreatedAt  time.Time `grove:"created_at,notnull,default:current_timestamp"`
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
