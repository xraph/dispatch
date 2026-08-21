package artifact

import (
	"time"

	"github.com/xraph/dispatch/id"
)

// Lifecycle determines whether Dispatch may delete an artifact's bytes.
type Lifecycle string

const (
	// Durable artifacts are written by the application and merely tracked
	// by Dispatch. They are read-only here and are never swept.
	Durable Lifecycle = "durable"

	// Ephemeral artifacts are created by Dispatch on a handler's behalf.
	// They are refcounted through links and swept once every owner is
	// terminal and the retention window has passed.
	Ephemeral Lifecycle = "ephemeral"
)

// Valid reports whether the lifecycle is a recognised value.
func (l Lifecycle) Valid() bool {
	return l == Durable || l == Ephemeral
}

// Role describes how an owner relates to an artifact.
type Role string

const (
	// RoleInput marks an artifact consumed by the owner.
	RoleInput Role = "input"
	// RoleOutput marks an artifact produced by the owner.
	RoleOutput Role = "output"
	// RoleIntermediate marks an artifact passed between workflow steps.
	RoleIntermediate Role = "intermediate"
)

// Valid reports whether the role is a recognised value.
func (r Role) Valid() bool {
	return r == RoleInput || r == RoleOutput || r == RoleIntermediate
}

// OwnerKind identifies which entity owns a link.
type OwnerKind string

const (
	// OwnerJob links an artifact to a job.
	OwnerJob OwnerKind = "job"
	// OwnerRun links an artifact to a workflow run.
	OwnerRun OwnerKind = "run"
	// OwnerStep links an artifact to a single workflow step.
	OwnerStep OwnerKind = "step"
)

// Valid reports whether the owner kind is a recognised value.
func (k OwnerKind) Valid() bool {
	return k == OwnerJob || k == OwnerRun || k == OwnerStep
}

// Ref is a lightweight handle to a tracked artifact. It is what callers
// pass to Bind, what handlers receive from Commit, and what workflow
// steps store in checkpoints — small enough to serialise freely.
type Ref struct {
	ID          id.ArtifactID `json:"id"`
	Backend     string        `json:"backend"`
	Bucket      string        `json:"bucket"`
	Key         string        `json:"key"`
	Size        int64         `json:"size"`
	ContentHash string        `json:"content_hash,omitempty"`
}

// IsZero reports whether the ref is unset.
func (r Ref) IsZero() bool { return r.ID.IsNil() }

// Artifact is a tracked object in external storage.
type Artifact struct {
	ID          id.ArtifactID `json:"id"`
	Backend     string        `json:"backend"`
	Bucket      string        `json:"bucket"`
	Key         string        `json:"key"`
	Size        int64         `json:"size"`
	ContentHash string        `json:"content_hash,omitempty"`
	ContentType string        `json:"content_type,omitempty"`
	Lifecycle   Lifecycle     `json:"lifecycle"`
	ScopeAppID  string        `json:"scope_app_id,omitempty"`
	ScopeOrgID  string        `json:"scope_org_id,omitempty"`
	ExpiresAt   *time.Time    `json:"expires_at,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	DeletedAt   *time.Time    `json:"deleted_at,omitempty"`
}

// Ref returns a lightweight handle to this artifact.
func (a *Artifact) Ref() Ref {
	return Ref{
		ID:          a.ID,
		Backend:     a.Backend,
		Bucket:      a.Bucket,
		Key:         a.Key,
		Size:        a.Size,
		ContentHash: a.ContentHash,
	}
}

// IsDeleted reports whether the artifact has been soft-deleted by the
// sweeper. A soft-deleted artifact is no longer served but its bytes
// survive until the purge pass.
func (a *Artifact) IsDeleted() bool { return a.DeletedAt != nil }

// Clone returns a deep copy so stores can hand out values callers may
// safely mutate.
func (a *Artifact) Clone() *Artifact {
	if a == nil {
		return nil
	}

	out := *a

	if a.ExpiresAt != nil {
		t := *a.ExpiresAt
		out.ExpiresAt = &t
	}

	if a.DeletedAt != nil {
		t := *a.DeletedAt
		out.DeletedAt = &t
	}

	return &out
}

// Link records that an owner references an artifact in a given role.
// Attempt scopes the link to one execution attempt so a retried job's
// outputs do not collide with its previous attempt's.
type Link struct {
	ArtifactID id.ArtifactID `json:"artifact_id"`
	OwnerKind  OwnerKind     `json:"owner_kind"`
	OwnerID    string        `json:"owner_id"`
	Role       Role          `json:"role"`
	Name       string        `json:"name"`
	Attempt    int           `json:"attempt"`
	CreatedAt  time.Time     `json:"created_at"`
}

// Clone returns a copy of the link.
func (l *Link) Clone() *Link {
	if l == nil {
		return nil
	}

	out := *l

	return &out
}

// Owner returns the OwnerRef this link belongs to.
func (l *Link) Owner() OwnerRef {
	return OwnerRef{Kind: l.OwnerKind, ID: l.OwnerID}
}

// ObjectInfo is what a Backend reports about a stored object.
type ObjectInfo struct {
	Size        int64
	ContentType string
	ETag        string
}
