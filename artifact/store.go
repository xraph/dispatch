package artifact

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
)

// OwnerRef identifies a link owner.
type OwnerRef struct {
	Kind OwnerKind
	ID   string
}

// Valid reports whether the owner reference is usable.
func (o OwnerRef) Valid() bool { return o.Kind.Valid() && o.ID != "" }

// ListOpts controls pagination and filtering for artifact list queries.
type ListOpts struct {
	// Limit is the maximum number of artifacts to return. Zero means no limit.
	Limit int
	// Offset is the number of artifacts to skip.
	Offset int
	// Lifecycle filters by lifecycle. Empty means all.
	Lifecycle Lifecycle
	// ScopeAppID filters by tenant application. Empty means all.
	ScopeAppID string
	// ScopeOrgID filters by tenant organization. Empty means all.
	ScopeOrgID string
	// IncludeDeleted includes soft-deleted artifacts. Default false.
	IncludeDeleted bool
}

// SweepOpts controls a lifecycle sweep.
type SweepOpts struct {
	// Retention is the grace period after the last owner reaches a
	// terminal state before an artifact becomes eligible.
	Retention time.Duration
	// Limit caps how many artifacts a single sweep call may mark.
	// Zero means no limit.
	Limit int
	// DryRun computes eligibility and returns the artifacts that would be
	// marked without modifying anything.
	DryRun bool
}

// Store defines the persistence contract for artifacts and their links.
//
// Implementations must guarantee that CreateArtifact inserts the artifact
// and its link atomically, so a zero-link artifact can only result from a
// partial failure and never from a normal race.
//
// SweepEphemeral and SweepOrphans must constrain themselves to
// Lifecycle == Ephemeral using a literal, never a value threaded through
// from a caller. Durable artifacts must be unreachable from both.
type Store interface {
	// CreateArtifact inserts an artifact and, when link is non-nil, its
	// first link atomically. Returns ErrExists if an artifact already
	// exists at the same backend, bucket, and key.
	CreateArtifact(ctx context.Context, a *Artifact, link *Link) error

	// GetArtifact retrieves an artifact by ID. Returns ErrNotFound if it
	// does not exist or has been soft-deleted.
	GetArtifact(ctx context.Context, artifactID id.ArtifactID) (*Artifact, error)

	// FindArtifactByKey retrieves an artifact by its storage coordinates.
	// Returns ErrNotFound if none exists.
	FindArtifactByKey(ctx context.Context, backend, bucket, key string) (*Artifact, error)

	// UpdateArtifact persists changes to size, content hash, content type,
	// and expiry. It must not permit changing lifecycle.
	UpdateArtifact(ctx context.Context, a *Artifact) error

	// ListArtifacts returns artifacts matching the given options.
	ListArtifacts(ctx context.Context, opts ListOpts) ([]*Artifact, error)

	// LinkArtifact records that an owner references an artifact. Linking
	// the same artifact, owner, name, and attempt twice is a no-op rather
	// than an error.
	LinkArtifact(ctx context.Context, link *Link) error

	// ListLinks returns every link belonging to the given owner.
	ListLinks(ctx context.Context, owner OwnerRef) ([]*Link, error)

	// FindLinkByName returns the link for an owner and name with the
	// highest attempt number, breaking ties by CreatedAt descending so
	// resolution is deterministic and favours the later writer. This is
	// what IfAbsent uses to detect that a prior attempt already produced
	// an output. Returns ErrNotFound if no attempt has produced it.
	//
	// Ties happen: track D's lease reclaim increments EvictCount, never
	// RetryCount, so a reclaimed (zombie) holder and the worker it was
	// fenced out for can both commit a link at the same (OwnerKind,
	// OwnerID, Name, Attempt) — CreateFenced closes the storage-key
	// collision between them, but nothing stops two Link rows with
	// different ArtifactIDs at the identical tuple. The CreatedAt
	// tie-break is the cheap half of the fix: it makes which of the two
	// wins deterministic instead of backend-dependent map/query order.
	// The complete fix — carrying the fence token on Link itself, so the
	// zombie's write is rejected rather than merely outrun — is a schema
	// change across all five backends and remains open.
	FindLinkByName(ctx context.Context, owner OwnerRef, name string) (*Link, error)

	// ListArtifactsByOwner returns the artifacts linked to an owner,
	// optionally filtered by role. An empty role returns all.
	ListArtifactsByOwner(ctx context.Context, owner OwnerRef, role Role) ([]*Artifact, error)

	// SweepEphemeral marks eligible ephemeral artifacts as deleted and
	// returns them.
	SweepEphemeral(ctx context.Context, opts SweepOpts) ([]*Artifact, error)

	// SweepOrphans marks ephemeral artifacts that have no links at all and
	// were created before the cutoff.
	SweepOrphans(ctx context.Context, cutoff time.Time, limit int) ([]*Artifact, error)

	// ListPurgeable returns soft-deleted artifacts whose deletion is older
	// than grace, so their bytes may be removed from the backend.
	ListPurgeable(ctx context.Context, grace time.Duration, limit int) ([]*Artifact, error)

	// PurgeArtifact hard-deletes an artifact row and its links after the
	// bytes have been removed from the backend.
	PurgeArtifact(ctx context.Context, artifactID id.ArtifactID) error
}
