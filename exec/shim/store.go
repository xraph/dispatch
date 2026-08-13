package shim

import (
	"context"
	"sync"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// memStore is a minimal, in-process artifact.Store for the sandboxed
// shim. It exists so exec/shim never has to import store/memory, which is
// the full composite store and transitively pulls the worker's
// database, object-store, and config-loader clients into the sandbox
// binary.
//
// The shim's use of a store is single-process, single-attempt, and
// short-lived: it backs one artifact.Service for the lifetime of one
// handler invocation, only ever seeded by seedPriorOutputs and read or
// written by that handler through artifact.Accessor. It is not a record
// of truth — the worker outside the sandbox verifies what actually landed
// on disk — so memStore implements real map semantics only for the
// methods that path reaches (CreateArtifact, GetArtifact,
// FindArtifactByKey, LinkArtifact, FindLinkByName) and honest stubs for
// the rest. A sandboxed handler never lists, sweeps, or purges: lifecycle
// management is the worker's job, not the sandbox's.
type memStore struct {
	mu        sync.Mutex
	artifacts map[string]*artifact.Artifact
	links     []*artifact.Link
}

var _ artifact.Store = (*memStore)(nil)

// newMemStore builds an empty memStore.
func newMemStore() *memStore {
	return &memStore{
		artifacts: make(map[string]*artifact.Artifact),
	}
}

// CreateArtifact inserts an artifact and, when link is non-nil, its first
// link, under the store's lock. Returns artifact.ErrExists if an artifact
// already exists at the same backend, bucket, and key.
func (s *memStore) CreateArtifact(_ context.Context, a *artifact.Artifact, link *artifact.Link) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, existing := range s.artifacts {
		if existing.DeletedAt != nil {
			continue
		}

		if existing.Backend == a.Backend && existing.Bucket == a.Bucket && existing.Key == a.Key {
			return artifact.ErrExists
		}
	}

	s.artifacts[a.ID.String()] = a.Clone()

	if link != nil {
		s.appendLinkLocked(link)
	}

	return nil
}

// appendLinkLocked adds link unless an identical one is already present.
// Callers must hold s.mu.
func (s *memStore) appendLinkLocked(link *artifact.Link) {
	for _, existing := range s.links {
		if existing.ArtifactID == link.ArtifactID &&
			existing.OwnerKind == link.OwnerKind &&
			existing.OwnerID == link.OwnerID &&
			existing.Name == link.Name &&
			existing.Attempt == link.Attempt {
			return
		}
	}

	s.links = append(s.links, link.Clone())
}

// GetArtifact retrieves an artifact by ID. Returns artifact.ErrNotFound if
// it does not exist or has been soft-deleted.
func (s *memStore) GetArtifact(_ context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	a, ok := s.artifacts[artifactID.String()]
	if !ok || a.DeletedAt != nil {
		return nil, artifact.ErrNotFound
	}

	return a.Clone(), nil
}

// FindArtifactByKey retrieves an artifact by its storage coordinates.
// Returns artifact.ErrNotFound if none exists.
func (s *memStore) FindArtifactByKey(_ context.Context, backend, bucket, key string) (*artifact.Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, a := range s.artifacts {
		if a.DeletedAt != nil {
			continue
		}

		if a.Backend == backend && a.Bucket == bucket && a.Key == key {
			return a.Clone(), nil
		}
	}

	return nil, artifact.ErrNotFound
}

// UpdateArtifact is an honest stub: no path the shim exercises calls it,
// since updating size, hash, content type, or expiry after the fact is a
// worker-side concern once the object has landed on the real backend. It
// returns nil rather than an error so a caller that reaches it anyway is
// not broken by a store that only the sandbox uses.
func (s *memStore) UpdateArtifact(_ context.Context, _ *artifact.Artifact) error {
	return nil
}

// ListArtifacts is an honest stub: the sandbox never lists, since it has
// no notion of "every artifact" beyond what this one attempt created or
// was seeded with. It returns an empty result rather than an error.
func (s *memStore) ListArtifacts(_ context.Context, _ artifact.ListOpts) ([]*artifact.Artifact, error) {
	return nil, nil
}

// LinkArtifact records that an owner references an artifact. Linking the
// same artifact, owner, name, and attempt twice is a no-op.
func (s *memStore) LinkArtifact(_ context.Context, link *artifact.Link) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.appendLinkLocked(link)

	return nil
}

// ListLinks is an honest stub: nothing in the shim's path enumerates an
// owner's links wholesale — FindLinkByName answers the one question
// IfAbsent needs. It returns an empty result rather than an error.
func (s *memStore) ListLinks(_ context.Context, _ artifact.OwnerRef) ([]*artifact.Link, error) {
	return nil, nil
}

// FindLinkByName returns the link for an owner and name with the highest
// attempt number. Returns artifact.ErrNotFound if no attempt has produced
// it.
func (s *memStore) FindLinkByName(_ context.Context, owner artifact.OwnerRef, name string) (*artifact.Link, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var best *artifact.Link

	for _, l := range s.links {
		if l.OwnerKind != owner.Kind || l.OwnerID != owner.ID || l.Name != name {
			continue
		}

		if best == nil || l.Attempt > best.Attempt {
			best = l
		}
	}

	if best == nil {
		return nil, artifact.ErrNotFound
	}

	return best.Clone(), nil
}

// ListArtifactsByOwner is an honest stub: the shim's accessor resolves
// inputs from the request's InputSlots on local disk, not by asking the
// store what is linked to the owner. It returns an empty result rather
// than an error.
func (s *memStore) ListArtifactsByOwner(
	_ context.Context,
	_ artifact.OwnerRef,
	_ artifact.Role,
) ([]*artifact.Artifact, error) {
	return nil, nil
}

// SweepEphemeral is an honest stub: the sandbox never sweeps. The worker
// outside the sandbox owns ephemeral-artifact lifecycle once the real
// object has landed on its backend. It returns an empty result rather
// than an error.
func (s *memStore) SweepEphemeral(_ context.Context, _ artifact.SweepOpts) ([]*artifact.Artifact, error) {
	return nil, nil
}

// SweepOrphans is an honest stub for the same reason as SweepEphemeral:
// orphan reclamation is the worker's business, not a single short-lived
// sandbox attempt's. It returns an empty result rather than an error.
func (s *memStore) SweepOrphans(_ context.Context, _ time.Time, _ int) ([]*artifact.Artifact, error) {
	return nil, nil
}

// ListPurgeable is an honest stub: purging soft-deleted rows is lifecycle
// management the worker performs, never the sandbox. It returns an empty
// result rather than an error.
func (s *memStore) ListPurgeable(_ context.Context, _ time.Duration, _ int) ([]*artifact.Artifact, error) {
	return nil, nil
}

// PurgeArtifact is an honest stub for the same reason as ListPurgeable. It
// returns nil rather than an error.
func (s *memStore) PurgeArtifact(_ context.Context, _ id.ArtifactID) error {
	return nil
}
