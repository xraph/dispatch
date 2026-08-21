package memory

import (
	"context"
	"sort"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// CreateArtifact inserts an artifact and, when link is non-nil, its first
// link atomically under the store lock.
func (s *Store) CreateArtifact(_ context.Context, a *artifact.Artifact, link *artifact.Link) error {
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

// appendLinkLocked adds a link if an identical one is not already present.
// Callers must hold s.mu.
func (s *Store) appendLinkLocked(link *artifact.Link) {
	for _, existing := range s.artifactLinks {
		if existing.ArtifactID == link.ArtifactID &&
			existing.OwnerKind == link.OwnerKind &&
			existing.OwnerID == link.OwnerID &&
			existing.Name == link.Name &&
			existing.Attempt == link.Attempt {
			return
		}
	}

	s.artifactLinks = append(s.artifactLinks, link.Clone())
}

// GetArtifact retrieves an artifact by ID, excluding soft-deleted ones.
func (s *Store) GetArtifact(_ context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	a, ok := s.artifacts[artifactID.String()]
	if !ok || a.DeletedAt != nil {
		return nil, artifact.ErrNotFound
	}

	return a.Clone(), nil
}

// FindArtifactByKey retrieves an artifact by its storage coordinates.
func (s *Store) FindArtifactByKey(_ context.Context, backend, bucket, key string) (*artifact.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

// UpdateArtifact persists changes to size, hash, content type, and expiry.
// Lifecycle is deliberately not updatable.
func (s *Store) UpdateArtifact(_ context.Context, a *artifact.Artifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.artifacts[a.ID.String()]
	if !ok {
		return artifact.ErrNotFound
	}

	updated := a.Clone()
	updated.Lifecycle = existing.Lifecycle
	updated.CreatedAt = existing.CreatedAt
	updated.DeletedAt = existing.DeletedAt
	s.artifacts[a.ID.String()] = updated

	return nil
}

// ListArtifacts returns artifacts matching the given options, newest first.
func (s *Store) ListArtifacts(_ context.Context, opts artifact.ListOpts) ([]*artifact.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*artifact.Artifact, 0, len(s.artifacts))

	for _, a := range s.artifacts {
		if a.DeletedAt != nil && !opts.IncludeDeleted {
			continue
		}

		if opts.Lifecycle != "" && a.Lifecycle != opts.Lifecycle {
			continue
		}

		if opts.ScopeAppID != "" && a.ScopeAppID != opts.ScopeAppID {
			continue
		}

		if opts.ScopeOrgID != "" && a.ScopeOrgID != opts.ScopeOrgID {
			continue
		}

		out = append(out, a.Clone())
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID.String() < out[j].ID.String()
		}

		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	return paginate(out, opts.Offset, opts.Limit), nil
}

func paginate(in []*artifact.Artifact, offset, limit int) []*artifact.Artifact {
	if offset > 0 {
		if offset >= len(in) {
			return nil
		}

		in = in[offset:]
	}

	if limit > 0 && limit < len(in) {
		in = in[:limit]
	}

	return in
}

// LinkArtifact records that an owner references an artifact. Linking the
// same tuple twice is a no-op.
func (s *Store) LinkArtifact(_ context.Context, link *artifact.Link) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.appendLinkLocked(link)

	return nil
}

// ListLinks returns every link belonging to the given owner.
func (s *Store) ListLinks(_ context.Context, owner artifact.OwnerRef) ([]*artifact.Link, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.linksForOwnerLocked(owner), nil
}

// linksForOwnerLocked collects an owner's links. Callers must hold s.mu.
func (s *Store) linksForOwnerLocked(owner artifact.OwnerRef) []*artifact.Link {
	var out []*artifact.Link

	for _, l := range s.artifactLinks {
		if l.OwnerKind == owner.Kind && l.OwnerID == owner.ID {
			out = append(out, l.Clone())
		}
	}

	return out
}

// FindLinkByName returns the link for an owner and name with the highest
// attempt number, breaking ties by CreatedAt descending — see the
// artifact.Store doc comment for why ties happen and what the tie-break
// does and does not fix.
func (s *Store) FindLinkByName(_ context.Context, owner artifact.OwnerRef, name string) (*artifact.Link, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var best *artifact.Link

	for _, l := range s.artifactLinks {
		if l.OwnerKind != owner.Kind || l.OwnerID != owner.ID || l.Name != name {
			continue
		}

		if best == nil || l.Attempt > best.Attempt ||
			(l.Attempt == best.Attempt && l.CreatedAt.After(best.CreatedAt)) {
			best = l
		}
	}

	if best == nil {
		return nil, artifact.ErrNotFound
	}

	return best.Clone(), nil
}

// ListArtifactsByOwner returns the artifacts linked to an owner,
// optionally filtered by role.
func (s *Store) ListArtifactsByOwner(
	_ context.Context,
	owner artifact.OwnerRef,
	role artifact.Role,
) ([]*artifact.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []*artifact.Artifact

	seen := make(map[string]bool)

	for _, l := range s.artifactLinks {
		if l.OwnerKind != owner.Kind || l.OwnerID != owner.ID {
			continue
		}

		if role != "" && l.Role != role {
			continue
		}

		key := l.ArtifactID.String()
		if seen[key] {
			continue
		}

		a, ok := s.artifacts[key]
		if !ok || a.DeletedAt != nil {
			continue
		}

		seen[key] = true

		out = append(out, a.Clone())
	}

	return out, nil
}

// SweepEphemeral marks eligible ephemeral artifacts as deleted.
//
// An artifact is eligible when every owner that links it is terminal and
// the retention window has elapsed since the last of them finished. The
// lifecycle guard is the first statement of the loop body and is written
// as a literal, mirroring the SQL backends.
func (s *Store) SweepEphemeral(_ context.Context, opts artifact.SweepOpts) ([]*artifact.Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()

	var out []*artifact.Artifact

	for _, a := range s.artifacts {
		if a.Lifecycle != artifact.Ephemeral {
			continue
		}

		if a.DeletedAt != nil {
			continue
		}

		if opts.Limit > 0 && len(out) >= opts.Limit {
			break
		}

		links := s.linksForArtifactLocked(a.ID)
		if len(links) == 0 {
			// Orphans are SweepOrphans' business, not ours.
			continue
		}

		terminalAt, ok := s.ownersTerminalAtLocked(links)
		if !ok {
			continue
		}

		if a.ExpiresAt != nil {
			if a.ExpiresAt.After(now) {
				continue
			}
		} else if terminalAt.Add(opts.Retention).After(now) {
			continue
		}

		if !opts.DryRun {
			deleted := now
			a.DeletedAt = &deleted
		}

		out = append(out, a.Clone())
	}

	return out, nil
}

// linksForArtifactLocked collects links pointing at an artifact.
// Callers must hold s.mu.
func (s *Store) linksForArtifactLocked(artifactID id.ArtifactID) []*artifact.Link {
	var out []*artifact.Link

	for _, l := range s.artifactLinks {
		if l.ArtifactID == artifactID {
			out = append(out, l)
		}
	}

	return out
}

// ownersTerminalAtLocked reports the latest terminal time across every
// owner in links, and whether all of them are in fact terminal. An owner
// that no longer exists counts as terminal at the link's creation time —
// its job or run row was purged, so it cannot still be running.
// Callers must hold s.mu.
func (s *Store) ownersTerminalAtLocked(links []*artifact.Link) (time.Time, bool) {
	var latest time.Time

	for _, l := range links {
		at, ok := s.ownerTerminalAtLocked(l)
		if !ok {
			return time.Time{}, false
		}

		if at.After(latest) {
			latest = at
		}
	}

	return latest, true
}

func (s *Store) ownerTerminalAtLocked(l *artifact.Link) (time.Time, bool) {
	switch l.OwnerKind {
	case artifact.OwnerJob:
		j, ok := s.jobs[l.OwnerID]
		if !ok {
			return l.CreatedAt, true
		}

		if !isTerminalJobState(string(j.State)) {
			return time.Time{}, false
		}

		if j.CompletedAt != nil {
			return *j.CompletedAt, true
		}

		return j.UpdatedAt, true

	case artifact.OwnerRun, artifact.OwnerStep:
		r, ok := s.runs[l.OwnerID]
		if !ok {
			return l.CreatedAt, true
		}

		if !isTerminalRunState(string(r.State)) {
			return time.Time{}, false
		}

		if r.CompletedAt != nil {
			return *r.CompletedAt, true
		}

		return r.UpdatedAt, true

	default:
		return l.CreatedAt, true
	}
}

func isTerminalJobState(state string) bool {
	switch state {
	case "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func isTerminalRunState(state string) bool {
	switch state {
	case "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

// SweepOrphans marks ephemeral artifacts with no links at all that were
// created before the cutoff.
func (s *Store) SweepOrphans(_ context.Context, cutoff time.Time, limit int) ([]*artifact.Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()

	var out []*artifact.Artifact

	for _, a := range s.artifacts {
		if a.Lifecycle != artifact.Ephemeral {
			continue
		}

		if a.DeletedAt != nil {
			continue
		}

		if limit > 0 && len(out) >= limit {
			break
		}

		if !a.CreatedAt.Before(cutoff) {
			continue
		}

		if len(s.linksForArtifactLocked(a.ID)) > 0 {
			continue
		}

		deleted := now
		a.DeletedAt = &deleted

		out = append(out, a.Clone())
	}

	return out, nil
}

// ListPurgeable returns soft-deleted artifacts older than grace.
func (s *Store) ListPurgeable(_ context.Context, grace time.Duration, limit int) ([]*artifact.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now().UTC()

	var out []*artifact.Artifact

	for _, a := range s.artifacts {
		if a.DeletedAt == nil {
			continue
		}

		if limit > 0 && len(out) >= limit {
			break
		}

		if a.DeletedAt.Add(grace).After(now) {
			continue
		}

		out = append(out, a.Clone())
	}

	return out, nil
}

// PurgeArtifact hard-deletes an artifact and its links.
func (s *Store) PurgeArtifact(_ context.Context, artifactID id.ArtifactID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.artifacts, artifactID.String())

	kept := s.artifactLinks[:0]

	for _, l := range s.artifactLinks {
		if l.ArtifactID != artifactID {
			kept = append(kept, l)
		}
	}

	s.artifactLinks = kept

	return nil
}
