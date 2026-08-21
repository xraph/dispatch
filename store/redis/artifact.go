package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// defaultSweepLimit bounds an unbounded sweep so a single pass can never
// touch an unbounded number of keys.
const defaultSweepLimit = 1000

// artifactEntity is the JSON shape stored under an artifact key.
type artifactEntity struct {
	ID          string     `json:"id"`
	Backend     string     `json:"backend"`
	Bucket      string     `json:"bucket"`
	Key         string     `json:"key"`
	Size        int64      `json:"size"`
	ContentHash string     `json:"content_hash,omitempty"`
	ContentType string     `json:"content_type,omitempty"`
	Lifecycle   string     `json:"lifecycle"`
	ScopeAppID  string     `json:"scope_app_id,omitempty"`
	ScopeOrgID  string     `json:"scope_org_id,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	DeletedAt   *time.Time `json:"deleted_at,omitempty"`
}

func toArtifactEntity(a *artifact.Artifact) *artifactEntity {
	return &artifactEntity{
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

func fromArtifactEntity(e *artifactEntity) (*artifact.Artifact, error) {
	aid, err := id.ParseArtifactID(e.ID)
	if err != nil {
		return nil, err
	}

	return &artifact.Artifact{
		ID:          aid,
		Backend:     e.Backend,
		Bucket:      e.Bucket,
		Key:         e.Key,
		Size:        e.Size,
		ContentHash: e.ContentHash,
		ContentType: e.ContentType,
		Lifecycle:   artifact.Lifecycle(e.Lifecycle),
		ScopeAppID:  e.ScopeAppID,
		ScopeOrgID:  e.ScopeOrgID,
		ExpiresAt:   e.ExpiresAt,
		CreatedAt:   e.CreatedAt,
		DeletedAt:   e.DeletedAt,
	}, nil
}

// linkEntity is the JSON shape stored per link.
type linkEntity struct {
	ArtifactID string    `json:"artifact_id"`
	OwnerKind  string    `json:"owner_kind"`
	OwnerID    string    `json:"owner_id"`
	Name       string    `json:"name"`
	Attempt    int       `json:"attempt"`
	Role       string    `json:"role"`
	CreatedAt  time.Time `json:"created_at"`
}

func toLinkEntity(l *artifact.Link) *linkEntity {
	return &linkEntity{
		ArtifactID: l.ArtifactID.String(),
		OwnerKind:  string(l.OwnerKind),
		OwnerID:    l.OwnerID,
		Name:       l.Name,
		Attempt:    l.Attempt,
		Role:       string(l.Role),
		CreatedAt:  l.CreatedAt,
	}
}

func fromLinkEntity(e *linkEntity) (*artifact.Link, error) {
	aid, err := id.ParseArtifactID(e.ArtifactID)
	if err != nil {
		return nil, err
	}

	return &artifact.Link{
		ArtifactID: aid,
		OwnerKind:  artifact.OwnerKind(e.OwnerKind),
		OwnerID:    e.OwnerID,
		Role:       artifact.Role(e.Role),
		Name:       e.Name,
		Attempt:    e.Attempt,
		CreatedAt:  e.CreatedAt,
	}, nil
}

// linkField is the hash field identifying one link within an owner's or
// artifact's link hash.
func linkField(name string, attempt int) string {
	return name + "\x00" + strconv.Itoa(attempt)
}

// CreateArtifact inserts an artifact and, when link is non-nil, its first
// link.
//
// The live-key guard is a SETNX on a dedicated key, which is what makes
// two concurrent creates at the same coordinates resolve to one winner
// and one ErrExists.
func (s *Store) CreateArtifact(ctx context.Context, a *artifact.Artifact, link *artifact.Link) error {
	guard := artifactKeyGuard(a.Backend, a.Bucket, a.Key)

	ok, err := s.rdb.SetNX(ctx, guard, a.ID.String(), 0).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: create artifact guard: %w", err)
	}

	if !ok {
		return artifact.ErrExists
	}

	if err := s.setEntity(ctx, artifactKey(a.ID.String()), toArtifactEntity(a)); err != nil {
		// Release the guard so the coordinates are not permanently burned.
		s.rdb.Del(ctx, guard)

		return fmt.Errorf("dispatch/redis: create artifact: %w", err)
	}

	if err := s.rdb.SAdd(ctx, artifactIDsKey, a.ID.String()).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: index artifact: %w", err)
	}

	// Only ephemeral artifacts enter the sweep index. This is the Redis
	// form of the SQL lifecycle literal: the sweeps read this index and
	// no durable artifact is ever a member.
	if a.Lifecycle == artifact.Ephemeral {
		score := float64(a.CreatedAt.UnixNano())
		if err := s.rdb.ZAdd(ctx, artifactEphemeralKey, goredis.Z{Score: score, Member: a.ID.String()}).Err(); err != nil {
			return fmt.Errorf("dispatch/redis: index ephemeral artifact: %w", err)
		}
	}

	if link == nil {
		return nil
	}

	return s.LinkArtifact(ctx, link)
}

// GetArtifact retrieves a live artifact by ID.
func (s *Store) GetArtifact(ctx context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	a, err := s.loadArtifact(ctx, artifactID.String())
	if err != nil {
		return nil, err
	}

	if a.IsDeleted() {
		return nil, artifact.ErrNotFound
	}

	return a, nil
}

// loadArtifact reads an artifact regardless of soft-deletion.
func (s *Store) loadArtifact(ctx context.Context, artifactID string) (*artifact.Artifact, error) {
	var e artifactEntity

	if err := s.getEntity(ctx, artifactKey(artifactID), &e); err != nil {
		if isNotFound(err) {
			return nil, artifact.ErrNotFound
		}

		return nil, fmt.Errorf("dispatch/redis: get artifact: %w", err)
	}

	return fromArtifactEntity(&e)
}

// FindArtifactByKey retrieves a live artifact by its storage coordinates.
func (s *Store) FindArtifactByKey(ctx context.Context, backend, bucket, key string) (*artifact.Artifact, error) {
	got, err := s.rdb.Get(ctx, artifactKeyGuard(backend, bucket, key)).Result()
	if err != nil {
		return nil, artifact.ErrNotFound
	}

	return s.GetArtifact(ctx, id.MustParse(got))
}

// UpdateArtifact persists size, hash, content type, and expiry. Lifecycle,
// created_at, and deleted_at are deliberately not updatable here.
func (s *Store) UpdateArtifact(ctx context.Context, a *artifact.Artifact) error {
	existing, err := s.loadArtifact(ctx, a.ID.String())
	if err != nil {
		return err
	}

	existing.Size = a.Size
	existing.ContentHash = a.ContentHash
	existing.ContentType = a.ContentType
	existing.ExpiresAt = a.ExpiresAt

	if err := s.setEntity(ctx, artifactKey(a.ID.String()), toArtifactEntity(existing)); err != nil {
		return fmt.Errorf("dispatch/redis: update artifact: %w", err)
	}

	return nil
}

// ListArtifacts returns artifacts matching the given options, newest first.
func (s *Store) ListArtifacts(ctx context.Context, opts artifact.ListOpts) ([]*artifact.Artifact, error) {
	ids, err := s.rdb.SMembers(ctx, artifactIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list artifact ids: %w", err)
	}

	out := make([]*artifact.Artifact, 0, len(ids))

	for _, got := range ids {
		a, lerr := s.loadArtifact(ctx, got)
		if lerr != nil {
			if errors.Is(lerr, artifact.ErrNotFound) {
				continue
			}

			return nil, lerr
		}

		if a.IsDeleted() && !opts.IncludeDeleted {
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

		out = append(out, a)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID.String() < out[j].ID.String()
		}

		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	if opts.Offset > 0 {
		if opts.Offset >= len(out) {
			return nil, nil
		}

		out = out[opts.Offset:]
	}

	if opts.Limit > 0 && opts.Limit < len(out) {
		out = out[:opts.Limit]
	}

	return out, nil
}

// LinkArtifact records that an owner references an artifact, idempotently.
// HSet on the same field is naturally idempotent.
func (s *Store) LinkArtifact(ctx context.Context, link *artifact.Link) error {
	raw, err := json.Marshal(toLinkEntity(link))
	if err != nil {
		return fmt.Errorf("dispatch/redis: marshal link: %w", err)
	}

	owner := artifact.OwnerRef{Kind: link.OwnerKind, ID: link.OwnerID}
	field := linkField(link.Name, link.Attempt)

	if err := s.rdb.HSet(ctx, ownerLinksKey(string(owner.Kind), owner.ID), field, raw).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: link artifact: %w", err)
	}

	member := string(link.OwnerKind) + "\x00" + link.OwnerID + "\x00" + field
	if err := s.rdb.SAdd(ctx, artifactLinksKey(link.ArtifactID.String()), member).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: index artifact link: %w", err)
	}

	return nil
}

// ListLinks returns every link belonging to the given owner.
func (s *Store) ListLinks(ctx context.Context, owner artifact.OwnerRef) ([]*artifact.Link, error) {
	vals, err := s.rdb.HGetAll(ctx, ownerLinksKey(string(owner.Kind), owner.ID)).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list links: %w", err)
	}

	out := make([]*artifact.Link, 0, len(vals))

	for _, raw := range vals {
		var e linkEntity
		if uerr := json.Unmarshal([]byte(raw), &e); uerr != nil {
			return nil, fmt.Errorf("dispatch/redis: unmarshal link: %w", uerr)
		}

		l, cerr := fromLinkEntity(&e)
		if cerr != nil {
			return nil, cerr
		}

		out = append(out, l)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].Attempt < out[j].Attempt
		}

		return out[i].Name < out[j].Name
	})

	return out, nil
}

// FindLinkByName returns the highest-attempt link for an owner and name,
// breaking ties by CreatedAt descending — see the artifact.Store doc
// comment for why ties happen and what the tie-break does and does not
// fix.
//
// In practice a tie cannot reach this loop on this backend: LinkArtifact
// stores each link in a hash keyed by linkField(name, attempt), so a
// second write to the same (owner, name, attempt) overwrites the first
// rather than coexisting as a second row the way the SQL and document
// backends allow. The CreatedAt compare is kept anyway, both so this
// backend agrees with the other four if that storage shape ever changes,
// and because ListLinks' own sort does not otherwise guarantee which of
// two equal-attempt entries — however they arose — comes first.
func (s *Store) FindLinkByName(
	ctx context.Context,
	owner artifact.OwnerRef,
	name string,
) (*artifact.Link, error) {
	links, err := s.ListLinks(ctx, owner)
	if err != nil {
		return nil, err
	}

	var best *artifact.Link

	for _, l := range links {
		if l.Name != name {
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

	return best, nil
}

// ListArtifactsByOwner returns live artifacts linked to an owner.
func (s *Store) ListArtifactsByOwner(
	ctx context.Context,
	owner artifact.OwnerRef,
	role artifact.Role,
) ([]*artifact.Artifact, error) {
	links, err := s.ListLinks(ctx, owner)
	if err != nil {
		return nil, err
	}

	var out []*artifact.Artifact

	seen := make(map[string]bool, len(links))

	for _, l := range links {
		if role != "" && l.Role != role {
			continue
		}

		key := l.ArtifactID.String()
		if seen[key] {
			continue
		}

		seen[key] = true

		a, lerr := s.loadArtifact(ctx, key)
		if lerr != nil {
			if errors.Is(lerr, artifact.ErrNotFound) {
				continue
			}

			return nil, lerr
		}

		if a.IsDeleted() {
			continue
		}

		out = append(out, a)
	}

	return out, nil
}

// SweepEphemeral marks eligible ephemeral artifacts as deleted.
//
// Candidates come from the ephemeral sorted set, which by construction
// contains no durable artifact. The lifecycle is re-checked after loading
// each candidate so the guard does not rest on index hygiene alone.
func (s *Store) SweepEphemeral(
	ctx context.Context,
	opts artifact.SweepOpts,
) ([]*artifact.Artifact, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultSweepLimit
	}

	ids, err := s.rdb.ZRange(ctx, artifactEphemeralKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: sweep ephemeral: %w", err)
	}

	nowAt := time.Now().UTC()

	var eligible []*artifact.Artifact

	for _, got := range ids {
		if len(eligible) >= limit {
			break
		}

		a, lerr := s.loadArtifact(ctx, got)
		if lerr != nil {
			if errors.Is(lerr, artifact.ErrNotFound) {
				continue
			}

			return nil, lerr
		}

		if a.Lifecycle != artifact.Ephemeral || a.IsDeleted() {
			continue
		}

		links, llerr := s.linksForArtifact(ctx, a.ID.String())
		if llerr != nil {
			return nil, llerr
		}

		if len(links) == 0 {
			// Orphans are SweepOrphans' business.
			continue
		}

		terminalAt, ok, terr := s.ownersTerminalAt(ctx, links)
		if terr != nil {
			return nil, terr
		}

		if !ok {
			continue
		}

		if a.ExpiresAt != nil {
			if a.ExpiresAt.After(nowAt) {
				continue
			}
		} else if terminalAt.Add(opts.Retention).After(nowAt) {
			continue
		}

		eligible = append(eligible, a)
	}

	if len(eligible) == 0 || opts.DryRun {
		return eligible, nil
	}

	return s.markDeleted(ctx, eligible, nowAt)
}

// linksForArtifact resolves every link pointing at an artifact.
func (s *Store) linksForArtifact(ctx context.Context, artifactID string) ([]*artifact.Link, error) {
	members, err := s.rdb.SMembers(ctx, artifactLinksKey(artifactID)).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list artifact links: %w", err)
	}

	out := make([]*artifact.Link, 0, len(members))

	for _, m := range members {
		kind, ownerID, field, ok := splitLinkMember(m)
		if !ok {
			continue
		}

		owner := artifact.OwnerRef{Kind: artifact.OwnerKind(kind), ID: ownerID}

		raw, herr := s.rdb.HGet(ctx, ownerLinksKey(string(owner.Kind), owner.ID), field).Result()
		if herr != nil {
			continue
		}

		var e linkEntity
		if uerr := json.Unmarshal([]byte(raw), &e); uerr != nil {
			return nil, fmt.Errorf("dispatch/redis: unmarshal link: %w", uerr)
		}

		l, cerr := fromLinkEntity(&e)
		if cerr != nil {
			return nil, cerr
		}

		out = append(out, l)
	}

	return out, nil
}

// splitLinkMember parses "kind\x00ownerID\x00name\x00attempt".
func splitLinkMember(m string) (kind, ownerID, field string, ok bool) {
	first := indexByteFrom(m, 0)
	if first < 0 {
		return "", "", "", false
	}

	second := indexByteFrom(m, first+1)
	if second < 0 {
		return "", "", "", false
	}

	return m[:first], m[first+1 : second], m[second+1:], true
}

func indexByteFrom(s string, from int) int {
	for i := from; i < len(s); i++ {
		if s[i] == 0 {
			return i
		}
	}

	return -1
}

// ownersTerminalAt reports the latest terminal time across an artifact's
// owners, and whether all of them are terminal. An owner that no longer
// exists counts as terminal at the link's creation time.
func (s *Store) ownersTerminalAt(ctx context.Context, links []*artifact.Link) (time.Time, bool, error) {
	var latest time.Time

	for _, l := range links {
		at, ok, err := s.ownerTerminalAt(ctx, l)
		if err != nil {
			return time.Time{}, false, err
		}

		if !ok {
			return time.Time{}, false, nil
		}

		if at.After(latest) {
			latest = at
		}
	}

	return latest, true, nil
}

func (s *Store) ownerTerminalAt(ctx context.Context, l *artifact.Link) (time.Time, bool, error) {
	switch l.OwnerKind {
	case artifact.OwnerJob:
		var e jobEntity
		if err := s.getEntity(ctx, jobKey(l.OwnerID), &e); err != nil {
			if isNotFound(err) {
				return l.CreatedAt, true, nil
			}

			return time.Time{}, false, fmt.Errorf("dispatch/redis: resolve artifact owner job: %w", err)
		}

		if !isTerminalOwnerState(e.State) {
			return time.Time{}, false, nil
		}

		if e.CompletedAt != nil {
			return *e.CompletedAt, true, nil
		}

		return e.UpdatedAt, true, nil

	case artifact.OwnerRun, artifact.OwnerStep:
		var e runEntity
		if err := s.getEntity(ctx, runKey(l.OwnerID), &e); err != nil {
			if isNotFound(err) {
				return l.CreatedAt, true, nil
			}

			return time.Time{}, false, fmt.Errorf("dispatch/redis: resolve artifact owner run: %w", err)
		}

		if !isTerminalOwnerState(e.State) {
			return time.Time{}, false, nil
		}

		if e.CompletedAt != nil {
			return *e.CompletedAt, true, nil
		}

		return e.UpdatedAt, true, nil

	default:
		return l.CreatedAt, true, nil
	}
}

func isTerminalOwnerState(state string) bool {
	switch state {
	case "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

// SweepOrphans marks link-less ephemeral artifacts created before cutoff.
func (s *Store) SweepOrphans(
	ctx context.Context,
	cutoff time.Time,
	limit int,
) ([]*artifact.Artifact, error) {
	if limit <= 0 {
		limit = defaultSweepLimit
	}

	// The ephemeral index is scored by creation time, so the cutoff is a
	// range query rather than a scan.
	ids, err := s.rdb.ZRangeByScore(ctx, artifactEphemeralKey, &goredis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(cutoff.UnixNano(), 10),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: sweep orphans: %w", err)
	}

	var orphans []*artifact.Artifact

	for _, got := range ids {
		if len(orphans) >= limit {
			break
		}

		a, lerr := s.loadArtifact(ctx, got)
		if lerr != nil {
			if errors.Is(lerr, artifact.ErrNotFound) {
				continue
			}

			return nil, lerr
		}

		if a.Lifecycle != artifact.Ephemeral || a.IsDeleted() {
			continue
		}

		if !a.CreatedAt.Before(cutoff) {
			continue
		}

		n, cerr := s.rdb.SCard(ctx, artifactLinksKey(a.ID.String())).Result()
		if cerr != nil {
			return nil, fmt.Errorf("dispatch/redis: count artifact links: %w", cerr)
		}

		if n > 0 {
			continue
		}

		orphans = append(orphans, a)
	}

	if len(orphans) == 0 {
		return nil, nil
	}

	return s.markDeleted(ctx, orphans, time.Now().UTC())
}

// markDeleted soft-deletes the given artifacts, re-checking the lifecycle
// on each so the write itself refuses to touch a durable artifact.
func (s *Store) markDeleted(
	ctx context.Context,
	artifacts []*artifact.Artifact,
	at time.Time,
) ([]*artifact.Artifact, error) {
	out := make([]*artifact.Artifact, 0, len(artifacts))

	for _, a := range artifacts {
		if a.Lifecycle != artifact.Ephemeral {
			continue
		}

		clone := a.Clone()
		deleted := at
		clone.DeletedAt = &deleted

		if err := s.setEntity(ctx, artifactKey(clone.ID.String()), toArtifactEntity(clone)); err != nil {
			return nil, fmt.Errorf("dispatch/redis: mark artifact deleted: %w", err)
		}

		// Release the live-key guard so the coordinates become reusable,
		// and index the deletion time for the purge pass.
		s.rdb.Del(ctx, artifactKeyGuard(clone.Backend, clone.Bucket, clone.Key))

		if err := s.rdb.ZAdd(ctx, artifactDeletedKey,
			goredis.Z{Score: float64(at.UnixNano()), Member: clone.ID.String()}).Err(); err != nil {
			return nil, fmt.Errorf("dispatch/redis: index deleted artifact: %w", err)
		}

		if err := s.rdb.ZRem(ctx, artifactEphemeralKey, clone.ID.String()).Err(); err != nil {
			return nil, fmt.Errorf("dispatch/redis: deindex ephemeral artifact: %w", err)
		}

		out = append(out, clone)
	}

	return out, nil
}

// ListPurgeable returns soft-deleted artifacts older than grace.
func (s *Store) ListPurgeable(
	ctx context.Context,
	grace time.Duration,
	limit int,
) ([]*artifact.Artifact, error) {
	if limit <= 0 {
		limit = defaultSweepLimit
	}

	cutoff := time.Now().UTC().Add(-grace)

	ids, err := s.rdb.ZRangeByScore(ctx, artifactDeletedKey, &goredis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(cutoff.UnixNano(), 10),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list purgeable: %w", err)
	}

	out := make([]*artifact.Artifact, 0, len(ids))

	for _, got := range ids {
		if len(out) >= limit {
			break
		}

		a, lerr := s.loadArtifact(ctx, got)
		if lerr != nil {
			if errors.Is(lerr, artifact.ErrNotFound) {
				continue
			}

			return nil, lerr
		}

		if !a.IsDeleted() {
			continue
		}

		out = append(out, a)
	}

	return out, nil
}

// PurgeArtifact hard-deletes an artifact and its link index entries.
func (s *Store) PurgeArtifact(ctx context.Context, artifactID id.ArtifactID) error {
	key := artifactID.String()

	links, err := s.linksForArtifact(ctx, key)
	if err != nil {
		return err
	}

	for _, l := range links {
		owner := artifact.OwnerRef{Kind: l.OwnerKind, ID: l.OwnerID}
		if herr := s.rdb.HDel(ctx, ownerLinksKey(string(owner.Kind), owner.ID), linkField(l.Name, l.Attempt)).Err(); herr != nil {
			return fmt.Errorf("dispatch/redis: purge artifact link: %w", herr)
		}
	}

	a, err := s.loadArtifact(ctx, key)
	if err == nil {
		s.rdb.Del(ctx, artifactKeyGuard(a.Backend, a.Bucket, a.Key))
	}

	pipe := s.rdb.TxPipeline()
	pipe.Del(ctx, artifactKey(key))
	pipe.Del(ctx, artifactLinksKey(key))
	pipe.SRem(ctx, artifactIDsKey, key)
	pipe.ZRem(ctx, artifactEphemeralKey, key)
	pipe.ZRem(ctx, artifactDeletedKey, key)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("dispatch/redis: purge artifact: %w", err)
	}

	return nil
}
