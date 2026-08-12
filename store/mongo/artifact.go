package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// defaultSweepLimit bounds an unbounded sweep so a single pass can never
// touch an unbounded number of documents.
const defaultSweepLimit = 1000

// ephemeralOnly is the lifecycle guard shared by every sweep path.
//
// It is a function returning a fresh literal rather than a package
// variable so no caller can mutate the shared map and widen a sweep to
// reach durable artifacts.
func ephemeralOnly() bson.M {
	return bson.M{
		"lifecycle":  string(artifact.Ephemeral),
		"deleted_at": nil,
	}
}

// CreateArtifact inserts an artifact and, when link is non-nil, its first
// link.
//
// Mongo multi-document atomicity needs a replica set. When the deployment
// supports transactions the pair is written in a session; otherwise the
// artifact is written first and the link second, and a crash between them
// leaves an orphan that the orphan sweep collects.
func (s *Store) CreateArtifact(ctx context.Context, a *artifact.Artifact, link *artifact.Link) error {
	_, err := s.mdb.Collection(colArtifacts).InsertOne(ctx, toArtifactModel(a))
	if err != nil {
		if isDuplicateKey(err) {
			return artifact.ErrExists
		}

		return fmt.Errorf("dispatch/mongo: create artifact: %w", err)
	}

	if link == nil {
		return nil
	}

	if lerr := s.LinkArtifact(ctx, link); lerr != nil {
		// The artifact exists but is unlinked. Rather than leave a
		// permanent orphan, drop it so the caller can retry cleanly.
		if _, derr := s.mdb.Collection(colArtifacts).
			DeleteOne(ctx, bson.M{"_id": a.ID.String()}); derr != nil {
			s.logger.Warn("dispatch/mongo: could not roll back unlinked artifact",
				log.String("artifact_id", a.ID.String()),
				log.String("error", derr.Error()),
			)
		}

		return lerr
	}

	return nil
}

// GetArtifact retrieves a live artifact by ID.
func (s *Store) GetArtifact(ctx context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	var m artifactModel

	err := s.mdb.Collection(colArtifacts).
		FindOne(ctx, bson.M{"_id": artifactID.String(), "deleted_at": nil}).
		Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, artifact.ErrNotFound
		}

		return nil, fmt.Errorf("dispatch/mongo: get artifact: %w", err)
	}

	return fromArtifactModel(&m)
}

// FindArtifactByKey retrieves a live artifact by its storage coordinates.
func (s *Store) FindArtifactByKey(ctx context.Context, backend, bucket, key string) (*artifact.Artifact, error) {
	var m artifactModel

	filter := bson.M{"backend": backend, "bucket": bucket, "key": key, "deleted_at": nil}

	err := s.mdb.Collection(colArtifacts).FindOne(ctx, filter).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, artifact.ErrNotFound
		}

		return nil, fmt.Errorf("dispatch/mongo: find artifact by key: %w", err)
	}

	return fromArtifactModel(&m)
}

// UpdateArtifact persists size, hash, content type, and expiry. Lifecycle,
// created_at, and deleted_at are deliberately not updatable here.
func (s *Store) UpdateArtifact(ctx context.Context, a *artifact.Artifact) error {
	update := bson.M{"$set": bson.M{
		"size":         a.Size,
		"content_hash": a.ContentHash,
		"content_type": a.ContentType,
		"expires_at":   a.ExpiresAt,
	}}

	res, err := s.mdb.Collection(colArtifacts).
		UpdateOne(ctx, bson.M{"_id": a.ID.String()}, update)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: update artifact: %w", err)
	}

	if res.MatchedCount == 0 {
		return artifact.ErrNotFound
	}

	return nil
}

// ListArtifacts returns artifacts matching the given options, newest first.
func (s *Store) ListArtifacts(ctx context.Context, opts artifact.ListOpts) ([]*artifact.Artifact, error) {
	filter := bson.M{}

	if !opts.IncludeDeleted {
		filter["deleted_at"] = nil
	}

	if opts.Lifecycle != "" {
		filter["lifecycle"] = string(opts.Lifecycle)
	}

	if opts.ScopeAppID != "" {
		filter["scope_app_id"] = opts.ScopeAppID
	}

	if opts.ScopeOrgID != "" {
		filter["scope_org_id"] = opts.ScopeOrgID
	}

	find := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}, {Key: "_id", Value: 1}})

	if opts.Limit > 0 {
		find = find.SetLimit(int64(opts.Limit))
	}

	if opts.Offset > 0 {
		find = find.SetSkip(int64(opts.Offset))
	}

	return s.findArtifacts(ctx, filter, find)
}

// findArtifacts runs a find and decodes the whole cursor.
func (s *Store) findArtifacts(
	ctx context.Context,
	filter bson.M,
	opts ...options.Lister[options.FindOptions],
) ([]*artifact.Artifact, error) {
	cur, err := s.mdb.Collection(colArtifacts).Find(ctx, filter, opts...)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: find artifacts: %w", err)
	}

	var models []artifactModel
	if err := cur.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: decode artifacts: %w", err)
	}

	return fromArtifactModels(models)
}

// LinkArtifact records that an owner references an artifact, idempotently.
func (s *Store) LinkArtifact(ctx context.Context, link *artifact.Link) error {
	m := toLinkModel(link)

	filter := bson.M{
		"artifact_id": m.ArtifactID,
		"owner_kind":  m.OwnerKind,
		"owner_id":    m.OwnerID,
		"name":        m.Name,
		"attempt":     m.Attempt,
	}

	_, err := s.mdb.Collection(colArtifactLinks).
		UpdateOne(ctx, filter, bson.M{"$setOnInsert": m}, options.UpdateOne().SetUpsert(true))
	if err != nil {
		if isDuplicateKey(err) {
			// A concurrent upsert won the race; the link exists either way.
			return nil
		}

		return fmt.Errorf("dispatch/mongo: link artifact: %w", err)
	}

	return nil
}

// ListLinks returns every link belonging to the given owner.
func (s *Store) ListLinks(ctx context.Context, owner artifact.OwnerRef) ([]*artifact.Link, error) {
	filter := bson.M{"owner_kind": string(owner.Kind), "owner_id": owner.ID}
	sort := options.Find().SetSort(bson.D{{Key: "name", Value: 1}, {Key: "attempt", Value: 1}})

	return s.findLinks(ctx, filter, sort)
}

func (s *Store) findLinks(
	ctx context.Context,
	filter bson.M,
	opts ...options.Lister[options.FindOptions],
) ([]*artifact.Link, error) {
	cur, err := s.mdb.Collection(colArtifactLinks).Find(ctx, filter, opts...)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: find links: %w", err)
	}

	var models []artifactLinkModel
	if err := cur.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: decode links: %w", err)
	}

	out := make([]*artifact.Link, 0, len(models))

	for i := range models {
		l, cerr := fromLinkModel(&models[i])
		if cerr != nil {
			return nil, cerr
		}

		out = append(out, l)
	}

	return out, nil
}

// FindLinkByName returns the highest-attempt link for an owner and name.
func (s *Store) FindLinkByName(
	ctx context.Context,
	owner artifact.OwnerRef,
	name string,
) (*artifact.Link, error) {
	var m artifactLinkModel

	filter := bson.M{"owner_kind": string(owner.Kind), "owner_id": owner.ID, "name": name}
	opt := options.FindOne().SetSort(bson.D{{Key: "attempt", Value: -1}})

	if err := s.mdb.Collection(colArtifactLinks).FindOne(ctx, filter, opt).Decode(&m); err != nil {
		if isNoDocuments(err) {
			return nil, artifact.ErrNotFound
		}

		return nil, fmt.Errorf("dispatch/mongo: find link by name: %w", err)
	}

	return fromLinkModel(&m)
}

// ListArtifactsByOwner returns live artifacts linked to an owner.
func (s *Store) ListArtifactsByOwner(
	ctx context.Context,
	owner artifact.OwnerRef,
	role artifact.Role,
) ([]*artifact.Artifact, error) {
	filter := bson.M{"owner_kind": string(owner.Kind), "owner_id": owner.ID}
	if role != "" {
		filter["role"] = string(role)
	}

	links, err := s.findLinks(ctx, filter)
	if err != nil {
		return nil, err
	}

	if len(links) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(links))
	seen := make(map[string]bool, len(links))

	for _, l := range links {
		key := l.ArtifactID.String()
		if seen[key] {
			continue
		}

		seen[key] = true

		ids = append(ids, key)
	}

	return s.findArtifacts(ctx, bson.M{"_id": bson.M{"$in": ids}, "deleted_at": nil})
}

// SweepEphemeral marks eligible ephemeral artifacts as deleted.
//
// Mongo cannot join links to jobs and runs in a single expressive
// statement the way SQL can, so eligibility is computed in two steps:
// candidates are narrowed by the ephemeral guard, then each candidate's
// owners are resolved and checked. The lifecycle guard is applied both
// when selecting candidates and again on the write.
func (s *Store) SweepEphemeral(
	ctx context.Context,
	opts artifact.SweepOpts,
) ([]*artifact.Artifact, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultSweepLimit
	}

	candidates, err := s.findArtifacts(ctx, ephemeralOnly(),
		options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}}))
	if err != nil {
		return nil, err
	}

	nowAt := now()

	var eligible []*artifact.Artifact

	for _, a := range candidates {
		if len(eligible) >= limit {
			break
		}

		links, lerr := s.findLinks(ctx, bson.M{"artifact_id": a.ID.String()})
		if lerr != nil {
			return nil, lerr
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

// ownersTerminalAt reports the latest terminal time across an artifact's
// owners, and whether all of them are terminal. An owner document that no
// longer exists counts as terminal at the link's creation time: its job or
// run was purged, so it cannot still be running.
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
	col, ok := ownerCollection(l.OwnerKind)
	if !ok {
		return l.CreatedAt, true, nil
	}

	var doc struct {
		State       string     `bson:"state"`
		CompletedAt *time.Time `bson:"completed_at"`
		UpdatedAt   time.Time  `bson:"updated_at"`
	}

	err := s.mdb.Collection(col).FindOne(ctx, bson.M{"_id": l.OwnerID}).Decode(&doc)
	if err != nil {
		if isNoDocuments(err) {
			return l.CreatedAt, true, nil
		}

		return time.Time{}, false, fmt.Errorf("dispatch/mongo: resolve artifact owner: %w", err)
	}

	if !isTerminalOwnerState(doc.State) {
		return time.Time{}, false, nil
	}

	if doc.CompletedAt != nil {
		return *doc.CompletedAt, true, nil
	}

	return doc.UpdatedAt, true, nil
}

func ownerCollection(kind artifact.OwnerKind) (string, bool) {
	switch kind {
	case artifact.OwnerJob:
		return colJobs, true
	case artifact.OwnerRun, artifact.OwnerStep:
		return colWorkflowRuns, true
	default:
		return "", false
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

	filter := ephemeralOnly()
	filter["created_at"] = bson.M{"$lt": cutoff}

	candidates, err := s.findArtifacts(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}}))
	if err != nil {
		return nil, err
	}

	var orphans []*artifact.Artifact

	for _, a := range candidates {
		if len(orphans) >= limit {
			break
		}

		n, cerr := s.mdb.Collection(colArtifactLinks).
			CountDocuments(ctx, bson.M{"artifact_id": a.ID.String()})
		if cerr != nil {
			return nil, fmt.Errorf("dispatch/mongo: count artifact links: %w", cerr)
		}

		if n > 0 {
			continue
		}

		orphans = append(orphans, a)
	}

	if len(orphans) == 0 {
		return nil, nil
	}

	return s.markDeleted(ctx, orphans, now())
}

// markDeleted soft-deletes the given artifacts. The ephemeral guard is
// repeated on the write so the update itself, not only the selection
// above it, refuses to touch a durable artifact.
func (s *Store) markDeleted(
	ctx context.Context,
	artifacts []*artifact.Artifact,
	at time.Time,
) ([]*artifact.Artifact, error) {
	ids := make([]string, 0, len(artifacts))
	for _, a := range artifacts {
		ids = append(ids, a.ID.String())
	}

	filter := ephemeralOnly()
	filter["_id"] = bson.M{"$in": ids}

	_, err := s.mdb.Collection(colArtifacts).
		UpdateMany(ctx, filter, bson.M{"$set": bson.M{"deleted_at": at}})
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: mark artifacts deleted: %w", err)
	}

	out := make([]*artifact.Artifact, 0, len(artifacts))

	for _, a := range artifacts {
		clone := a.Clone()
		deleted := at
		clone.DeletedAt = &deleted
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

	filter := bson.M{"deleted_at": bson.M{"$ne": nil, "$lte": now().Add(-grace)}}

	find := options.Find().
		SetSort(bson.D{{Key: "deleted_at", Value: 1}}).
		SetLimit(int64(limit))

	return s.findArtifacts(ctx, filter, find)
}

// PurgeArtifact hard-deletes an artifact and its links.
func (s *Store) PurgeArtifact(ctx context.Context, artifactID id.ArtifactID) error {
	if _, err := s.mdb.Collection(colArtifactLinks).
		DeleteMany(ctx, bson.M{"artifact_id": artifactID.String()}); err != nil {
		return fmt.Errorf("dispatch/mongo: purge artifact links: %w", err)
	}

	if _, err := s.mdb.Collection(colArtifacts).
		DeleteOne(ctx, bson.M{"_id": artifactID.String()}); err != nil {
		return fmt.Errorf("dispatch/mongo: purge artifact: %w", err)
	}

	return nil
}
