package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// CreateArtifact inserts an artifact and, when link is non-nil, its first
// link in a single transaction.
func (s *Store) CreateArtifact(ctx context.Context, a *artifact.Artifact, link *artifact.Link) error {
	if link == nil {
		_, err := s.pgdb.NewInsert(toArtifactModel(a)).Exec(ctx)
		if err != nil {
			if isDuplicateKey(err) {
				return artifact.ErrExists
			}

			return fmt.Errorf(errPrefix+"create artifact: %w", err)
		}

		return nil
	}

	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf(errPrefix+"create artifact: begin: %w", err)
	}

	defer func() {
		if rerr := tx.Rollback(); rerr != nil && !errors.Is(rerr, sql.ErrTxDone) {
			s.logger.Warn(errPrefix+"artifact tx rollback", log.String("error", rerr.Error()))
		}
	}()

	if _, err := tx.Exec(ctx, insertArtifactSQL, artifactInsertArgs(a)...); err != nil {
		if isDuplicateKey(err) {
			return artifact.ErrExists
		}

		return fmt.Errorf(errPrefix+"create artifact: %w", err)
	}

	if _, err := tx.Exec(ctx, insertLinkSQL, linkInsertArgs(link)...); err != nil {
		return fmt.Errorf(errPrefix+"create artifact link: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(errPrefix+"create artifact: commit: %w", err)
	}

	return nil
}

const insertArtifactSQL = `
	INSERT INTO dispatch_artifacts
		(id, backend, bucket, key, size, content_hash, content_type,
		 lifecycle, scope_app_id, scope_org_id, expires_at, created_at, deleted_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

func artifactInsertArgs(a *artifact.Artifact) []any {
	return []any{
		a.ID.String(), a.Backend, a.Bucket, a.Key, a.Size,
		nullString(a.ContentHash), nullString(a.ContentType),
		string(a.Lifecycle), nullString(a.ScopeAppID), nullString(a.ScopeOrgID),
		a.ExpiresAt, a.CreatedAt, a.DeletedAt,
	}
}

const insertLinkSQL = `
	INSERT INTO dispatch_artifact_links
		(artifact_id, owner_kind, owner_id, name, attempt, role, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (artifact_id, owner_kind, owner_id, name, attempt) DO NOTHING`

func linkInsertArgs(l *artifact.Link) []any {
	return []any{
		l.ArtifactID.String(), string(l.OwnerKind), l.OwnerID,
		l.Name, l.Attempt, string(l.Role), l.CreatedAt,
	}
}

// nullString maps the empty string to SQL NULL so nullable text columns
// stay NULL rather than storing an empty value.
func nullString(s string) any {
	if s == "" {
		return nil
	}

	return s
}

// GetArtifact retrieves a live artifact by ID.
func (s *Store) GetArtifact(ctx context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	var m artifactModel

	err := s.pgdb.NewSelect(&m).
		Where("id = ?", artifactID.String()).
		Where("deleted_at IS NULL").
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, artifact.ErrNotFound
		}

		return nil, fmt.Errorf(errPrefix+"get artifact: %w", err)
	}

	return fromArtifactModel(&m)
}

// FindArtifactByKey retrieves a live artifact by its storage coordinates.
func (s *Store) FindArtifactByKey(ctx context.Context, backend, bucket, key string) (*artifact.Artifact, error) {
	var m artifactModel

	err := s.pgdb.NewSelect(&m).
		Where("backend = ?", backend).
		Where("bucket = ?", bucket).
		Where("key = ?", key).
		Where("deleted_at IS NULL").
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, artifact.ErrNotFound
		}

		return nil, fmt.Errorf(errPrefix+"find artifact by key: %w", err)
	}

	return fromArtifactModel(&m)
}

// UpdateArtifact persists size, hash, content type, and expiry. Lifecycle,
// created_at, and deleted_at are deliberately not updatable here.
func (s *Store) UpdateArtifact(ctx context.Context, a *artifact.Artifact) error {
	res, err := s.pgdb.NewRaw(`
		UPDATE dispatch_artifacts
		SET size = $1, content_hash = $2, content_type = $3, expires_at = $4
		WHERE id = $5`,
		a.Size, nullString(a.ContentHash), nullString(a.ContentType), a.ExpiresAt, a.ID.String(),
	).Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"update artifact: %w", err)
	}

	n, err := res.RowsAffected()
	if err == nil && n == 0 {
		return artifact.ErrNotFound
	}

	return nil
}

// ListArtifacts returns artifacts matching the given options, newest first.
func (s *Store) ListArtifacts(ctx context.Context, opts artifact.ListOpts) ([]*artifact.Artifact, error) {
	var models []artifactModel

	q := s.pgdb.NewSelect(&models)

	if !opts.IncludeDeleted {
		q = q.Where("deleted_at IS NULL")
	}

	if opts.Lifecycle != "" {
		q = q.Where("lifecycle = ?", string(opts.Lifecycle))
	}

	if opts.ScopeAppID != "" {
		q = q.Where("scope_app_id = ?", opts.ScopeAppID)
	}

	if opts.ScopeOrgID != "" {
		q = q.Where("scope_org_id = ?", opts.ScopeOrgID)
	}

	q = q.OrderExpr("created_at DESC, id ASC")

	if opts.Limit > 0 {
		q = q.Limit(opts.Limit)
	}

	if opts.Offset > 0 {
		q = q.Offset(opts.Offset)
	}

	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf(errPrefix+"list artifacts: %w", err)
	}

	return fromArtifactModels(models)
}

// LinkArtifact records that an owner references an artifact, idempotently.
func (s *Store) LinkArtifact(ctx context.Context, link *artifact.Link) error {
	_, err := s.pgdb.Exec(ctx, insertLinkSQL, linkInsertArgs(link)...)
	if err != nil {
		return fmt.Errorf(errPrefix+"link artifact: %w", err)
	}

	return nil
}

// ListLinks returns every link belonging to the given owner.
func (s *Store) ListLinks(ctx context.Context, owner artifact.OwnerRef) ([]*artifact.Link, error) {
	var models []artifactLinkModel

	err := s.pgdb.NewSelect(&models).
		Where("owner_kind = ?", string(owner.Kind)).
		Where("owner_id = ?", owner.ID).
		OrderExpr("name ASC, attempt ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf(errPrefix+"list links: %w", err)
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

// FindLinkByName returns the highest-attempt link for an owner and name,
// breaking ties by created_at descending — see the artifact.Store doc
// comment for why ties happen and what the tie-break does and does not
// fix.
func (s *Store) FindLinkByName(
	ctx context.Context,
	owner artifact.OwnerRef,
	name string,
) (*artifact.Link, error) {
	var m artifactLinkModel

	err := s.pgdb.NewSelect(&m).
		Where("owner_kind = ?", string(owner.Kind)).
		Where("owner_id = ?", owner.ID).
		Where("name = ?", name).
		OrderExpr("attempt DESC, created_at DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, artifact.ErrNotFound
		}

		return nil, fmt.Errorf(errPrefix+"find link by name: %w", err)
	}

	return fromLinkModel(&m)
}

// ListArtifactsByOwner returns live artifacts linked to an owner.
func (s *Store) ListArtifactsByOwner(
	ctx context.Context,
	owner artifact.OwnerRef,
	role artifact.Role,
) ([]*artifact.Artifact, error) {
	var models []artifactModel

	query := `
		SELECT DISTINCT a.* FROM dispatch_artifacts a
		JOIN dispatch_artifact_links l ON l.artifact_id = a.id
		WHERE l.owner_kind = $1 AND l.owner_id = $2 AND a.deleted_at IS NULL`

	args := []any{string(owner.Kind), owner.ID}

	if role != "" {
		query += ` AND l.role = $3`

		args = append(args, string(role))
	}

	if err := s.pgdb.NewRaw(query, args...).Scan(ctx, &models); err != nil {
		return nil, fmt.Errorf(errPrefix+"list artifacts by owner: %w", err)
	}

	return fromArtifactModels(models)
}

// terminalJobStates and terminalRunStates are the states after which an
// owner can no longer touch its artifacts.
const (
	terminalJobStatesSQL = `('completed', 'failed', 'cancelled')`
	terminalRunStatesSQL = `('completed', 'failed', 'cancelled')`
)

// eligibleEphemeralSQL selects ephemeral artifacts whose every linked
// owner is terminal and whose retention window has elapsed.
//
// The lifecycle predicate is a literal. It is never bound from a
// parameter, so no caller can widen this statement to reach a durable
// artifact.
//
// An owner row that no longer exists counts as terminal at the link's
// creation time: its job or run was purged, so it cannot still be running.
const eligibleEphemeralSQL = `
	SELECT a.id
	FROM dispatch_artifacts a
	JOIN dispatch_artifact_links l ON l.artifact_id = a.id
	LEFT JOIN dispatch_jobs j
		ON l.owner_kind = 'job' AND j.id = l.owner_id
	LEFT JOIN dispatch_workflow_runs r
		ON l.owner_kind IN ('run', 'step') AND r.id = l.owner_id
	WHERE a.lifecycle = 'ephemeral'
	  AND a.deleted_at IS NULL
	GROUP BY a.id, a.expires_at
	HAVING bool_and(
		CASE
			WHEN l.owner_kind = 'job'
				THEN j.id IS NULL OR j.state IN ` + terminalJobStatesSQL + `
			WHEN l.owner_kind IN ('run', 'step')
				THEN r.id IS NULL OR r.state IN ` + terminalRunStatesSQL + `
			ELSE TRUE
		END
	)
	AND (
		CASE
			WHEN a.expires_at IS NOT NULL THEN a.expires_at <= NOW()
			ELSE MAX(
				COALESCE(j.completed_at, j.updated_at, r.completed_at, r.updated_at, l.created_at)
			) + make_interval(secs => $1::double precision) <= NOW()
		END
	)`

// SweepEphemeral marks eligible ephemeral artifacts as deleted.
func (s *Store) SweepEphemeral(
	ctx context.Context,
	opts artifact.SweepOpts,
) ([]*artifact.Artifact, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultSweepLimit
	}

	selectSQL := eligibleEphemeralSQL + `
	LIMIT $2`

	if opts.DryRun {
		var models []artifactModel

		query := `
			SELECT * FROM dispatch_artifacts
			WHERE id IN (` + selectSQL + `)
			ORDER BY created_at ASC`

		if err := s.pgdb.NewRaw(query, opts.Retention.Seconds(), limit).Scan(ctx, &models); err != nil {
			return nil, fmt.Errorf(errPrefix+"sweep ephemeral (dry run): %w", err)
		}

		return fromArtifactModels(models)
	}

	var models []artifactModel

	query := `
		UPDATE dispatch_artifacts
		SET deleted_at = NOW()
		WHERE lifecycle = 'ephemeral'
		  AND deleted_at IS NULL
		  AND id IN (` + selectSQL + `)
		RETURNING *`

	if err := s.pgdb.NewRaw(query, opts.Retention.Seconds(), limit).Scan(ctx, &models); err != nil {
		return nil, fmt.Errorf(errPrefix+"sweep ephemeral: %w", err)
	}

	return fromArtifactModels(models)
}

// defaultSweepLimit bounds an unbounded sweep so a single pass can never
// lock an unbounded number of rows.
const defaultSweepLimit = 1000

// SweepOrphans marks link-less ephemeral artifacts created before cutoff.
func (s *Store) SweepOrphans(
	ctx context.Context,
	cutoff time.Time,
	limit int,
) ([]*artifact.Artifact, error) {
	if limit <= 0 {
		limit = defaultSweepLimit
	}

	var models []artifactModel

	// lifecycle = 'ephemeral' is a literal here for the same reason as in
	// eligibleEphemeralSQL: durable artifacts must be unreachable.
	query := `
		UPDATE dispatch_artifacts
		SET deleted_at = NOW()
		WHERE lifecycle = 'ephemeral'
		  AND deleted_at IS NULL
		  AND id IN (
		    SELECT a.id FROM dispatch_artifacts a
		    WHERE a.lifecycle = 'ephemeral'
		      AND a.deleted_at IS NULL
		      AND a.created_at < $1
		      AND NOT EXISTS (
		        SELECT 1 FROM dispatch_artifact_links l WHERE l.artifact_id = a.id
		      )
		    ORDER BY a.created_at ASC
		    LIMIT $2
		  )
		RETURNING *`

	if err := s.pgdb.NewRaw(query, cutoff, limit).Scan(ctx, &models); err != nil {
		return nil, fmt.Errorf(errPrefix+"sweep orphans: %w", err)
	}

	return fromArtifactModels(models)
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

	var models []artifactModel

	query := `
		SELECT * FROM dispatch_artifacts
		WHERE deleted_at IS NOT NULL
		  AND deleted_at + make_interval(secs => $1::double precision) <= NOW()
		ORDER BY deleted_at ASC
		LIMIT $2`

	if err := s.pgdb.NewRaw(query, grace.Seconds(), limit).Scan(ctx, &models); err != nil {
		return nil, fmt.Errorf(errPrefix+"list purgeable: %w", err)
	}

	return fromArtifactModels(models)
}

// PurgeArtifact hard-deletes an artifact. Links cascade.
func (s *Store) PurgeArtifact(ctx context.Context, artifactID id.ArtifactID) error {
	_, err := s.pgdb.NewRaw(
		`DELETE FROM dispatch_artifacts WHERE id = $1`, artifactID.String(),
	).Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"purge artifact: %w", err)
	}

	return nil
}
