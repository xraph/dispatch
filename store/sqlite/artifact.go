package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// defaultSweepLimit bounds an unbounded sweep so a single pass can never
// touch an unbounded number of rows.
const defaultSweepLimit = 1000

const insertArtifactSQL = `
	INSERT INTO dispatch_artifacts
		(id, backend, bucket, key, size, content_hash, content_type,
		 lifecycle, scope_app_id, scope_org_id, expires_at, created_at, deleted_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

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
	VALUES (?, ?, ?, ?, ?, ?, ?)
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

// CreateArtifact inserts an artifact and, when link is non-nil, its first
// link in a single transaction.
func (s *Store) CreateArtifact(ctx context.Context, a *artifact.Artifact, link *artifact.Link) error {
	if link == nil {
		if _, err := s.sdb.Exec(ctx, insertArtifactSQL, artifactInsertArgs(a)...); err != nil {
			if isDuplicateKey(err) {
				return artifact.ErrExists
			}

			return fmt.Errorf("dispatch/sqlite: create artifact: %w", err)
		}

		return nil
	}

	tx, err := s.sdb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: create artifact: begin: %w", err)
	}

	defer func() {
		if rerr := tx.Rollback(); rerr != nil && !errors.Is(rerr, sql.ErrTxDone) {
			s.logger.Warn("dispatch/sqlite: artifact tx rollback", log.String("error", rerr.Error()))
		}
	}()

	if _, err := tx.Exec(ctx, insertArtifactSQL, artifactInsertArgs(a)...); err != nil {
		if isDuplicateKey(err) {
			return artifact.ErrExists
		}

		return fmt.Errorf("dispatch/sqlite: create artifact: %w", err)
	}

	if _, err := tx.Exec(ctx, insertLinkSQL, linkInsertArgs(link)...); err != nil {
		return fmt.Errorf("dispatch/sqlite: create artifact link: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("dispatch/sqlite: create artifact: commit: %w", err)
	}

	return nil
}

// GetArtifact retrieves a live artifact by ID.
func (s *Store) GetArtifact(ctx context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	m := new(artifactModel)

	err := s.sdb.NewSelect(m).
		Where("id = ?", artifactID.String()).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, artifact.ErrNotFound
		}

		return nil, fmt.Errorf("dispatch/sqlite: get artifact: %w", err)
	}

	return fromArtifactModel(m)
}

// FindArtifactByKey retrieves a live artifact by its storage coordinates.
func (s *Store) FindArtifactByKey(ctx context.Context, backend, bucket, key string) (*artifact.Artifact, error) {
	m := new(artifactModel)

	err := s.sdb.NewSelect(m).
		Where("backend = ?", backend).
		Where("bucket = ?", bucket).
		Where("key = ?", key).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, artifact.ErrNotFound
		}

		return nil, fmt.Errorf("dispatch/sqlite: find artifact by key: %w", err)
	}

	return fromArtifactModel(m)
}

// UpdateArtifact persists size, hash, content type, and expiry. Lifecycle,
// created_at, and deleted_at are deliberately not updatable here.
func (s *Store) UpdateArtifact(ctx context.Context, a *artifact.Artifact) error {
	res, err := s.sdb.Exec(ctx, `
		UPDATE dispatch_artifacts
		SET size = ?, content_hash = ?, content_type = ?, expires_at = ?
		WHERE id = ?`,
		a.Size, nullString(a.ContentHash), nullString(a.ContentType), a.ExpiresAt, a.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: update artifact: %w", err)
	}

	if n, rerr := res.RowsAffected(); rerr == nil && n == 0 {
		return artifact.ErrNotFound
	}

	return nil
}

// ListArtifacts returns artifacts matching the given options, newest first.
func (s *Store) ListArtifacts(ctx context.Context, opts artifact.ListOpts) ([]*artifact.Artifact, error) {
	var models []artifactModel

	q := s.sdb.NewSelect(&models)

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
		return nil, fmt.Errorf("dispatch/sqlite: list artifacts: %w", err)
	}

	return fromArtifactModels(models)
}

// LinkArtifact records that an owner references an artifact, idempotently.
func (s *Store) LinkArtifact(ctx context.Context, link *artifact.Link) error {
	if _, err := s.sdb.Exec(ctx, insertLinkSQL, linkInsertArgs(link)...); err != nil {
		return fmt.Errorf("dispatch/sqlite: link artifact: %w", err)
	}

	return nil
}

// ListLinks returns every link belonging to the given owner.
func (s *Store) ListLinks(ctx context.Context, owner artifact.OwnerRef) ([]*artifact.Link, error) {
	var models []artifactLinkModel

	err := s.sdb.NewSelect(&models).
		Where("owner_kind = ?", string(owner.Kind)).
		Where("owner_id = ?", owner.ID).
		OrderExpr("name ASC, attempt ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list links: %w", err)
	}

	return fromLinkModels(models)
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
	m := new(artifactLinkModel)

	err := s.sdb.NewSelect(m).
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

		return nil, fmt.Errorf("dispatch/sqlite: find link by name: %w", err)
	}

	return fromLinkModel(m)
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
		WHERE l.owner_kind = ? AND l.owner_id = ? AND a.deleted_at IS NULL`

	args := []any{string(owner.Kind), owner.ID}

	if role != "" {
		query += ` AND l.role = ?`

		args = append(args, string(role))
	}

	if err := s.sdb.NewRaw(query, args...).Scan(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list artifacts by owner: %w", err)
	}

	return fromArtifactModels(models)
}

// eligibleEphemeralSQL selects ephemeral artifacts whose every linked
// owner is terminal and whose retention window has elapsed.
//
// The lifecycle predicate is a literal. It is never bound from a
// parameter, so no caller can widen this statement to reach a durable
// artifact.
//
// SQLite has no bool_and, so the all-terminal test is expressed as
// MIN(CASE ... END) = 1. Timestamps are ISO8601 text and compare
// lexicographically, so the retention cutoff is computed in Go and bound
// as a formatted time rather than built with SQL interval arithmetic.
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
	HAVING MIN(
		CASE
			WHEN l.owner_kind = 'job'
				THEN CASE WHEN j.id IS NULL
					OR j.state IN ('completed', 'failed', 'cancelled') THEN 1 ELSE 0 END
			WHEN l.owner_kind IN ('run', 'step')
				THEN CASE WHEN r.id IS NULL
					OR r.state IN ('completed', 'failed', 'cancelled') THEN 1 ELSE 0 END
			ELSE 1
		END
	) = 1
	AND (
		CASE
			WHEN a.expires_at IS NOT NULL THEN a.expires_at <= ?
			ELSE MAX(
				COALESCE(j.completed_at, j.updated_at, r.completed_at, r.updated_at, l.created_at)
			) <= ?
		END
	)
	LIMIT ?`

// SweepEphemeral marks eligible ephemeral artifacts as deleted.
func (s *Store) SweepEphemeral(
	ctx context.Context,
	opts artifact.SweepOpts,
) ([]*artifact.Artifact, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultSweepLimit
	}

	now := time.Now().UTC()
	cutoff := now.Add(-opts.Retention)

	ids, err := s.selectIDs(ctx, eligibleEphemeralSQL, now, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: sweep ephemeral: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	if opts.DryRun {
		return s.artifactsByIDs(ctx, ids)
	}

	if err := s.markDeleted(ctx, ids, now); err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: sweep ephemeral: %w", err)
	}

	return s.artifactsByIDs(ctx, ids)
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

	// lifecycle = 'ephemeral' is a literal for the same reason as above.
	const query = `
		SELECT a.id FROM dispatch_artifacts a
		WHERE a.lifecycle = 'ephemeral'
		  AND a.deleted_at IS NULL
		  AND a.created_at < ?
		  AND NOT EXISTS (
		    SELECT 1 FROM dispatch_artifact_links l WHERE l.artifact_id = a.id
		  )
		ORDER BY a.created_at ASC
		LIMIT ?`

	ids, err := s.selectIDs(ctx, query, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: sweep orphans: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	if err := s.markDeleted(ctx, ids, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: sweep orphans: %w", err)
	}

	return s.artifactsByIDs(ctx, ids)
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

	cutoff := time.Now().UTC().Add(-grace)

	err := s.sdb.NewSelect(&models).
		Where("deleted_at IS NOT NULL").
		Where("deleted_at <= ?", cutoff).
		OrderExpr("deleted_at ASC").
		Limit(limit).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list purgeable: %w", err)
	}

	return fromArtifactModels(models)
}

// PurgeArtifact hard-deletes an artifact. Links cascade.
func (s *Store) PurgeArtifact(ctx context.Context, artifactID id.ArtifactID) error {
	if _, err := s.sdb.Exec(ctx,
		`DELETE FROM dispatch_artifact_links WHERE artifact_id = ?`, artifactID.String(),
	); err != nil {
		return fmt.Errorf("dispatch/sqlite: purge artifact links: %w", err)
	}

	if _, err := s.sdb.Exec(ctx,
		`DELETE FROM dispatch_artifacts WHERE id = ?`, artifactID.String(),
	); err != nil {
		return fmt.Errorf("dispatch/sqlite: purge artifact: %w", err)
	}

	return nil
}

// selectIDs runs a query whose first column is an artifact ID.
func (s *Store) selectIDs(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := s.sdb.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("dispatch/sqlite: close artifact id rows", log.String("error", cerr.Error()))
		}
	}()

	var ids []string

	for rows.Next() {
		var got string
		if serr := rows.Scan(&got); serr != nil {
			return nil, serr
		}

		ids = append(ids, got)
	}

	return ids, rows.Err()
}

// markDeleted soft-deletes the given artifacts. The lifecycle literal is
// repeated here so the write itself, not only the selection above it,
// refuses to touch a durable artifact.
func (s *Store) markDeleted(ctx context.Context, ids []string, at time.Time) error {
	query := `
		UPDATE dispatch_artifacts
		SET deleted_at = ?
		WHERE lifecycle = 'ephemeral'
		  AND deleted_at IS NULL
		  AND id IN (` + placeholders(len(ids)) + `)`

	args := make([]any, 0, len(ids)+1)
	args = append(args, at)

	for _, got := range ids {
		args = append(args, got)
	}

	_, err := s.sdb.Exec(ctx, query, args...)

	return err
}

// artifactsByIDs loads artifacts by ID, including soft-deleted ones, so a
// sweep can return what it just marked.
func (s *Store) artifactsByIDs(ctx context.Context, ids []string) ([]*artifact.Artifact, error) {
	var models []artifactModel

	query := `SELECT * FROM dispatch_artifacts WHERE id IN (` + placeholders(len(ids)) + `)`

	args := make([]any, 0, len(ids))
	for _, got := range ids {
		args = append(args, got)
	}

	if err := s.sdb.NewRaw(query, args...).Scan(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: load artifacts by id: %w", err)
	}

	return fromArtifactModels(models)
}

// placeholders builds "?, ?, ?" for an IN clause of n values.
func placeholders(n int) string {
	if n == 0 {
		return "NULL"
	}

	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}
