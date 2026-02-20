// Package sqlite implements store.Store using modernc.org/sqlite for
// embedded/edge deployments, CLI tools, and standalone applications.
//
// The store uses WAL mode for concurrent read/write access and stores
// timestamps as ISO 8601 text strings, arrays as JSON text, and binary
// data as BLOBs.
//
// Usage:
//
//	s, err := sqlite.New(":memory:") // or a file path
//	if err != nil { ... }
//	defer s.Close(ctx)
//	if err := s.Migrate(ctx); err != nil { ... }
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite" // SQLite driver

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Compile-time interface checks.
var (
	_ job.Store      = (*Store)(nil)
	_ workflow.Store = (*Store)(nil)
	_ cron.Store     = (*Store)(nil)
	_ dlq.Store      = (*Store)(nil)
	_ event.Store    = (*Store)(nil)
	_ cluster.Store  = (*Store)(nil)
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Option configures the Store.
type Option func(*Store)

// WithLogger sets a custom logger.
func WithLogger(l *slog.Logger) Option {
	return func(s *Store) { s.logger = l }
}

// Store implements the composite store.Store interface backed by SQLite.
type Store struct {
	db     *sql.DB
	logger *slog.Logger
}

// New opens (or creates) a SQLite database at the given path and returns a
// Store. Use ":memory:" for an in-memory database.
func New(path string, opts ...Option) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: open: %w", err)
	}

	// Enable WAL mode and foreign keys.
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.ExecContext(context.Background(), pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("dispatch/sqlite: %s: %w", pragma, err)
		}
	}

	s := &Store{db: db, logger: slog.Default()}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// DB returns the underlying *sql.DB.
func (s *Store) DB() *sql.DB { return s.db }

// Migrate applies embedded SQL migration files.
func (s *Store) Migrate(ctx context.Context) error {
	// Create tracking table.
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS dispatch_migrations (
		filename TEXT PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: create migrations table: %w", err)
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: read migrations dir: %w", err)
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}

		// Check if already applied.
		var count int
		err := s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM dispatch_migrations WHERE filename = ?", name,
		).Scan(&count)
		if err != nil {
			return fmt.Errorf("dispatch/sqlite: check migration %s: %w", name, err)
		}
		if count > 0 {
			continue
		}

		data, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("dispatch/sqlite: read migration %s: %w", name, err)
		}

		if _, err := s.db.ExecContext(ctx, string(data)); err != nil {
			return fmt.Errorf("dispatch/sqlite: apply migration %s: %w", name, err)
		}

		if _, err := s.db.ExecContext(ctx,
			"INSERT INTO dispatch_migrations (filename) VALUES (?)", name,
		); err != nil {
			return fmt.Errorf("dispatch/sqlite: record migration %s: %w", name, err)
		}

		s.logger.InfoContext(ctx, "applied migration", "file", name)
	}
	return nil
}

// Ping verifies the database connection.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database connection.
func (s *Store) Close(_ context.Context) error {
	return s.db.Close()
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

const isoFormat = "2006-01-02T15:04:05.000Z"

func nowISO() string {
	return time.Now().UTC().Format(isoFormat)
}

func parseTime(s string) (time.Time, error) {
	// Try multiple formats for robustness.
	for _, fmt := range []string{
		isoFormat,
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(fmt, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("dispatch/sqlite: cannot parse time %q", s)
}

func scanTime(val *sql.NullString) (*time.Time, error) {
	if !val.Valid || val.String == "" {
		return nil, nil
	}
	t, err := parseTime(val.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func timeToNull(t *time.Time) sql.NullString {
	if t == nil || t.IsZero() {
		return sql.NullString{}
	}
	return sql.NullString{String: t.UTC().Format(isoFormat), Valid: true}
}

func timeToStr(t time.Time) string {
	return t.UTC().Format(isoFormat)
}

// ---------------------------------------------------------------------------
// JSON helpers for arrays and maps
// ---------------------------------------------------------------------------

func stringsToJSON(ss []string) string {
	if ss == nil {
		return "[]"
	}
	b, _ := json.Marshal(ss) //nolint:errcheck // []string never fails
	return string(b)
}

func jsonToStrings(s string) []string {
	if s == "" || s == "[]" {
		return nil
	}
	var ss []string
	_ = json.Unmarshal([]byte(s), &ss) //nolint:errcheck // best effort
	return ss
}

func mapToJSON(m map[string]string) string {
	if m == nil {
		return "{}"
	}
	b, _ := json.Marshal(m) //nolint:errcheck // map[string]string never fails
	return string(b)
}

func jsonToMap(s string) map[string]string {
	if s == "" || s == "{}" {
		return nil
	}
	m := make(map[string]string)
	_ = json.Unmarshal([]byte(s), &m) //nolint:errcheck // best effort
	return m
}

// ---------------------------------------------------------------------------
// Job Store
// ---------------------------------------------------------------------------

// EnqueueJob persists a new job in pending state.
func (s *Store) EnqueueJob(ctx context.Context, j *job.Job) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dispatch_jobs (
			id, name, queue, payload, state, priority, max_retries, retry_count,
			last_error, scope_app_id, scope_org_id, worker_id,
			run_at, started_at, completed_at, heartbeat_at, created_at, updated_at, timeout
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID.String(), j.Name, j.Queue, j.Payload, string(j.State),
		j.Priority, j.MaxRetries, j.RetryCount,
		nullStr(j.LastError), nullStr(j.ScopeAppID), nullStr(j.ScopeOrgID), nullStr(j.WorkerID.String()),
		timeToStr(j.RunAt), timeToNull(j.StartedAt), timeToNull(j.CompletedAt), timeToNull(j.HeartbeatAt),
		timeToStr(j.CreatedAt), timeToStr(j.UpdatedAt), j.Timeout.Nanoseconds(),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/sqlite: enqueue job: %w", err)
	}
	return nil
}

// DequeueJobs atomically claims up to limit pending jobs.
// SQLite doesn't support FOR UPDATE SKIP LOCKED, so we use BEGIN IMMEDIATE
// with a subquery + UPDATE pattern.
func (s *Store) DequeueJobs(ctx context.Context, queues []string, limit int) ([]*job.Job, error) {
	now := nowISO()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: dequeue begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // commit follows

	// Build queue placeholders.
	placeholders := make([]string, len(queues))
	args := make([]any, 0, len(queues)+2)
	for i, q := range queues {
		placeholders[i] = "?"
		args = append(args, q)
	}
	args = append(args, now, limit)

	//nolint:gosec // placeholders are safe "?" marks, not user input
	query := fmt.Sprintf(`
		UPDATE dispatch_jobs
		SET state = 'running', started_at = ?, updated_at = ?
		WHERE id IN (
			SELECT id FROM dispatch_jobs
			WHERE state IN ('pending', 'retrying')
			  AND queue IN (%s)
			  AND run_at <= ?
			ORDER BY priority DESC, run_at ASC
			LIMIT ?
		)`,
		strings.Join(placeholders, ","),
	)

	// Prepend started_at and updated_at params.
	execArgs := make([]any, 0, len(args)+2)
	execArgs = append(execArgs, now, now)
	execArgs = append(execArgs, args...)

	_, err = tx.ExecContext(ctx, query, execArgs...)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: dequeue update: %w", err)
	}

	// Select the just-updated rows.
	//nolint:gosec // placeholders are safe "?" marks, not user input
	selectQuery := fmt.Sprintf(`
		SELECT id, name, queue, payload, state, priority, max_retries, retry_count,
			last_error, scope_app_id, scope_org_id, worker_id,
			run_at, started_at, completed_at, heartbeat_at, created_at, updated_at, timeout
		FROM dispatch_jobs
		WHERE state = 'running' AND started_at = ? AND queue IN (%s)
		ORDER BY priority DESC, run_at ASC`,
		strings.Join(placeholders, ","),
	)

	selectArgs := make([]any, 0, len(queues)+1)
	selectArgs = append(selectArgs, now)
	for _, q := range queues {
		selectArgs = append(selectArgs, q)
	}

	rows, err := tx.QueryContext(ctx, selectQuery, selectArgs...)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: dequeue select: %w", err)
	}
	defer rows.Close()

	jobs, err := scanJobs(rows)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: dequeue commit: %w", err)
	}
	return jobs, nil
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, queue, payload, state, priority, max_retries, retry_count,
			last_error, scope_app_id, scope_org_id, worker_id,
			run_at, started_at, completed_at, heartbeat_at, created_at, updated_at, timeout
		FROM dispatch_jobs WHERE id = ? LIMIT 1`,
		jobID.String(),
	)
	j, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, dispatch.ErrJobNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get job: %w", err)
	}
	return j, nil
}

// UpdateJob persists changes to an existing job.
func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	now := nowISO()
	res, err := s.db.ExecContext(ctx, `
		UPDATE dispatch_jobs SET
			name = ?, queue = ?, payload = ?, state = ?, priority = ?,
			max_retries = ?, retry_count = ?, last_error = ?,
			scope_app_id = ?, scope_org_id = ?, worker_id = ?,
			run_at = ?, started_at = ?, completed_at = ?, heartbeat_at = ?,
			updated_at = ?, timeout = ?
		WHERE id = ?`,
		j.Name, j.Queue, j.Payload, string(j.State), j.Priority,
		j.MaxRetries, j.RetryCount, nullStr(j.LastError),
		nullStr(j.ScopeAppID), nullStr(j.ScopeOrgID), nullStr(j.WorkerID.String()),
		timeToStr(j.RunAt), timeToNull(j.StartedAt), timeToNull(j.CompletedAt), timeToNull(j.HeartbeatAt),
		now, j.Timeout.Nanoseconds(),
		j.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: update job: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrJobNotFound
	}
	return nil
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, jobID id.JobID) error {
	res, err := s.db.ExecContext(ctx, "DELETE FROM dispatch_jobs WHERE id = ?", jobID.String())
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: delete job: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ListJobsByState returns jobs matching the given state.
func (s *Store) ListJobsByState(ctx context.Context, state job.State, opts job.ListOpts) ([]*job.Job, error) {
	q := "SELECT id, name, queue, payload, state, priority, max_retries, retry_count, " +
		"last_error, scope_app_id, scope_org_id, worker_id, " +
		"run_at, started_at, completed_at, heartbeat_at, created_at, updated_at, timeout " +
		"FROM dispatch_jobs WHERE state = ?"
	args := []any{string(state)}

	if opts.Queue != "" {
		q += " AND queue = ?"
		args = append(args, opts.Queue)
	}
	q += " ORDER BY created_at ASC"
	if opts.Limit > 0 {
		q += " LIMIT " + strconv.Itoa(opts.Limit)
	}
	if opts.Offset > 0 {
		q += " OFFSET " + strconv.Itoa(opts.Offset)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows)
}

// HeartbeatJob updates the heartbeat timestamp for a running job.
func (s *Store) HeartbeatJob(ctx context.Context, jobID id.JobID, _ id.WorkerID) error {
	now := nowISO()
	res, err := s.db.ExecContext(ctx,
		"UPDATE dispatch_jobs SET heartbeat_at = ?, updated_at = ? WHERE id = ?",
		now, now, jobID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: heartbeat job: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrJobNotFound
	}
	return nil
}

// ReapStaleJobs returns running jobs whose last heartbeat is older than threshold.
func (s *Store) ReapStaleJobs(ctx context.Context, threshold time.Duration) ([]*job.Job, error) {
	cutoff := time.Now().UTC().Add(-threshold).Format(isoFormat)
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, queue, payload, state, priority, max_retries, retry_count,
			last_error, scope_app_id, scope_org_id, worker_id,
			run_at, started_at, completed_at, heartbeat_at, created_at, updated_at, timeout
		FROM dispatch_jobs
		WHERE state = 'running' AND heartbeat_at IS NOT NULL AND heartbeat_at < ?`,
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: reap stale jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows)
}

// CountJobs returns the number of jobs matching the given options.
func (s *Store) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	q := "SELECT COUNT(*) FROM dispatch_jobs WHERE 1=1"
	args := []any{}
	if opts.Queue != "" {
		q += " AND queue = ?"
		args = append(args, opts.Queue)
	}
	if opts.State != "" {
		q += " AND state = ?"
		args = append(args, string(opts.State))
	}
	var count int64
	err := s.db.QueryRowContext(ctx, q, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: count jobs: %w", err)
	}
	return count, nil
}

// ---------------------------------------------------------------------------
// Job scanner helpers
// ---------------------------------------------------------------------------

func scanJob(row *sql.Row) (*job.Job, error) {
	var (
		jID, name, queue, stateStr          string
		payload                             []byte
		priority, maxRetries, retryCount    int
		lastError, scopeApp, scopeOrg       sql.NullString
		workerID                            sql.NullString
		runAt, createdAt, updatedAt         string
		startedAt, completedAt, heartbeatAt sql.NullString
		timeout                             int64
	)
	err := row.Scan(
		&jID, &name, &queue, &payload, &stateStr, &priority, &maxRetries, &retryCount,
		&lastError, &scopeApp, &scopeOrg, &workerID,
		&runAt, &startedAt, &completedAt, &heartbeatAt, &createdAt, &updatedAt, &timeout,
	)
	if err != nil {
		return nil, err
	}
	return buildJob(jID, name, queue, payload, stateStr, priority, maxRetries, retryCount,
		lastError, scopeApp, scopeOrg, workerID,
		runAt, startedAt, completedAt, heartbeatAt, createdAt, updatedAt, timeout)
}

func scanJobs(rows *sql.Rows) ([]*job.Job, error) {
	var jobs []*job.Job
	for rows.Next() {
		var (
			jID, name, queue, stateStr              string
			payload                                 []byte
			priority, maxRetries, retryCount        int
			lastError, scopeApp, scopeOrg, workerID sql.NullString
			runAt, createdAt, updatedAt             string
			startedAt, completedAt, heartbeatAt     sql.NullString
			timeout                                 int64
		)
		err := rows.Scan(
			&jID, &name, &queue, &payload, &stateStr, &priority, &maxRetries, &retryCount,
			&lastError, &scopeApp, &scopeOrg, &workerID,
			&runAt, &startedAt, &completedAt, &heartbeatAt, &createdAt, &updatedAt, &timeout,
		)
		if err != nil {
			return nil, fmt.Errorf("dispatch/sqlite: scan job: %w", err)
		}
		j, err := buildJob(jID, name, queue, payload, stateStr, priority, maxRetries, retryCount,
			lastError, scopeApp, scopeOrg, workerID,
			runAt, startedAt, completedAt, heartbeatAt, createdAt, updatedAt, timeout)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func buildJob(
	jID, name, queue string, payload []byte, stateStr string,
	priority, maxRetries, retryCount int,
	lastError, scopeApp, scopeOrg, workerID sql.NullString,
	runAtStr string, startedAt, completedAt, heartbeatAt sql.NullString,
	createdAtStr, updatedAtStr string, timeout int64,
) (*job.Job, error) {
	jobID, err := id.ParseJobID(jID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse job id: %w", err)
	}
	runAt, err := parseTime(runAtStr)
	if err != nil {
		return nil, err
	}
	createdAt, err := parseTime(createdAtStr)
	if err != nil {
		return nil, err
	}
	updatedAt, err := parseTime(updatedAtStr)
	if err != nil {
		return nil, err
	}
	sa, err := scanTime(&startedAt)
	if err != nil {
		return nil, err
	}
	ca, err := scanTime(&completedAt)
	if err != nil {
		return nil, err
	}
	ha, err := scanTime(&heartbeatAt)
	if err != nil {
		return nil, err
	}

	j := &job.Job{
		Entity:      dispatch.Entity{CreatedAt: createdAt, UpdatedAt: updatedAt},
		ID:          jobID,
		Name:        name,
		Queue:       queue,
		Payload:     payload,
		State:       job.State(stateStr),
		Priority:    priority,
		MaxRetries:  maxRetries,
		RetryCount:  retryCount,
		LastError:   lastError.String,
		ScopeAppID:  scopeApp.String,
		ScopeOrgID:  scopeOrg.String,
		RunAt:       runAt,
		StartedAt:   sa,
		CompletedAt: ca,
		HeartbeatAt: ha,
		Timeout:     time.Duration(timeout),
	}
	if workerID.Valid && workerID.String != "" {
		wid, err := id.ParseWorkerID(workerID.String)
		if err != nil {
			return nil, fmt.Errorf("dispatch/sqlite: parse worker id: %w", err)
		}
		j.WorkerID = wid
	}
	return j, nil
}

// ---------------------------------------------------------------------------
// Workflow Store
// ---------------------------------------------------------------------------

// CreateRun persists a new workflow run.
func (s *Store) CreateRun(ctx context.Context, run *workflow.Run) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dispatch_workflow_runs (
			id, name, state, input, output, error,
			scope_app_id, scope_org_id,
			started_at, completed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		run.ID.String(), run.Name, string(run.State), run.Input, run.Output, nullStr(run.Error),
		nullStr(run.ScopeAppID), nullStr(run.ScopeOrgID),
		timeToStr(run.StartedAt), timeToNull(run.CompletedAt),
		timeToStr(run.CreatedAt), timeToStr(run.UpdatedAt),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return dispatch.ErrJobAlreadyExists
		}
		return fmt.Errorf("dispatch/sqlite: create run: %w", err)
	}
	return nil
}

// GetRun retrieves a workflow run by ID.
func (s *Store) GetRun(ctx context.Context, runID id.RunID) (*workflow.Run, error) {
	var (
		rID, name, stateStr        string
		input, output              []byte
		errStr, scopeApp, scopeOrg sql.NullString
		startedAt, completedAt     sql.NullString
		createdAt, updatedAt       string
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, state, input, output, error,
			scope_app_id, scope_org_id,
			started_at, completed_at, created_at, updated_at
		FROM dispatch_workflow_runs WHERE id = ? LIMIT 1`,
		runID.String(),
	).Scan(&rID, &name, &stateStr, &input, &output, &errStr,
		&scopeApp, &scopeOrg, &startedAt, &completedAt, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dispatch.ErrRunNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get run: %w", err)
	}
	return buildRun(rID, name, stateStr, input, output, errStr,
		scopeApp, scopeOrg, startedAt, completedAt, createdAt, updatedAt)
}

// UpdateRun persists changes to an existing workflow run.
func (s *Store) UpdateRun(ctx context.Context, run *workflow.Run) error {
	now := nowISO()
	res, err := s.db.ExecContext(ctx, `
		UPDATE dispatch_workflow_runs SET
			name = ?, state = ?, input = ?, output = ?, error = ?,
			scope_app_id = ?, scope_org_id = ?,
			started_at = ?, completed_at = ?, updated_at = ?
		WHERE id = ?`,
		run.Name, string(run.State), run.Input, run.Output, nullStr(run.Error),
		nullStr(run.ScopeAppID), nullStr(run.ScopeOrgID),
		timeToStr(run.StartedAt), timeToNull(run.CompletedAt), now,
		run.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: update run: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrRunNotFound
	}
	return nil
}

// ListRuns returns workflow runs matching the given options.
func (s *Store) ListRuns(ctx context.Context, opts workflow.ListOpts) ([]*workflow.Run, error) {
	q := "SELECT id, name, state, input, output, error, " +
		"scope_app_id, scope_org_id, started_at, completed_at, created_at, updated_at " +
		"FROM dispatch_workflow_runs WHERE 1=1"
	args := []any{}

	if opts.State != "" {
		q += " AND state = ?"
		args = append(args, string(opts.State))
	}
	q += " ORDER BY created_at ASC"
	if opts.Limit > 0 {
		q += " LIMIT " + strconv.Itoa(opts.Limit)
	}
	if opts.Offset > 0 {
		q += " OFFSET " + strconv.Itoa(opts.Offset)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list runs: %w", err)
	}
	defer rows.Close()

	var runs []*workflow.Run
	for rows.Next() {
		var (
			rID, name, stateStr        string
			input, output              []byte
			errStr, scopeApp, scopeOrg sql.NullString
			startedAt, completedAt     sql.NullString
			createdAt, updatedAt       string
		)
		if err := rows.Scan(&rID, &name, &stateStr, &input, &output, &errStr,
			&scopeApp, &scopeOrg, &startedAt, &completedAt, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("dispatch/sqlite: scan run: %w", err)
		}
		r, err := buildRun(rID, name, stateStr, input, output, errStr,
			scopeApp, scopeOrg, startedAt, completedAt, createdAt, updatedAt)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// SaveCheckpoint persists checkpoint data for a workflow step.
func (s *Store) SaveCheckpoint(ctx context.Context, runID id.RunID, stepName string, data []byte) error {
	now := nowISO()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dispatch_checkpoints (id, run_id, step_name, data, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (run_id, step_name) DO UPDATE SET data = excluded.data, created_at = excluded.created_at`,
		id.NewCheckpointID().String(), runID.String(), stepName, data, now,
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: save checkpoint: %w", err)
	}
	return nil
}

// GetCheckpoint retrieves checkpoint data for a specific workflow step.
func (s *Store) GetCheckpoint(ctx context.Context, runID id.RunID, stepName string) ([]byte, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT data FROM dispatch_checkpoints WHERE run_id = ? AND step_name = ? LIMIT 1",
		runID.String(), stepName,
	).Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // no checkpoint is not an error
		}
		return nil, fmt.Errorf("dispatch/sqlite: get checkpoint: %w", err)
	}
	return data, nil
}

// ListCheckpoints returns all checkpoints for a workflow run.
func (s *Store) ListCheckpoints(ctx context.Context, runID id.RunID) ([]*workflow.Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, run_id, step_name, data, created_at FROM dispatch_checkpoints WHERE run_id = ? ORDER BY created_at ASC",
		runID.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list checkpoints: %w", err)
	}
	defer rows.Close()

	var checkpoints []*workflow.Checkpoint
	for rows.Next() {
		var cpID, rID, step string
		var data []byte
		var createdAtStr string
		if err := rows.Scan(&cpID, &rID, &step, &data, &createdAtStr); err != nil {
			return nil, fmt.Errorf("dispatch/sqlite: scan checkpoint: %w", err)
		}
		cid, err := id.ParseCheckpointID(cpID)
		if err != nil {
			return nil, fmt.Errorf("dispatch/sqlite: parse checkpoint id: %w", err)
		}
		rid, err := id.ParseRunID(rID)
		if err != nil {
			return nil, fmt.Errorf("dispatch/sqlite: parse run id: %w", err)
		}
		ca, err := parseTime(createdAtStr)
		if err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, &workflow.Checkpoint{
			ID:        cid,
			RunID:     rid,
			StepName:  step,
			Data:      data,
			CreatedAt: ca,
		})
	}
	return checkpoints, rows.Err()
}

func buildRun(
	rID, name, stateStr string, input, output []byte, errStr sql.NullString,
	scopeApp, scopeOrg, startedAt, completedAt sql.NullString,
	createdAtStr, updatedAtStr string,
) (*workflow.Run, error) {
	runID, err := id.ParseRunID(rID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse run id: %w", err)
	}
	createdAt, err := parseTime(createdAtStr)
	if err != nil {
		return nil, err
	}
	updatedAt, err := parseTime(updatedAtStr)
	if err != nil {
		return nil, err
	}
	sa, err := scanTime(&startedAt)
	if err != nil {
		return nil, err
	}
	ca, err := scanTime(&completedAt)
	if err != nil {
		return nil, err
	}

	var startedAtVal time.Time
	if sa != nil {
		startedAtVal = *sa
	}

	return &workflow.Run{
		Entity:      dispatch.Entity{CreatedAt: createdAt, UpdatedAt: updatedAt},
		ID:          runID,
		Name:        name,
		State:       workflow.RunState(stateStr),
		Input:       input,
		Output:      output,
		Error:       errStr.String,
		ScopeAppID:  scopeApp.String,
		ScopeOrgID:  scopeOrg.String,
		StartedAt:   startedAtVal,
		CompletedAt: ca,
	}, nil
}

// ---------------------------------------------------------------------------
// Cron Store
// ---------------------------------------------------------------------------

// RegisterCron persists a new cron entry.
func (s *Store) RegisterCron(ctx context.Context, entry *cron.Entry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dispatch_cron_entries (
			id, name, schedule, job_name, queue, payload,
			scope_app_id, scope_org_id,
			last_run_at, next_run_at, locked_by, locked_until, enabled,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ID.String(), entry.Name, entry.Schedule, entry.JobName, entry.Queue, entry.Payload,
		nullStr(entry.ScopeAppID), nullStr(entry.ScopeOrgID),
		timeToNull(entry.LastRunAt), timeToNull(entry.NextRunAt),
		nullStr(entry.LockedBy), timeToNull(entry.LockedUntil),
		boolToInt(entry.Enabled),
		timeToStr(entry.CreatedAt), timeToStr(entry.UpdatedAt),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return dispatch.ErrDuplicateCron
		}
		return fmt.Errorf("dispatch/sqlite: register cron: %w", err)
	}
	return nil
}

// GetCron retrieves a cron entry by ID.
func (s *Store) GetCron(ctx context.Context, entryID id.CronID) (*cron.Entry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, schedule, job_name, queue, payload,
			scope_app_id, scope_org_id,
			last_run_at, next_run_at, locked_by, locked_until, enabled,
			created_at, updated_at
		FROM dispatch_cron_entries WHERE id = ? LIMIT 1`,
		entryID.String(),
	)
	entry, err := scanCronEntry(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, dispatch.ErrCronNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get cron: %w", err)
	}
	return entry, nil
}

// ListCrons returns all cron entries.
func (s *Store) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, schedule, job_name, queue, payload,
			scope_app_id, scope_org_id,
			last_run_at, next_run_at, locked_by, locked_until, enabled,
			created_at, updated_at
		FROM dispatch_cron_entries ORDER BY created_at ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list crons: %w", err)
	}
	defer rows.Close()
	return scanCronEntries(rows)
}

// AcquireCronLock attempts to acquire a distributed lock for a cron entry.
func (s *Store) AcquireCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	now := nowISO()
	until := time.Now().UTC().Add(ttl).Format(isoFormat)
	wID := workerID.String()

	res, err := s.db.ExecContext(ctx, `
		UPDATE dispatch_cron_entries
		SET locked_by = ?, locked_until = ?, updated_at = ?
		WHERE id = ?
		  AND (locked_by IS NULL OR locked_until < ? OR locked_by = ?)`,
		wID, until, now,
		entryID.String(), now, wID,
	)
	if err != nil {
		return false, fmt.Errorf("dispatch/sqlite: acquire cron lock: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		// Check if entry exists.
		var count int
		err := s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM dispatch_cron_entries WHERE id = ?", entryID.String(),
		).Scan(&count)
		if err != nil {
			return false, fmt.Errorf("dispatch/sqlite: check cron exists: %w", err)
		}
		if count == 0 {
			return false, dispatch.ErrCronNotFound
		}
		return false, nil
	}
	return true, nil
}

// ReleaseCronLock releases the distributed lock for a cron entry.
func (s *Store) ReleaseCronLock(ctx context.Context, entryID id.CronID, workerID id.WorkerID) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE dispatch_cron_entries
		SET locked_by = NULL, locked_until = NULL, updated_at = ?
		WHERE id = ? AND locked_by = ?`,
		nowISO(), entryID.String(), workerID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: release cron lock: %w", err)
	}
	return nil
}

// UpdateCronLastRun records when a cron entry last fired.
func (s *Store) UpdateCronLastRun(ctx context.Context, entryID id.CronID, at time.Time) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE dispatch_cron_entries SET last_run_at = ?, updated_at = ? WHERE id = ?",
		timeToStr(at), nowISO(), entryID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: update cron last run: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrCronNotFound
	}
	return nil
}

// UpdateCronEntry updates a cron entry.
func (s *Store) UpdateCronEntry(ctx context.Context, entry *cron.Entry) error {
	now := nowISO()
	res, err := s.db.ExecContext(ctx, `
		UPDATE dispatch_cron_entries SET
			name = ?, schedule = ?, job_name = ?, queue = ?, payload = ?,
			scope_app_id = ?, scope_org_id = ?,
			last_run_at = ?, next_run_at = ?, locked_by = ?, locked_until = ?,
			enabled = ?, updated_at = ?
		WHERE id = ?`,
		entry.Name, entry.Schedule, entry.JobName, entry.Queue, entry.Payload,
		nullStr(entry.ScopeAppID), nullStr(entry.ScopeOrgID),
		timeToNull(entry.LastRunAt), timeToNull(entry.NextRunAt),
		nullStr(entry.LockedBy), timeToNull(entry.LockedUntil),
		boolToInt(entry.Enabled), now,
		entry.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: update cron entry: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrCronNotFound
	}
	return nil
}

// DeleteCron removes a cron entry by ID.
func (s *Store) DeleteCron(ctx context.Context, entryID id.CronID) error {
	res, err := s.db.ExecContext(ctx,
		"DELETE FROM dispatch_cron_entries WHERE id = ?", entryID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: delete cron: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrCronNotFound
	}
	return nil
}

func scanCronEntry(row *sql.Row) (*cron.Entry, error) {
	var (
		eID, name, schedule, jobName, queue string
		payload                             []byte
		scopeApp, scopeOrg                  sql.NullString
		lastRunAt, nextRunAt                sql.NullString
		lockedBy, lockedUntil               sql.NullString
		enabled                             int
		createdAt, updatedAt                string
	)
	err := row.Scan(
		&eID, &name, &schedule, &jobName, &queue, &payload,
		&scopeApp, &scopeOrg,
		&lastRunAt, &nextRunAt, &lockedBy, &lockedUntil, &enabled,
		&createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}
	return buildCronEntry(eID, name, schedule, jobName, queue, payload,
		scopeApp, scopeOrg, lastRunAt, nextRunAt, lockedBy, lockedUntil,
		enabled, createdAt, updatedAt)
}

func scanCronEntries(rows *sql.Rows) ([]*cron.Entry, error) {
	var entries []*cron.Entry
	for rows.Next() {
		var (
			eID, name, schedule, jobName, queue string
			payload                             []byte
			scopeApp, scopeOrg                  sql.NullString
			lastRunAt, nextRunAt                sql.NullString
			lockedBy, lockedUntil               sql.NullString
			enabled                             int
			createdAt, updatedAt                string
		)
		if err := rows.Scan(
			&eID, &name, &schedule, &jobName, &queue, &payload,
			&scopeApp, &scopeOrg,
			&lastRunAt, &nextRunAt, &lockedBy, &lockedUntil, &enabled,
			&createdAt, &updatedAt,
		); err != nil {
			return nil, fmt.Errorf("dispatch/sqlite: scan cron: %w", err)
		}
		entry, err := buildCronEntry(eID, name, schedule, jobName, queue, payload,
			scopeApp, scopeOrg, lastRunAt, nextRunAt, lockedBy, lockedUntil,
			enabled, createdAt, updatedAt)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func buildCronEntry(
	eID, name, schedule, jobName, queue string, payload []byte,
	scopeApp, scopeOrg, lastRunAt, nextRunAt, lockedBy, lockedUntil sql.NullString,
	enabled int, createdAtStr, updatedAtStr string,
) (*cron.Entry, error) {
	cronID, err := id.ParseCronID(eID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse cron id: %w", err)
	}
	createdAt, err := parseTime(createdAtStr)
	if err != nil {
		return nil, err
	}
	updatedAt, err := parseTime(updatedAtStr)
	if err != nil {
		return nil, err
	}
	lr, err := scanTime(&lastRunAt)
	if err != nil {
		return nil, err
	}
	nr, err := scanTime(&nextRunAt)
	if err != nil {
		return nil, err
	}
	lu, err := scanTime(&lockedUntil)
	if err != nil {
		return nil, err
	}

	return &cron.Entry{
		Entity:      dispatch.Entity{CreatedAt: createdAt, UpdatedAt: updatedAt},
		ID:          cronID,
		Name:        name,
		Schedule:    schedule,
		JobName:     jobName,
		Queue:       queue,
		Payload:     payload,
		ScopeAppID:  scopeApp.String,
		ScopeOrgID:  scopeOrg.String,
		LastRunAt:   lr,
		NextRunAt:   nr,
		LockedBy:    lockedBy.String,
		LockedUntil: lu,
		Enabled:     enabled != 0,
	}, nil
}

// ---------------------------------------------------------------------------
// DLQ Store
// ---------------------------------------------------------------------------

// PushDLQ persists a dead-letter entry.
func (s *Store) PushDLQ(ctx context.Context, entry *dlq.Entry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dispatch_dlq (
			id, job_id, job_name, queue, payload, error, retry_count, max_retries,
			scope_app_id, scope_org_id, failed_at, replayed_at, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ID.String(), entry.JobID.String(), entry.JobName, entry.Queue,
		entry.Payload, entry.Error, entry.RetryCount, entry.MaxRetries,
		nullStr(entry.ScopeAppID), nullStr(entry.ScopeOrgID),
		timeToStr(entry.FailedAt), timeToNull(entry.ReplayedAt),
		timeToStr(entry.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: push dlq: %w", err)
	}
	return nil
}

// GetDLQEntry retrieves a DLQ entry by ID.
func (s *Store) GetDLQ(ctx context.Context, entryID id.DLQID) (*dlq.Entry, error) {
	var (
		dID, jobIDStr, jobName, queue, errStr string
		payload                               []byte
		retryCount, maxRetries                int
		scopeApp, scopeOrg                    sql.NullString
		failedAt, createdAt                   string
		replayedAt                            sql.NullString
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, job_name, queue, payload, error, retry_count, max_retries,
			scope_app_id, scope_org_id, failed_at, replayed_at, created_at
		FROM dispatch_dlq WHERE id = ? LIMIT 1`,
		entryID.String(),
	).Scan(&dID, &jobIDStr, &jobName, &queue, &payload, &errStr,
		&retryCount, &maxRetries, &scopeApp, &scopeOrg,
		&failedAt, &replayedAt, &createdAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get dlq entry: %w", err)
	}
	return buildDLQEntry(dID, jobIDStr, jobName, queue, payload, errStr,
		retryCount, maxRetries, scopeApp, scopeOrg, failedAt, replayedAt, createdAt)
}

// ListDLQ returns dead-letter entries matching the given options.
func (s *Store) ListDLQ(ctx context.Context, opts dlq.ListOpts) ([]*dlq.Entry, error) {
	q := "SELECT id, job_id, job_name, queue, payload, error, retry_count, max_retries, " +
		"scope_app_id, scope_org_id, failed_at, replayed_at, created_at " +
		"FROM dispatch_dlq WHERE 1=1"
	args := []any{}

	if opts.Queue != "" {
		q += " AND queue = ?"
		args = append(args, opts.Queue)
	}
	q += " ORDER BY failed_at DESC"
	if opts.Limit > 0 {
		q += " LIMIT " + strconv.Itoa(opts.Limit)
	}
	if opts.Offset > 0 {
		q += " OFFSET " + strconv.Itoa(opts.Offset)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list dlq: %w", err)
	}
	defer rows.Close()

	var entries []*dlq.Entry
	for rows.Next() {
		var (
			dID, jobIDStr, jobName, queue, errStr string
			payload                               []byte
			retryCount, maxRetries                int
			scopeApp, scopeOrg                    sql.NullString
			failedAt, createdAt                   string
			replayedAt                            sql.NullString
		)
		if err := rows.Scan(&dID, &jobIDStr, &jobName, &queue, &payload, &errStr,
			&retryCount, &maxRetries, &scopeApp, &scopeOrg,
			&failedAt, &replayedAt, &createdAt); err != nil {
			return nil, fmt.Errorf("dispatch/sqlite: scan dlq: %w", err)
		}
		entry, err := buildDLQEntry(dID, jobIDStr, jobName, queue, payload, errStr,
			retryCount, maxRetries, scopeApp, scopeOrg, failedAt, replayedAt, createdAt)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// ReplayDLQ marks a DLQ entry as replayed.
func (s *Store) ReplayDLQ(ctx context.Context, entryID id.DLQID) error {
	now := nowISO()
	res, err := s.db.ExecContext(ctx,
		"UPDATE dispatch_dlq SET replayed_at = ? WHERE id = ? AND replayed_at IS NULL",
		now, entryID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: replay dlq: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrDLQNotFound
	}
	return nil
}

// PurgeDLQ removes DLQ entries older than the given threshold.
func (s *Store) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		"DELETE FROM dispatch_dlq WHERE failed_at < ?", timeToStr(before),
	)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: purge dlq: %w", err)
	}
	n, _ := res.RowsAffected() //nolint:errcheck // sqlite driver returns nil
	return n, nil
}

// CountDLQ returns the total number of entries in the dead letter queue.
func (s *Store) CountDLQ(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dispatch_dlq").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: count dlq: %w", err)
	}
	return count, nil
}

func buildDLQEntry(
	dID, jobIDStr, jobName, queue string, payload []byte, errStr string,
	retryCount, maxRetries int, scopeApp, scopeOrg sql.NullString,
	failedAtStr string, replayedAt sql.NullString, createdAtStr string,
) (*dlq.Entry, error) {
	dlqID, err := id.ParseDLQID(dID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse dlq id: %w", err)
	}
	jobID, err := id.ParseJobID(jobIDStr)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse job id: %w", err)
	}
	fa, err := parseTime(failedAtStr)
	if err != nil {
		return nil, err
	}
	ca, err := parseTime(createdAtStr)
	if err != nil {
		return nil, err
	}
	ra, err := scanTime(&replayedAt)
	if err != nil {
		return nil, err
	}

	return &dlq.Entry{
		ID:         dlqID,
		JobID:      jobID,
		JobName:    jobName,
		Queue:      queue,
		Payload:    payload,
		Error:      errStr,
		RetryCount: retryCount,
		MaxRetries: maxRetries,
		ScopeAppID: scopeApp.String,
		ScopeOrgID: scopeOrg.String,
		FailedAt:   fa,
		ReplayedAt: ra,
		CreatedAt:  ca,
	}, nil
}

// ---------------------------------------------------------------------------
// Event Store
// ---------------------------------------------------------------------------

// PublishEvent persists a new event.
func (s *Store) PublishEvent(ctx context.Context, e *event.Event) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dispatch_events (id, name, payload, scope_app_id, scope_org_id, acked, created_at)
		VALUES (?, ?, ?, ?, ?, 0, ?)`,
		e.ID.String(), e.Name, e.Payload,
		nullStr(e.ScopeAppID), nullStr(e.ScopeOrgID),
		timeToStr(e.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: publish event: %w", err)
	}
	return nil
}

// SubscribeEvent waits for an unacknowledged event with the given name.
// Uses polling with a context-aware sleep.
func (s *Store) SubscribeEvent(ctx context.Context, name string, timeout time.Duration) (*event.Event, error) {
	deadline := time.Now().Add(timeout)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if time.Now().After(deadline) {
			return nil, nil
		}

		var (
			eID, eName         string
			payload            []byte
			scopeApp, scopeOrg sql.NullString
			acked              int
			createdAtStr       string
		)
		err := s.db.QueryRowContext(ctx, `
			SELECT id, name, payload, scope_app_id, scope_org_id, acked, created_at
			FROM dispatch_events
			WHERE name = ? AND acked = 0
			ORDER BY created_at ASC LIMIT 1`,
			name,
		).Scan(&eID, &eName, &payload, &scopeApp, &scopeOrg, &acked, &createdAtStr)
		if err == nil {
			evID, parseErr := id.ParseEventID(eID)
			if parseErr != nil {
				return nil, fmt.Errorf("dispatch/sqlite: parse event id: %w", parseErr)
			}
			ca, parseErr := parseTime(createdAtStr)
			if parseErr != nil {
				return nil, parseErr
			}
			return &event.Event{
				ID:         evID,
				Name:       eName,
				Payload:    payload,
				ScopeAppID: scopeApp.String,
				ScopeOrgID: scopeOrg.String,
				Acked:      acked != 0,
				CreatedAt:  ca,
			}, nil
		}
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("dispatch/sqlite: subscribe event: %w", err)
		}
		// Poll sleep.
		if err := sleepCtx(ctx, 50*time.Millisecond); err != nil {
			return nil, ctx.Err()
		}
	}
}

// AckEvent marks an event as acknowledged.
func (s *Store) AckEvent(ctx context.Context, eventID id.EventID) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE dispatch_events SET acked = 1 WHERE id = ?",
		eventID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: ack event: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrEventNotFound
	}
	return nil
}

// ---------------------------------------------------------------------------
// Cluster Store
// ---------------------------------------------------------------------------

// RegisterWorker adds a new worker to the cluster registry.
func (s *Store) RegisterWorker(ctx context.Context, w *cluster.Worker) error {
	lastSeen := timeToStr(w.LastSeen)
	createdAt := timeToStr(w.CreatedAt)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dispatch_workers (id, hostname, queues, concurrency, state, is_leader, leader_until, last_seen, metadata, created_at)
		VALUES (?, ?, ?, ?, ?, 0, NULL, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			hostname = excluded.hostname,
			queues = excluded.queues,
			concurrency = excluded.concurrency,
			state = excluded.state,
			last_seen = excluded.last_seen,
			metadata = excluded.metadata`,
		w.ID.String(), w.Hostname, stringsToJSON(w.Queues), w.Concurrency,
		string(w.State), lastSeen, mapToJSON(w.Metadata), createdAt,
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: register worker: %w", err)
	}
	return nil
}

// DeregisterWorker removes a worker from the cluster registry.
func (s *Store) DeregisterWorker(ctx context.Context, workerID id.WorkerID) error {
	res, err := s.db.ExecContext(ctx,
		"DELETE FROM dispatch_workers WHERE id = ?", workerID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: deregister worker: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// HeartbeatWorker updates the last-seen timestamp for a worker.
func (s *Store) HeartbeatWorker(ctx context.Context, workerID id.WorkerID) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE dispatch_workers SET last_seen = ? WHERE id = ?",
		nowISO(), workerID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: heartbeat worker: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// ListWorkers returns all registered workers.
func (s *Store) ListWorkers(ctx context.Context) ([]*cluster.Worker, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, hostname, queues, concurrency, state, is_leader, leader_until, last_seen, metadata, created_at
		FROM dispatch_workers ORDER BY created_at ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list workers: %w", err)
	}
	defer rows.Close()
	return scanWorkers(rows)
}

// ReapDeadWorkers returns workers whose last-seen timestamp is older than threshold.
func (s *Store) ReapDeadWorkers(ctx context.Context, threshold time.Duration) ([]*cluster.Worker, error) {
	cutoff := time.Now().UTC().Add(-threshold).Format(isoFormat)
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, hostname, queues, concurrency, state, is_leader, leader_until, last_seen, metadata, created_at
		FROM dispatch_workers WHERE last_seen < ?`,
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: reap dead workers: %w", err)
	}
	defer rows.Close()
	return scanWorkers(rows)
}

// AcquireLeadership attempts to become the cluster leader.
func (s *Store) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	now := nowISO()
	until := time.Now().UTC().Add(ttl).Format(isoFormat)

	// Step 1: Clear expired leader.
	_, err := s.db.ExecContext(ctx,
		"UPDATE dispatch_workers SET is_leader = 0, leader_until = NULL WHERE is_leader = 1 AND leader_until < ?",
		now,
	)
	if err != nil {
		return false, fmt.Errorf("dispatch/sqlite: clear expired leader: %w", err)
	}

	// Step 2: Check for active leader.
	var activeID string
	err = s.db.QueryRowContext(ctx,
		"SELECT id FROM dispatch_workers WHERE is_leader = 1 AND leader_until >= ? LIMIT 1",
		now,
	).Scan(&activeID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("dispatch/sqlite: check leader: %w", err)
	}
	if err == nil && activeID != wID {
		return false, nil
	}

	// Step 3: Claim leadership.
	res, err := s.db.ExecContext(ctx,
		"UPDATE dispatch_workers SET is_leader = 1, leader_until = ? WHERE id = ?",
		until, wID,
	)
	if err != nil {
		return false, fmt.Errorf("dispatch/sqlite: claim leadership: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return false, nil
	}
	return true, nil
}

// RenewLeadership extends the leader's hold.
func (s *Store) RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	until := time.Now().UTC().Add(ttl).Format(isoFormat)
	res, err := s.db.ExecContext(ctx,
		"UPDATE dispatch_workers SET leader_until = ? WHERE id = ? AND is_leader = 1",
		until, workerID.String(),
	)
	if err != nil {
		return false, fmt.Errorf("dispatch/sqlite: renew leadership: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 { //nolint:errcheck // sqlite driver returns nil
		return false, nil
	}
	return true, nil
}

// GetLeader returns the current cluster leader, or nil if none.
func (s *Store) GetLeader(ctx context.Context) (*cluster.Worker, error) {
	now := nowISO()
	var (
		wID, hostname, queuesJSON, stateStr, metadataJSON string
		concurrency, isLeader                             int
		leaderUntil, lastSeen                             sql.NullString
		createdAt                                         string
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT id, hostname, queues, concurrency, state, is_leader, leader_until, last_seen, metadata, created_at
		FROM dispatch_workers WHERE is_leader = 1 AND leader_until >= ? LIMIT 1`,
		now,
	).Scan(&wID, &hostname, &queuesJSON, &concurrency, &stateStr,
		&isLeader, &leaderUntil, &lastSeen, &metadataJSON, &createdAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("dispatch/sqlite: get leader: %w", err)
	}
	return buildWorker(wID, hostname, queuesJSON, concurrency, stateStr,
		isLeader, leaderUntil, lastSeen, metadataJSON, createdAt)
}

func scanWorkers(rows *sql.Rows) ([]*cluster.Worker, error) {
	var workers []*cluster.Worker
	for rows.Next() {
		var (
			wID, hostname, queuesJSON, stateStr, metadataJSON string
			concurrency, isLeader                             int
			leaderUntil, lastSeen                             sql.NullString
			createdAt                                         string
		)
		if err := rows.Scan(&wID, &hostname, &queuesJSON, &concurrency, &stateStr,
			&isLeader, &leaderUntil, &lastSeen, &metadataJSON, &createdAt); err != nil {
			return nil, fmt.Errorf("dispatch/sqlite: scan worker: %w", err)
		}
		w, err := buildWorker(wID, hostname, queuesJSON, concurrency, stateStr,
			isLeader, leaderUntil, lastSeen, metadataJSON, createdAt)
		if err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

func buildWorker(
	wID, hostname, queuesJSON string, concurrency int, stateStr string,
	isLeader int, leaderUntil, lastSeen sql.NullString,
	metadataJSON, createdAtStr string,
) (*cluster.Worker, error) {
	workerID, err := id.ParseWorkerID(wID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse worker id: %w", err)
	}
	ca, err := parseTime(createdAtStr)
	if err != nil {
		return nil, err
	}
	ls, err := scanTime(&lastSeen)
	if err != nil {
		return nil, err
	}
	lu, err := scanTime(&leaderUntil)
	if err != nil {
		return nil, err
	}

	var lastSeenVal time.Time
	if ls != nil {
		lastSeenVal = *ls
	}

	return &cluster.Worker{
		ID:          workerID,
		Hostname:    hostname,
		Queues:      jsonToStrings(queuesJSON),
		Concurrency: concurrency,
		State:       cluster.WorkerState(stateStr),
		IsLeader:    isLeader != 0,
		LeaderUntil: lu,
		LastSeen:    lastSeenVal,
		Metadata:    jsonToMap(metadataJSON),
		CreatedAt:   ca,
	}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func nullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	// modernc.org/sqlite returns errors containing "UNIQUE constraint failed".
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
