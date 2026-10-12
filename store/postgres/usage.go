package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// jobUsageModel is one attempt's measurements.
//
// The predicted resource set is stored as JSON rather than scalar columns
// because, unlike the job row, nothing queries it: this table is read in
// bulk by whatever computes estimates, and a blob keeps custom resource
// keys intact without a column per key.
type jobUsageModel struct {
	grove.BaseModel `grove:"table:dispatch_job_usage"`

	ID          string    `grove:"id,pk"`
	JobID       string    `grove:"job_id,notnull"`
	Name        string    `grove:"name,notnull"`
	Queue       string    `grove:"queue,notnull"`
	Attempt     int       `grove:"attempt,notnull,default:0"`
	Status      string    `grove:"status,notnull"`
	InputBytes  int64     `grove:"input_bytes,notnull,default:0"`
	Resources   []byte    `grove:"resources,type:jsonb"`
	WallTimeNS  int64     `grove:"wall_time_ns,notnull,default:0"`
	CPUTimeNS   int64     `grove:"cpu_time_ns,notnull,default:0"`
	PeakRSS     int64     `grove:"peak_rss,notnull,default:0"`
	DiskWritten int64     `grove:"disk_written,notnull,default:0"`
	Executor    string    `grove:"executor"`
	ScopeAppID  string    `grove:"scope_app_id"`
	ScopeOrgID  string    `grove:"scope_org_id"`
	WorkerID    string    `grove:"worker_id"`
	RecordedAt  time.Time `grove:"recorded_at,notnull,default:current_timestamp"`
}

func toUsageModel(u *job.Usage) (*jobUsageModel, error) {
	res, err := resource.EncodeSet(u.Resources)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: encode usage resources: %w", err)
	}

	return &jobUsageModel{
		ID:          u.ID.String(),
		JobID:       u.JobID.String(),
		Name:        u.Name,
		Queue:       u.Queue,
		Attempt:     u.Attempt,
		Status:      string(u.Status),
		InputBytes:  u.InputBytes,
		Resources:   res,
		WallTimeNS:  int64(u.WallTime),
		CPUTimeNS:   int64(u.CPUTime),
		PeakRSS:     u.PeakRSS,
		DiskWritten: u.DiskWritten,
		Executor:    u.Executor,
		ScopeAppID:  u.ScopeAppID,
		ScopeOrgID:  u.ScopeOrgID,
		WorkerID:    u.WorkerID.String(),
		RecordedAt:  u.RecordedAt,
	}, nil
}

func fromUsageModel(m *jobUsageModel) (*job.Usage, error) {
	uid, err := id.ParseUsageID(m.ID)
	if err != nil {
		return nil, err
	}

	jid, err := id.ParseJobID(m.JobID)
	if err != nil {
		return nil, err
	}

	res, err := resource.DecodeSet(m.Resources)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: decode usage resources: %w", err)
	}

	u := &job.Usage{
		ID:          uid,
		JobID:       jid,
		Name:        m.Name,
		Queue:       m.Queue,
		Attempt:     m.Attempt,
		Status:      job.State(m.Status),
		InputBytes:  m.InputBytes,
		Resources:   res,
		WallTime:    time.Duration(m.WallTimeNS),
		CPUTime:     time.Duration(m.CPUTimeNS),
		PeakRSS:     m.PeakRSS,
		DiskWritten: m.DiskWritten,
		Executor:    m.Executor,
		ScopeAppID:  m.ScopeAppID,
		ScopeOrgID:  m.ScopeOrgID,
		RecordedAt:  m.RecordedAt,
	}

	// A worker id is absent on any attempt recorded outside a pool.
	if m.WorkerID != "" {
		wid, werr := id.ParseWorkerID(m.WorkerID)
		if werr != nil {
			return nil, werr
		}

		u.WorkerID = wid
	}

	return u, nil
}

// RecordJobUsage persists one attempt's measurements.
func (s *Store) RecordJobUsage(ctx context.Context, u *job.Usage) error {
	m, err := toUsageModel(u)
	if err != nil {
		return err
	}

	if _, err := s.pgdb.NewInsert(m).Exec(ctx); err != nil {
		return fmt.Errorf("dispatch/postgres: record job usage: %w", err)
	}

	return nil
}

// ListJobUsage returns recorded attempts, most recent first.
func (s *Store) ListJobUsage(ctx context.Context, opts job.UsageListOpts) ([]*job.Usage, error) {
	var models []jobUsageModel

	q := s.pgdb.NewSelect(&models)

	if opts.Name != "" {
		q = q.Where("name = ?", opts.Name)
	}

	if !opts.Since.IsZero() {
		q = q.Where("recorded_at >= ?", opts.Since)
	}

	q = q.OrderExpr("recorded_at DESC, id DESC")

	if opts.Limit > 0 {
		q = q.Limit(opts.Limit)
	}

	if opts.Offset > 0 {
		q = q.Offset(opts.Offset)
	}

	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("dispatch/postgres: list job usage: %w", err)
	}

	out := make([]*job.Usage, 0, len(models))

	for i := range models {
		u, cerr := fromUsageModel(&models[i])
		if cerr != nil {
			return nil, cerr
		}

		out = append(out, u)
	}

	return out, nil
}

// PurgeJobUsage deletes records older than before, up to limit rows.
//
// Postgres will not take a LIMIT on DELETE, so the rows are selected
// first. That is also what makes the bound meaningful: a retention sweep
// over a table this size has to be able to work in batches rather than
// locking the whole thing in one statement.
func (s *Store) PurgeJobUsage(ctx context.Context, before time.Time, limit int) (int64, error) {
	query := `
		DELETE FROM dispatch_job_usage
		WHERE id IN (
		  SELECT id FROM dispatch_job_usage
		  WHERE recorded_at < $1
		  ORDER BY recorded_at ASC`

	args := []any{before}

	if limit > 0 {
		query += `
		  LIMIT $2`

		args = append(args, limit)
	}

	query += `
		)`

	res, err := s.pgdb.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("dispatch/postgres: purge job usage: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return 0, nil //nolint:nilerr // the rows are gone; the count is advisory
	}

	return n, nil
}
