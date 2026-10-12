package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// jobUsageModel is one attempt's measurements.
//
// Durations are integer nanoseconds so the numbers survive a round trip
// unchanged: BSON has no duration type, and storing them as a document
// would make the aggregation an estimator wants harder for nothing.
type jobUsageModel struct {
	grove.BaseModel `grove:"table:dispatch_job_usage"`

	ID          string    `grove:"id,pk"        bson:"_id"`
	JobID       string    `bson:"job_id"`
	Name        string    `bson:"name"`
	Queue       string    `bson:"queue"`
	Attempt     int       `bson:"attempt"`
	Status      string    `bson:"status"`
	InputBytes  int64     `bson:"input_bytes"`
	Resources   []byte    `bson:"resources,omitempty"`
	WallTimeNS  int64     `bson:"wall_time_ns"`
	CPUTimeNS   int64     `bson:"cpu_time_ns"`
	PeakRSS     int64     `bson:"peak_rss"`
	DiskWritten int64     `bson:"disk_written"`
	Executor    string    `bson:"executor,omitempty"`
	ScopeAppID  string    `bson:"scope_app_id,omitempty"`
	ScopeOrgID  string    `bson:"scope_org_id,omitempty"`
	WorkerID    string    `bson:"worker_id,omitempty"`
	RecordedAt  time.Time `bson:"recorded_at"`
}

func toUsageModel(u *job.Usage) (*jobUsageModel, error) {
	res, err := resource.EncodeSet(u.Resources)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: encode usage resources: %w", err)
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
		return nil, fmt.Errorf("dispatch/mongo: decode usage resources: %w", err)
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

	if _, err := s.mdb.Collection(colJobUsage).InsertOne(ctx, m); err != nil {
		return fmt.Errorf("dispatch/mongo: record job usage: %w", err)
	}

	return nil
}

// ListJobUsage returns recorded attempts, most recent first.
func (s *Store) ListJobUsage(ctx context.Context, opts job.UsageListOpts) ([]*job.Usage, error) {
	filter := bson.M{}

	if opts.Name != "" {
		filter["name"] = opts.Name
	}

	if !opts.Since.IsZero() {
		filter["recorded_at"] = bson.M{"$gte": opts.Since}
	}

	find := options.Find().
		SetSort(bson.D{{Key: "recorded_at", Value: -1}, {Key: "_id", Value: -1}})

	if opts.Limit > 0 {
		find = find.SetLimit(int64(opts.Limit))
	}

	if opts.Offset > 0 {
		find = find.SetSkip(int64(opts.Offset))
	}

	cur, err := s.mdb.Collection(colJobUsage).Find(ctx, filter, find)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list job usage: %w", err)
	}

	var models []jobUsageModel
	if err := cur.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: decode job usage: %w", err)
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
// DeleteMany takes no limit, so a bounded purge selects its victims
// first. An unbounded one deletes in a single statement, which is what
// the caller asked for by passing zero.
func (s *Store) PurgeJobUsage(ctx context.Context, before time.Time, limit int) (int64, error) {
	col := s.mdb.Collection(colJobUsage)
	filter := bson.M{"recorded_at": bson.M{"$lt": before}}

	if limit <= 0 {
		res, err := col.DeleteMany(ctx, filter)
		if err != nil {
			return 0, fmt.Errorf("dispatch/mongo: purge job usage: %w", err)
		}

		return res.DeletedCount, nil
	}

	find := options.Find().
		SetSort(bson.D{{Key: "recorded_at", Value: 1}}).
		SetLimit(int64(limit)).
		SetProjection(bson.M{"_id": 1})

	cur, err := col.Find(ctx, filter, find)
	if err != nil {
		return 0, fmt.Errorf("dispatch/mongo: purge job usage: %w", err)
	}

	var victims []struct {
		ID string `bson:"_id"`
	}

	if aerr := cur.All(ctx, &victims); aerr != nil {
		return 0, fmt.Errorf("dispatch/mongo: purge job usage: %w", aerr)
	}

	if len(victims) == 0 {
		return 0, nil
	}

	ids := make([]string, 0, len(victims))
	for _, v := range victims {
		ids = append(ids, v.ID)
	}

	res, err := col.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return 0, fmt.Errorf("dispatch/mongo: purge job usage: %w", err)
	}

	return res.DeletedCount, nil
}
