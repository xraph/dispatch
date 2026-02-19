package bunstore

import (
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// ── Job model ─────────────────────────────────────────────────────

type jobModel struct {
	bun.BaseModel `bun:"table:dispatch_jobs"`

	ID          string     `bun:"id,pk"`
	Name        string     `bun:"name,notnull"`
	Queue       string     `bun:"queue,notnull,default:'default'"`
	Payload     []byte     `bun:"payload,notnull,type:bytea"`
	State       string     `bun:"state,notnull,default:'pending'"`
	Priority    int        `bun:"priority,notnull,default:0"`
	MaxRetries  int        `bun:"max_retries,notnull,default:3"`
	RetryCount  int        `bun:"retry_count,notnull,default:0"`
	LastError   string     `bun:"last_error"`
	ScopeAppID  string     `bun:"scope_app_id"`
	ScopeOrgID  string     `bun:"scope_org_id"`
	WorkerID    string     `bun:"worker_id"`
	RunAt       time.Time  `bun:"run_at,notnull,default:current_timestamp"`
	StartedAt   *time.Time `bun:"started_at"`
	CompletedAt *time.Time `bun:"completed_at"`
	HeartbeatAt *time.Time `bun:"heartbeat_at"`
	Timeout     int64      `bun:"timeout,notnull,default:0"`
	CreatedAt   time.Time  `bun:"created_at,notnull,default:current_timestamp"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull,default:current_timestamp"`
}

func toJobModel(j *job.Job) *jobModel {
	return &jobModel{
		ID:          j.ID.String(),
		Name:        j.Name,
		Queue:       j.Queue,
		Payload:     j.Payload,
		State:       string(j.State),
		Priority:    j.Priority,
		MaxRetries:  j.MaxRetries,
		RetryCount:  j.RetryCount,
		LastError:   j.LastError,
		ScopeAppID:  j.ScopeAppID,
		ScopeOrgID:  j.ScopeOrgID,
		WorkerID:    j.WorkerID.String(),
		RunAt:       j.RunAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		HeartbeatAt: j.HeartbeatAt,
		Timeout:     j.Timeout.Nanoseconds(),
		CreatedAt:   j.CreatedAt,
		UpdatedAt:   j.UpdatedAt,
	}
}

func fromJobModel(m *jobModel) (*job.Job, error) {
	parsedID, err := id.ParseJobID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: parse job id %q: %w", m.ID, err)
	}

	j := &job.Job{
		Entity: dispatch.Entity{
			CreatedAt: m.CreatedAt,
			UpdatedAt: m.UpdatedAt,
		},
		ID:          parsedID,
		Name:        m.Name,
		Queue:       m.Queue,
		Payload:     m.Payload,
		State:       job.State(m.State),
		Priority:    m.Priority,
		MaxRetries:  m.MaxRetries,
		RetryCount:  m.RetryCount,
		LastError:   m.LastError,
		ScopeAppID:  m.ScopeAppID,
		ScopeOrgID:  m.ScopeOrgID,
		RunAt:       m.RunAt,
		StartedAt:   m.StartedAt,
		CompletedAt: m.CompletedAt,
		HeartbeatAt: m.HeartbeatAt,
		Timeout:     time.Duration(m.Timeout),
	}

	if m.WorkerID != "" {
		parsedWorker, wErr := id.ParseWorkerID(m.WorkerID)
		if wErr == nil {
			j.WorkerID = parsedWorker
		}
	}

	return j, nil
}

// ── Workflow run model ────────────────────────────────────────────

type workflowRunModel struct {
	bun.BaseModel `bun:"table:dispatch_workflow_runs"`

	ID          string     `bun:"id,pk"`
	Name        string     `bun:"name,notnull"`
	State       string     `bun:"state,notnull,default:'running'"`
	Input       []byte     `bun:"input,type:bytea"`
	Output      []byte     `bun:"output,type:bytea"`
	Error       string     `bun:"error"`
	ScopeAppID  string     `bun:"scope_app_id"`
	ScopeOrgID  string     `bun:"scope_org_id"`
	StartedAt   time.Time  `bun:"started_at,notnull,default:current_timestamp"`
	CompletedAt *time.Time `bun:"completed_at"`
	CreatedAt   time.Time  `bun:"created_at,notnull,default:current_timestamp"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull,default:current_timestamp"`
}

func toRunModel(r *workflow.Run) *workflowRunModel {
	return &workflowRunModel{
		ID:          r.ID.String(),
		Name:        r.Name,
		State:       string(r.State),
		Input:       r.Input,
		Output:      r.Output,
		Error:       r.Error,
		ScopeAppID:  r.ScopeAppID,
		ScopeOrgID:  r.ScopeOrgID,
		StartedAt:   r.StartedAt,
		CompletedAt: r.CompletedAt,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
	}
}

func fromRunModel(m *workflowRunModel) (*workflow.Run, error) {
	parsedID, err := id.ParseRunID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: parse run id %q: %w", m.ID, err)
	}

	return &workflow.Run{
		Entity: dispatch.Entity{
			CreatedAt: m.CreatedAt,
			UpdatedAt: m.UpdatedAt,
		},
		ID:          parsedID,
		Name:        m.Name,
		State:       workflow.RunState(m.State),
		Input:       m.Input,
		Output:      m.Output,
		Error:       m.Error,
		ScopeAppID:  m.ScopeAppID,
		ScopeOrgID:  m.ScopeOrgID,
		StartedAt:   m.StartedAt,
		CompletedAt: m.CompletedAt,
	}, nil
}

// ── Checkpoint model ──────────────────────────────────────────────

type checkpointModel struct {
	bun.BaseModel `bun:"table:dispatch_checkpoints"`

	ID        string    `bun:"id,pk"`
	RunID     string    `bun:"run_id,notnull"`
	StepName  string    `bun:"step_name,notnull"`
	Data      []byte    `bun:"data,notnull,type:bytea"`
	CreatedAt time.Time `bun:"created_at,notnull,default:current_timestamp"`
}

func fromCheckpointModel(m *checkpointModel) (*workflow.Checkpoint, error) {
	parsedID, err := id.ParseCheckpointID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: parse checkpoint id %q: %w", m.ID, err)
	}

	parsedRunID, err := id.ParseRunID(m.RunID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: parse run id %q: %w", m.RunID, err)
	}

	return &workflow.Checkpoint{
		ID:        parsedID,
		RunID:     parsedRunID,
		StepName:  m.StepName,
		Data:      m.Data,
		CreatedAt: m.CreatedAt,
	}, nil
}

// ── Cron entry model ──────────────────────────────────────────────

type cronEntryModel struct {
	bun.BaseModel `bun:"table:dispatch_cron_entries"`

	ID          string     `bun:"id,pk"`
	Name        string     `bun:"name,notnull,unique"`
	Schedule    string     `bun:"schedule,notnull"`
	JobName     string     `bun:"job_name,notnull"`
	Queue       string     `bun:"queue,notnull,default:''"`
	Payload     []byte     `bun:"payload,type:bytea"`
	ScopeAppID  string     `bun:"scope_app_id"`
	ScopeOrgID  string     `bun:"scope_org_id"`
	LastRunAt   *time.Time `bun:"last_run_at"`
	NextRunAt   *time.Time `bun:"next_run_at"`
	LockedBy    *string    `bun:"locked_by"`
	LockedUntil *time.Time `bun:"locked_until"`
	Enabled     bool       `bun:"enabled,notnull,default:true"`
	CreatedAt   time.Time  `bun:"created_at,notnull,default:current_timestamp"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull,default:current_timestamp"`
}

func toCronModel(e *cron.Entry) *cronEntryModel {
	m := &cronEntryModel{
		ID:          e.ID.String(),
		Name:        e.Name,
		Schedule:    e.Schedule,
		JobName:     e.JobName,
		Queue:       e.Queue,
		Payload:     e.Payload,
		ScopeAppID:  e.ScopeAppID,
		ScopeOrgID:  e.ScopeOrgID,
		LastRunAt:   e.LastRunAt,
		NextRunAt:   e.NextRunAt,
		LockedUntil: e.LockedUntil,
		Enabled:     e.Enabled,
		CreatedAt:   e.CreatedAt,
		UpdatedAt:   e.UpdatedAt,
	}
	if e.LockedBy != "" {
		m.LockedBy = &e.LockedBy
	}
	return m
}

func fromCronModel(m *cronEntryModel) (*cron.Entry, error) {
	parsedID, err := id.ParseCronID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: parse cron id %q: %w", m.ID, err)
	}

	e := &cron.Entry{
		Entity: dispatch.Entity{
			CreatedAt: m.CreatedAt,
			UpdatedAt: m.UpdatedAt,
		},
		ID:          parsedID,
		Name:        m.Name,
		Schedule:    m.Schedule,
		JobName:     m.JobName,
		Queue:       m.Queue,
		Payload:     m.Payload,
		ScopeAppID:  m.ScopeAppID,
		ScopeOrgID:  m.ScopeOrgID,
		LastRunAt:   m.LastRunAt,
		NextRunAt:   m.NextRunAt,
		LockedUntil: m.LockedUntil,
		Enabled:     m.Enabled,
	}
	if m.LockedBy != nil {
		e.LockedBy = *m.LockedBy
	}
	return e, nil
}

// ── DLQ entry model ───────────────────────────────────────────────

type dlqEntryModel struct {
	bun.BaseModel `bun:"table:dispatch_dlq"`

	ID         string     `bun:"id,pk"`
	JobID      string     `bun:"job_id,notnull"`
	JobName    string     `bun:"job_name,notnull"`
	Queue      string     `bun:"queue,notnull"`
	Payload    []byte     `bun:"payload,notnull,type:bytea"`
	Error      string     `bun:"error,notnull"`
	RetryCount int        `bun:"retry_count,notnull"`
	MaxRetries int        `bun:"max_retries,notnull,default:3"`
	ScopeAppID string     `bun:"scope_app_id"`
	ScopeOrgID string     `bun:"scope_org_id"`
	FailedAt   time.Time  `bun:"failed_at,notnull,default:current_timestamp"`
	ReplayedAt *time.Time `bun:"replayed_at"`
	CreatedAt  time.Time  `bun:"created_at,notnull,default:current_timestamp"`
}

func toDLQModel(e *dlq.Entry) *dlqEntryModel {
	return &dlqEntryModel{
		ID:         e.ID.String(),
		JobID:      e.JobID.String(),
		JobName:    e.JobName,
		Queue:      e.Queue,
		Payload:    e.Payload,
		Error:      e.Error,
		RetryCount: e.RetryCount,
		MaxRetries: e.MaxRetries,
		ScopeAppID: e.ScopeAppID,
		ScopeOrgID: e.ScopeOrgID,
		FailedAt:   e.FailedAt,
		ReplayedAt: e.ReplayedAt,
		CreatedAt:  e.CreatedAt,
	}
}

func fromDLQModel(m *dlqEntryModel) (*dlq.Entry, error) {
	parsedID, err := id.ParseDLQID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: parse dlq id %q: %w", m.ID, err)
	}

	parsedJobID, err := id.ParseJobID(m.JobID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: parse job id %q: %w", m.JobID, err)
	}

	return &dlq.Entry{
		ID:         parsedID,
		JobID:      parsedJobID,
		JobName:    m.JobName,
		Queue:      m.Queue,
		Payload:    m.Payload,
		Error:      m.Error,
		RetryCount: m.RetryCount,
		MaxRetries: m.MaxRetries,
		ScopeAppID: m.ScopeAppID,
		ScopeOrgID: m.ScopeOrgID,
		FailedAt:   m.FailedAt,
		ReplayedAt: m.ReplayedAt,
		CreatedAt:  m.CreatedAt,
	}, nil
}

// ── Event model ───────────────────────────────────────────────────

type eventModel struct {
	bun.BaseModel `bun:"table:dispatch_events"`

	ID         string    `bun:"id,pk"`
	Name       string    `bun:"name,notnull"`
	Payload    []byte    `bun:"payload,type:bytea"`
	ScopeAppID string    `bun:"scope_app_id"`
	ScopeOrgID string    `bun:"scope_org_id"`
	Acked      bool      `bun:"acked,notnull,default:false"`
	CreatedAt  time.Time `bun:"created_at,notnull,default:current_timestamp"`
}

func toEventModel(evt *event.Event) *eventModel {
	return &eventModel{
		ID:         evt.ID.String(),
		Name:       evt.Name,
		Payload:    evt.Payload,
		ScopeAppID: evt.ScopeAppID,
		ScopeOrgID: evt.ScopeOrgID,
		Acked:      evt.Acked,
		CreatedAt:  evt.CreatedAt,
	}
}

func fromEventModel(m *eventModel) (*event.Event, error) {
	parsedID, err := id.ParseEventID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: parse event id %q: %w", m.ID, err)
	}

	return &event.Event{
		ID:         parsedID,
		Name:       m.Name,
		Payload:    m.Payload,
		ScopeAppID: m.ScopeAppID,
		ScopeOrgID: m.ScopeOrgID,
		Acked:      m.Acked,
		CreatedAt:  m.CreatedAt,
	}, nil
}

// ── Worker model ──────────────────────────────────────────────────

type workerModel struct {
	bun.BaseModel `bun:"table:dispatch_workers"`

	ID          string            `bun:"id,pk"`
	Hostname    string            `bun:"hostname,notnull"`
	Queues      []string          `bun:"queues,array"`
	Concurrency int               `bun:"concurrency,notnull,default:10"`
	State       string            `bun:"state,notnull,default:'active'"`
	IsLeader    bool              `bun:"is_leader,notnull,default:false"`
	LeaderUntil *time.Time        `bun:"leader_until"`
	LastSeen    time.Time         `bun:"last_seen,notnull,default:current_timestamp"`
	Metadata    map[string]string `bun:"metadata,type:jsonb"`
	CreatedAt   time.Time         `bun:"created_at,notnull,default:current_timestamp"`
}

func toWorkerModel(w *cluster.Worker) *workerModel {
	return &workerModel{
		ID:          w.ID.String(),
		Hostname:    w.Hostname,
		Queues:      w.Queues,
		Concurrency: w.Concurrency,
		State:       string(w.State),
		IsLeader:    w.IsLeader,
		LeaderUntil: w.LeaderUntil,
		LastSeen:    w.LastSeen,
		Metadata:    w.Metadata,
		CreatedAt:   w.CreatedAt,
	}
}

func fromWorkerModel(m *workerModel) (*cluster.Worker, error) {
	parsedID, err := id.ParseWorkerID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: parse worker id %q: %w", m.ID, err)
	}

	return &cluster.Worker{
		ID:          parsedID,
		Hostname:    m.Hostname,
		Queues:      m.Queues,
		Concurrency: m.Concurrency,
		State:       cluster.WorkerState(m.State),
		IsLeader:    m.IsLeader,
		LeaderUntil: m.LeaderUntil,
		LastSeen:    m.LastSeen,
		Metadata:    m.Metadata,
		CreatedAt:   m.CreatedAt,
	}, nil
}
