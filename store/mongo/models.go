package mongo

import (
	"fmt"
	"time"

	"github.com/xraph/grove"

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
	grove.BaseModel `grove:"table:dispatch_jobs"`

	ID          string     `grove:"id,pk"          bson:"_id"`
	Name        string     `grove:"name,notnull"   bson:"name"`
	Queue       string     `grove:"queue,notnull"  bson:"queue"`
	Payload     []byte     `grove:"payload,notnull" bson:"payload"`
	State       string     `grove:"state,notnull"  bson:"state"`
	Priority    int        `grove:"priority,notnull" bson:"priority"`
	MaxRetries  int        `grove:"max_retries,notnull" bson:"max_retries"`
	RetryCount  int        `grove:"retry_count,notnull" bson:"retry_count"`
	LastError   string     `grove:"last_error"     bson:"last_error"`
	ScopeAppID  string     `grove:"scope_app_id"   bson:"scope_app_id"`
	ScopeOrgID  string     `grove:"scope_org_id"   bson:"scope_org_id"`
	WorkerID    string     `grove:"worker_id"      bson:"worker_id"`
	RunAt       time.Time  `grove:"run_at,notnull" bson:"run_at"`
	StartedAt   *time.Time `grove:"started_at"     bson:"started_at,omitempty"`
	CompletedAt *time.Time `grove:"completed_at"   bson:"completed_at,omitempty"`
	HeartbeatAt *time.Time `grove:"heartbeat_at"   bson:"heartbeat_at,omitempty"`
	Timeout     int64      `grove:"timeout,notnull" bson:"timeout"`
	CreatedAt   time.Time  `grove:"created_at,notnull" bson:"created_at"`
	UpdatedAt   time.Time  `grove:"updated_at,notnull" bson:"updated_at"`
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
		return nil, fmt.Errorf("dispatch/mongo: parse job id %q: %w", m.ID, err)
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
	grove.BaseModel `grove:"table:dispatch_workflow_runs"`

	ID          string     `grove:"id,pk"          bson:"_id"`
	Name        string     `grove:"name,notnull"   bson:"name"`
	State       string     `grove:"state,notnull"  bson:"state"`
	Input       []byte     `grove:"input"          bson:"input,omitempty"`
	Output      []byte     `grove:"output"         bson:"output,omitempty"`
	Error       string     `grove:"error"          bson:"error"`
	ScopeAppID  string     `grove:"scope_app_id"   bson:"scope_app_id"`
	ScopeOrgID  string     `grove:"scope_org_id"   bson:"scope_org_id"`
	StartedAt   time.Time  `grove:"started_at,notnull" bson:"started_at"`
	CompletedAt *time.Time `grove:"completed_at"   bson:"completed_at,omitempty"`
	CreatedAt   time.Time  `grove:"created_at,notnull" bson:"created_at"`
	UpdatedAt   time.Time  `grove:"updated_at,notnull" bson:"updated_at"`
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
		return nil, fmt.Errorf("dispatch/mongo: parse run id %q: %w", m.ID, err)
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
	grove.BaseModel `grove:"table:dispatch_checkpoints"`

	ID        string    `grove:"id,pk"          bson:"_id"`
	RunID     string    `grove:"run_id,notnull" bson:"run_id"`
	StepName  string    `grove:"step_name,notnull" bson:"step_name"`
	Data      []byte    `grove:"data,notnull"   bson:"data"`
	CreatedAt time.Time `grove:"created_at,notnull" bson:"created_at"`
}

func fromCheckpointModel(m *checkpointModel) (*workflow.Checkpoint, error) {
	parsedID, err := id.ParseCheckpointID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: parse checkpoint id %q: %w", m.ID, err)
	}

	parsedRunID, err := id.ParseRunID(m.RunID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: parse run id %q: %w", m.RunID, err)
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
	grove.BaseModel `grove:"table:dispatch_cron_entries"`

	ID          string     `grove:"id,pk"          bson:"_id"`
	Name        string     `grove:"name,notnull,unique" bson:"name"`
	Schedule    string     `grove:"schedule,notnull" bson:"schedule"`
	JobName     string     `grove:"job_name,notnull" bson:"job_name"`
	Queue       string     `grove:"queue,notnull"  bson:"queue"`
	Payload     []byte     `grove:"payload"        bson:"payload,omitempty"`
	ScopeAppID  string     `grove:"scope_app_id"   bson:"scope_app_id"`
	ScopeOrgID  string     `grove:"scope_org_id"   bson:"scope_org_id"`
	LastRunAt   *time.Time `grove:"last_run_at"    bson:"last_run_at,omitempty"`
	NextRunAt   *time.Time `grove:"next_run_at"    bson:"next_run_at,omitempty"`
	LockedBy    *string    `grove:"locked_by"      bson:"locked_by,omitempty"`
	LockedUntil *time.Time `grove:"locked_until"   bson:"locked_until,omitempty"`
	Enabled     bool       `grove:"enabled,notnull" bson:"enabled"`
	CreatedAt   time.Time  `grove:"created_at,notnull" bson:"created_at"`
	UpdatedAt   time.Time  `grove:"updated_at,notnull" bson:"updated_at"`
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
		return nil, fmt.Errorf("dispatch/mongo: parse cron id %q: %w", m.ID, err)
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
	grove.BaseModel `grove:"table:dispatch_dlq"`

	ID         string     `grove:"id,pk"          bson:"_id"`
	JobID      string     `grove:"job_id,notnull" bson:"job_id"`
	JobName    string     `grove:"job_name,notnull" bson:"job_name"`
	Queue      string     `grove:"queue,notnull"  bson:"queue"`
	Payload    []byte     `grove:"payload,notnull" bson:"payload"`
	Error      string     `grove:"error,notnull"  bson:"error"`
	RetryCount int        `grove:"retry_count,notnull" bson:"retry_count"`
	MaxRetries int        `grove:"max_retries,notnull" bson:"max_retries"`
	ScopeAppID string     `grove:"scope_app_id"   bson:"scope_app_id"`
	ScopeOrgID string     `grove:"scope_org_id"   bson:"scope_org_id"`
	FailedAt   time.Time  `grove:"failed_at,notnull" bson:"failed_at"`
	ReplayedAt *time.Time `grove:"replayed_at"    bson:"replayed_at,omitempty"`
	CreatedAt  time.Time  `grove:"created_at,notnull" bson:"created_at"`
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
		return nil, fmt.Errorf("dispatch/mongo: parse dlq id %q: %w", m.ID, err)
	}

	parsedJobID, err := id.ParseJobID(m.JobID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: parse job id %q: %w", m.JobID, err)
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
	grove.BaseModel `grove:"table:dispatch_events"`

	ID         string    `grove:"id,pk"          bson:"_id"`
	Name       string    `grove:"name,notnull"   bson:"name"`
	Payload    []byte    `grove:"payload"        bson:"payload,omitempty"`
	ScopeAppID string    `grove:"scope_app_id"   bson:"scope_app_id"`
	ScopeOrgID string    `grove:"scope_org_id"   bson:"scope_org_id"`
	Acked      bool      `grove:"acked,notnull"  bson:"acked"`
	CreatedAt  time.Time `grove:"created_at,notnull" bson:"created_at"`
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
		return nil, fmt.Errorf("dispatch/mongo: parse event id %q: %w", m.ID, err)
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
	grove.BaseModel `grove:"table:dispatch_workers"`

	ID          string            `grove:"id,pk"          bson:"_id"`
	Hostname    string            `grove:"hostname,notnull" bson:"hostname"`
	Queues      []string          `grove:"queues"         bson:"queues"`
	Concurrency int               `grove:"concurrency,notnull" bson:"concurrency"`
	State       string            `grove:"state,notnull"  bson:"state"`
	IsLeader    bool              `grove:"is_leader,notnull" bson:"is_leader"`
	LeaderUntil *time.Time        `grove:"leader_until"   bson:"leader_until,omitempty"`
	LastSeen    time.Time         `grove:"last_seen,notnull" bson:"last_seen"`
	Metadata    map[string]string `grove:"metadata"       bson:"metadata,omitempty"`
	CreatedAt   time.Time         `grove:"created_at,notnull" bson:"created_at"`
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
		return nil, fmt.Errorf("dispatch/mongo: parse worker id %q: %w", m.ID, err)
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
