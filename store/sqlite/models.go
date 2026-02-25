package sqlite

import (
	"encoding/json"
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

	ID          string     `grove:"id,pk"`
	Name        string     `grove:"name,notnull"`
	Queue       string     `grove:"queue,notnull,default:'default'"`
	Payload     []byte     `grove:"payload,notnull"`
	State       string     `grove:"state,notnull,default:'pending'"`
	Priority    int        `grove:"priority,notnull,default:0"`
	MaxRetries  int        `grove:"max_retries,notnull,default:3"`
	RetryCount  int        `grove:"retry_count,notnull,default:0"`
	LastError   string     `grove:"last_error"`
	ScopeAppID  string     `grove:"scope_app_id"`
	ScopeOrgID  string     `grove:"scope_org_id"`
	WorkerID    string     `grove:"worker_id"`
	RunAt       time.Time  `grove:"run_at,notnull"`
	StartedAt   *time.Time `grove:"started_at"`
	CompletedAt *time.Time `grove:"completed_at"`
	HeartbeatAt *time.Time `grove:"heartbeat_at"`
	Timeout     int64      `grove:"timeout,notnull,default:0"`
	CreatedAt   time.Time  `grove:"created_at,notnull"`
	UpdatedAt   time.Time  `grove:"updated_at,notnull"`
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
		return nil, fmt.Errorf("dispatch/sqlite: parse job id %q: %w", m.ID, err)
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

	ID          string     `grove:"id,pk"`
	Name        string     `grove:"name,notnull"`
	State       string     `grove:"state,notnull,default:'running'"`
	Input       []byte     `grove:"input"`
	Output      []byte     `grove:"output"`
	Error       string     `grove:"error"`
	ScopeAppID  string     `grove:"scope_app_id"`
	ScopeOrgID  string     `grove:"scope_org_id"`
	StartedAt   time.Time  `grove:"started_at,notnull"`
	CompletedAt *time.Time `grove:"completed_at"`
	CreatedAt   time.Time  `grove:"created_at,notnull"`
	UpdatedAt   time.Time  `grove:"updated_at,notnull"`
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
		return nil, fmt.Errorf("dispatch/sqlite: parse run id %q: %w", m.ID, err)
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

	ID        string    `grove:"id,pk"`
	RunID     string    `grove:"run_id,notnull"`
	StepName  string    `grove:"step_name,notnull"`
	Data      []byte    `grove:"data,notnull"`
	CreatedAt time.Time `grove:"created_at,notnull"`
}

func fromCheckpointModel(m *checkpointModel) (*workflow.Checkpoint, error) {
	parsedID, err := id.ParseCheckpointID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse checkpoint id %q: %w", m.ID, err)
	}

	parsedRunID, err := id.ParseRunID(m.RunID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse run id %q: %w", m.RunID, err)
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

	ID          string     `grove:"id,pk"`
	Name        string     `grove:"name,notnull,unique"`
	Schedule    string     `grove:"schedule,notnull"`
	JobName     string     `grove:"job_name,notnull"`
	Queue       string     `grove:"queue,notnull,default:''"`
	Payload     []byte     `grove:"payload"`
	ScopeAppID  string     `grove:"scope_app_id"`
	ScopeOrgID  string     `grove:"scope_org_id"`
	LastRunAt   *time.Time `grove:"last_run_at"`
	NextRunAt   *time.Time `grove:"next_run_at"`
	LockedBy    *string    `grove:"locked_by"`
	LockedUntil *time.Time `grove:"locked_until"`
	Enabled     bool       `grove:"enabled,notnull,default:true"`
	CreatedAt   time.Time  `grove:"created_at,notnull"`
	UpdatedAt   time.Time  `grove:"updated_at,notnull"`
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
		return nil, fmt.Errorf("dispatch/sqlite: parse cron id %q: %w", m.ID, err)
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

	ID         string     `grove:"id,pk"`
	JobID      string     `grove:"job_id,notnull"`
	JobName    string     `grove:"job_name,notnull"`
	Queue      string     `grove:"queue,notnull"`
	Payload    []byte     `grove:"payload,notnull"`
	Error      string     `grove:"error,notnull"`
	RetryCount int        `grove:"retry_count,notnull"`
	MaxRetries int        `grove:"max_retries,notnull,default:3"`
	ScopeAppID string     `grove:"scope_app_id"`
	ScopeOrgID string     `grove:"scope_org_id"`
	FailedAt   time.Time  `grove:"failed_at,notnull"`
	ReplayedAt *time.Time `grove:"replayed_at"`
	CreatedAt  time.Time  `grove:"created_at,notnull"`
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
		return nil, fmt.Errorf("dispatch/sqlite: parse dlq id %q: %w", m.ID, err)
	}

	parsedJobID, err := id.ParseJobID(m.JobID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse job id %q: %w", m.JobID, err)
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

	ID         string    `grove:"id,pk"`
	Name       string    `grove:"name,notnull"`
	Payload    []byte    `grove:"payload"`
	ScopeAppID string    `grove:"scope_app_id"`
	ScopeOrgID string    `grove:"scope_org_id"`
	Acked      bool      `grove:"acked,notnull,default:false"`
	CreatedAt  time.Time `grove:"created_at,notnull"`
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
		return nil, fmt.Errorf("dispatch/sqlite: parse event id %q: %w", m.ID, err)
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

	ID          string    `grove:"id,pk"`
	Hostname    string    `grove:"hostname,notnull"`
	Queues      string    `grove:"queues,notnull,default:'[]'"`
	Concurrency int       `grove:"concurrency,notnull,default:10"`
	State       string    `grove:"state,notnull,default:'active'"`
	IsLeader    bool      `grove:"is_leader,notnull,default:false"`
	LeaderUntil *string   `grove:"leader_until"`
	LastSeen    time.Time `grove:"last_seen,notnull"`
	Metadata    string    `grove:"metadata,notnull,default:'{}'"`
	CreatedAt   time.Time `grove:"created_at,notnull"`
}

func toWorkerModel(w *cluster.Worker) *workerModel {
	return &workerModel{
		ID:          w.ID.String(),
		Hostname:    w.Hostname,
		Queues:      stringsToJSON(w.Queues),
		Concurrency: w.Concurrency,
		State:       string(w.State),
		IsLeader:    w.IsLeader,
		LeaderUntil: timeToStr(w.LeaderUntil),
		LastSeen:    w.LastSeen,
		Metadata:    mapToJSON(w.Metadata),
		CreatedAt:   w.CreatedAt,
	}
}

func fromWorkerModel(m *workerModel) (*cluster.Worker, error) {
	parsedID, err := id.ParseWorkerID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: parse worker id %q: %w", m.ID, err)
	}

	return &cluster.Worker{
		ID:          parsedID,
		Hostname:    m.Hostname,
		Queues:      jsonToStrings(m.Queues),
		Concurrency: m.Concurrency,
		State:       cluster.WorkerState(m.State),
		IsLeader:    m.IsLeader,
		LeaderUntil: strToTime(m.LeaderUntil),
		LastSeen:    m.LastSeen,
		Metadata:    jsonToMap(m.Metadata),
		CreatedAt:   m.CreatedAt,
	}, nil
}

// ── JSON helpers ──────────────────────────────────────────────────

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

func timeToStr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}

func strToTime(s *string) *time.Time {
	if s == nil || *s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, *s)
	if err != nil {
		return nil
	}
	return &t
}
