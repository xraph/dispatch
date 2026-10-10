package operator

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

const DiscoveryBudget = 32

type NamespaceInput struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}
type Namespace struct {
	Namespace string `json:"namespace"`
	AppID     string `json:"app_id"`
	TenantID  string `json:"tenant_id"`
}

func (s *Service) Namespaces(ctx context.Context, p security.Principal, in NamespaceInput) (Page[Namespace], error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	out := observed([]Namespace{})
	if err := p.Validate(); err != nil {
		return out, s.denied(ctx, p, Discover, err)
	}
	if !s.audit.DurableReadsReady() {
		return out, security.ErrUnavailable
	}
	limit, err := pageLimit(in.Limit)
	if err != nil {
		return out, err
	}
	bind := binding(p, s.installation, Discover, struct {
		Limit int
		Order string
	}{limit, "namespace_asc"})
	state, err := s.cursors.open(in.Cursor, bind)
	if err != nil {
		return out, err
	}
	for _, ns := range state.Scope {
		if checkErr := s.check(ctx, p, Discover, durable.Key{Namespace: ns}); checkErr != nil {
			return out, checkErr
		}
	}
	candidates, err := s.catalog.ListNamespaces(ctx, durable.NamespaceList{InstallationID: s.installation, After: state.Position, Limit: DiscoveryBudget})
	if err != nil {
		return out, safeError(err)
	}
	out.Complete = len(candidates) < DiscoveryBudget
	last := state.Position
	scope := []string{}
	for i, n := range candidates {
		last = n.Namespace
		err = s.check(ctx, p, Discover, durable.Key{Namespace: n.Namespace})
		if err != nil {
			if errors.Is(err, security.ErrForbidden) && !errors.Is(err, security.ErrUnavailable) {
				continue
			}
			return out, err
		}
		out.Items = append(out.Items, Namespace{Namespace: n.Namespace, AppID: n.AppID, TenantID: n.TenantID})
		scope = append(scope, n.Namespace)
		if len(out.Items) == limit {
			out.Complete = out.Complete && i == len(candidates)-1
			break
		}
	}
	err = nil
	if !out.Complete {
		out.Cursor, err = s.cursors.seal(cursorState{Position: last, Scope: scope}, bind)
	}
	out.Observation = "bounded_catalog_scan"
	return out, err
}

type Execution struct {
	Namespace           string        `json:"namespace"`
	WorkflowID          string        `json:"workflow_id"`
	RunID               string        `json:"run_id"`
	WorkflowType        string        `json:"workflow_type"`
	BuildID             string        `json:"build_id"`
	State               durable.State `json:"state"`
	Revision            string        `json:"revision"`
	LastSequence        string        `json:"last_sequence"`
	RunNumber           string        `json:"run_number"`
	RetryAttempt        string        `json:"retry_attempt"`
	CreatedAt           time.Time     `json:"created_at"`
	UpdatedAt           time.Time     `json:"updated_at"`
	RunDeadlineAt       *time.Time    `json:"run_deadline_at"`
	ExecutionDeadlineAt *time.Time    `json:"execution_deadline_at"`
	RunAvailableAt      time.Time     `json:"run_available_at"`
	Payload             string        `json:"payload"`
	Runtime             string        `json:"runtime"`
}

func optionalTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
func (s *Service) project(e durable.Execution) Execution {
	runtime := "unavailable"
	if s.hasRuntime(e.Namespace, e.BuildID) {
		runtime = "available"
	}
	return Execution{Namespace: e.Namespace, WorkflowID: e.WorkflowID, RunID: e.RunID, WorkflowType: e.WorkflowType, BuildID: e.BuildID, State: e.State, Revision: strconv.FormatInt(e.Revision, 10), LastSequence: strconv.FormatInt(e.LastSequence, 10), RunNumber: strconv.FormatInt(e.RunNumber, 10), RetryAttempt: strconv.FormatInt(e.RetryAttempt, 10), CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt, RunDeadlineAt: optionalTime(e.RunDeadlineAt), ExecutionDeadlineAt: optionalTime(e.ExecutionDeadlineAt), RunAvailableAt: e.RunAvailableAt, Payload: "restricted", Runtime: runtime}
}
func (s *Service) Executions(ctx context.Context, p security.Principal, in durable.ExecutionList) (Page[Execution], error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	out := observed([]Execution{})
	if err := s.check(ctx, p, ListExecutions, durable.Key{Namespace: in.Namespace}); err != nil {
		return out, err
	}
	limit, err := pageLimit(in.Limit)
	if err != nil {
		return out, err
	}
	in.Limit = limit
	token := in.Cursor
	in.Cursor = ""
	bind := binding(p, s.installation, ListExecutions, in)
	state, err := s.cursors.open(token, bind)
	if err != nil {
		return out, err
	}
	in.Cursor = state.Position
	rows, next, err := s.reads.ListExecutions(ctx, in)
	if err != nil {
		return out, safeError(err)
	}
	for _, e := range rows {
		out.Items = append(out.Items, s.project(e))
	}
	out.Complete = next == ""
	if next != "" {
		out.Cursor, err = s.cursors.seal(cursorState{Position: next}, bind)
	}
	return out, err
}

type Detail struct {
	Execution
	Links           []Link    `json:"links"`
	LinksRestricted bool      `json:"links_restricted"`
	AsOf            time.Time `json:"as_of"`
}
type Link struct {
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

func (s *Service) resolve(ctx context.Context, p security.Principal, action string, key durable.Key) (durable.Execution, error) {
	if err := s.check(ctx, p, action, key); err != nil {
		return durable.Execution{}, err
	}
	if key.Validate() != nil {
		return durable.Execution{}, durable.ErrInvalid
	}
	e, err := s.store.GetExecution(ctx, key)
	return e, safeError(err)
}
func (s *Service) Detail(ctx context.Context, p security.Principal, key durable.Key) (Detail, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	e, err := s.resolve(ctx, p, ReadExecution, key)
	if err != nil {
		return Detail{}, err
	}
	out := Detail{Execution: s.project(e), Links: []Link{}, AsOf: time.Now().UTC()}
	for _, link := range []struct{ kind, id string }{{"first", e.FirstRunID}, {"previous", e.PreviousRunID}, {"next", e.NextRunID}} {
		if link.id == "" || link.id == e.RunID {
			continue
		}
		target := key
		target.RunID = link.id
		if _, err = s.resolve(ctx, p, ReadExecution, target); err != nil {
			if onlyDenied(err) || errors.Is(err, durable.ErrNotFound) {
				out.LinksRestricted = true
				continue
			}
			return Detail{}, err
		}
		out.Links = append(out.Links, Link{Kind: link.kind, Namespace: key.Namespace, WorkflowID: key.WorkflowID, RunID: link.id})
	}
	return out, nil
}

type RunInput struct {
	durable.Key
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}
type Event struct {
	Type     string    `json:"type"`
	Sequence string    `json:"sequence"`
	Time     time.Time `json:"time"`
}

func (s *Service) History(ctx context.Context, p security.Principal, in RunInput) (Page[Event], error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	out := observed([]Event{})
	e, err := s.resolve(ctx, p, ReadHistory, in.Key)
	if err != nil {
		return out, err
	}
	limit, err := pageLimit(in.Limit)
	if err != nil {
		return out, err
	}
	in.Limit = limit
	token := in.Cursor
	in.Cursor = ""
	bind := binding(p, s.installation, ReadHistory, in)
	state, err := s.cursors.open(token, bind)
	if err != nil {
		return out, err
	}
	if token == "" {
		state.HighWater = e.LastSequence
		state.Revision = e.Revision
	}
	var after int64
	if state.Position != "" {
		after, err = strconv.ParseInt(state.Position, 10, 64)
		if err != nil {
			return out, durable.ErrInvalid
		}
	}
	rows, err := s.store.ReadHistory(ctx, in.Key, after, limit+1)
	if err != nil {
		return out, safeError(err)
	}
	for _, event := range rows {
		if event.Sequence > state.HighWater {
			break
		}
		if len(out.Items) == limit {
			out.Complete = false
			break
		}
		out.Items = append(out.Items, Event{Type: event.Type, Sequence: strconv.FormatInt(event.Sequence, 10), Time: event.Time})
	}
	out.Observation = "captured_history_high_water"
	out.Revision = strconv.FormatInt(state.Revision, 10)
	out.HighWater = strconv.FormatInt(state.HighWater, 10)
	if !out.Complete {
		state.Position = out.Items[len(out.Items)-1].Sequence
		out.Cursor, err = s.cursors.seal(state, bind)
	}
	return out, err
}

type Task struct {
	ID          string           `json:"id"`
	Kind        durable.TaskKind `json:"kind"`
	State       string           `json:"state"`
	Attempt     string           `json:"attempt"`
	Version     string           `json:"version"`
	AvailableAt time.Time        `json:"available_at"`
	LeaseUntil  *time.Time       `json:"lease_until"`
	DeadlineAt  *time.Time       `json:"deadline_at"`
	HeartbeatAt *time.Time       `json:"heartbeat_at"`
}

func (s *Service) Tasks(ctx context.Context, p security.Principal, in durable.TaskList) (Page[Task], error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	out := observed([]Task{})
	e, err := s.resolve(ctx, p, ReadTasks, in.Key)
	if err != nil {
		return out, err
	}
	limit, err := pageLimit(in.Limit)
	if err != nil {
		return out, err
	}
	in.Limit = limit
	token := in.Cursor
	in.Cursor = ""
	bind := binding(p, s.installation, ReadTasks, in)
	state, err := s.cursors.open(token, bind)
	if err != nil {
		return out, err
	}
	in.Cursor = state.Position
	rows, next, err := s.reads.ListTasks(ctx, in)
	if err != nil {
		return out, safeError(err)
	}
	for _, t := range rows {
		status := "pending"
		switch {
		case t.Done:
			status = "done"
		case t.LeaseKind == durable.LeaseAsync:
			status = "awaiting_callback"
		case t.LeaseUntil.After(out.AsOf):
			status = "leased"
		}
		out.Items = append(out.Items, Task{ID: t.ID, Kind: t.Kind, State: status, Attempt: strconv.FormatInt(t.Attempt, 10), Version: strconv.FormatInt(t.Version, 10), AvailableAt: t.AvailableAt, LeaseUntil: optionalTime(t.LeaseUntil), DeadlineAt: optionalTime(t.DeadlineAt), HeartbeatAt: optionalTime(t.HeartbeatAt)})
	}
	out.Revision = strconv.FormatInt(e.Revision, 10)
	out.Complete = next == ""
	if next != "" {
		out.Cursor, err = s.cursors.seal(cursorState{Position: next}, bind)
	}
	return out, err
}

type Payload struct {
	State    string `json:"state"`
	Encoding string `json:"encoding"`
	Input    []byte `json:"input"`
	Output   []byte `json:"output"`
	Revision string `json:"revision"`
}

func (s *Service) Payload(ctx context.Context, p security.Principal, key durable.Key) (Payload, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	e, err := s.resolve(ctx, p, ReadPayload, key)
	if err != nil {
		return Payload{}, err
	}
	if len(e.Input)+len(e.Output) > 1024*1024 {
		return Payload{}, security.ErrUnavailable
	}
	if err := s.audit.RecordDurableRead(ctx, p, ReadPayload, "allowed", key.Namespace, key); err != nil {
		return Payload{}, err
	}
	return Payload{State: "revealed", Encoding: "base64", Input: e.Input, Output: e.Output, Revision: strconv.FormatInt(e.Revision, 10)}, nil
}
