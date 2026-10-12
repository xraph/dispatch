package durable

import (
	"encoding/json"
	"fmt"
	"time"
)

// ParentClosePolicy controls an open child when its parent closes.
type ParentClosePolicy string

const (
	ParentCloseTerminate     ParentClosePolicy = "terminate"
	ParentCloseRequestCancel ParentClosePolicy = "request_cancel"
	ParentCloseAbandon       ParentClosePolicy = "abandon"
	EventChildStarted                          = "workflow.child_started"
)

// ChildStartSpec creates a new child with an immutable parent relationship.
// ParentQueue must match the source workflow task's queue.
type ChildStartSpec struct {
	CommandID         string            `json:"command_id"`
	Start             StartRequest      `json:"start"`
	ParentQueue       string            `json:"parent_queue"`
	ParentClosePolicy ParentClosePolicy `json:"parent_close_policy"`
}

// ChildExecution combines the saved relationship with the child's current state.
type ChildExecution struct {
	Parent     Key `json:"parent"`
	CurrentKey Key `json:"current_key"`
	ChildStartSpec
	State     State     `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ChildStarted is appended to the parent history atomically with child creation.
type ChildStarted struct {
	Version           int               `json:"version"`
	CommandID         string            `json:"command_id"`
	Child             Key               `json:"child"`
	WorkflowType      string            `json:"workflow_type"`
	BuildID           string            `json:"build_id"`
	Queue             string            `json:"queue"`
	ParentClosePolicy ParentClosePolicy `json:"parent_close_policy"`
}

// ChildStartError identifies the child that prevented an atomic parent decision.
// Coordinators may record a deterministic start failure for a definitive conflict.
type ChildStartError struct {
	CommandID string
	Err       error
}

func (e *ChildStartError) Error() string { return fmt.Sprintf("child %q: %v", e.CommandID, e.Err) }
func (e *ChildStartError) Unwrap() error { return e.Err }

// Validate rejects cross-namespace children, invalid routing and oversized input.
func (c ChildStartSpec) Validate(parent Key) error {
	if err := parent.Validate(); err != nil {
		return err
	}
	if err := c.Start.Validate(); err != nil {
		return err
	}
	if c.Start.Namespace != parent.Namespace || c.Start.WorkflowID == parent.WorkflowID || !identifier(c.CommandID) || !identifier(c.ParentQueue) || len(c.Start.Input) > 1<<20 {
		return fmt.Errorf("%w: invalid child identity, queue or input", ErrInvalid)
	}
	switch c.ParentClosePolicy {
	case ParentCloseTerminate, ParentCloseRequestCancel, ParentCloseAbandon:
		return nil
	default:
		return fmt.Errorf("%w: invalid parent close policy", ErrInvalid)
	}
}

func validateChildren(r CommitRequest) error {
	if len(r.Events)+len(r.Children) > 1000 {
		return fmt.Errorf("%w: child start events exceed transition event limit", ErrInvalid)
	}
	if len(r.Children) == 0 {
		return nil
	}
	if (r.State != "" && r.State != StateRunning) || (r.TaskUpdate != nil && r.TaskUpdate.Action != TaskComplete) {
		return fmt.Errorf("%w: child creation requires a running execution and completed source task", ErrInvalid)
	}
	commands, workflows := make(map[string]bool), make(map[string]bool)
	for _, child := range r.Children {
		if err := child.Validate(r.Key); err != nil {
			return err
		}
		if commands[child.CommandID] || workflows[child.Start.WorkflowID] {
			return fmt.Errorf("%w: duplicate child command or workflow identity", ErrInvalid)
		}
		commands[child.CommandID], workflows[child.Start.WorkflowID] = true, true
	}
	return nil
}

// ValidateChildSource binds child routing to an ordinary parent workflow grant.
func ValidateChildSource(task Task, children []ChildStartSpec) error {
	for _, child := range children {
		if task.Kind != TaskWorkflow || task.LeaseKind != "" || child.ParentQueue != task.Queue {
			return fmt.Errorf("%w: child creation requires its parent workflow queue and grant", ErrInvalid)
		}
	}
	return nil
}

// ChildStartEvents returns the store-generated events in request order.
func ChildStartEvents(children []ChildStartSpec) ([]EventInput, error) {
	events := make([]EventInput, 0, len(children))
	for _, child := range children {
		payload, err := json.Marshal(ChildStarted{Version: 1, CommandID: child.CommandID, Child: child.Start.Key, WorkflowType: child.Start.WorkflowType, BuildID: child.Start.BuildID, Queue: child.Start.Queue, ParentClosePolicy: child.ParentClosePolicy})
		if err != nil {
			return nil, err
		}
		events = append(events, EventInput{Type: EventChildStarted, Payload: payload})
	}
	return events, nil
}
