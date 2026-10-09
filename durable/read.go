package durable

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"time"
)

const MaxReadPage = 100

// ReadStore is trusted persistence. Callers authorize the complete namespace
// predicate before reading it. Pages observe current rows, not a shared snapshot.
type ReadStore interface {
	ListExecutions(context.Context, ExecutionList) ([]Execution, string, error)
	ListTasks(context.Context, TaskList) ([]Task, string, error)
	ReadDeliveryStatus(context.Context, ScopedDeliveryStatus) (DeliveryStatus, error)
	ReadBuildFacts(context.Context, string, string) (BuildFacts, error)
}

type ExecutionList struct {
	Namespace    string `json:"namespace"`
	WorkflowID   string `json:"workflow_id,omitempty"`
	WorkflowType string `json:"workflow_type,omitempty"`
	BuildID      string `json:"build_id,omitempty"`
	State        State  `json:"state,omitempty"`
	Cursor       string `json:"cursor,omitempty"`
	Limit        int    `json:"limit"`
}

type TaskList struct {
	Key
	Kind   TaskKind `json:"kind,omitempty"`
	Cursor string   `json:"cursor,omitempty"`
	Limit  int      `json:"limit"`
}

type ReadPosition struct {
	CreatedAt  time.Time `json:"created_at"`
	WorkflowID string    `json:"workflow_id"`
	ID         string    `json:"id"`
	Binding    string    `json:"binding"`
}

func readBinding(v any) string {
	b, _ := json.Marshal(v) //nolint:errcheck // Only closed read request structs with JSON primitives reach this helper.
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func readPosition(cursor string, filter any) (ReadPosition, error) {
	if cursor == "" {
		return ReadPosition{}, nil
	}
	var p ReadPosition
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if len(cursor) > 4096 || err != nil || json.Unmarshal(b, &p) != nil || p.Binding != readBinding(filter) || !DeliveryIdentifier(p.ID) {
		return ReadPosition{}, ErrInvalid
	}
	return p, nil
}
func readCursor(p ReadPosition, filter any) string {
	p.Binding = readBinding(filter)
	b, _ := json.Marshal(p) //nolint:errcheck // The position contains validated store timestamps and primitive fields.
	return base64.RawURLEncoding.EncodeToString(b)
}
func (r ExecutionList) Position() (ReadPosition, error) {
	if !DeliveryIdentifier(r.Namespace) || r.Limit < 1 || r.Limit > MaxReadPage {
		return ReadPosition{}, ErrInvalid
	}
	for _, v := range []string{r.WorkflowID, r.WorkflowType, r.BuildID} {
		if v != "" && !DeliveryIdentifier(v) {
			return ReadPosition{}, ErrInvalid
		}
	}
	switch r.State {
	case "", StateRunning, StateCompleted, StateFailed, StateCancelled, StateTerminated, StateTimedOut, StateContinuedAsNew:
	default:
		return ReadPosition{}, ErrInvalid
	}
	cursor := r.Cursor
	r.Cursor = ""
	p, err := readPosition(cursor, r)
	if cursor != "" && (p.CreatedAt.IsZero() || !DeliveryIdentifier(p.WorkflowID)) {
		return ReadPosition{}, ErrInvalid
	}
	return p, err
}
func (r ExecutionList) Next(e Execution) string {
	r.Cursor = ""
	return readCursor(ReadPosition{CreatedAt: e.CreatedAt, WorkflowID: e.WorkflowID, ID: e.RunID}, r)
}
func (r TaskList) Position() (ReadPosition, error) {
	if r.Validate() != nil || r.Limit < 1 || r.Limit > MaxReadPage {
		return ReadPosition{}, ErrInvalid
	}
	switch r.Kind {
	case "", TaskWorkflow, TaskActivity, TaskTimer:
	default:
		return ReadPosition{}, ErrInvalid
	}
	cursor := r.Cursor
	r.Cursor = ""
	return readPosition(cursor, r)
}
func (r TaskList) Next(t Task) string { r.Cursor = ""; return readCursor(ReadPosition{ID: t.ID}, r) }

type ScopedDeliveryStatus struct {
	DeliveryStatusRequest
	Key
}

func (r ScopedDeliveryStatus) Validate() error {
	if r.DeliveryStatusRequest.Validate() != nil || !DeliveryIdentifier(r.Namespace) {
		return ErrInvalid
	}
	if (r.WorkflowID == "") != (r.RunID == "") {
		return ErrInvalid
	}
	if r.WorkflowID != "" {
		return r.Key.Validate()
	}
	return nil
}

type BuildFacts struct{ Executions, Running, PendingTasks int64 }
