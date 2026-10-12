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

// JSON escapes at most six bytes per input byte. A position has two persisted
// IDs, a 64-character hex binding and an RFC3339Nano timestamp (at most 35
// bytes). The literal accounts for every key, quote and delimiter. Raw base64
// needs ceil(8*n/6) bytes. Keep this budget shared with encrypted outer cursors.
const maxReadPositionJSON = len(`{"created_at":"","workflow_id":"","id":"","binding":""}`) + 2*6*MaxIdentifierBytes + 64 + len("2006-01-02T15:04:05.999999999-07:00")

// MaxReadCursorBytes bounds the encoded execution and task position.
const MaxReadCursorBytes = (8*maxReadPositionJSON + 5) / 6

// ValidateBuildRead applies persisted identifier limits, not catalog limits.
func ValidateBuildRead(namespace, build string) error {
	if !identifier(namespace) || !identifier(build) {
		return ErrInvalid
	}
	return nil
}

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
	if len(cursor) > MaxReadCursorBytes {
		return p, ErrInvalid
	}
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(b) > maxReadPositionJSON || json.Unmarshal(b, &p) != nil || p.Binding != readBinding(filter) || !identifier(p.ID) || (p.WorkflowID != "" && !identifier(p.WorkflowID)) {
		return ReadPosition{}, ErrInvalid
	}
	return p, nil
}
func readCursor(p ReadPosition, filter any) (string, error) {
	if !identifier(p.ID) || (p.WorkflowID != "" && !identifier(p.WorkflowID)) {
		return "", ErrInvalid
	}
	p.Binding = readBinding(filter)
	b, err := json.Marshal(p)
	if err != nil || len(b) > maxReadPositionJSON {
		return "", ErrInvalid
	}
	token := base64.RawURLEncoding.EncodeToString(b)
	if len(token) > MaxReadCursorBytes {
		return "", ErrInvalid
	}
	return token, nil
}
func (r ExecutionList) Position() (ReadPosition, error) {
	if !identifier(r.Namespace) || r.Limit < 1 || r.Limit > MaxReadPage {
		return ReadPosition{}, ErrInvalid
	}
	for _, v := range []string{r.WorkflowID, r.WorkflowType, r.BuildID} {
		if v != "" && !identifier(v) {
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
	if cursor != "" && (p.CreatedAt.IsZero() || !identifier(p.WorkflowID)) {
		return ReadPosition{}, ErrInvalid
	}
	return p, err
}
func (r ExecutionList) Next(e Execution) (string, error) {
	if e.CreatedAt.IsZero() || !identifier(e.WorkflowID) {
		return "", ErrInvalid
	}
	r.Cursor = ""
	if _, err := r.Position(); err != nil {
		return "", err
	}
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
func (r TaskList) Next(t Task) (string, error) {
	r.Cursor = ""
	if _, err := r.Position(); err != nil {
		return "", err
	}
	return readCursor(ReadPosition{ID: t.ID}, r)
}

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
