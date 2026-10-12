package durable

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// EventCancellationRequested records accepted workflow cancellation independently
// of task fencing, cleanup and terminal state.
const EventCancellationRequested = "workflow.cancellation_requested"

// ExecutionCancellation is the versioned request retained in workflow history.
// The first accepted request supplies the reason used by cancellation cleanup.
type ExecutionCancellation struct {
	Version   int    `json:"version"`
	RequestID string `json:"request_id"`
	Reason    string `json:"reason,omitempty"`
}

// CancelExecutionRequest targets one run, or the current open run with empty
// RunID. Retry the complete original request to recover its original target.
// Request IDs are scoped by namespace/workflow within the cancellation API.
type CancelExecutionRequest struct {
	Key
	RequestID string `json:"request_id"`
	BuildID   string `json:"build_id"`
	Reason    string `json:"reason,omitempty"`
}

// CancelExecutionReceipt confirms durable acceptance, not completed cancellation.
type CancelExecutionReceipt struct {
	Key
	Receipt
}

// Validate bounds accepted data before persistence or replay.
func (r ExecutionCancellation) Validate() error {
	if r.Version != 1 || !identifier(r.RequestID) || len(r.Reason) > 4096 || !utf8.ValidString(r.Reason) || strings.ContainsRune(r.Reason, 0) {
		return fmt.Errorf("%w: invalid cancellation version, request ID or reason", ErrInvalid)
	}
	return nil
}

// Validate requires namespace/build and permits selecting the current open run.
func (r CancelExecutionRequest) Validate() error {
	if !identifier(r.Namespace) || !identifier(r.WorkflowID) || (r.RunID != "" && !identifier(r.RunID)) || !identifier(r.BuildID) {
		return fmt.Errorf("%w: invalid cancellation target or build", ErrInvalid)
	}
	return (ExecutionCancellation{Version: 1, RequestID: r.RequestID, Reason: r.Reason}).Validate()
}
