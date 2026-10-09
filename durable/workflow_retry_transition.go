package durable

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

const (
	EventWorkflowRetryScheduled = "workflow.retry_scheduled"
	FailureWorkflowRunTimeout   = "workflow_run_timeout"
)

// WorkflowRetryScheduled binds the original failed/timed-out outcome to its
// successor. It precedes that terminal event, which remains the run's last event.
type WorkflowRetryScheduled struct {
	Version     int           `json:"version"`
	Next        RunMetadata   `json:"next"`
	Delay       time.Duration `json:"delay"`
	FailureType string        `json:"failure_type"`
}

type workflowFailure struct {
	Type         string `json:"type"`
	Message      string `json:"message"`
	NonRetryable bool   `json:"non_retryable,omitempty"`
}

// PrepareTransitionSuccessor stages continuation or an opt-in workflow retry.
// The caller has validated its ordinary grant, revision and execution deadline.
func PrepareTransitionSuccessor(current Execution, task Task, r CommitRequest, history []Event, now time.Time) (*ContinuationBatch, error) {
	if r.Continuation != nil {
		return PrepareContinuation(current, task, r, history, now)
	}
	if current.RetryPolicy == nil || r.State != StateFailed {
		return nil, nil
	}
	if task.Kind != TaskWorkflow || task.LeaseKind != "" || len(r.Output) != 0 {
		return nil, ErrInvalid
	}
	return PrepareWorkflowRetry(current, r.State, r.Events, history, task.Queue, now)
}

// PrepareWorkflowRetry stages a successor from a saved policy and authoritative
// store time. Timeout processors call it without loading a workflow handler.
func PrepareWorkflowRetry(current Execution, state State, inputs []EventInput, history []Event, queue string, now time.Time) (*ContinuationBatch, error) {
	if current.RetryPolicy == nil || state != StateFailed && state != StateTimedOut {
		return nil, nil
	}
	if current.State != StateRunning || current.NextRunID != "" || ValidateRunMetadata(current) != nil || len(inputs) == 0 {
		return nil, ErrInvalid
	}
	if !current.ExecutionDeadlineAt.IsZero() && !now.Before(current.ExecutionDeadlineAt) {
		return nil, nil
	}
	for _, event := range history {
		if event.Type == EventCancellationRequested || event.Type == "workflow.cancellation_started" {
			return nil, nil
		}
	}
	failureType, nonRetryable, err := workflowRetryFailure(current, state, inputs[len(inputs)-1])
	if err != nil {
		return nil, err
	}
	delay, err := WorkflowRetryDelay(current.RetryPolicy, current.WorkflowAttempt(), failureType, nonRetryable)
	if err != nil || delay == 0 {
		return nil, err
	}
	available := now.Add(delay)
	if rounded := Timestamp(available); rounded.Before(available) {
		available = rounded.Add(time.Microsecond)
	} else {
		available = rounded
	}
	if !validRunTime(available) {
		return nil, fmt.Errorf("%w: retry availability outside supported range", ErrInvalid)
	}
	if !current.ExecutionDeadlineAt.IsZero() && !available.Before(current.ExecutionDeadlineAt) {
		return nil, nil
	}
	if len(inputs) >= 1000 {
		return nil, fmt.Errorf("%w: no room for retry history", ErrInvalid)
	}
	for _, event := range inputs[:len(inputs)-1] {
		switch event.Type {
		case EventCancellationRequested, "workflow.cancellation_started":
			return nil, nil
		case EventWorkflowRetryScheduled, EventWorkflowContinued, "workflow.completed", "workflow.failed", "workflow.cancelled", EventWorkflowTerminated, EventWorkflowTimedOut:
			return nil, ErrInvalid
		}
	}
	id, err := Fingerprint("workflow-retry", current.Key)
	if err != nil {
		return nil, err
	}
	spec := ContinueSpec{RunID: "retry-" + id, WorkflowType: current.WorkflowType, BuildID: current.BuildID, Queue: queue, Input: current.Input, RunTimeout: current.RunTimeout}
	batch, err := prepareRunSuccessor(current, spec, history, inputs, now, current.WorkflowAttempt()+1, available)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(WorkflowRetryScheduled{Version: 1, Next: RunMetadataOf(batch.Execution), Delay: delay, FailureType: failureType})
	if err != nil {
		return nil, err
	}
	batch.RetryEvent = &EventInput{Type: EventWorkflowRetryScheduled, Payload: payload}
	return batch, nil
}

func workflowRetryFailure(current Execution, state State, event EventInput) (failureType string, nonRetryable bool, err error) {
	if state == StateTimedOut {
		var timeout ExecutionTimeout
		kind, deadline := current.Deadline()
		if event.Type != EventWorkflowTimedOut || decodeRunPayload(event.Payload, &timeout) != nil || timeout.Validate() != nil || timeout.Kind != kind || !timeout.DeadlineAt.Equal(deadline) {
			return "", false, ErrInvalid
		}
		return FailureWorkflowRunTimeout, timeout.Kind != TimeoutRun, nil
	}
	var failure workflowFailure
	if event.Type != "workflow.failed" || decodeRunPayload(event.Payload, &failure) != nil || !identifier(failure.Type) || len(failure.Type) > 200 || !utf8.ValidString(failure.Message) {
		return "", false, ErrInvalid
	}
	return failure.Type, failure.NonRetryable, nil
}

// AddWorkflowRetryEvent adjusts source coordinates and inserts the retry link
// immediately before its terminal event. Continuations use their existing append.
func AddWorkflowRetryEvent(next *Execution, receipt *Receipt, inputs []EventInput, batch *ContinuationBatch) ([]EventInput, error) {
	if batch == nil || batch.RetryEvent == nil {
		return inputs, nil
	}
	if next.LastSequence == math.MaxInt64 || len(inputs) == 0 || len(inputs) >= 1000 {
		return nil, ErrInvalid
	}
	next.LastSequence++
	receipt.LastSequence++
	events := append([]EventInput(nil), inputs[:len(inputs)-1]...)
	events = append(events, *batch.RetryEvent, inputs[len(inputs)-1])
	return events, nil
}
