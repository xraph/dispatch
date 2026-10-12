package durable

import (
	"context"
	"errors"
	"math"
	"time"
)

const WorkflowTaskDeferralPolicyVersion = 1

var ErrAdmissionChanged = errors.New("durable: target admission changed")

type DeferralTargetState string

const (
	DeferralUnregistered DeferralTargetState = "unregistered"
	DeferralRetiring     DeferralTargetState = "retiring"
	DeferralRetired      DeferralTargetState = "retired"
)

type DeferralReferenceKind string

const (
	DeferralChild        DeferralReferenceKind = "child"
	DeferralContinuation DeferralReferenceKind = "continuation"
)

type WorkflowTaskDeferralRequest struct {
	Key
	RequestID             string
	Token                 TaskToken
	ExpectedRevision      int64
	TargetBuildID         string
	TargetRetirementEpoch int64
	TargetState           DeferralTargetState
	ReferenceKind         DeferralReferenceKind
	CommandID             string
}

func (r WorkflowTaskDeferralRequest) Validate() error {
	if r.Key.Validate() != nil || !DeliveryIdentifier(r.RequestID) || r.Token.Validate() != nil || r.Token.LeaseKind != "" || r.ExpectedRevision < 1 || ValidateBuildID(r.TargetBuildID) != nil {
		return ErrInvalid
	}
	switch r.TargetState {
	case DeferralUnregistered:
		if r.TargetRetirementEpoch != 0 {
			return ErrInvalid
		}
	case DeferralRetiring, DeferralRetired:
		if r.TargetRetirementEpoch < 1 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if r.ReferenceKind == DeferralChild {
		if !identifier(r.CommandID) {
			return ErrInvalid
		}
	} else if r.ReferenceKind != DeferralContinuation || r.CommandID != "" {
		return ErrInvalid
	}
	return nil
}
func NewWorkflowTaskDeferralRequest(task Task, revision int64, refusal *BuildAdmissionError) (WorkflowTaskDeferralRequest, error) {
	if refusal == nil {
		return WorkflowTaskDeferralRequest{}, ErrInvalid
	}
	r := WorkflowTaskDeferralRequest{Key: task.Key, Token: task.Token(), ExpectedRevision: revision, TargetBuildID: refusal.BuildID, TargetRetirementEpoch: refusal.RetirementEpoch, TargetState: DeferralTargetState(refusal.State), ReferenceKind: DeferralReferenceKind(refusal.ReferenceKind), CommandID: refusal.CommandID}
	digest, err := Fingerprint("workflow-task.defer.identity.v1", r)
	if err != nil {
		return r, err
	}
	r.RequestID = "defer:" + digest
	return r, r.Validate()
}

type WorkflowTaskDeferralReceipt struct {
	Receipt
	TaskVersion           int64
	DeferralCount         int64
	RecordedAt            time.Time
	RetryAt               time.Time
	Reason                string
	TargetBuildID         string
	TargetRetirementEpoch int64
	TargetState           DeferralTargetState
	PolicyVersion         int
}
type WorkflowTaskDeferral struct {
	Key
	TaskID         string
	RequestID      string
	SourceEpoch    int64
	SourceRevision int64
	ReferenceKind  DeferralReferenceKind
	CommandID      string
	WorkflowTaskDeferralReceipt
	Active bool
}
type WorkflowTaskDeferralStore interface {
	DeferWorkflowTask(context.Context, WorkflowTaskDeferralRequest) (WorkflowTaskDeferralReceipt, error)
	GetWorkflowTaskDeferral(context.Context, Key, string) (WorkflowTaskDeferral, error)
}

// PrepareWorkflowTaskDeferral uses locked store facts and never changes history.
func PrepareWorkflowTaskDeferral(e Execution, task Task, r WorkflowTaskDeferralRequest, target BuildAdmission, previous WorkflowTaskDeferral, now time.Time) (Task, WorkflowTaskDeferral, error) {
	var result WorkflowTaskDeferral
	if err := r.Validate(); err != nil {
		return task, result, err
	}
	if e.State != StateRunning {
		return task, result, ErrClosed
	}
	if e.Revision != r.ExpectedRevision {
		return task, result, ErrRevisionConflict
	}
	if e.Key != r.Key || task.Key != r.Key || task.Kind != TaskWorkflow {
		return task, result, ErrInvalid
	}
	if err := CheckExecutionDeadline(e, now); err != nil {
		return task, result, err
	}
	if err := CheckLease(task, r.Token, now); err != nil {
		return task, result, err
	}
	if target.BuildID != r.TargetBuildID || target.State != string(r.TargetState) || target.Epoch != r.TargetRetirementEpoch {
		return task, result, ErrAdmissionChanged
	}
	count := int64(1)
	if previous.TargetBuildID == r.TargetBuildID && previous.TargetRetirementEpoch == r.TargetRetirementEpoch && previous.TargetState == r.TargetState {
		if previous.DeferralCount < 1 || previous.DeferralCount == math.MaxInt64 {
			return task, result, ErrInvalid
		}
		count = previous.DeferralCount + 1
	}
	delay := min(30*time.Second, time.Second<<min(count-1, 5))
	retryAt, err := TaskTimeAfter(now, delay)
	if err != nil {
		return task, result, err
	}
	next, err := UpdateTask(task, &TaskUpdate{Action: TaskRetry, RetryAt: retryAt}, now)
	if err != nil {
		return task, result, err
	}
	receipt := WorkflowTaskDeferralReceipt{Receipt: Receipt{Revision: e.Revision}, TaskVersion: next.Version, DeferralCount: count, RecordedAt: now, RetryAt: retryAt, Reason: "target_" + string(r.TargetState), TargetBuildID: r.TargetBuildID, TargetRetirementEpoch: r.TargetRetirementEpoch, TargetState: r.TargetState, PolicyVersion: WorkflowTaskDeferralPolicyVersion}
	result = WorkflowTaskDeferral{Key: r.Key, TaskID: task.ID, RequestID: r.RequestID, SourceEpoch: task.Epoch, SourceRevision: e.Revision, ReferenceKind: r.ReferenceKind, CommandID: r.CommandID, WorkflowTaskDeferralReceipt: receipt}
	return next, result, nil
}
func (d WorkflowTaskDeferral) IsActive(e Execution, task Task) bool {
	return e.Key == d.Key && e.State == StateRunning && task.Key == d.Key && task.ID == d.TaskID && !task.Done && task.Owner == "" && task.Version == d.TaskVersion && task.AvailableAt.Equal(d.RetryAt)
}
