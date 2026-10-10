package operator

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xraph/dispatch/durable"
)

type TaskDeferral struct {
	Active                bool                          `json:"active"`
	Reason                string                        `json:"reason"`
	TargetBuildID         string                        `json:"target_build_id"`
	TargetState           durable.DeferralTargetState   `json:"target_state"`
	TargetRetirementEpoch string                        `json:"target_retirement_epoch"`
	SourceEpoch           string                        `json:"source_epoch"`
	SourceRevision        string                        `json:"source_revision"`
	TaskVersion           string                        `json:"task_version"`
	Count                 string                        `json:"count"`
	ReferenceKind         durable.DeferralReferenceKind `json:"reference_kind"`
	CommandID             string                        `json:"command_id"`
	RecordedAt            time.Time                     `json:"recorded_at"`
	RetryAt               time.Time                     `json:"retry_at"`
	PolicyVersion         string                        `json:"policy_version"`
}

func (s *Service) taskDeferral(ctx context.Context, key durable.Key, id string) (*TaskDeferral, error) {
	store, ok := s.store.(durable.WorkflowTaskDeferralStore)
	if !ok {
		return nil, nil
	}
	d, err := store.GetWorkflowTaskDeferral(ctx, key, id)
	if errors.Is(err, durable.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, safeError(err)
	}
	return &TaskDeferral{Active: d.Active, Reason: d.Reason, TargetBuildID: d.TargetBuildID, TargetState: d.TargetState, TargetRetirementEpoch: strconv.FormatInt(d.TargetRetirementEpoch, 10), SourceEpoch: strconv.FormatInt(d.SourceEpoch, 10), SourceRevision: strconv.FormatInt(d.SourceRevision, 10), TaskVersion: strconv.FormatInt(d.TaskVersion, 10), Count: strconv.FormatInt(d.DeferralCount, 10), ReferenceKind: d.ReferenceKind, CommandID: d.CommandID, RecordedAt: d.RecordedAt, RetryAt: d.RetryAt, PolicyVersion: strconv.Itoa(d.PolicyVersion)}, nil
}
