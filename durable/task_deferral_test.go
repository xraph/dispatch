package durable

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestWorkflowDeferralPolicyBounds(t *testing.T) {
	now := Timestamp(time.Now())
	key := Key{Namespace: "n", WorkflowID: "w", RunID: "r"}
	e := Execution{Key: key, State: StateRunning, Revision: 1}
	task := Task{Key: key, TaskSpec: TaskSpec{ID: "task", Kind: TaskWorkflow}, Owner: "owner", Epoch: 1, Version: 1, LeaseUntil: now.Add(time.Minute)}
	request, err := NewWorkflowTaskDeferralRequest(task, 1, &BuildAdmissionError{BuildID: "target", State: "retired", RetirementEpoch: 2, ReferenceKind: "continuation"})
	if err != nil {
		t.Fatal(err)
	}
	target := BuildAdmission{BuildTarget: BuildTarget{BuildID: "target"}, State: "retired", Epoch: 2}
	previous := WorkflowTaskDeferral{WorkflowTaskDeferralReceipt: WorkflowTaskDeferralReceipt{TargetBuildID: "target", TargetState: DeferralRetired, TargetRetirementEpoch: 2, DeferralCount: 500}}
	_, d, err := PrepareWorkflowTaskDeferral(e, task, request, target, previous, now)
	if err != nil || d.DeferralCount != 501 || d.RetryAt.Sub(now) != 30*time.Second {
		t.Fatalf("cap: %+v %v", d, err)
	}
	previous.DeferralCount = math.MaxInt64
	if _, _, err = PrepareWorkflowTaskDeferral(e, task, request, target, previous, now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("overflow: %v", err)
	}
	oversized := request
	oversized.RequestID = strings.Repeat("r", 257)
	if oversized.Validate() == nil {
		t.Fatal("unbounded audit request identity accepted")
	}
	for _, pair := range []struct {
		state DeferralTargetState
		epoch int64
	}{{DeferralUnregistered, 1}, {DeferralRetiring, 0}, {DeferralRetired, -1}, {"other", 0}} {
		request.TargetState, request.TargetRetirementEpoch = pair.state, pair.epoch
		if request.Validate() == nil {
			t.Fatalf("invalid pair accepted: %+v", pair)
		}
	}
}
