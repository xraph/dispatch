package durable

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

var ErrBuildAdmission = errors.New("durable: build admission refused")
var ErrRetirementBlocked = errors.New("durable: build retirement blocked")

const (
	BuildAccepting                                 = "accepting"
	BuildRetiring                                  = "retiring"
	BuildRetired                                   = "retired"
	OperationRegisterBuild      LifecycleOperation = "build.register"
	OperationBeginRetirement    LifecycleOperation = "build.retirement.begin"
	OperationFinalizeRetirement LifecycleOperation = "build.retirement.finalize"
	OperationAbortRetirement    LifecycleOperation = "build.retirement.abort"
)

type BuildAdmissionError struct {
	BuildID         string
	State           string
	RetirementEpoch int64
	ReferenceKind   string
	CommandID       string
}

func (e *BuildAdmissionError) Error() string {
	return fmt.Sprintf("%v: build %q state %s epoch %d", ErrBuildAdmission, e.BuildID, e.State, e.RetirementEpoch)
}
func (e *BuildAdmissionError) Unwrap() error { return ErrBuildAdmission }

type RegisterBuildRequest struct {
	BuildTarget
	RequestID       string
	ExpectedVersion int64
}

func (r RegisterBuildRequest) Validate() error {
	if r.BuildTarget.Validate() != nil || !DeliveryIdentifier(r.RequestID) || r.ExpectedVersion < 0 {
		return ErrInvalid
	}
	return nil
}

type BuildRetirementRequest struct {
	BuildTarget
	RequestID       string
	ExpectedVersion int64
	ExpectedEpoch   int64
}

func (r BuildRetirementRequest) Validate() error {
	if r.BuildTarget.Validate() != nil || !DeliveryIdentifier(r.RequestID) || r.ExpectedVersion < 1 || r.ExpectedEpoch < 1 {
		return ErrInvalid
	}
	return nil
}

type BuildBlockers struct {
	OpenExecutions         int64
	PendingTasks           int64
	AsyncCallbacks         int64
	DelayedRuns            int64
	PendingChildDeliveries int64
	ChildObligations       int64
}

func (b BuildBlockers) Empty() bool {
	return b.OpenExecutions == 0 && b.PendingTasks == 0 && b.AsyncCallbacks == 0 && b.DelayedRuns == 0 && b.PendingChildDeliveries == 0 && b.ChildObligations == 0
}

// ObservationVersion identifies catalog state only. Blocker counts can change
// at the same version; Finalize always computes them again under coordination.
type ObservationVersion struct {
	CompatibilityVersion int64
	BuildVersion         int64
	ObservedAt           time.Time
}
type BuildLifecycleFacts struct {
	Admission          BuildAdmission
	Blockers           BuildBlockers
	ObservationVersion ObservationVersion
}

type LifecycleStore interface {
	RetirementEnrollmentStore
	RegisterBuild(context.Context, RegisterBuildRequest) (LifecycleReceipt, error)
	InspectBuildLifecycle(context.Context, BuildTarget) (BuildLifecycleFacts, error)
	BeginBuildRetirement(context.Context, BuildRetirementRequest) (LifecycleReceipt, error)
	FinalizeBuildRetirement(context.Context, BuildRetirementRequest) (LifecycleReceipt, error)
	AbortBuildRetirement(context.Context, BuildRetirementRequest) (LifecycleReceipt, error)
}

// AdmissionEpoch derives eligibility from a persisted immediate predecessor or
// parent. Callers cannot acquire inherited eligibility by choosing a request ID.
func AdmissionEpoch(build BuildAdmission, source *Execution, kind, command string) (int64, error) {
	inherited := source != nil && source.BuildID == build.BuildID
	if build.State == BuildAccepting {
		if inherited {
			return source.AdmissionEpoch, nil
		}
		return build.Epoch, nil
	}
	if build.State == BuildRetiring && inherited && source.AdmissionEpoch <= build.CutoffEpoch {
		return source.AdmissionEpoch, nil
	}
	return 0, &BuildAdmissionError{BuildID: build.BuildID, State: build.State, RetirementEpoch: build.Epoch, ReferenceKind: kind, CommandID: command}
}

func TransitionBuild(current BuildAdmission, r BuildRetirementRequest, operation LifecycleOperation, blockers BuildBlockers, now time.Time) (BuildAdmission, error) {
	if current.Version != r.ExpectedVersion || current.Epoch != r.ExpectedEpoch {
		return current, ErrRevisionConflict
	}
	if current.Version == math.MaxInt64 {
		return current, ErrInvalid
	}
	switch operation {
	case OperationBeginRetirement:
		if current.State != BuildAccepting {
			return current, ErrRequestConflict
		}
		if current.Epoch == math.MaxInt64 {
			return current, ErrInvalid
		}
		current.CutoffEpoch = current.Epoch
		current.Epoch++
		current.State = BuildRetiring
	case OperationFinalizeRetirement:
		if current.State != BuildRetiring {
			return current, ErrRequestConflict
		}
		if !blockers.Empty() {
			return current, ErrRetirementBlocked
		}
		current.State = BuildRetired
	case OperationAbortRetirement:
		if current.State == BuildAccepting {
			return current, ErrRequestConflict
		}
		if current.Epoch == math.MaxInt64 {
			return current, ErrInvalid
		}
		current.Epoch++
		current.CutoffEpoch = 0
		current.State = BuildAccepting
	default:
		return current, ErrInvalid
	}
	current.Version++
	current.ChangedAt = now
	return current, nil
}
