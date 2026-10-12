package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

var ErrDeferralUnknown = errors.New("durable runtime: task deferral outcome unknown")

// deferWorkflow holds renewal ownership until exact-request reconciliation ends.
// An unknown transfer cannot be followed by an ordinary renewal of the old token.
func (w *Worker) deferWorkflow(ctx context.Context, task durable.Task, revision int64, refusal *durable.BuildAdmissionError, lease *taskLease) error {
	store, ok := w.store.(durable.WorkflowTaskDeferralStore)
	if !ok {
		return fmt.Errorf("%w: workflow task deferral capability missing", durable.ErrWriterCompatibility)
	}
	request, err := durable.NewWorkflowTaskDeferralRequest(task, revision, refusal)
	if err != nil {
		return err
	}
	if gateErr := takeGate(ctx, lease.gate); gateErr != nil {
		return gateErr
	}
	defer func() { lease.gate <- struct{}{} }()
	unknown := false
	var last error
	for attempt := range 3 {
		if ctx.Err() != nil {
			last = context.Cause(ctx)
			break
		}
		sent := false
		_, last = storeCall(ctx, w, func(callCtx context.Context) (durable.WorkflowTaskDeferralReceipt, error) {
			if callCtx.Err() != nil {
				return durable.WorkflowTaskDeferralReceipt{}, context.Cause(callCtx)
			}
			sent = true
			return store.DeferWorkflowTask(callCtx, request)
		})
		if last == nil {
			lease.detached = true
			return nil
		}
		if (!unknown && definitiveCommitError(last)) || deferralConditionError(last) {
			return last
		}
		unknown = unknown || sent
		if attempt < 2 {
			if waitErr := wait(ctx, time.Duration(attempt+1)*10*time.Millisecond); waitErr != nil {
				last = waitErr
				break
			}
		}
	}
	if unknown {
		// Retain truthful failure even for direct RunOnce, after this active claim
		// leaves accounting. A later drain cannot certify the unresolved transfer.
		l := &w.lifecycle
		l.mu.Lock()
		l.state, l.failure = WorkerFailed, "deferral_unknown"
		l.closeAdmission()
		l.mu.Unlock()
		unknownErr := errors.Join(ErrDeferralUnknown, last)
		lease.cancel(unknownErr)
		return unknownErr
	}
	return last
}

// These conditions follow exact receipt recovery in the deferral store contract.
// Coordination, compatibility and corruption failures can precede that recovery.
func deferralConditionError(err error) bool {
	return errors.Is(err, durable.ErrAdmissionChanged) || errors.Is(err, durable.ErrRevisionConflict) || errors.Is(err, durable.ErrTaskConflict) || errors.Is(err, durable.ErrLeaseLost) || errors.Is(err, durable.ErrTaskDeadline) || errors.Is(err, durable.ErrExecutionDeadline) || errors.Is(err, durable.ErrClosed) || errors.Is(err, durable.ErrRequestConflict)
}
