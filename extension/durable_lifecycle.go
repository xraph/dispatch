package extension

import (
	"context"

	"github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
)

// DurableStatus reports process readiness separately from storage Health.
func (e *Extension) DurableStatus() (runtime.WorkerStatus, error) {
	if e.eng == nil {
		return runtime.WorkerStatus{}, engine.ErrDurableDisabled
	}
	return e.eng.DurableStatus()
}

// BeginDurableDrain keeps the operator boundary and publisher available while
// the explicitly bounded operation finishes. Stop by itself is a terminal stop.
func (e *Extension) BeginDurableDrain(ctx context.Context, request runtime.DrainRequest) (runtime.DrainHandle, error) {
	if err := e.lockLifecycle(ctx); err != nil {
		return runtime.DrainHandle{}, err
	}
	defer e.unlockLifecycle()
	if e.eng == nil {
		return runtime.DrainHandle{}, engine.ErrDurableDisabled
	}
	return e.eng.BeginDurableDrain(ctx, request)
}

// WaitDurableDrain does not inherit cancellation from another observer.
func (e *Extension) WaitDurableDrain(ctx context.Context, handle runtime.DrainHandle) (runtime.DrainResult, error) {
	if e.eng == nil {
		return runtime.DrainResult{}, engine.ErrDurableDisabled
	}
	return e.eng.WaitDurableDrain(ctx, handle)
}

// DurableReadiness leaves database liveness and trusted host qualification separate.
func (e *Extension) DurableReadiness(ctx context.Context) (runtime.WorkerReadiness, error) {
	if e.eng == nil {
		return runtime.WorkerReadiness{}, engine.ErrDurableDisabled
	}
	return e.eng.DurableReadiness(ctx)
}
