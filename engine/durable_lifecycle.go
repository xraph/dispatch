package engine

import (
	"context"
	"errors"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

// DurableStatus reports worker readiness independently of Health's database ping.
func (eng *Engine) DurableStatus() (drt.WorkerStatus, error) {
	worker := eng.DurableWorker()
	if worker == nil {
		return drt.WorkerStatus{}, ErrDurableDisabled
	}
	return worker.Status(), nil
}

// BeginDurableDrain closes polling with an explicit operation-owned deadline.
// Publisher delivery, authorized queries and storage remain available.
func (eng *Engine) BeginDurableDrain(ctx context.Context, request drt.DrainRequest) (drt.DrainHandle, error) {
	if err := eng.lockLifecycle(ctx); err != nil {
		return drt.DrainHandle{}, err
	}
	defer eng.unlockLifecycle()
	worker := eng.DurableWorker()
	if worker == nil {
		return drt.DrainHandle{}, ErrDurableDisabled
	}
	if eng.stopping || eng.stopped {
		if worker.Status().Drain == nil {
			return drt.DrainHandle{}, durable.ErrClosed
		}
	}
	return worker.BeginDrain(ctx, request)
}

// WaitDurableDrain only waits. Its context never shortens an accepted drain.
func (eng *Engine) WaitDurableDrain(ctx context.Context, handle drt.DrainHandle) (drt.DrainResult, error) {
	worker := eng.DurableWorker()
	if worker == nil {
		return drt.DrainResult{}, ErrDurableDisabled
	}
	return worker.WaitDrain(ctx, handle)
}

// waitDurableDrain precedes terminal quiescence when the host explicitly began
// a graceful drain. Stop alone retains its existing force-stop semantics.
func (eng *Engine) waitDurableDrain(ctx context.Context) error {
	worker := eng.DurableWorker()
	if worker == nil {
		return nil
	}
	status := worker.Status()
	if status.Drain == nil {
		return nil
	}
	_, err := worker.WaitDrain(ctx, *status.Drain)
	if errors.Is(err, drt.ErrDrainIncomplete) {
		// stopDurable retains the incomplete result after actual quiescence.
		return nil
	}
	return err
}
