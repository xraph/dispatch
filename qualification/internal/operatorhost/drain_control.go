package operatorhost

import (
	"context"

	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
)

// DrainObserver provides private qualification barriers around process invocation.
// The durable receipt already exists when BeforeDrain runs. Returning an error
// never proves that an earlier invocation did not happen.
type DrainObserver interface {
	BeforeDrain(context.Context, drt.DrainRequest) error
	AfterDrain(context.Context, drt.DrainHandle) error
}

type observedWorkerControl struct {
	operator.WorkerControl
	observer DrainObserver
}

func (c observedWorkerControl) BeginDrain(ctx context.Context, request drt.DrainRequest) (drt.DrainHandle, error) {
	if err := c.observer.BeforeDrain(ctx, request); err != nil {
		return drt.DrainHandle{}, err
	}
	handle, err := c.WorkerControl.BeginDrain(ctx, request)
	if err != nil {
		return handle, err
	}
	if err := c.observer.AfterDrain(ctx, handle); err != nil {
		return drt.DrainHandle{}, err
	}
	return handle, nil
}
