//go:build linux

package managedhost

import (
	"context"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

type remoteWorkerControl struct {
	host     *RuntimeHost
	id       string
	identity durable.QueryRuntimeIdentity
	binding  launcher.ServiceBinding
	queue    string
}

// WorkerControl fixes a handle to one authenticated creation and process boot.
// Each operation checks the same binding before and after its challenged response.
func (h *RuntimeHost) WorkerControl(ctx context.Context, target durable.QueryRuntimeTarget) (operator.WorkerControl, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, identity, err := h.fixed(target)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	slot := h.slots[id]
	if slot == nil || slot.binding == nil {
		return nil, durable.ErrQueryRetention
	}
	return &remoteWorkerControl{host: h, id: id, identity: identity, binding: *slot.binding, queue: slot.startup.Queue}, nil
}
func (c *remoteWorkerControl) unchanged() bool {
	c.host.mu.Lock()
	defer c.host.mu.Unlock()
	slot := c.host.slots[c.id]
	return slot != nil && slot.binding != nil && *slot.binding == c.binding && slot.startup.Queue == c.queue
}
func (c *remoteWorkerControl) observe(ctx context.Context, kind string) (ServiceResponse, error) {
	if !c.unchanged() {
		return ServiceResponse{}, durable.ErrQueryRetention
	}
	response, err := c.host.call(ctx, c.id, ServiceCommand{Kind: kind})
	if err != nil {
		return ServiceResponse{}, err
	}
	if !c.unchanged() {
		return ServiceResponse{}, durable.ErrQueryRetention
	}
	return response, nil
}
func (c *remoteWorkerControl) Status(ctx context.Context) (drt.WorkerStatus, error) {
	response, err := c.observe(ctx, "status")
	if err != nil {
		return drt.WorkerStatus{}, err
	}
	if response.Status == nil || !workerMatches(*response.Status, c.identity) || response.Status.Queue != c.queue {
		return drt.WorkerStatus{}, durable.ErrQueryRetention
	}
	return *response.Status, nil
}
func (c *remoteWorkerControl) Readiness(ctx context.Context) (drt.WorkerReadiness, error) {
	response, err := c.observe(ctx, "readiness")
	if err != nil {
		return drt.WorkerReadiness{}, err
	}
	if response.Readiness == nil || !workerMatches(response.Readiness.Worker, c.identity) || response.Readiness.Worker.Queue != c.queue {
		return drt.WorkerReadiness{}, durable.ErrQueryRetention
	}
	return *response.Readiness, nil
}
func (c *remoteWorkerControl) BeginDrain(ctx context.Context, request drt.DrainRequest) (drt.DrainHandle, error) {
	if !c.unchanged() {
		return drt.DrainHandle{}, durable.ErrQueryRetention
	}
	result, err := c.host.BeginDrain(ctx, c.identity.QueryRuntimeTarget, request)
	if err != nil {
		return drt.DrainHandle{}, err
	}
	if !c.unchanged() {
		return drt.DrainHandle{}, durable.ErrQueryRetention
	}
	return result, nil
}
func (c *remoteWorkerControl) WaitDrain(ctx context.Context, handle drt.DrainHandle) (drt.DrainResult, error) {
	if !c.unchanged() {
		return drt.DrainResult{}, durable.ErrQueryRetention
	}
	result, err := c.host.WaitDrain(ctx, c.identity.QueryRuntimeTarget, handle)
	if err != nil {
		return drt.DrainResult{}, err
	}
	if !c.unchanged() {
		return drt.DrainResult{}, durable.ErrQueryRetention
	}
	return result, nil
}

var _ operator.WorkerControl = (*remoteWorkerControl)(nil)
