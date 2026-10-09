package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

// Durable configuration errors prevent fallback to checkpoint execution.
var (
	ErrDurableUnsupported = errors.New("dispatch: store does not implement durable.Store")
	ErrDurableDisabled    = errors.New("dispatch: durable workflows are not enabled")
)

type durableEngine struct {
	options drt.Options
	worker  *drt.Worker
	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	stopped bool
	err     error
}

// WithDurableWorkflows enables the history-based runtime alongside the existing
// checkpoint workflow API. Its store must explicitly implement durable.Store.
func WithDurableWorkflows(options drt.Options) Option {
	return func(eng *Engine) { eng.durable = &durableEngine{options: options} }
}

func (eng *Engine) buildDurable(store any) error {
	if eng.durable == nil {
		return nil
	}
	backend, ok := store.(durable.Store)
	if !ok {
		return ErrDurableUnsupported
	}
	worker, err := drt.NewWorker(backend, eng.durable.options)
	if err != nil {
		return fmt.Errorf("configure durable workflows: %w", err)
	}
	eng.durable.worker = worker
	return nil
}

// DurableWorker returns the configured runtime, or nil when disabled. Engine
// owns its polling lifecycle.
func (eng *Engine) DurableWorker() *drt.Worker {
	if eng.durable == nil {
		return nil
	}
	return eng.durable.worker
}

// StartDurableWorkflow persists the run and its initial task. Calls may precede
// Engine.Start. The caller supplies stable IDs so ambiguous outcomes are retryable.
func (eng *Engine) StartDurableWorkflow(ctx context.Context, request durable.StartRequest) (durable.Receipt, error) {
	worker := eng.DurableWorker()
	if worker == nil {
		return durable.Receipt{}, ErrDurableDisabled
	}
	return worker.StartExecution(ctx, request)
}

// CompleteDurableActivity publishes an asynchronous result or failure. Authorize
// callers before exposing this trusted Go API through a remote endpoint.
func (eng *Engine) CompleteDurableActivity(ctx context.Context, request drt.AsyncCompletionRequest) (durable.Receipt, error) {
	worker := eng.DurableWorker()
	if worker == nil {
		return durable.Receipt{}, ErrDurableDisabled
	}
	return worker.CompleteAsyncActivity(ctx, request)
}

// HeartbeatDurableActivity persists progress for a deferred activity. The caller
// supplies a stable request ID and the next consecutive heartbeat sequence.
func (eng *Engine) HeartbeatDurableActivity(ctx context.Context, request drt.AsyncHeartbeatRequest) (durable.Receipt, error) {
	worker := eng.DurableWorker()
	if worker == nil {
		return durable.Receipt{}, ErrDurableDisabled
	}
	return worker.HeartbeatAsyncActivity(ctx, request)
}

// SignalDurableWorkflow accepts a message for an explicit or current open run.
// You must authorize callers before exposing this trusted Go method remotely.
func (eng *Engine) SignalDurableWorkflow(ctx context.Context, request durable.SignalRequest) (durable.SignalReceipt, error) {
	worker := eng.DurableWorker()
	if worker == nil {
		return durable.SignalReceipt{}, ErrDurableDisabled
	}
	return worker.SignalExecution(ctx, request)
}

// SignalWithStartDurableWorkflow accepts a message and creates a run when needed.
// Reuse the whole request to recover its original target after a lost response.
func (eng *Engine) SignalWithStartDurableWorkflow(ctx context.Context, request durable.SignalWithStartRequest) (durable.SignalReceipt, error) {
	worker := eng.DurableWorker()
	if worker == nil {
		return durable.SignalReceipt{}, ErrDurableDisabled
	}
	return worker.SignalWithStart(ctx, request)
}

// QueryDurableWorkflow reconstructs one run without claiming tasks or writing
// history. Authorize callers before exposing this trusted Go API remotely.
func (eng *Engine) QueryDurableWorkflow(ctx context.Context, request drt.QueryRequest) (drt.QueryResult, error) {
	worker := eng.DurableWorker()
	if worker == nil {
		return drt.QueryResult{}, ErrDurableDisabled
	}
	return worker.QueryExecution(ctx, request)
}

func (eng *Engine) startDurable(ctx context.Context) error {
	d := eng.durable
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return durable.ErrClosed
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if d.done != nil {
		return d.err
	}
	workCtx, cancel := context.WithCancel(ctx)
	d.cancel, d.done = cancel, make(chan struct{})
	go func() {
		err := d.worker.Run(workCtx)
		d.mu.Lock()
		d.err = err
		d.mu.Unlock()
		if err != nil {
			eng.logger.Error("durable workflow worker stopped", log.String("error", err.Error()))
		}
		close(d.done)
	}()
	return nil
}

func (eng *Engine) stopDurable(ctx context.Context) error {
	d := eng.durable
	if d == nil {
		return nil
	}
	d.mu.Lock()
	d.stopped = true
	if d.cancel != nil {
		d.cancel()
	}
	done := d.done
	d.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return eng.durableError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (eng *Engine) durableError() error {
	if eng.durable == nil {
		return nil
	}
	eng.durable.mu.Lock()
	defer eng.durable.mu.Unlock()
	return eng.durable.err
}
