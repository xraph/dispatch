package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/xraph/dispatch/durable"
)

// ErrHandlerNotFound leaves a task available for a worker with compatible code.
var ErrHandlerNotFound = errors.New("durable runtime: handler not registered")

// ActivityFunc performs external work under a renewable, fenced task lease.
// Observe cancellation, and make external effects idempotent using ActivityInfo.
type ActivityFunc func(context.Context, ActivityInfo, []byte) ([]byte, error)

// ActivityInfo identifies an operation independently of its retry attempt.
type ActivityInfo struct {
	Key       durable.Key
	CommandID string
	BuildID   string
	// Attempt counts durable starts for version 2 activities. Legacy activities
	// expose the task claim count, which can include claims lost before execution.
	Attempt          int64
	deferCompletion  func(context.Context) (AsyncActivityHandle, error)
	heartbeat        func(context.Context, []byte) error
	heartbeatDetails []byte
}

// IdempotencyKey is stable across attempts, and distinct across runs/namespaces.
func (a ActivityInfo) IdempotencyKey() string {
	key := fmt.Sprintf("%q/%q/%q/%q", a.Key.Namespace, a.Key.WorkflowID, a.Key.RunID, a.CommandID)
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Options pin a worker to a namespace, queue and build. Register all handlers
// before construction; NewWorker copies the maps. Concurrency is per task kind.
type Options struct {
	Namespace     string
	Queue         string
	BuildID       string
	Owner         string
	LeaseDuration time.Duration
	PollInterval  time.Duration
	StoreTimeout  time.Duration
	Concurrency   int
	Workflows     map[string]WorkflowFunc
	Activities    map[string]ActivityFunc
}

// Worker claims durable tasks. Multiple workers may share a queue and store.
type Worker struct {
	store   durable.Store
	options Options
}

// NewWorker validates routing and timing before any task can be claimed.
func NewWorker(store durable.Store, options Options) (*Worker, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: execution store is required", durable.ErrInvalid)
	}
	if options.LeaseDuration == 0 {
		options.LeaseDuration = 30 * time.Second
	}
	if options.StoreTimeout == 0 {
		options.StoreTimeout = min(5*time.Second, options.LeaseDuration/4)
	}
	if options.PollInterval == 0 {
		options.PollInterval = 100 * time.Millisecond
	}
	if options.Concurrency == 0 {
		options.Concurrency = 1
	}
	claim := durable.ClaimRequest{Namespace: options.Namespace, Queue: options.Queue,
		BuildID: options.BuildID, Owner: options.Owner, Kind: durable.TaskWorkflow, LeaseDuration: options.LeaseDuration}
	if err := claim.Validate(); err != nil {
		return nil, err
	}
	if options.BuildID == "" || !validID(options.Queue) || options.LeaseDuration < 30*time.Millisecond || options.StoreTimeout <= 0 ||
		options.StoreTimeout > options.LeaseDuration/3 || options.PollInterval <= 0 || options.Concurrency < 1 || options.Concurrency > 256 {
		return nil, fmt.Errorf("%w: invalid worker build, timing or concurrency", durable.ErrInvalid)
	}
	for name, handler := range options.Workflows {
		if !validID(name) || handler == nil {
			return nil, fmt.Errorf("%w: invalid workflow registration", durable.ErrInvalid)
		}
	}
	for name, handler := range options.Activities {
		if !validID(name) || handler == nil {
			return nil, fmt.Errorf("%w: invalid activity registration", durable.ErrInvalid)
		}
	}
	options.Workflows, options.Activities = maps.Clone(options.Workflows), maps.Clone(options.Activities)
	return &Worker{store: store, options: options}, nil
}

// StartExecution validates the worker's namespace/build and persists its first
// task. Reuse the entire request when the outcome of a start is unknown.
func (w *Worker) StartExecution(ctx context.Context, request durable.StartRequest) (durable.Receipt, error) {
	if request.Namespace != w.options.Namespace || request.BuildID != w.options.BuildID || request.Queue != w.options.Queue {
		return durable.Receipt{}, fmt.Errorf("%w: start does not match worker namespace, queue and build", durable.ErrInvalid)
	}
	if w.options.Workflows[request.WorkflowType] == nil {
		return durable.Receipt{}, fmt.Errorf("%w: workflow %q", ErrHandlerNotFound, request.WorkflowType)
	}
	return storeCall(ctx, w, func(callCtx context.Context) (durable.Receipt, error) {
		return w.store.StartExecution(callCtx, request)
	})
}

// Run polls each task kind until cancellation or the first processing error.
// Processing errors are returned to the supervisor, never silently discarded.
// Cancellation leaves unfinished tasks for replacement workers after expiry.
func (w *Worker) Run(ctx context.Context) error {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	failures := make(chan error, 1)
	var group sync.WaitGroup
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity, durable.TaskTimer, TaskTimeout, TaskChildDelivery} {
		for range w.options.Concurrency {
			group.Go(func() {
				for workCtx.Err() == nil {
					worked, err := w.RunOnce(workCtx, kind)
					if err != nil && !normalContention(err) {
						if workCtx.Err() == nil {
							select {
							case failures <- err:
							default:
							}
							cancel()
						}
						return
					}
					if !worked && wait(workCtx, w.options.PollInterval) != nil {
						return
					}
				}
			})
		}
	}
	group.Wait()
	select {
	case err := <-failures:
		return err
	default:
		return nil
	}
}

// RunOnce claims and processes at most one task. Worked is true after a claim,
// including when processing fails. It is safe to call concurrently.
func (w *Worker) RunOnce(ctx context.Context, kind durable.TaskKind) (worked bool, err error) {
	if kind == TaskChildDelivery {
		return w.runChildDelivery(ctx)
	}
	task, err := storeCall(ctx, w, func(callCtx context.Context) (*durable.Task, error) {
		if kind == TaskTimeout {
			return w.store.ClaimTimeoutTask(callCtx, durable.TimeoutClaimRequest{Namespace: w.options.Namespace, BuildID: w.options.BuildID, Owner: w.options.Owner, LeaseDuration: w.options.LeaseDuration})
		}
		return w.store.ClaimTask(callCtx, durable.ClaimRequest{Namespace: w.options.Namespace,
			Queue: w.options.Queue, BuildID: w.options.BuildID, Kind: kind, Owner: w.options.Owner,
			LeaseDuration: w.options.LeaseDuration})
	})
	if err != nil || task == nil {
		return false, err
	}
	lease := newTaskLease(ctx, w, *task)
	defer lease.close()
	if kind == durable.TaskWorkflow {
		err = w.processWorkflow(lease.ctx, *task)
	} else {
		err = w.processEffect(lease.ctx, *task, lease)
	}
	if err != nil && lease.ctx.Err() != nil {
		return true, context.Cause(lease.ctx)
	}
	return true, err
}

func storeCall[T any](ctx context.Context, w *Worker, call func(context.Context) (T, error)) (T, error) {
	callCtx, cancel := context.WithTimeout(ctx, w.options.StoreTimeout)
	defer cancel()
	return call(callCtx)
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func normalContention(err error) bool {
	return errors.Is(err, durable.ErrTaskDeadline) || errors.Is(err, durable.ErrTaskConflict) || errors.Is(err, durable.ErrClosed) || errors.Is(err, durable.ErrLeaseLost) || errors.Is(err, durable.ErrRevisionConflict)
}
