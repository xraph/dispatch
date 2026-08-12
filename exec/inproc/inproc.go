package inproc

import (
	"context"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Name is the identifier this executor registers under.
const Name = "inprocess"

// Executor runs handlers in the worker process.
type Executor struct {
	registry *job.Registry
}

var _ exec.Executor = (*Executor)(nil)

// New creates an in-process executor backed by a handler registry.
func New(r *job.Registry) *Executor {
	return &Executor{registry: r}
}

// Name identifies the executor.
func (e *Executor) Name() string { return Name }

// Level reports that this executor provides no isolation.
func (e *Executor) Level() exec.Level { return exec.LevelNone }

// Run looks the handler up by name and calls it.
func (e *Executor) Run(ctx context.Context, req *exec.Request) (*exec.Result, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	handler, ok := e.registry.Get(req.Name)
	if !ok {
		// The handler never ran, so this is a launch failure rather than
		// a job failure, and must not consume the retry budget.
		return &exec.Result{
			Status:     exec.StatusLaunchFailed,
			HandlerErr: "no handler registered for job " + req.Name,
		}, nil
	}

	start := time.Now()
	err := handler(ctx, req.Payload)
	elapsed := time.Since(start)

	res := &exec.Result{
		Status: exec.StatusOK,
		Usage:  exec.Usage{WallTime: elapsed},
	}
	if err != nil {
		res.Status = exec.StatusHandlerError
		res.HandlerErr = err.Error()
	}

	return res, nil
}

// Reclaim is a no-op. An in-process handler cannot outlive the worker
// that called it, so there is never anything to reclaim.
func (e *Executor) Reclaim(context.Context, id.WorkerID) error { return nil }

// Close is a no-op. The executor owns no resources of its own.
func (e *Executor) Close() error { return nil }
