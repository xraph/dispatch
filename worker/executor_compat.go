package worker

import (
	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
)

// Executor is the former name of Runner.
//
// The type was renamed because it orchestrates an attempt — middleware,
// retry, DLQ, state, events — and was never the thing that invokes the
// handler. That is now exec.Executor. This alias keeps existing code
// compiling.
//
// Deprecated: use Runner.
type Executor = Runner

// NewExecutor creates a Runner with no executor registry, so handlers are
// called directly in-process exactly as before.
//
// Deprecated: use NewRunner, which takes an *exec.Registry.
func NewExecutor(
	registry *job.Registry,
	extensions *ext.Registry,
	store job.Store,
	dlqService *dlq.Service,
	bo backoff.Strategy,
	logger log.Logger,
	mws ...middleware.Middleware,
) *Runner {
	return NewRunner(registry, extensions, store, dlqService, bo, nil, logger, mws...)
}
