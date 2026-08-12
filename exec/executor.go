package exec

import (
	"context"

	"github.com/xraph/dispatch/id"
)

// Executor runs one job attempt. Implementations form an escalating ladder
// of isolation, and every one of them must pass the shared conformance
// suite in exec/exectest.
type Executor interface {
	// Name identifies the executor in configuration, logs, and metrics.
	Name() string

	// Level reports the isolation this executor actually provides, which
	// is what Registry.Select matches a Policy against.
	Level() Level

	// Run executes one attempt.
	//
	// The returned error is reserved for failures to launch — the handler
	// never ran. A handler that ran and failed is reported through
	// Result.Status, so the caller can tell a business failure from a
	// dead sandbox without inspecting error text.
	Run(ctx context.Context, req *Request) (*Result, error)

	// Reclaim releases sandboxes this worker leaked across a restart. It
	// runs once when the pool starts, and on the leader's behalf for
	// workers the cluster has declared dead.
	Reclaim(ctx context.Context, workerID id.WorkerID) error

	// Close releases the executor's own resources.
	Close() error
}
