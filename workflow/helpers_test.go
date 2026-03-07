package workflow_test

import (
	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// testLogger returns a silent logger for tests.
func testLogger() log.Logger {
	return log.NewNoopLogger()
}

// newTestRunnerWithStore creates a runner using an explicit store.
func newTestRunnerWithStore(s *memory.Store) (*workflow.Runner, *workflow.Registry) {
	reg := workflow.NewRegistry()
	logger := testLogger()
	runner := workflow.NewRunner(reg, s, s, noopEmitter{}, logger)
	return runner, reg
}
