package memory_test

import (
	"testing"

	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/store/storetest"
)

func TestDLQReplayConformance(t *testing.T) {
	storetest.RunDLQReplaySuite(t, func(_ *testing.T) storetest.DLQReplayStore {
		return memory.New()
	})
}

func TestCronConformance(t *testing.T) {
	storetest.RunCronSuite(t, func(_ *testing.T) storetest.CronStore {
		return memory.New()
	})
}

func TestWorkflowConformance(t *testing.T) {
	storetest.RunWorkflowSuite(t, func(_ *testing.T) storetest.WorkflowStore {
		return memory.New()
	})
}
