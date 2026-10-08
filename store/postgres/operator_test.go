//go:build integration

package postgres_test

import (
	"testing"

	"github.com/xraph/dispatch/store/storetest"
)

// The operator action suites share one container each, the way
// TestListConformance does: every case works on entries, crons and runs it
// created itself and never asserts a total.

func TestDLQReplayConformance(t *testing.T) {
	shared := setupTestStore(t)

	storetest.RunDLQReplaySuite(t, func(t *testing.T) storetest.DLQReplayStore {
		t.Helper()

		return shared
	})
}

func TestCronConformance(t *testing.T) {
	shared := setupTestStore(t)

	storetest.RunCronSuite(t, func(t *testing.T) storetest.CronStore {
		t.Helper()

		return shared
	})
}

func TestWorkflowConformance(t *testing.T) {
	shared := setupTestStore(t)

	storetest.RunWorkflowSuite(t, func(t *testing.T) storetest.WorkflowStore {
		t.Helper()

		return shared
	})
}
