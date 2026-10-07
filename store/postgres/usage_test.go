//go:build integration

package postgres_test

import (
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/job/jobtest"
)

// TestUsageRecorderConformance runs the shared job.UsageRecorder suite
// against Postgres. Each subtest gets its own container because the
// suite asserts absolute row counts.
func TestUsageRecorderConformance(t *testing.T) {
	jobtest.RunUsageRecorderSuite(t, func() job.UsageRecorder {
		return setupTestStore(t)
	})
}
