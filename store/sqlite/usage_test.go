package sqlite_test

import (
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/job/jobtest"
)

// TestUsageRecorderConformance runs the shared job.UsageRecorder suite
// against SQLite.
func TestUsageRecorderConformance(t *testing.T) {
	jobtest.RunUsageRecorderSuite(t, func() job.UsageRecorder {
		return openSqliteStore(t)
	})
}
