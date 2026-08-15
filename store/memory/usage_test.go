package memory_test

import (
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/job/jobtest"
	"github.com/xraph/dispatch/store/memory"
)

func TestUsageRecorderConformance(t *testing.T) {
	jobtest.RunUsageRecorderSuite(t, func() job.UsageRecorder { return memory.New() })
}
