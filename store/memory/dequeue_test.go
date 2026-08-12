package memory_test

import (
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/store/storetest"
)

// TestDequeueConformance runs the resource-aware dequeue suite against the
// memory store — the reference implementation the SQL and document backends
// (Tasks 15-18) are checked against.
func TestDequeueConformance(t *testing.T) {
	storetest.RunDequeueSuite(t, func(t *testing.T) job.Store {
		t.Helper()

		return memory.New()
	})
}
