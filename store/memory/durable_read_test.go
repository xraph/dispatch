package memory

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestDurableReads(t *testing.T) {
	s := New()
	durabletest.RunReads(t, s, func(keys []durable.Key) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, key := range keys {
			s.executions[key].execution.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			s.executions[key].execution.FirstStartedAt = s.executions[key].execution.CreatedAt
			s.executions[key].execution.RunAvailableAt = s.executions[key].execution.CreatedAt
		}
	})
}
