package memory_test

import (
	"testing"

	"github.com/xraph/dispatch/durable/durabletest"
	"github.com/xraph/dispatch/store/memory"
)

func TestDurableOutbox(t *testing.T) { durabletest.RunOutbox(t, memory.New()) }

func TestDurableAuditedConformance(t *testing.T) {
	durabletest.Run(t, durabletest.RegisteredAuditStore{AuditStore: memory.New()})
}
