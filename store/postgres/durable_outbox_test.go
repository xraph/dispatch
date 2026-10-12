//go:build integration

package postgres_test

import (
	"testing"

	"github.com/xraph/dispatch/durable/durabletest"
)

func TestDurableOutbox(t *testing.T) { durabletest.RunOutbox(t, setupTestStore(t)) }

func TestDurableAuditedConformance(t *testing.T) {
	durabletest.Run(t, durabletest.RegisteredAuditStore{AuditStore: setupTestStore(t)})
}
