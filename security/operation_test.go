package security_test

import (
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/security"
)

func TestAuditSelectorsRejectUnsafeOrUnboundedValues(t *testing.T) {
	for _, raw := range []string{"credential\nvalue", strings.Repeat("x", 97), " surrounding "} {
		if target, err := security.CreationTarget("job-selector", raw, "default"); err == nil || target != "invalid-target" {
			t.Fatal(raw, target, err)
		}
	}
	if _, err := security.ResourceTarget(id.PrefixJob, id.NewRunID().String()); err == nil {
		t.Fatal("foreign identifier type accepted")
	}
	if _, err := security.BulkTarget("queue", 1001, time.Time{}, false); err == nil {
		t.Fatal("unbounded replay accepted")
	}
	before := time.Date(2026, 10, 9, 12, 0, 0, 0, time.FixedZone("offset", 3600))
	target, err := security.BulkTarget("", 0, before, true)
	if err != nil || target != `{"kind":"dlq-selection","before":"2026-10-09T11:00:00Z","dry_run":true}` {
		t.Fatal(target, err)
	}
}
