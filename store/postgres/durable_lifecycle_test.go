package postgres_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch/durable/durabletest"
)

func TestRetirementEnrollment(t *testing.T) {
	dsn := os.Getenv("DISPATCH_LIFECYCLE_TEST_DSN")
	if dsn == "" {
		if os.Getenv("DISPATCH_LIFECYCLE_REQUIRED") == "1" {
			t.Fatal("DISPATCH_LIFECYCLE_TEST_DSN required")
		}
		t.Skip("dedicated PostgreSQL fixture required")
	}
	durabletest.RunRetirementEnrollment(t, openWakeStore(t, dsn))
}

func TestBuildRetirement(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunBuildRetirement(t, s)
}
func TestRetiringContinuationLineage(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunRetiringContinuationLineage(t, s)
}
func TestRetirementLateChildBlocker(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunRetirementLateChildBlocker(t, s)
}

// Reuse the durable conformance suite on the explicitly supplied bounded fixture.
// This covers unenrolled compatibility after the retirement expansion migrations.
func TestRetirementExpansionConformance(t *testing.T) {
	_, dsn, _, _ := retirementFixture(t)
	admin := retirementConn(t, dsn)
	name := pgx.Identifier{fmt.Sprintf("dispatch_conformance_%d", time.Now().UnixNano())}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = name[1 : len(name)-1]
	durabletest.Run(t, openWakeStore(t, parsed.String()))
}

func TestWorkflowTaskDeferral(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunWorkflowTaskDeferral(t, s)
}
