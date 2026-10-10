package memory

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestQueryReceiptRecoveryEvidence(t *testing.T) {
	s := New()
	durabletest.RunQueryReceiptRecovery(t, s, func(t *testing.T, r durable.LifecycleReceipt) {
		t.Helper()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.lifecycleReceipts[lifecycleReceiptKey{r.Namespace, r.Operation, r.RequestID}] = r.Clone()
	})
}

type identityBarrierContext struct {
	context.Context
	once     sync.Once
	captured chan struct{}
	release  chan struct{}
}

func (c *identityBarrierContext) Err() error {
	c.once.Do(func() { close(c.captured); <-c.release })
	return c.Context.Err()
}
func TestQueryBuildIdentityCapturedBeforeCoordination(t *testing.T) {
	s := New()
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: "capture"}, BuildID: "b"}
	if _, err := s.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: target.Namespace, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	original := durabletest.QueryIdentityFixture()
	callerOwned := original
	request := durable.RegisterBuildRequest{BuildTarget: target, RequestID: "capture", Identity: &callerOwned}
	digest, err := durable.Fingerprint(string(durable.OperationRegisterBuild), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &identityBarrierContext{Context: t.Context(), captured: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	var receipt durable.LifecycleReceipt
	var callErr error
	go func() { receipt, callErr = s.RegisterBuild(ctx, request); close(done) }()
	<-ctx.captured // Validation/fingerprint have finished; mutation holds coordination.
	callerOwned.ArtifactDigest = strings.Repeat("f", 64)
	close(ctx.release)
	<-done
	if callErr != nil || receipt.Build == nil || receipt.Build.QueryIdentity != original || receipt.RequestDigest != digest {
		t.Fatalf("caller changed captured identity: %+v %v", receipt, callErr)
	}
	request.Identity = &original
	replay, err := s.RegisterBuild(t.Context(), request)
	if err != nil || !reflect.DeepEqual(replay, receipt) {
		t.Fatalf("original request replay changed: %+v %v", replay, err)
	}
}

func TestQueryMutationIntentRollback(t *testing.T) {
	s := New()
	durabletest.RunQueryIntentRollback(t, s, func(t *testing.T, _ durable.NamespaceTarget) any {
		t.Helper()
		s.mu.RLock()
		defer s.mu.RUnlock()
		receipts := maps.Clone(s.lifecycleReceipts)
		for key, r := range receipts {
			receipts[key] = r.Clone()
		}
		return []any{maps.Clone(s.queryRuntimes), receipts, maps.Clone(s.outbox)}
	}, func(t *testing.T, action string) func() {
		t.Helper()
		s.outboxPrepare = func(d durable.Delivery) error {
			if d.Action == action {
				return errors.New("injected query intent failure")
			}
			return nil
		}
		return func() { s.outboxPrepare = nil }
	})
}
