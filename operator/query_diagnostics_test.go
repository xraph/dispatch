package operator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestQueryRejectionObserverCannotChangeResult(t *testing.T) {
	s, _, host, grants := queryFixture(t)
	in := QueryRuntimeCommand{QueryRuntimeInput: queryInput("observed"), RequestID: "register", ExpectedVersion: "0"}
	if _, err := s.RegisterQueryRuntime(t.Context(), reader(), in); err != nil {
		t.Fatal(err)
	}
	host.failure = durable.NewQueryRejection(durable.ErrQueryRetention, durable.QueryRejectionDiagnostic{Stage: "host_drain", Reason: "admission"})
	calls := 0
	s.queryRejection = func(_ context.Context, d durable.QueryRejectionDiagnostic) {
		calls++
		if d.Target.RuntimeID != "observed" || d.Reason != "admission" {
			t.Error("lost rejection context")
		}
		d.Reason = "mutated"
		panic("private observer panic")
	}
	in.RequestID, in.ExpectedVersion = "verify", "1"
	_, err := s.VerifyQueryRuntime(t.Context(), reader(), in)
	//nolint:errorlint // Public normalization must return the original sentinel.
	if err != durable.ErrQueryRetention || calls != 1 {
		t.Fatalf("observer changed public result: %v calls=%d", err, calls)
	}
	(*grants)["allowed"] = false
	if _, err = s.VerifyQueryRuntime(t.Context(), reader(), in); errors.Is(err, durable.ErrQueryRetention) || calls != 1 {
		t.Fatal("diagnostic disclosed before authorization")
	}
}

type aheadQueryHost struct{ *queryHostFixture }

func (h aheadQueryHost) Verify(_ context.Context, b durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error) {
	p := durabletest.QueryProofFixture(b.QueryRuntimeIdentity, "future")
	p.VerifiedAt = p.VerifiedAt.Add(time.Hour)
	p.ValidUntil = p.VerifiedAt.Add(time.Second)
	return p, nil
}
func TestQueryRejectionObserverReceivesStoreFailureOutsideLock(t *testing.T) {
	s, store, host, _ := queryFixture(t)
	in := QueryRuntimeCommand{QueryRuntimeInput: queryInput("store-observed"), RequestID: "register", ExpectedVersion: "0"}
	if _, err := s.RegisterQueryRuntime(t.Context(), reader(), in); err != nil {
		t.Fatal(err)
	}
	s.queryHost = aheadQueryHost{host}
	observed := false
	s.queryRejection = func(ctx context.Context, d durable.QueryRejectionDiagnostic) {
		observed = d.Stage == "proof" && d.Reason == "verified_future" && !d.ObservedAt.IsZero()
		if _, err := store.ListQueryRuntimes(ctx, durable.QueryRuntimeList{NamespaceTarget: d.Target.NamespaceTarget, Limit: 10}); err != nil {
			t.Error(err)
		}
		panic("observer must not change store refusal")
	}
	in.RequestID, in.ExpectedVersion = "verify", "1"
	//nolint:errorlint // Public normalization must return the original sentinel.
	if _, err := s.VerifyQueryRuntime(t.Context(), reader(), in); err != durable.ErrQueryRetention || !observed {
		t.Fatalf("store diagnostic/public error lost: %v", err)
	}
}
