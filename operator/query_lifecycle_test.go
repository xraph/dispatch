package operator

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type queryHostFixture struct {
	resolves, probes, finishes, aborts int
	failure                            error
	settled                            bool
}

func (h *queryHostFixture) ResolveBinding(_ context.Context, target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	h.resolves++
	return durable.QueryRuntimeIdentity{QueryRuntimeTarget: target, InstanceID: "instance-" + target.RuntimeID, IdentityVersion: 1, BuildIdentity: durabletest.QueryIdentityFixture()}, h.failure
}
func (h *queryHostFixture) Verify(_ context.Context, b durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error) {
	h.probes++
	return durabletest.QueryProofFixture(b.QueryRuntimeIdentity, "proof-"+b.RuntimeID), h.failure
}
func (h *queryHostFixture) VerifyRemoved(context.Context, durable.QueryRemovalFence) error {
	h.finishes++
	if !h.settled {
		return errors.New("private unknown issued operation")
	}
	return h.failure
}
func (h *queryHostFixture) VerifyAbort(_ context.Context, b durable.QueryRuntimeBinding, f durable.QueryRemovalFence) (durable.QueryRemovalSettlement, durable.QueryRuntimeVerification, error) {
	h.aborts++
	if !h.settled {
		return durable.QueryRemovalSettlement{}, durable.QueryRuntimeVerification{}, errors.New("private unknown issued operation")
	}
	digest, _ := durable.Fingerprint("query_runtime.removal_fence.v1", f)
	settlement := durable.QueryRemovalSettlement{FenceDigest: digest, VerifierID: b.BuildIdentity.VerifierID, EvidenceDigest: strings.Repeat("a", 64), Disposition: durable.QueryRemovalNotIssued, ExternalOperationID: "operation", SettledAt: durable.Timestamp(time.Now())}
	return settlement, durabletest.QueryProofFixture(b.QueryRuntimeIdentity, "abort-proof"), h.failure
}
func queryFixture(t *testing.T) (*Service, *memory.Store, *queryHostFixture, *map[string]bool) {
	t.Helper()
	s, store, grants := fixture(t)
	if _, err := s.EnrollRetirement(t.Context(), reader(), EnrollmentInput{NamespaceLifecycleInput: NamespaceLifecycleInput{Namespace: "allowed"}, RequestID: "enroll"}); err != nil {
		t.Fatal(err)
	}
	s.buildIdentity = func(context.Context, durable.BuildTarget) (durable.BuildQueryIdentity, error) {
		return durabletest.QueryIdentityFixture(), nil
	}
	if _, err := s.RegisterBuild(t.Context(), reader(), RegisterBuildInput{BuildInput: BuildInput{Namespace: "allowed", BuildID: "build"}, RequestID: "register-build", ExpectedVersion: "0"}); err != nil {
		t.Fatal(err)
	}
	host := &queryHostFixture{}
	s.queryHost = host
	return s, store, host, grants
}
func queryInput(runtime string) QueryRuntimeInput {
	return QueryRuntimeInput{BuildInput: BuildInput{Namespace: "allowed", BuildID: "build"}, RuntimeID: runtime}
}
func seedQueryBinding(t *testing.T, s *Service, runtime string) {
	t.Helper()
	in := QueryRuntimeCommand{QueryRuntimeInput: queryInput(runtime), RequestID: "register-" + runtime, ExpectedVersion: "0"}
	if _, err := s.RegisterQueryRuntime(t.Context(), reader(), in); err != nil {
		t.Fatal(err)
	}
	if _, err := s.VerifyQueryRuntime(t.Context(), reader(), QueryRuntimeCommand{QueryRuntimeInput: in.QueryRuntimeInput, RequestID: "verify-" + runtime, ExpectedVersion: "1"}); err != nil {
		t.Fatal(err)
	}
}
func reserveQueryFixture(t *testing.T, s *Service) QueryRemovalInput {
	t.Helper()
	seedQueryBinding(t, s, "candidate")
	seedQueryBinding(t, s, "survivor")
	in := QueryRuntimeCommand{QueryRuntimeInput: queryInput("candidate"), RequestID: "remove", ExpectedVersion: "2"}
	out, err := s.BeginQueryRuntimeRemoval(t.Context(), reader(), in)
	if err != nil || out.Binding.State != durable.QueryRuntimeRemoving || out.Fence.SurvivorRuntimeID != "survivor" {
		t.Fatalf("reservation %+v %v", out, err)
	}
	return QueryRemovalInput{QueryReservationInput: QueryReservationInput{QueryRuntimeInput: in.QueryRuntimeInput, ReservationRequestID: in.RequestID, ReservationVersion: in.ExpectedVersion}, RequestID: "finish"}
}
func TestQueryRegistrationReplayAfterRemovalSkipsNewIssuance(t *testing.T) {
	s, _, host, grants := queryFixture(t)
	policyCalls := 0
	s.queryRegistrationPolicy = func(context.Context, durable.RegisterQueryRuntimeRequest) error { policyCalls++; return nil }
	in := reserveQueryFixture(t, s)
	if _, err := s.FinishQueryRuntimeRemoval(t.Context(), reader(), in); !errors.Is(err, security.ErrUnavailable) {
		t.Fatalf("unknown deletion accepted: %v", err)
	}
	if b, err := s.QueryRuntime(t.Context(), reader(), in.QueryRuntimeInput); err != nil || b.State != durable.QueryRuntimeRemoving {
		t.Fatalf("unknown deletion released reservation %+v %v", b, err)
	}
	host.settled = true
	accepted, err := s.FinishQueryRuntimeRemoval(t.Context(), reader(), in)
	if err != nil || accepted.Binding.State != durable.QueryRuntimeRemoved {
		t.Fatalf("finish %+v %v", accepted, err)
	}
	host.failure = errors.New("host gone")
	if replay, e := s.FinishQueryRuntimeRemoval(t.Context(), reader(), in); e != nil || !reflect.DeepEqual(replay, accepted) || host.finishes != 2 {
		t.Fatalf("finish replay recaptured %+v %v", replay, e)
	}
	s.queryRegistrationPolicy = func(context.Context, durable.RegisterQueryRuntimeRequest) error {
		policyCalls++
		return security.ErrForbidden
	}
	registration := QueryRuntimeCommand{QueryRuntimeInput: in.QueryRuntimeInput, RequestID: "register-candidate", ExpectedVersion: "0"}
	if replay, e := s.RegisterQueryRuntime(t.Context(), reader(), registration); e != nil || replay.Binding.Version != "1" || host.resolves != 2 || policyCalls != 2 {
		t.Fatalf("accepted registration reissued %+v %v %d %d", replay, e, host.resolves, policyCalls)
	}
	if b, e := s.QueryRuntime(t.Context(), reader(), in.QueryRuntimeInput); e != nil || b.State != durable.QueryRuntimeRemoved {
		t.Fatalf("registration replay reactivated %+v %v", b, e)
	}
	(*grants)["allowed"] = false
	if _, e := s.RegisterQueryRuntime(t.Context(), reader(), registration); !errors.Is(e, security.ErrForbidden) {
		t.Fatalf("revoked registration replay: %v", e)
	}
}

func TestQueryAbortRequiresSettledOperationThenFreshProof(t *testing.T) {
	s, _, host, grants := queryFixture(t)
	in := reserveQueryFixture(t, s)
	in.RequestID = "abort"
	if _, err := s.AbortQueryRuntimeRemoval(t.Context(), reader(), in); !errors.Is(err, security.ErrUnavailable) {
		t.Fatalf("unknown abort accepted: %v", err)
	}
	if b, err := s.QueryRuntime(t.Context(), reader(), in.QueryRuntimeInput); err != nil || b.State != durable.QueryRuntimeRemoving {
		t.Fatalf("unknown abort released reservation %+v %v", b, err)
	}
	host.settled = true
	accepted, err := s.AbortQueryRuntimeRemoval(t.Context(), reader(), in)
	if err != nil || accepted.Binding.State != durable.QueryRuntimeActive {
		t.Fatalf("settled abort %+v %v", accepted, err)
	}
	host.failure = errors.New("probe unavailable")
	if replay, e := s.AbortQueryRuntimeRemoval(t.Context(), reader(), in); e != nil || !reflect.DeepEqual(replay, accepted) || host.aborts != 2 {
		t.Fatalf("abort replay recaptured %+v %v", replay, e)
	}
	(*grants)["allowed"] = false
	if _, e := s.AbortQueryRuntimeRemoval(t.Context(), reader(), in); !errors.Is(e, security.ErrForbidden) {
		t.Fatalf("revoked abort replay: %v", e)
	}
}

func TestQueryFinishReauthorizesBothRemovalAndCompletion(t *testing.T) {
	s, _, host, _ := queryFixture(t)
	in := reserveQueryFixture(t, s)
	host.settled = true
	if _, err := s.FinishQueryRuntimeRemoval(t.Context(), reader(), in); err != nil {
		t.Fatal(err)
	}
	original := s.authorizer
	for _, denied := range []string{RemoveQueryRuntime, FinishQueryRuntime} {
		s.authorizer = AuthorizerFunc(func(ctx context.Context, p security.Principal, action string, r Resource) error {
			if action == denied {
				return security.ErrForbidden
			}
			return original.Authorize(ctx, p, action, r)
		})
		if _, err := s.FinishQueryRuntimeRemoval(t.Context(), reader(), in); !errors.Is(err, security.ErrForbidden) {
			t.Fatalf("revoked %s allowed finish replay: %v", denied, err)
		}
	}
	if host.finishes != 1 {
		t.Fatal("denied replay recaptured host evidence")
	}
}

type lostQueryStore struct {
	*memory.Store
	register, verify bool
	corrupt          bool
}

func (s *lostQueryStore) RegisterQueryRuntime(ctx context.Context, r durable.RegisterQueryRuntimeRequest) (durable.LifecycleReceipt, error) {
	receipt, err := s.Store.RegisterQueryRuntime(ctx, r)
	if err == nil && s.register {
		s.register = false
		if s.corrupt {
			return durable.LifecycleReceipt{}, nil
		}
		return durable.LifecycleReceipt{}, errors.New("lost registration reply")
	}
	return receipt, err
}
func (s *lostQueryStore) RecordQueryRuntimeVerification(ctx context.Context, r durable.VerifyQueryRuntimeRequest) (durable.LifecycleReceipt, error) {
	receipt, err := s.Store.RecordQueryRuntimeVerification(ctx, r)
	if err == nil && s.verify {
		s.verify = false
		if s.corrupt {
			return durable.LifecycleReceipt{}, nil
		}
		return durable.LifecycleReceipt{}, errors.New("lost verification reply")
	}
	return receipt, err
}
func TestQueryCapturedEvidenceRecoversBeforeHostCall(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		s, store, host, grants := queryFixture(t)
		s.store = &lostQueryStore{Store: store, register: true, verify: true, corrupt: corrupt}
		in := QueryRuntimeCommand{QueryRuntimeInput: queryInput("candidate"), RequestID: "register", ExpectedVersion: "0"}
		if _, err := s.RegisterQueryRuntime(t.Context(), reader(), in); !errors.Is(err, security.ErrUnavailable) {
			t.Fatalf("lost/corrupt registration: %v", err)
		}
		host.failure = errors.New("private resolver unavailable")
		if out, err := s.RegisterQueryRuntime(t.Context(), reader(), in); err != nil || out.Binding.Version != "1" || host.resolves != 1 {
			t.Fatalf("registration recaptured %+v %v", out, err)
		}
		host.failure = nil
		in.RequestID = "verify"
		in.ExpectedVersion = "1"
		if _, err := s.VerifyQueryRuntime(t.Context(), reader(), in); !errors.Is(err, security.ErrUnavailable) {
			t.Fatalf("lost/corrupt verification: %v", err)
		}
		host.failure = errors.New("private probe unavailable")
		if out, err := s.VerifyQueryRuntime(t.Context(), reader(), in); err != nil || out.Binding.Version != "2" || host.probes != 1 {
			t.Fatalf("verification recaptured %+v %v", out, err)
		}
		(*grants)["allowed"] = false
		if _, err := s.VerifyQueryRuntime(t.Context(), reader(), in); !errors.Is(err, security.ErrForbidden) {
			t.Fatalf("revoked verification replay: %v", err)
		}
	}
}
