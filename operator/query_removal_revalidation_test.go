package operator

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type guardVerifierFixture struct {
	evidence   durable.QueryRemovalEvidence
	permission durable.QueryRemovalPermission
	closure    durable.QueryRemovalClosure
	calls      int
	failure    error
}

func (v *guardVerifierFixture) VerifyRevalidation(context.Context, durable.QueryRemovalGuardProbe) (durable.QueryRemovalEvidence, error) {
	v.calls++
	return v.evidence, v.failure
}
func (v *guardVerifierFixture) VerifyPermission(context.Context, durable.QueryRemovalGuardReference, string) (durable.QueryRemovalPermission, error) {
	v.calls++
	return v.permission, v.failure
}
func (v *guardVerifierFixture) VerifyClosure(context.Context, durable.QueryRemovalGuardReference, string) (durable.QueryRemovalClosure, error) {
	v.calls++
	return v.closure, v.failure
}

func TestQueryGuardOperatorRecoveryAndAuthorization(t *testing.T) {
	store := memory.New()
	f := durabletest.NewQueryGuardFixture(t, store)
	allowed := true
	audit := security.Boundary{Resource: security.Resource{InstallationID: "i", PolicyTenant: "t"}, Audit: &security.AuditService{}}
	if err := audit.Audit.Activate(t.Context(), store, store, audit.Resource, f.Candidate.Namespace, false); err != nil {
		t.Fatal(err)
	}
	verifier := &guardVerifierFixture{evidence: f.Request.Evidence}
	service, err := New(Options{Store: store, Reads: store, Catalog: store, InstallationID: "i", Audit: audit, QueryRemovalGuardVerifier: verifier, CursorKeys: CursorKeys{Active: "v1", Keys: map[string][]byte{"v1": make([]byte, 32)}}, Authorizer: AuthorizerFunc(func(_ context.Context, p security.Principal, action string, r Resource) error {
		if r.Namespace != f.Candidate.Namespace || r.InstallationID != "i" || r.BuildID != f.Candidate.BuildID || r.RuntimeID != f.Candidate.RuntimeID || (allowed && r.InstanceID != f.Candidate.InstanceID) {
			t.Errorf("guard scope lost: %+v", r)
		}
		if !queryLifecycleAction(action) {
			t.Errorf("unclassified guard action %s", action)
		}
		if !allowed || p.Subject != "reader" {
			return security.ErrForbidden
		}
		return nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	original := QueryReservationInput{QueryRuntimeInput: QueryRuntimeInput{BuildInput: BuildInput{Namespace: f.Candidate.Namespace, BuildID: f.Candidate.BuildID}, RuntimeID: f.Candidate.RuntimeID}, ReservationRequestID: f.Begin.RequestID, ReservationVersion: strconv.FormatInt(f.Candidate.Version-1, 10)}
	scope := f.Request.ExternalScope
	in := QueryRevalidationInput{QueryReservationInput: original, RequestID: "revalidate", Scope: QueryGuardScope{AuthorityID: scope.AuthorityID, OperationScopeID: scope.OperationScopeID, AttemptID: scope.AttemptID, ProviderOperationID: scope.ProviderOperationID, CoverageDigest: scope.CoverageDigest, ExcludedInstancesDigest: scope.ExcludedInstancesDigest, ProducerID: scope.ProducerID, ProfileID: scope.ProfileID, ProfileVersion: "1", VerifierID: scope.VerifierID}, EvidenceReference: "trusted-record", SurvivorRuntimeID: f.Survivor.RuntimeID, SurvivorVersion: strconv.FormatInt(f.Survivor.Version, 10)}
	service.queryRemovalGuardVerifier = nil
	if _, err = service.RevalidateQueryRuntimeRemoval(t.Context(), reader(), in); !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatalf("trust unavailable: %v", err)
	}
	service.queryRemovalGuardVerifier = verifier
	r, err := service.RevalidateQueryRuntimeRemoval(t.Context(), reader(), in)
	if err != nil {
		t.Fatal(err)
	}
	guard := QueryGuardInput{QueryReservationInput: original, GuardID: r.GuardID, RootDigest: r.RootDigest, OriginalDigest: r.OriginalDigest}
	verifier.permission = durabletest.QueryGuardCommitFixture(f, r).Permission
	commit := QueryCommitInput{QueryGuardInput: guard, RequestID: "commit", PermissionReference: "permission-record", ExpectedCurrentDigest: r.MaterialDigest, CurrentRequestID: r.RequestID}
	accepted, err := service.BindQueryRemovalCommit(t.Context(), reader(), commit)
	if err != nil {
		t.Fatal(err)
	}
	verifier.closure = durabletest.QueryGuardCloseFixture(f, r).Closure
	closeInput := QueryGuardCloseInput{QueryGuardInput: guard, RequestID: "close", ClosureReference: "cutoff-record"}
	closed, err := service.CloseQueryRemovalGuard(t.Context(), reader(), closeInput)
	if err != nil {
		t.Fatal(err)
	}
	late := commit
	late.RequestID = "bind-after-close"
	if _, err = service.BindQueryRemovalCommit(t.Context(), reader(), late); !errors.Is(err, durable.ErrQueryRemovalGuard) {
		t.Fatalf("closed guard obstruction lost: %v", err)
	}
	calls := verifier.calls
	verifier.failure = errors.New("verifier offline")
	service.queryRemovalGuardVerifier = nil
	if replay, e := service.RevalidateQueryRuntimeRemoval(t.Context(), reader(), in); e != nil || replay != r {
		t.Fatalf("revalidation replay: %v", e)
	}
	if replay, e := service.BindQueryRemovalCommit(t.Context(), reader(), commit); e != nil || !reflect.DeepEqual(replay, accepted) {
		t.Fatalf("commit replay: %v", e)
	}
	if replay, e := service.CloseQueryRemovalGuard(t.Context(), reader(), closeInput); e != nil || !reflect.DeepEqual(replay, closed) {
		t.Fatalf("close replay: %v", e)
	}
	if verifier.calls != calls {
		t.Fatal("historical replay invoked verifier")
	}
	status, err := service.CheckQueryRemovalCommit(t.Context(), reader(), QueryGuardReadInput{QueryReservationInput: original, RequestID: accepted.RequestID, CommandDigest: accepted.CommandDigest})
	if err != nil || status.State != "closed" {
		t.Fatalf("closed observation: %+v %v", status, err)
	}
	commit.PermissionReference = "changed"
	if _, err = service.BindQueryRemovalCommit(t.Context(), reader(), commit); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("different command replay: %v", err)
	}
	allowed = false
	commit.PermissionReference = "permission-record"
	if _, err = service.BindQueryRemovalCommit(t.Context(), reader(), commit); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("denied commit replay: %v", err)
	}
	if _, err = service.RevalidateQueryRuntimeRemoval(t.Context(), reader(), in); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("denied revalidation replay: %v", err)
	}
	if _, err = service.CloseQueryRemovalGuard(t.Context(), reader(), closeInput); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("denied close replay: %v", err)
	}
	if _, err = service.CheckQueryRemovalCommit(t.Context(), reader(), QueryGuardReadInput{QueryReservationInput: original, RequestID: accepted.RequestID, CommandDigest: accepted.CommandDigest}); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("denied commit read: %v", err)
	}
}
