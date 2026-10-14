package managedhost

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

type creationTestIssuer struct{ grant managed.ProviderStepGrant }

func (a *creationTestIssuer) AuthorizeManagedProviderStep(_ context.Context, _ managed.ProviderStepAuthorization) (managed.ProviderStepGrant, error) {
	grant := a.grant
	a.grant.NewlyIssued = false
	return grant, nil
}

func TestCreationAdmissionConsumesFreshGrantAndRefusesReplacement(t *testing.T) {
	c, _, accepted, transport, command := newCreationTest(t)
	accepted.target.Revision = accepted.acceptance.Permission.Revision
	accepted.target.LeaseOwner = accepted.authorization.Owner
	accepted.target.LeaseEpoch = accepted.authorization.LeaseEpoch
	accepted.target.LeaseUntil = time.Now().Add(time.Minute)
	issuer := &creationTestIssuer{grant: managed.ProviderStepGrant{Permission: accepted.acceptance.Permission, NewlyIssued: true}}
	adapter, err := NewCreationAdmission(issuer, c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	invocation, err := adapter.Authorize(ctx, accepted.authorization, command)
	if err != nil {
		t.Fatal(err)
	}
	if err = adapter.Verify(ctx, invocation); err != nil {
		t.Fatal(err)
	}
	forged := invocation
	forged.Slot = "forged"
	if err = adapter.Dispatch(ctx, admission.Delegation{Invocation: forged}); !errors.Is(err, managed.ErrAuthority) {
		t.Fatalf("forged delegate: %v", err)
	}
	if err = adapter.Verify(ctx, invocation); err != nil {
		t.Fatalf("forgery consumed valid grant: %v", err)
	}
	if err = adapter.Verify(ctx, forged); !errors.Is(err, managed.ErrAuthority) {
		t.Fatalf("replacement: %v", err)
	}
	if err = adapter.Dispatch(ctx, admission.Delegation{Invocation: invocation}); err != nil {
		t.Fatal(err)
	}
	if err = adapter.Dispatch(ctx, admission.Delegation{Invocation: invocation}); !errors.Is(err, managed.ErrAuthority) {
		t.Fatalf("resend: %v", err)
	}
	if transport.calls != 1 {
		t.Fatalf("calls=%d", transport.calls)
	}
	if _, err = adapter.Authorize(ctx, accepted.authorization, command); !errors.Is(err, managed.ErrUnavailable) {
		t.Fatalf("historical grant: %v", err)
	}
}

func TestCreationAdmissionStaleLeaseAndProcessReplacementRefuse(t *testing.T) {
	c, _, accepted, transport, command := newCreationTest(t)
	accepted.target.Revision = accepted.acceptance.Permission.Revision
	accepted.target.LeaseOwner = accepted.authorization.Owner
	accepted.target.LeaseEpoch = accepted.authorization.LeaseEpoch
	accepted.target.LeaseUntil = time.Now().Add(time.Minute)
	issuer := &creationTestIssuer{grant: managed.ProviderStepGrant{Permission: accepted.acceptance.Permission, NewlyIssued: true}}
	adapter, err := NewCreationAdmission(issuer, c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	invocation, err := adapter.Authorize(ctx, accepted.authorization, command)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := NewCreationAdmission(issuer, c)
	if err != nil {
		t.Fatal(err)
	}
	if err = replacement.Verify(ctx, invocation); !errors.Is(err, managed.ErrAuthority) {
		t.Fatalf("replacement restored grant: %v", err)
	}
	accepted.target.LeaseUntil = time.Now().Add(-time.Second)
	if err = adapter.Dispatch(ctx, admission.Delegation{Invocation: invocation}); !errors.Is(err, managed.ErrAuthority) || transport.calls != 0 {
		t.Fatalf("expired lease: %v calls=%d", err, transport.calls)
	}
}

type creationTestDirectory map[string]*creationTestAcceptances

func (d creationTestDirectory) ReadManagedProviderStepAcceptance(ctx context.Context, q managed.ProviderStepAuthorization) (managed.ProviderStepAcceptance, error) {
	accepted := d[q.ID.String()]
	if accepted == nil {
		return managed.ProviderStepAcceptance{}, managed.ErrUnavailable
	}
	return accepted.ReadManagedProviderStepAcceptance(ctx, q)
}
func (d creationTestDirectory) ReadManagedTarget(ctx context.Context, key managed.Key) (*managed.Target, error) {
	for _, accepted := range d {
		if accepted.target.Key == key {
			return accepted.ReadManagedTarget(ctx, key)
		}
	}
	return nil, managed.ErrUnavailable
}

func TestCreationAdmissionPrunesAbandonedCapacityAndConsumesFailedDispatch(t *testing.T) {
	c, _, _, _, _ := newCreationTest(t)
	directory := creationTestDirectory{}
	c.acceptances = directory
	issuer := &creationTestIssuer{}
	adapter, err := NewCreationAdmission(issuer, c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var invocation admission.Invocation
	for i := range managed.MaxMembers*2 + 2 {
		_, _, accepted, _, command := newCreationTest(t)
		accepted.target.Revision = accepted.acceptance.Permission.Revision
		accepted.target.LeaseOwner = accepted.authorization.Owner
		accepted.target.LeaseEpoch = accepted.authorization.LeaseEpoch
		accepted.target.LeaseUntil = time.Now().Add(time.Minute)
		if i == managed.MaxMembers*2+1 {
			if len(adapter.grants) != managed.MaxMembers*2+1 {
				t.Fatalf("capacity not exercised: %d", len(adapter.grants))
			}
			for _, old := range directory {
				old.target.LeaseUntil = time.Now().Add(-time.Second)
			}
		}
		directory[accepted.authorization.ID.String()] = accepted
		issuer.grant = managed.ProviderStepGrant{Permission: accepted.acceptance.Permission, NewlyIssued: true}
		invocation, err = adapter.Authorize(ctx, accepted.authorization, command)
		if err != nil {
			t.Fatalf("authorize %d: %v", i, err)
		}
	}
	if len(adapter.grants) != 1 {
		t.Fatalf("stale grants=%d", len(adapter.grants))
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err = adapter.Dispatch(cancelled, admission.Delegation{Invocation: invocation}); err == nil {
		t.Fatal("cancelled dispatch succeeded")
	}
	if len(adapter.grants) != 0 {
		t.Fatal("failed dispatch retained its grant")
	}
	if err = adapter.Verify(ctx, invocation); !errors.Is(err, managed.ErrAuthority) {
		t.Fatalf("discarded authority restored: %v", err)
	}
}
