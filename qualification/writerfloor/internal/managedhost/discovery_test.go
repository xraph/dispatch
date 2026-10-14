package managedhost

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
)

type discoveryTestReader struct {
	head     managed.WorkHead
	calls    int
	mismatch bool
	loop     bool
}

func (s *discoveryTestReader) ReadManagedWorkHead(context.Context, managed.Key) (managed.WorkHead, error) {
	return s.head, nil
}
func (s *discoveryTestReader) ListManagedUnresolvedWork(_ context.Context, q managed.WorkPageRequest) (managed.WorkPage, error) {
	s.calls++
	reference := s.head.Desired
	reference.Ordinal = managed.Decimal(strconv.Itoa(s.calls))
	next := strconv.Itoa(s.calls)
	if s.loop {
		next = q.Cursor
	}
	return managed.WorkPage{Schema: managed.DiscoverySchema, Key: q.Key, Items: []managed.WorkReference{reference}, NextCursor: next}, nil
}
func (s *discoveryTestReader) ReadManagedWork(_ context.Context, q managed.WorkReference) (managed.WorkReference, error) {
	if s.mismatch {
		q.Digest = strings.Repeat("f", 64)
	}
	return q, nil
}
func (s *discoveryTestReader) ReadManagedOperation(_ context.Context, q managed.OperationKey) (managed.OperationState, error) {
	return managed.OperationState{Key: q, Receipt: managed.ActionReceipt{CanonicalDigest: s.head.Desired.Digest}}, nil
}
func (*discoveryTestReader) RetainManagedPhaseCommand(context.Context, managed.PhaseCommandRequest) (managed.PhaseCommandGrant, error) {
	return managed.PhaseCommandGrant{}, managed.ErrUnsupported
}
func (*discoveryTestReader) ReadManagedPhaseCommand(context.Context, managed.WorkReference) (managed.PhaseCommandRecord, error) {
	return managed.PhaseCommandRecord{}, managed.ErrUnsupported
}

func newDiscoveryTest() *discoveryTestReader {
	key := managed.Key{TenantID: "tenant", InstanceID: id.New(id.PrefixInstance)}
	action := id.New(managed.PrefixOperation)
	ref := managed.WorkReference{Schema: managed.DiscoverySchema, Key: key, Ordinal: "1", Operation: managed.OperationKey{Key: key, ActionID: action}, Kind: "action", ID: action, Digest: strings.Repeat("a", 64)}
	return &discoveryTestReader{head: managed.WorkHead{Schema: managed.DiscoverySchema, Key: key, LastOrdinal: "4", Provisioning: ref, Desired: ref}}
}

func TestDiscoverySweepStopsAtFiniteBudgetWithoutClearingMembership(t *testing.T) {
	store := newDiscoveryTest()
	result, err := InspectWork(context.Background(), store, store.head.Key, "", 4)
	if err != nil {
		t.Fatal(err)
	}
	if store.calls != 4 || len(result.Items) != 4 || result.NextCursor != "4" || result.Head.Desired != store.head.Desired {
		t.Fatalf("bounded sweep %#v calls=%d", result, store.calls)
	}
	if _, err = InspectWork(context.Background(), store, store.head.Key, "", 5); !errors.Is(err, managed.ErrInvalid) || store.calls != 4 {
		t.Fatalf("invalid budget %v calls=%d", err, store.calls)
	}
}

func TestDiscoverySweepRefusesChangedReferenceAndNonprogressingCursor(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		store := newDiscoveryTest()
		store.mismatch = mismatch
		store.loop = !mismatch
		if _, err := InspectWork(context.Background(), store, store.head.Key, "opaque-start", 4); err == nil {
			t.Fatal("inconsistent discovery accepted")
		}
		if store.calls != 1 {
			t.Fatalf("continued after inconsistency: %d", store.calls)
		}
	}
}
