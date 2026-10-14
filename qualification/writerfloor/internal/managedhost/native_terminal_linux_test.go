//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
)

type nativeTerminalRefusalFixture struct {
	nativeTerminalCore
	calls int
}

func (f *nativeTerminalRefusalFixture) ReadManagedRemovalGuardClose(_ context.Context, _ managed.RecoveryExactRequest) (managed.RecoveryRecord[managed.RecoveryNativeCloseFacts], error) {
	f.calls++
	return managed.RecoveryRecord[managed.RecoveryNativeCloseFacts]{}, managed.ErrUnavailable
}
func TestNativeFinishRefusesMissingOrTamperedAcceptedClosureAnchor(t *testing.T) {
	core := &nativeTerminalRefusalFixture{}
	journal := &creationTestJournal{rows: map[string][]byte{}}
	host := &NativeGuardHost{core: core, journal: journal, key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, 32))}
	_, binding := nativeWorkerFixture(t)
	fence := durable.QueryRemovalFence{Candidate: binding.QueryRuntimeIdentity, CandidateStateVersion: 2, RemovalEpoch: 1}
	if host.VerifyRemoved(t.Context(), fence) == nil {
		t.Fatal("missing closure anchor authorized Finish")
	}
	if core.calls != 0 {
		t.Fatal("missing anchor reached authority reader")
	}
	anchor := nativeTerminalAnchor{Fence: fence}
	unsigned, err := bounded(anchor)
	if err != nil {
		t.Fatal(err)
	}
	anchor.Signature = ed25519.Sign(host.key, unsigned)
	address, err := nativeTerminalKey(fence)
	if err != nil {
		t.Fatal(err)
	}
	anchor.Fence.RemovalEpoch++
	journal.rows[address], err = bounded(anchor)
	if err != nil {
		t.Fatal(err)
	}
	if host.VerifyRemoved(t.Context(), fence) == nil {
		t.Fatal("changed signed fence authorized Finish")
	}
	if core.calls != 0 {
		t.Fatal("tampered anchor reached authority reader")
	}
	anchor.Fence = fence
	journal.rows[address], err = bounded(anchor)
	if err != nil {
		t.Fatal(err)
	}
	if host.VerifyRemoved(t.Context(), fence) == nil {
		t.Fatal("host signature replaced independent accepted core receipt")
	}
	if core.calls != 1 {
		t.Fatal("exact signed anchor did not require independent authority")
	}
}
