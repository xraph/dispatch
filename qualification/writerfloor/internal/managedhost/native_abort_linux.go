//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

type nativePreparedAbort struct {
	Binding   managed.AttemptBindingKey        `json:"binding"`
	Close     managed.RecoveryAssociation      `json:"close"`
	Input     operator.QueryRemovalInput       `json:"input"`
	Request   durable.AbortQueryRemovalRequest `json:"request"`
	Signature []byte                           `json:"signature"`
}

func nativeAbortKey(fence durable.QueryRemovalFence) (string, error) {
	return durable.Fingerprint("writerfloor.native.prepared-abort.v1", fence)
}

func (h *NativeGuardHost) loadAbort(ctx context.Context, fence durable.QueryRemovalFence) (nativePreparedAbort, error) {
	var record nativePreparedAbort
	address, err := nativeAbortKey(fence)
	if err != nil {
		return record, err
	}
	raw, err := h.journal.Load(ctx, address)
	if err != nil {
		return record, err
	}
	if len(raw) == 0 {
		return record, durable.ErrNotFound
	}
	if exactDecode(raw, &record) != nil {
		return record, managed.ErrAuthority
	}
	signature := record.Signature
	record.Signature = nil
	unsigned, err := bounded(record)
	record.Signature = signature
	if err != nil || !ed25519.Verify(ed25519.PublicKey(h.key[ed25519.SeedSize:]), unsigned, signature) || record.Request.Fence != fence || record.Request.Validate() != nil {
		return nativePreparedAbort{}, managed.ErrAuthority
	}
	commandDigest, err := durable.Fingerprint("operator.query_runtime.abort.v1", record.Input)
	if err != nil || record.Input.RequestID != record.Request.RequestID || record.Request.CommandDigest != commandDigest {
		return nativePreparedAbort{}, managed.ErrAuthority
	}
	closed, reference, err := h.terminalClose(ctx, fence)
	if err != nil {
		return nativePreparedAbort{}, err
	}
	if record.Close != reference || record.Binding != closed.Sources.Binding.Binding.Key || !reflect.DeepEqual(record.Input.QueryReservationInput, closed.Sources.Binding.Reservation) || closed.Request.Closure.Disposition != "settled_without_deletion" {
		return nativePreparedAbort{}, managed.ErrAuthority
	}
	return record, nil
}

// PrepareAbort captures real fresh query evidence before the core command is
// authorized. One closed fence has one immutable preparation; expiry cannot mint
// a replacement request. Exact historical preparation reads issue no authority.
func (h *NativeGuardHost) PrepareAbort(ctx context.Context, binding durable.QueryRuntimeBinding, fence durable.QueryRemovalFence, input operator.QueryRemovalInput) (durable.AbortQueryRemovalRequest, error) {
	var zero durable.AbortQueryRemovalRequest
	if binding.QueryRuntimeIdentity != fence.Candidate || binding.Removal != fence || binding.State != durable.QueryRuntimeRemoving || !durable.DeliveryIdentifier(input.RequestID) {
		return zero, managed.ErrAuthority
	}
	saved, err := h.loadAbort(ctx, fence)
	if err == nil {
		if !reflect.DeepEqual(saved.Input, input) {
			return zero, managed.ErrConflict
		}
		return saved.Request, nil
	}
	if !errors.Is(err, durable.ErrNotFound) {
		return zero, err
	}
	closed, reference, err := h.terminalClose(ctx, fence)
	if err != nil {
		return zero, err
	}
	if !reflect.DeepEqual(input.QueryReservationInput, closed.Sources.Binding.Reservation) {
		return zero, managed.ErrAuthority
	}
	core, err := h.core.ReadManagedAttemptBinding(ctx, closed.Sources.Binding.Binding.Key)
	if err != nil {
		return zero, err
	}
	if core.Fence == nil || core.Witness == nil || core.Fence.NativeFenceDigest != closed.Sources.Binding.Original.FenceDigest {
		return zero, managed.ErrAuthority
	}
	settlement, proof, err := h.freshAbort(ctx, binding, fence)
	if err != nil {
		return zero, err
	}
	digest, err := durable.Fingerprint("operator.query_runtime.abort.v1", input)
	if err != nil {
		return zero, err
	}
	request := durable.AbortQueryRemovalRequest{VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: binding.QueryRuntimeTarget, RequestID: input.RequestID, ExpectedVersion: fence.CandidateStateVersion, CommandDigest: digest, Verification: proof}, Fence: fence, Settlement: settlement}
	if request.Validate() != nil || proof.Validate(binding.QueryRuntimeIdentity, time.Now().UTC()) != nil || proof.VerifiedAt.Before(core.Witness.WitnessedAt) {
		return zero, managed.ErrAuthority
	}
	record := nativePreparedAbort{Binding: closed.Sources.Binding.Binding.Key, Close: reference, Input: input, Request: request}
	unsigned, err := bounded(record)
	if err != nil {
		return zero, err
	}
	record.Signature = ed25519.Sign(h.key, unsigned)
	raw, err := bounded(record)
	if err != nil {
		return zero, err
	}
	address, err := nativeAbortKey(fence)
	if err != nil {
		return zero, err
	}
	accepted, err := h.journal.CompareAndSwap(ctx, address, nil, raw)
	if err != nil {
		return zero, err
	}
	if !accepted {
		return zero, managed.ErrBlocked
	}
	return request, nil
}

// VerifyAbort refuses callers that cannot name the exact prepared operator input.
// The published operator uses VerifyPreparedAbort when this companion exists.
func (h *NativeGuardHost) VerifyAbort(context.Context, durable.QueryRuntimeBinding, durable.QueryRemovalFence) (durable.QueryRemovalSettlement, durable.QueryRuntimeVerification, error) {
	return durable.QueryRemovalSettlement{}, durable.QueryRuntimeVerification{}, managed.ErrAuthority
}

// VerifyPreparedAbort authenticates current permanent closure, exact incoming
// request and accepted core command. It returns the retained real query proof
// without generating different native request bytes after authorization.
func (h *NativeGuardHost) VerifyPreparedAbort(ctx context.Context, binding durable.QueryRuntimeBinding, fence durable.QueryRemovalFence, requestID, commandDigest string) (durable.QueryRemovalSettlement, durable.QueryRuntimeVerification, error) {
	var settlement durable.QueryRemovalSettlement
	var proof durable.QueryRuntimeVerification
	record, err := h.loadAbort(ctx, fence)
	if err != nil {
		return settlement, proof, err
	}
	if matchErr := preparedAbortMatches(record, binding, fence, requestID, commandDigest, time.Now().UTC()); matchErr != nil {
		return settlement, proof, matchErr
	}
	core, err := h.core.ReadManagedAttemptBinding(ctx, record.Binding)
	if err != nil {
		return settlement, proof, err
	}
	original, err := json.Marshal(record.Input)
	if err != nil {
		return settlement, proof, err
	}
	native, err := json.Marshal(struct {
		Operation string `json:"operation"`
		Request   any    `json:"request"`
	}{string(durable.OperationAbortQueryRemoval), record.Request})
	if err != nil {
		return settlement, proof, err
	}
	found := false
	for _, command := range core.Commands {
		if command.Key.Kind != managed.AttemptAbort {
			continue
		}
		if found || command.Key.AttemptBindingKey != record.Binding || command.NativeCommand.RequestID != requestID || command.NativeCommand.OriginalDigest != managed.Digest(original) || command.NativeCommand.NativeDigest != managed.Digest(native) || !bytes.Equal(command.NativeCommand.Original, original) || !bytes.Equal(command.NativeCommand.Native, native) {
			return settlement, proof, managed.ErrAuthority
		}
		found = true
	}
	if !found || core.Fence == nil || core.Witness == nil || record.Request.Verification.VerifiedAt.Before(core.Witness.WitnessedAt) {
		return settlement, proof, managed.ErrAuthority
	}
	return record.Request.Settlement, record.Request.Verification, nil
}

func preparedAbortMatches(record nativePreparedAbort, binding durable.QueryRuntimeBinding, fence durable.QueryRemovalFence, requestID, commandDigest string, now time.Time) error {
	if binding.State != durable.QueryRuntimeRemoving || binding.Removal != fence || binding.QueryRuntimeIdentity != fence.Candidate || binding.Version != fence.CandidateStateVersion || record.Request.Fence != fence || record.Input.RequestID != requestID || record.Request.RequestID != requestID || record.Request.CommandDigest != commandDigest || record.Request.ExpectedVersion != binding.Version || record.Request.QueryRuntimeTarget != binding.QueryRuntimeTarget {
		return managed.ErrAuthority
	}
	if record.Request.Verification.Validate(binding.QueryRuntimeIdentity, now) != nil || record.Request.Settlement.Validate(fence, now) != nil || record.Request.Verification.VerifiedAt.Before(record.Request.Settlement.SettledAt) {
		return managed.ErrAuthority
	}
	return nil
}

var _ durable.QueryRemovalPreparedAbortVerifier = (*NativeGuardHost)(nil)
