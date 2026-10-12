package durable

import (
	"crypto/subtle"
	"fmt"
)

// ReceiptRequest resolves an accepted client intent, even after its run closes.
// The coordinator must hash the complete client request, including operation,
// identity and credentials, before preparing a state-dependent transition.
// This is a trusted store operation, not a remote authorization boundary.
type ReceiptRequest struct {
	Key
	RequestID    string `json:"request_id"`
	IntentDigest string `json:"intent_digest"`
}

// Validate rejects malformed lookups without disclosing their proof.
func (r ReceiptRequest) Validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if !identifier(r.RequestID) || !validHex256(r.IntentDigest) {
		return fmt.Errorf("%w: request ID and canonical SHA-256 intent digest are required", ErrInvalid)
	}
	return nil
}

// CheckReceiptIntent rejects mismatched proofs and older receipts without intent.
// Callers validate the requested digest before loading the stored receipt.
func CheckReceiptIntent(stored, requested string) error {
	if stored == "" || subtle.ConstantTimeCompare([]byte(stored), []byte(requested)) != 1 {
		return ErrRequestConflict
	}
	return nil
}
