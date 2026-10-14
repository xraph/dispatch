package durable

import "context"

// QueryRemovalPreparedAbortVerifier binds retained host preparation to the exact
// incoming operator request. Hosts implementing it are never called through the
// legacy verifier for Abort, including when prepared verification refuses.
// commandDigest is the operator.query_runtime.abort.v1 fingerprint of the input.
type QueryRemovalPreparedAbortVerifier interface {
	VerifyPreparedAbort(ctx context.Context, binding QueryRuntimeBinding, fence QueryRemovalFence, requestID, commandDigest string) (QueryRemovalSettlement, QueryRuntimeVerification, error)
}
