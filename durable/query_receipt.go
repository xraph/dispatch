package durable

import "math"

func queryLifecycleOperation(operation LifecycleOperation) bool {
	switch operation {
	case OperationRegisterQueryRuntime, OperationVerifyQueryRuntime, OperationBeginQueryRemoval, OperationFinishQueryRemoval, OperationAbortQueryRemoval:
		return true
	default:
		return false
	}
}

// acceptedQueryProof validates historical evidence at its original acceptance.
// Expiry today does not invalidate an immutable accepted response.
func acceptedQueryProof(p QueryRuntimeVerification, identity QueryRuntimeIdentity) bool {
	return !p.AcceptedAt.IsZero() && !p.AcceptedAt.Before(p.VerifiedAt) && p.Validate(identity, p.AcceptedAt) == nil
}
func acceptedQueryFence(f QueryRemovalFence) bool {
	if f.Candidate.Validate() != nil || f.CandidateStateVersion < 2 || f.RemovalEpoch < 1 || f.BuildEpoch < 1 || f.BuildVersion < 1 || !DeliveryIdentifier(f.RequestID) || f.AcceptedAt.IsZero() || !f.ValidUntil.After(f.AcceptedAt) || f.ValidUntil.Sub(f.AcceptedAt) > f.Candidate.BuildIdentity.MaximumProofValidity {
		return false
	}
	if f.BuildState != BuildAccepting && f.BuildState != BuildRetiring && f.BuildState != BuildRetired {
		return false
	}
	until, err := TaskTimeAfter(f.AcceptedAt, f.Candidate.BuildIdentity.MaximumProofValidity)
	if err != nil {
		return false
	}
	if f.Survivor == (QueryRuntimeIdentity{}) {
		return f.BuildState == BuildRetired && f.SurvivorStateVersion == 0 && f.SurvivorProof == (QueryRuntimeVerification{}) && f.ValidUntil.Equal(until)
	}
	if f.SurvivorProof.ValidUntil.Before(until) {
		until = f.SurvivorProof.ValidUntil
	}
	return f.ValidUntil.Equal(until) && f.Survivor.Validate() == nil && f.Survivor.BuildTarget == f.Candidate.BuildTarget && f.Survivor.BuildIdentity == f.Candidate.BuildIdentity && f.Survivor.RuntimeID != f.Candidate.RuntimeID && f.Survivor.InstanceID != f.Candidate.InstanceID && f.SurvivorStateVersion >= 2 && acceptedQueryProof(f.SurvivorProof, f.Survivor) && !f.SurvivorProof.AcceptedAt.After(f.AcceptedAt) && f.SurvivorProof.Validate(f.Survivor, f.AcceptedAt) == nil && !f.ValidUntil.After(f.SurvivorProof.ValidUntil)
}
func (r LifecycleReceipt) validQueryResult() bool {
	b := r.QueryRuntime
	if b == nil || r.Enrollment != nil || r.Build != nil || b.Validate() != nil || b.NamespaceTarget != r.NamespaceTarget || !b.ChangedAt.Equal(r.AcceptedAt) || b.CreatedAt.After(r.AcceptedAt) {
		return false
	}
	if b.Verification != (QueryRuntimeVerification{}) && (!acceptedQueryProof(b.Verification, b.QueryRuntimeIdentity) || b.Verification.AcceptedAt.After(r.AcceptedAt)) {
		return false
	}
	if r.Operation != OperationAbortQueryRemoval && r.QueryAbort != nil {
		return false
	}
	switch r.Operation {
	case OperationRegisterQueryRuntime:
		return b.State == QueryRuntimeActive && b.Version == 1 && b.RemovalEpoch == 0 && b.Verification == (QueryRuntimeVerification{}) && b.Removal == (QueryRemovalFence{}) && b.CreatedAt.Equal(r.AcceptedAt)
	case OperationVerifyQueryRuntime:
		return b.State == QueryRuntimeActive && b.Version >= 2 && b.Removal == (QueryRemovalFence{}) && acceptedQueryProof(b.Verification, b.QueryRuntimeIdentity) && b.Verification.AcceptedAt.Equal(r.AcceptedAt)
	case OperationBeginQueryRemoval:
		f := b.Removal
		return b.State == QueryRuntimeRemoving && acceptedQueryFence(f) && f.Candidate == b.QueryRuntimeIdentity && f.CandidateStateVersion == b.Version && f.RemovalEpoch == b.RemovalEpoch && f.RequestID == r.RequestID && f.AcceptedAt.Equal(r.AcceptedAt)
	case OperationFinishQueryRemoval:
		f := b.Removal
		return b.State == QueryRuntimeRemoved && acceptedQueryFence(f) && f.Candidate == b.QueryRuntimeIdentity && f.CandidateStateVersion < math.MaxInt64 && f.CandidateStateVersion+1 == b.Version && f.RemovalEpoch == b.RemovalEpoch && !f.AcceptedAt.After(r.AcceptedAt) && (b.Verification == (QueryRuntimeVerification{}) || !b.Verification.AcceptedAt.After(f.AcceptedAt))
	case OperationAbortQueryRemoval:
		if r.QueryAbort == nil {
			return false
		}
		f := r.QueryAbort.Fence
		s := r.QueryAbort.Settlement
		return b.State == QueryRuntimeActive && b.Removal == (QueryRemovalFence{}) && acceptedQueryFence(f) && f.Candidate == b.QueryRuntimeIdentity && f.CandidateStateVersion < math.MaxInt64 && f.CandidateStateVersion+1 == b.Version && f.RemovalEpoch == b.RemovalEpoch && s.Validate(f, r.AcceptedAt) == nil && acceptedQueryProof(b.Verification, b.QueryRuntimeIdentity) && b.Verification.AcceptedAt.Equal(r.AcceptedAt) && !b.Verification.VerifiedAt.Before(s.SettledAt)
	default:
		return false
	}
}
