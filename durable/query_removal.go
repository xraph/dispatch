package durable

import (
	"math"
	"sort"
	"time"
)

func QueryRetention(build BuildAdmission, retained int64, bindings []QueryRuntimeBinding, now time.Time) QueryRetentionFacts {
	f := QueryRetentionFacts{BuildTarget: build.BuildTarget, RetainedExecutions: retained, ObservedAt: now}
	for _, b := range bindings {
		if b.BuildTarget != build.BuildTarget || b.State != QueryRuntimeActive {
			continue
		}
		f.ActiveBindings++
		if b.BuildIdentity == build.QueryIdentity && b.Verification.Validate(b.QueryRuntimeIdentity, now) == nil {
			f.VerifiedBindings++
		}
	}
	return f
}

// ReserveQueryRemoval selects an independent verified survivor under exclusive
// namespace coordination. Removing bindings never count as survivors.
func ReserveQueryRemoval(candidate QueryRuntimeBinding, r BeginQueryRemovalRequest, build BuildAdmission, retained int64, bindings []QueryRuntimeBinding, now time.Time) (QueryRuntimeBinding, error) {
	if candidate.QueryRuntimeTarget != r.QueryRuntimeTarget || candidate.Version != r.ExpectedVersion {
		return candidate, ErrRevisionConflict
	}
	if candidate.State != QueryRuntimeActive {
		return candidate, ErrRequestConflict
	}
	if candidate.Version == math.MaxInt64 || candidate.RemovalEpoch == math.MaxInt64 {
		return candidate, ErrInvalid
	}
	if candidate.BuildIdentity != build.QueryIdentity || build.QueryIdentity.Validate() != nil {
		return candidate, ErrQueryRetention
	}
	until, err := TaskTimeAfter(now, candidate.BuildIdentity.MaximumProofValidity)
	if err != nil {
		return candidate, err
	}
	f := QueryRemovalFence{Candidate: candidate.QueryRuntimeIdentity, CandidateStateVersion: candidate.Version + 1, BuildEpoch: build.Epoch, BuildVersion: build.Version, BuildState: build.State, RemovalEpoch: candidate.RemovalEpoch + 1, RequestID: r.RequestID, AcceptedAt: now, ValidUntil: until}
	if retained > 0 || build.State != BuildRetired {
		ordered := append([]QueryRuntimeBinding(nil), bindings...)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].RuntimeID < ordered[j].RuntimeID })
		for _, survivor := range ordered {
			if survivor.BuildTarget != build.BuildTarget || survivor.State != QueryRuntimeActive || survivor.RuntimeID == candidate.RuntimeID || survivor.InstanceID == candidate.InstanceID || survivor.BuildIdentity != build.QueryIdentity || survivor.Verification.Validate(survivor.QueryRuntimeIdentity, now) != nil {
				continue
			}
			f.Survivor = survivor.QueryRuntimeIdentity
			f.SurvivorStateVersion = survivor.Version
			f.SurvivorProof = survivor.Verification
			if f.SurvivorProof.ValidUntil.Before(f.ValidUntil) {
				f.ValidUntil = f.SurvivorProof.ValidUntil
			}
			break
		}
		if f.Survivor.RuntimeID == "" {
			return candidate, ErrQueryRetention
		}
	}
	candidate.Version++
	candidate.RemovalEpoch++
	candidate.State = QueryRuntimeRemoving
	candidate.ChangedAt = now
	candidate.Removal = f
	return candidate, nil
}

// CheckQueryRemoval checks current permission, not just an accepted reservation.
func CheckQueryRemoval(candidate QueryRuntimeBinding, f QueryRemovalFence, build BuildAdmission, retained int64, bindings []QueryRuntimeBinding, now time.Time) error {
	if candidate.State != QueryRuntimeRemoving || candidate.Removal != f || candidate.QueryRuntimeIdentity != f.Candidate || candidate.Version != f.CandidateStateVersion || candidate.RemovalEpoch != f.RemovalEpoch || !f.ValidUntil.After(now) || build.BuildTarget != f.Candidate.BuildTarget || build.Epoch != f.BuildEpoch || build.Version != f.BuildVersion || build.State != f.BuildState {
		return ErrQueryFence
	}
	if f.Survivor.RuntimeID == "" {
		if retained != 0 || build.State != BuildRetired {
			return ErrQueryFence
		}
		return nil
	}
	for _, b := range bindings {
		if b.QueryRuntimeIdentity == f.Survivor && b.Version == f.SurvivorStateVersion && b.State == QueryRuntimeActive && b.InstanceID != candidate.InstanceID && b.BuildIdentity == build.QueryIdentity && b.Verification == f.SurvivorProof && b.Verification.Validate(b.QueryRuntimeIdentity, now) == nil {
			return nil
		}
	}
	return ErrQueryFence
}

// FinishQueryRemoval records an external effect against its original reservation.
// It deliberately does not turn an expired reservation into new permission.
func FinishQueryRemoval(b QueryRuntimeBinding, r FinishQueryRemovalRequest, now time.Time) (QueryRuntimeBinding, error) {
	if b.State != QueryRuntimeRemoving || b.Removal != r.Fence || b.Version != r.Fence.CandidateStateVersion || b.RemovalEpoch != r.Fence.RemovalEpoch {
		return b, ErrQueryFence
	}
	if b.Version == math.MaxInt64 {
		return b, ErrInvalid
	}
	b.State = QueryRuntimeRemoved
	b.Version++
	b.ChangedAt = now
	return b, nil
}
