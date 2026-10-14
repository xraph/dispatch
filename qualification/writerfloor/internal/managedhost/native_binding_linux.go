//go:build linux

package managedhost

import (
	"context"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

// NativeCore reads the same accepted authority used by the controller. Each read
// authenticates its retained independent receipt through the published core API.
type NativeCore interface {
	ReadManagedTarget(context.Context, managed.Key) (*managed.Target, error)
	ReadManagedRemovalAttempt(context.Context, managed.AttemptKey) (managed.AttemptRecord, error)
	ReadManagedAttemptBinding(context.Context, managed.AttemptBindingKey) (managed.AttemptBindingRecord, error)
	ReadManagedOriginalLifecycle(context.Context, managed.OriginalLifecycleKey) (managed.OriginalLifecycleRecord, error)
	ReadManagedRecoveryNativeIntent(context.Context, managed.RecoveryExactRequest) (managed.RecoveryRecord[managed.RecoveryNativeIntent], error)
}

// NativeStore is the actual Dispatch authority, used only for exact reads here.
// Mutations use NativeClient through the existing authenticated Forge contract.
type NativeStore interface {
	durable.QueryRuntimeStore
	durable.RetirementEnrollmentStore
	durable.QueryRemovalRevalidationStore
}

type nativeBinding struct {
	Binding      managed.RemovalRecoveryBinding
	Reservation  operator.QueryReservationInput
	Original     durable.QueryRemovalOriginal
	Candidate    durable.QueryRuntimeBinding
	Registration managed.OriginalLifecycleRecord
	Drain        managed.OriginalLifecycleRecord
	Exclusions   []string
	Scope        durable.QueryRemovalExternalScope
}

func (h *NativeGuardHost) originalPair(ctx context.Context, target *managed.Target, reservation managed.Reservation) (registrationRecord, drainRecord managed.OriginalLifecycleRecord, resultErr error) {
	var zero managed.OriginalLifecycleRecord
	if reservation.Phase != managed.Accepted || reservation.Drain == nil {
		return zero, zero, managed.ErrUnavailable
	}
	registrationRef, err := managed.ParseOriginalReference(reservation.Receipt)
	if err != nil {
		return zero, zero, err
	}
	drainRef, err := managed.ParseOriginalReference(reservation.Drain.Evidence)
	if err != nil {
		return zero, zero, err
	}
	registration, err := h.core.ReadManagedOriginalLifecycle(ctx, managed.OriginalLifecycleKey{Key: target.Key, Kind: managed.OriginalRegistration, OperationID: registrationRef.OperationID})
	if err != nil {
		return zero, zero, err
	}
	drain, err := h.core.ReadManagedOriginalLifecycle(ctx, managed.OriginalLifecycleKey{Key: target.Key, Kind: managed.OriginalDrain, OperationID: drainRef.OperationID})
	if err != nil {
		return zero, zero, err
	}
	if registration.Reference != registrationRef || drain.Reference != drainRef || registration.Material.Registration == nil || drain.Material.Drain == nil || registration.Material.Command.ReservationID != reservation.ID || drain.Material.Command.ReservationID != reservation.ID || registration.Material.Registration.Process != drain.Material.Drain.Process || !drain.Material.Drain.ProcessAccepted || !drain.Material.Drain.AdmissionClosed {
		return zero, zero, managed.ErrAuthority
	}
	return registration, drain, nil
}
func (h *NativeGuardHost) nativeRuntime(ctx context.Context, target durable.QueryRuntimeTarget) (durable.QueryRuntimeBinding, error) {
	var zero durable.QueryRuntimeBinding
	after := ""
	for range 4 {
		page, err := h.native.ListQueryRuntimes(ctx, durable.QueryRuntimeList{NamespaceTarget: target.NamespaceTarget, BuildID: target.BuildID, After: after, Limit: 32})
		if err != nil {
			return zero, err
		}
		if len(page.Items) > 32 {
			return zero, managed.ErrCapacity
		}
		for _, binding := range page.Items {
			if binding.QueryRuntimeTarget == target {
				if binding.Validate() != nil {
					return zero, managed.ErrUnavailable
				}
				return binding, nil
			}
		}
		if page.Next == "" {
			return zero, durable.ErrNotFound
		}
		if page.Next <= after {
			return zero, managed.ErrUnavailable
		}
		after = page.Next
	}
	return zero, managed.ErrCapacity
}
func (h *NativeGuardHost) readBinding(ctx context.Context, key managed.AttemptBindingKey) (nativeBinding, error) {
	var result nativeBinding
	target, err := h.core.ReadManagedTarget(ctx, key.Key)
	if err != nil {
		return result, err
	}
	attempt, err := h.core.ReadManagedRemovalAttempt(ctx, key.AttemptKey)
	if err != nil {
		return result, err
	}
	binding, err := h.core.ReadManagedAttemptBinding(ctx, key)
	if err != nil {
		return result, err
	}
	if target == nil || target.Key != key.Key || binding.Key != key || binding.Fence == nil || attempt.State.AdmissionDigest == "" || len(target.Reservations) < 1 || len(target.Reservations) > 16 || len(attempt.Descriptor.ReservationIDs) != len(target.Reservations) || binding.DescriptorDigest != attempt.State.AttemptRef.DescriptorDigest {
		return result, managed.ErrUnavailable
	}
	var begin *managed.AttemptCommandRecord
	for i := range binding.Commands {
		if binding.Commands[i].Key.Kind == managed.AttemptBegin {
			if begin != nil {
				return result, managed.ErrUnavailable
			}
			begin = &binding.Commands[i]
		}
	}
	if begin == nil || begin.Key.AttemptBindingKey != key {
		return result, managed.ErrUnavailable
	}
	type member struct {
		Reservation  string `json:"reservation"`
		Registration string `json:"registration"`
		Drain        string `json:"drain"`
	}
	coverage := make([]member, 0, len(target.Reservations))
	exclusions := make([]string, 0, len(target.Reservations))
	found := false
	for _, reservationID := range attempt.Descriptor.ReservationIDs {
		index := slices.IndexFunc(target.Reservations, func(v managed.Reservation) bool { return v.ID == reservationID })
		if index < 0 {
			return result, managed.ErrUnavailable
		}
		reservation := target.Reservations[index]
		registration, drain, readErr := h.originalPair(ctx, target, reservation)
		if readErr != nil {
			return result, readErr
		}
		coverage = append(coverage, member{reservation.ID.String(), registration.Reference.MaterialDigest, drain.Reference.MaterialDigest})
		exclusions = append(exclusions, registration.Material.Registration.Process.InstanceID)
		if reservation.ID == key.ReservationID {
			result.Registration = registration
			result.Drain = drain
			found = true
		}
	}
	if !found || begin.BindingReference.OriginalRegistrationOperationID != result.Registration.Reference.OperationID || begin.BindingReference.OriginalRegistrationDigest != result.Registration.Reference.MaterialDigest {
		return result, managed.ErrAuthority
	}
	process := result.Registration.Material.Registration.Process
	nativeTarget := durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: process.Installation, Namespace: process.Namespace}, BuildID: process.Build}, RuntimeID: process.RuntimeID}
	original, err := h.native.LookupLifecycleReceipt(ctx, durable.LifecycleReceiptLookup{NamespaceTarget: nativeTarget.NamespaceTarget, Operation: durable.OperationBeginQueryRemoval, RequestID: binding.Fence.NativeAcceptance.RequestID, RequestDigest: binding.Fence.NativeAcceptance.RequestDigest})
	if err != nil {
		return result, err
	}
	if original.QueryRuntime == nil {
		return result, managed.ErrUnavailable
	}
	fence := original.QueryRuntime.Removal
	result.Original = durable.QueryRemovalOriginal{QueryRuntimeTarget: nativeTarget, BeginRequestID: original.RequestID, BeginRequestDigest: original.RequestDigest, FenceDigest: binding.Fence.NativeFenceDigest, CandidateStateVersion: fence.CandidateStateVersion, RemovalEpoch: fence.RemovalEpoch}
	if result.Original.MatchOriginal(original) != nil {
		return result, managed.ErrAuthority
	}
	result.Candidate, err = h.nativeRuntime(ctx, nativeTarget)
	if err != nil {
		return result, err
	}
	identity := result.Candidate.QueryRuntimeIdentity
	if identity != fence.Candidate || !nativeIdentityMatchesOriginal(identity, process) {
		return result, managed.ErrAuthority
	}
	result.Binding = managed.RemovalRecoveryBinding{Key: key, DescriptorDigest: binding.DescriptorDigest, ProviderOperationID: attempt.Descriptor.ProviderOperationID, OriginalRegistrationDigest: result.Registration.Reference.MaterialDigest, OriginalDrainDigest: result.Drain.Reference.MaterialDigest, OriginalCommandDigest: binding.Fence.CommandDigest, OriginalBeginRequestID: original.RequestID, OriginalBeginRequestDigest: original.RequestDigest, NativeFenceDigest: binding.Fence.NativeFenceDigest, OriginalProcessDigest: begin.BindingReference.ProcessDigest, AdmissionDigest: attempt.State.AdmissionDigest}
	result.Reservation = operator.QueryReservationInput{QueryRuntimeInput: operator.QueryRuntimeInput{BuildInput: operator.BuildInput{Namespace: process.Namespace, BuildID: process.Build}, RuntimeID: process.RuntimeID}, ReservationRequestID: original.RequestID, ReservationVersion: string(begin.ExpectedNativeVersion)}
	slices.Sort(exclusions)
	result.Exclusions = slices.Compact(exclusions)
	exclusionDigest, err := durable.Fingerprint("dispatch.query-removal.exclusions.v1", result.Exclusions)
	if err != nil {
		return result, err
	}
	coverageDigest, err := durable.Fingerprint("writerfloor.native.coverage.v1", struct {
		Descriptor managed.AttemptDescriptor
		Members    []member
	}{attempt.Descriptor, coverage})
	if err != nil {
		return result, err
	}
	result.Scope = durable.QueryRemovalExternalScope{AuthorityID: h.authority, OperationScopeID: attempt.Descriptor.ActionID.String(), AttemptID: key.AttemptID.String(), ProviderOperationID: attempt.Descriptor.ProviderOperationID.String(), CoverageDigest: coverageDigest, ExcludedInstancesDigest: exclusionDigest, ProducerID: h.identity.ProducerID, ProfileID: h.identity.Profile, ProfileVersion: 1, VerifierID: h.identity.VerifierID}
	if result.Scope.Validate() != nil {
		return result, managed.ErrAuthority
	}
	return result, nil
}
func sameNativeBinding(left, right nativeBinding) bool {
	return left.Binding == right.Binding && left.Original == right.Original && left.Scope == right.Scope && left.Reservation == right.Reservation && reflect.DeepEqual(left.Exclusions, right.Exclusions)
}

func nativeIdentityMatchesOriginal(identity durable.QueryRuntimeIdentity, process managed.OriginalProcessFacts) bool {
	version, err := strconv.ParseInt(string(process.IdentityVersion), 10, 64)
	if err != nil {
		return false
	}
	policy, err := strconv.ParseInt(string(process.ProbePolicyVersion), 10, 64)
	if err != nil {
		return false
	}
	validity, err := strconv.ParseInt(string(process.MaximumProofValidityNS), 10, 64)
	if err != nil || validity <= 0 {
		return false
	}
	expected := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: process.Installation, Namespace: process.Namespace}, BuildID: process.Build}, RuntimeID: process.RuntimeID}, InstanceID: process.InstanceID, IdentityVersion: version, BuildIdentity: durable.BuildQueryIdentity{ArtifactDigest: process.ArtifactDigest, ConfigurationDigest: process.ConfigurationDigest, ConfigurationVersion: process.ConfigurationVersion, EnrollmentEvidenceDigest: process.EnrollmentEvidenceDigest, ProbePolicyID: process.ProbePolicyID, ProbePolicyVersion: policy, VerifierID: process.VerifierID, MaximumProofValidity: time.Duration(validity)}}
	return expected.Validate() == nil && identity == expected
}
