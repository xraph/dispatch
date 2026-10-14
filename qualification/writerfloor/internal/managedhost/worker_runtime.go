package managedhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"slices"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
)

// RetainedProbe names an exact saved workflow query and its expected output digest.
type RetainedProbe struct {
	Name           string           `json:"name"`
	Request        drt.QueryRequest `json:"request"`
	ExpectedDigest string           `json:"expected_digest"`
}

// WorkerRuntime serves the real worker in this process. Its identity comes from
// authenticated provider-service enrollment and is compared to actual worker state.
// It has no API for adopting a process from a caller-provided runtime label.
type WorkerRuntime struct {
	worker   *drt.Worker
	store    durable.QueryRuntimeStore
	identity durable.QueryRuntimeIdentity
	probes   []RetainedProbe
}

// NewWorkerRuntime measures this executable and binds one exact enrolled worker.
// The composition must authenticate enrollment before construction. No request may
// change the identity, fixed historical probes or native store after this call.
func NewWorkerRuntime(worker *drt.Worker, store durable.QueryRuntimeStore, identity durable.QueryRuntimeIdentity, probes []RetainedProbe) (*WorkerRuntime, error) {
	if worker == nil || nilInterface(store) || identity.Validate() != nil || len(probes) < 1 || len(probes) > 8 || identity.BuildIdentity.MaximumProofValidity > time.Minute {
		return nil, durable.ErrInvalid
	}
	path, err := os.Executable()
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	size, readErr := io.Copy(hash, io.LimitReader(file, (512<<20)+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if size > 512<<20 || hex.EncodeToString(hash.Sum(nil)) != identity.BuildIdentity.ArtifactDigest {
		return nil, durable.ErrQueryRetention
	}
	status := worker.Status()
	if !workerMatches(status, identity) {
		return nil, durable.ErrQueryRetention
	}
	names := map[string]bool{}
	copied := slices.Clone(probes)
	for i, probe := range copied {
		if !durable.DeliveryIdentifier(probe.Name) || names[probe.Name] || probe.Request.Validate() != nil || probe.Request.Selection != durable.RunExplicit || probe.Request.Namespace != identity.Namespace || probe.Request.BuildID != identity.BuildID || !objectID(probe.ExpectedDigest) {
			return nil, durable.ErrInvalid
		}
		names[probe.Name] = true
		copied[i].Request.Input = slices.Clone(probe.Request.Input)
	}
	return &WorkerRuntime{worker: worker, store: store, identity: identity, probes: copied}, nil
}

func workerMatches(status drt.WorkerStatus, identity durable.QueryRuntimeIdentity) bool {
	return status.Namespace == identity.Namespace && status.BuildID == identity.BuildID && status.RuntimeID == identity.RuntimeID && status.InstanceID == identity.InstanceID
}

// ResolveBinding returns the configured identity only while actual worker IDs agree.
func (h *WorkerRuntime) ResolveBinding(ctx context.Context, target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	if err := ctx.Err(); err != nil {
		return durable.QueryRuntimeIdentity{}, err
	}
	if target != h.identity.QueryRuntimeTarget || !workerMatches(h.worker.Status(), h.identity) {
		return durable.QueryRuntimeIdentity{}, durable.ErrQueryRetention
	}
	return h.identity, nil
}

type probeObservation struct {
	Name         string      `json:"name"`
	Key          durable.Key `json:"key"`
	Revision     int64       `json:"revision"`
	LastSequence int64       `json:"last_sequence"`
	OutputDigest string      `json:"output_digest"`
}

// Verify runs every configured probe against the worker's real replay engine after
// claim admission closes. Store failure, changed identity or any wrong output refuses.
func (h *WorkerRuntime) Verify(ctx context.Context, binding durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error) {
	var empty durable.QueryRuntimeVerification
	identity, err := h.ResolveBinding(ctx, binding.QueryRuntimeTarget)
	if err != nil {
		return empty, err
	}
	if identity != binding.QueryRuntimeIdentity {
		return empty, durable.ErrQueryRetention
	}
	status := h.worker.Status()
	if !status.AdmissionClosed || status.InFlight != 0 || status.UnknownClaims != 0 || status.State == drt.WorkerFailed {
		return empty, durable.ErrQueryRetention
	}
	before, err := h.store.InspectQueryRetention(ctx, identity.BuildTarget)
	if err != nil {
		return empty, err
	}
	if before.BuildTarget != identity.BuildTarget || before.ObservedAt.IsZero() || !before.ObservedAt.Equal(durable.Timestamp(before.ObservedAt)) {
		return empty, durable.ErrQueryRetention
	}
	observations := make([]probeObservation, 0, len(h.probes))
	for _, probe := range h.probes {
		result, queryErr := h.worker.QueryExecution(ctx, probe.Request)
		if queryErr != nil {
			return empty, queryErr
		}
		output := sha256.Sum256(result.Output)
		digest := hex.EncodeToString(output[:])
		if result.Key != probe.Request.Key || digest != probe.ExpectedDigest {
			return empty, durable.ErrQueryRetention
		}
		observations = append(observations, probeObservation{Name: probe.Name, Key: result.Key, Revision: result.Revision, LastSequence: result.LastSequence, OutputDigest: digest})
	}
	after, err := h.store.InspectQueryRetention(ctx, identity.BuildTarget)
	if err != nil {
		return empty, err
	}
	status = h.worker.Status()
	until := before.ObservedAt.Add(identity.BuildIdentity.MaximumProofValidity)
	if after.BuildTarget != identity.BuildTarget || !after.ObservedAt.Equal(durable.Timestamp(after.ObservedAt)) || after.ObservedAt.Before(before.ObservedAt) || !until.After(after.ObservedAt) || !workerMatches(status, identity) || !status.AdmissionClosed || status.InFlight != 0 || status.UnknownClaims != 0 || status.State == drt.WorkerFailed {
		return empty, durable.ErrQueryRetention
	}
	evidence, err := durable.Fingerprint("writerfloor.managed.probes.v1", struct {
		Identity     durable.QueryRuntimeIdentity
		Observations []probeObservation
	}{identity, observations})
	if err != nil {
		return empty, err
	}
	proofID, err := durable.Fingerprint("writerfloor.managed.proof.v1", struct {
		Evidence string
		At       time.Time
	}{evidence, before.ObservedAt})
	if err != nil {
		return empty, err
	}
	return durable.QueryRuntimeVerification{Identity: identity, ProofID: proofID, VerifierID: identity.BuildIdentity.VerifierID, EvidenceDigest: evidence, ProbePolicyID: identity.BuildIdentity.ProbePolicyID, ProbePolicyVersion: identity.BuildIdentity.ProbePolicyVersion, VerifiedAt: before.ObservedAt, ValidUntil: until}, nil
}

// Status reads the actual process-owned worker rather than a retained remote label.
func (h *WorkerRuntime) Status(ctx context.Context) (drt.WorkerStatus, error) {
	if _, err := h.ResolveBinding(ctx, h.identity.QueryRuntimeTarget); err != nil {
		return drt.WorkerStatus{}, err
	}
	return h.worker.Status(), nil
}

// BeginDrain permanently closes admission in this worker using its original deadline.
func (h *WorkerRuntime) BeginDrain(ctx context.Context, request drt.DrainRequest) (drt.DrainHandle, error) {
	if _, err := h.ResolveBinding(ctx, h.identity.QueryRuntimeTarget); err != nil {
		return drt.DrainHandle{}, err
	}
	return h.worker.BeginDrainBeforeDeadline(ctx, request)
}

// WaitDrain observes the same process and exact immutable drain handle.
func (h *WorkerRuntime) WaitDrain(ctx context.Context, handle drt.DrainHandle) (drt.DrainResult, error) {
	if _, err := h.ResolveBinding(ctx, h.identity.QueryRuntimeTarget); err != nil {
		return drt.DrainResult{}, err
	}
	if handle.RuntimeID != h.identity.RuntimeID {
		return drt.DrainResult{}, durable.ErrQueryRetention
	}
	return h.worker.WaitDrain(ctx, handle)
}

var _ operator.QueryRuntimeHost = (*WorkerRuntime)(nil)
