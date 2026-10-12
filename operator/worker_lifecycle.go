package operator

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
)

// WorkerControl is a trusted handle for exactly one immutable process incarnation.
// A host must never reconnect this handle to a replacement RuntimeID.
// BeginDrain must atomically refuse first acceptance after the original deadline
// with ErrDrainDeadline, without closing admission or cancelling work. Exact
// accepted replay remains available. Remote hosts enforce this at the process.
type WorkerControl interface {
	Status(context.Context) (drt.WorkerStatus, error)
	Readiness(context.Context) (drt.WorkerReadiness, error)
	BeginDrain(context.Context, drt.DrainRequest) (drt.DrainHandle, error)
	WaitDrain(context.Context, drt.DrainHandle) (drt.DrainResult, error)
}

// LocalWorkerControl exposes an already constructed Worker, including before Run.
type LocalWorkerControl struct{ Worker *drt.Worker }

func (c LocalWorkerControl) Status(ctx context.Context) (drt.WorkerStatus, error) {
	if err := ctx.Err(); err != nil {
		return drt.WorkerStatus{}, err
	}
	if c.Worker == nil {
		return drt.WorkerStatus{}, ErrRuntimeUnavailable
	}
	return c.Worker.Status(), nil
}
func (c LocalWorkerControl) Readiness(ctx context.Context) (drt.WorkerReadiness, error) {
	if c.Worker == nil {
		return drt.WorkerReadiness{}, ErrRuntimeUnavailable
	}
	return c.Worker.Readiness(ctx)
}
func (c LocalWorkerControl) BeginDrain(ctx context.Context, r drt.DrainRequest) (drt.DrainHandle, error) {
	if c.Worker == nil {
		return drt.DrainHandle{}, ErrRuntimeUnavailable
	}
	return c.Worker.BeginDrainBeforeDeadline(ctx, r)
}
func (c LocalWorkerControl) WaitDrain(ctx context.Context, h drt.DrainHandle) (drt.DrainResult, error) {
	if c.Worker == nil {
		return drt.DrainResult{}, ErrRuntimeUnavailable
	}
	return c.Worker.WaitDrain(ctx, h)
}

type WorkerInput struct {
	BuildInput
	RuntimeID string `json:"runtime_id"`
}

func (s *Service) workerTarget(in WorkerInput) (durable.QueryRuntimeTarget, error) {
	build, err := s.buildTarget(in.BuildInput)
	if err != nil {
		return durable.QueryRuntimeTarget{}, err
	}
	target := durable.QueryRuntimeTarget{BuildTarget: build, RuntimeID: in.RuntimeID}
	return target, target.Validate()
}
func (s *Service) process(ctx context.Context, target durable.QueryRuntimeTarget) (WorkerControl, durable.WorkerProcessIdentity, error) {
	if s.workerControl == nil {
		return nil, durable.WorkerProcessIdentity{}, ErrRuntimeUnavailable
	}
	control, err := s.workerControl(ctx, target)
	if err != nil || control == nil {
		return nil, durable.WorkerProcessIdentity{}, ErrRuntimeUnavailable
	}
	status, err := control.Status(ctx)
	if err != nil {
		return nil, durable.WorkerProcessIdentity{}, ErrRuntimeUnavailable
	}
	identity := durable.WorkerProcessIdentity{BuildTarget: target.BuildTarget, Queue: status.Queue, RuntimeID: status.RuntimeID, InstanceID: status.InstanceID}
	if identity.Validate() != nil || status.Namespace != target.Namespace || status.BuildID != target.BuildID || status.RuntimeID != target.RuntimeID {
		return nil, durable.WorkerProcessIdentity{}, ErrRuntimeUnavailable
	}
	return control, identity, nil
}
func (s *Service) checkWorker(ctx context.Context, p security.Principal, action string, identity durable.WorkerProcessIdentity) error {
	if err := s.checkResourceFacts(ctx, p, action, durable.Key{Namespace: identity.Namespace}, "", identity.BuildID, identity.RuntimeID, identity.InstanceID, identity.Queue); err != nil {
		return err
	}
	life, err := s.lifecycleStore()
	if err != nil {
		return err
	}
	facts, err := life.InspectBuildLifecycle(ctx, identity.BuildTarget)
	if err != nil {
		return commandError(err)
	}
	if facts.Admission.BuildTarget != identity.BuildTarget {
		return security.ErrUnavailable
	}
	return nil
}

type WorkerObservation struct {
	WorkerInput
	InstanceID        string                   `json:"instance_id"`
	Queue             string                   `json:"queue"`
	State             string                   `json:"state"`
	Ready             bool                     `json:"ready"`
	AdmissionClosed   bool                     `json:"admission_closed"`
	InFlight          string                   `json:"in_flight"`
	UnknownClaims     string                   `json:"unknown_claims"`
	Failure           string                   `json:"failure,omitempty"`
	HostQualification string                   `json:"host_qualification"`
	Claims            []WorkerClaimObservation `json:"claims"`
	RetirementStatus  string                   `json:"retirement_status"`
	Compatibility     Compatibility            `json:"compatibility"`
	ObservedAt        time.Time                `json:"observed_at"`
}
type WorkerClaimObservation struct {
	Kind       string `json:"kind"`
	Claiming   string `json:"claiming"`
	Processing string `json:"processing"`
}

func workerObservation(r drt.WorkerReadiness) WorkerObservation {
	w := r.Worker
	out := WorkerObservation{HostQualification: "not_observed", WorkerInput: WorkerInput{BuildInput: BuildInput{Namespace: w.Namespace, BuildID: w.BuildID}, RuntimeID: w.RuntimeID}, InstanceID: w.InstanceID, Queue: w.Queue, State: string(w.State), Ready: r.Ready, AdmissionClosed: w.AdmissionClosed, InFlight: strconv.FormatInt(w.InFlight, 10), UnknownClaims: strconv.FormatInt(w.UnknownClaims, 10), Failure: w.Failure, RetirementStatus: r.RetirementStatus, Compatibility: compatibility(r.Compatibility), ObservedAt: w.ObservedAt}
	if out.Failure != "" && out.Failure != "processing_failed" {
		out.Failure = "unavailable"
	}
	for _, claim := range w.Claims {
		out.Claims = append(out.Claims, WorkerClaimObservation{Kind: string(claim.Kind), Claiming: strconv.FormatInt(claim.Claiming, 10), Processing: strconv.FormatInt(claim.Processing, 10)})
	}
	return out
}
func (s *Service) WorkerStatus(ctx context.Context, p security.Principal, in WorkerInput) (WorkerObservation, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	if p.Validate() != nil {
		return WorkerObservation{}, s.denied(ctx, p, ReadWorker, security.ErrUnauthenticated)
	}
	target, err := s.workerTarget(in)
	if err != nil {
		return WorkerObservation{}, err
	}
	control, identity, err := s.process(ctx, target)
	if err != nil {
		if checkErr := s.checkResourceFacts(ctx, p, ReadWorker, durable.Key{Namespace: target.Namespace}, "", target.BuildID, target.RuntimeID, "", ""); checkErr != nil {
			return WorkerObservation{}, checkErr
		}
		return WorkerObservation{}, err
	}
	if checkErr := s.checkWorker(ctx, p, ReadWorker, identity); checkErr != nil {
		return WorkerObservation{}, checkErr
	}
	ready, readErr := control.Readiness(ctx)
	if ready.Worker.RuntimeID != identity.RuntimeID || ready.Worker.InstanceID != identity.InstanceID || ready.Worker.Namespace != identity.Namespace || ready.Worker.BuildID != identity.BuildID || ready.Worker.Queue != identity.Queue {
		return WorkerObservation{}, ErrRuntimeUnavailable
	}
	if readErr != nil && !errors.Is(readErr, durable.ErrWriterCompatibility) {
		ready.Ready = false
		ready.RetirementStatus = "unavailable"
	}
	if err = s.audit.RecordDurableRead(ctx, p, ReadWorker, "allowed", identity.Namespace); err != nil {
		return WorkerObservation{}, security.ErrUnavailable
	}
	return workerObservation(ready), nil
}

type WorkerDrainInput struct {
	WorkerInput
	RequestID   string    `json:"request_id"`
	OperationID string    `json:"operation_id"`
	Deadline    time.Time `json:"deadline"`
}
type WorkerDrainAcceptance struct {
	LifecycleAcceptance
	RuntimeID       string    `json:"runtime_id"`
	InstanceID      string    `json:"instance_id"`
	Queue           string    `json:"queue"`
	OperationID     string    `json:"operation_id"`
	Deadline        time.Time `json:"deadline"`
	Process         string    `json:"process"`
	Complete        bool      `json:"complete"`
	Quiescent       bool      `json:"quiescent"`
	DeadlineExpired bool      `json:"deadline_expired"`
	InFlight        string    `json:"in_flight"`
	UnknownClaims   string    `json:"unknown_claims"`
}

func workerDrainAcceptance(r durable.LifecycleReceipt) WorkerDrainAcceptance {
	out := WorkerDrainAcceptance{LifecycleAcceptance: lifecycleAcceptance(r), Process: "unknown"}
	out.Status = "requested"
	if d := r.WorkerDrain; d != nil {
		out.BuildID = d.BuildID
		out.RuntimeID = d.RuntimeID
		out.InstanceID = d.InstanceID
		out.Queue = d.Queue
		out.OperationID = d.OperationID
		out.Deadline = d.Deadline
	}
	return out
}

// RequestWorkerDrain recovers persistence before invoking the same process handle.
// Lost replies leave acceptance separate from process completion.
func (s *Service) RequestWorkerDrain(ctx context.Context, p security.Principal, in WorkerDrainInput) (WorkerDrainAcceptance, error) {
	return s.workerDrain(ctx, p, in, false)
}

// WorkerDrainReceipt reads acceptance without invoking or changing the process.
func (s *Service) WorkerDrainReceipt(ctx context.Context, p security.Principal, in WorkerDrainInput) (WorkerDrainAcceptance, error) {
	return s.workerDrain(ctx, p, in, true)
}
func (s *Service) workerDrain(ctx context.Context, p security.Principal, in WorkerDrainInput, readOnly bool) (WorkerDrainAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	target, err := s.workerTarget(in.WorkerInput)
	if err != nil {
		return WorkerDrainAcceptance{}, err
	}
	if !durable.DeliveryIdentifier(in.RequestID) || !durable.DeliveryIdentifier(in.OperationID) || in.Deadline.IsZero() {
		return WorkerDrainAcceptance{}, durable.ErrInvalid
	}
	if p.Validate() != nil {
		return WorkerDrainAcceptance{}, s.denied(ctx, p, DrainWorker, security.ErrUnauthenticated)
	}
	life, err := s.lifecycleStore()
	if err != nil {
		return WorkerDrainAcceptance{}, err
	}
	digest, err := durable.Fingerprint("operator.worker.drain.v1", in)
	if err != nil {
		return WorkerDrainAcceptance{}, durable.ErrInvalid
	}
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: durable.OperationRequestWorkerDrain, RequestID: in.RequestID, CommandDigest: digest}
	receipt, lookupErr := life.LookupLifecycleReceipt(ctx, lookup)
	var identity durable.WorkerProcessIdentity
	switch {
	case lookupErr == nil:
		if receipt.Match(lookup) != nil || receipt.WorkerDrain == nil || receipt.WorkerDrain.BuildTarget != target.BuildTarget || receipt.WorkerDrain.RuntimeID != target.RuntimeID {
			return WorkerDrainAcceptance{}, security.ErrUnavailable
		}
		identity = receipt.WorkerDrain.WorkerProcessIdentity
	case errors.Is(lookupErr, durable.ErrNotFound):
		_, identity, err = s.process(ctx, target)
		if err != nil {
			if checkErr := s.checkResourceFacts(ctx, p, DrainWorker, durable.Key{Namespace: target.Namespace}, "", target.BuildID, target.RuntimeID, "", ""); checkErr != nil {
				return WorkerDrainAcceptance{}, checkErr
			}
			return WorkerDrainAcceptance{}, err
		}
	default:
		if checkErr := s.checkResourceFacts(ctx, p, DrainWorker, durable.Key{Namespace: target.Namespace}, "", target.BuildID, target.RuntimeID, "", ""); checkErr != nil {
			return WorkerDrainAcceptance{}, checkErr
		}
		return WorkerDrainAcceptance{}, commandError(lookupErr)
	}
	if checkErr := s.checkWorker(ctx, p, DrainWorker, identity); checkErr != nil {
		return WorkerDrainAcceptance{}, checkErr
	}
	if readOnly {
		if checkErr := s.checkWorker(ctx, p, ReadWorker, identity); checkErr != nil {
			return WorkerDrainAcceptance{}, checkErr
		}
		if lookupErr != nil {
			return WorkerDrainAcceptance{}, durable.ErrNotFound
		}
		if err = s.audit.RecordDurableRead(ctx, p, ReadWorker, "allowed", identity.Namespace); err != nil {
			return WorkerDrainAcceptance{}, security.ErrUnavailable
		}
		return workerDrainAcceptance(receipt), nil
	}
	if errors.Is(lookupErr, durable.ErrNotFound) {
		request := durable.WorkerDrainRequest{WorkerProcessIdentity: identity, RequestID: in.RequestID, CommandDigest: digest, OperationID: in.OperationID, Deadline: in.Deadline}
		receipt, err = life.RequestWorkerDrain(commandContext(ctx, p, in.RequestID), request)
		if err != nil {
			return WorkerDrainAcceptance{}, commandError(err)
		}
	}
	if receipt.Match(lookup) != nil || receipt.WorkerDrain == nil || receipt.WorkerDrain.WorkerProcessIdentity != identity {
		return WorkerDrainAcceptance{}, security.ErrUnavailable
	}
	out := workerDrainAcceptance(receipt)
	out.DeadlineExpired = !receipt.WorkerDrain.Deadline.After(time.Now())
	// Resolve again after acceptance, and reject a changed process or routing.
	control, current, resolveErr := s.process(ctx, target)
	out.DeadlineExpired = !receipt.WorkerDrain.Deadline.After(time.Now())
	if resolveErr != nil || current != identity {
		return out, nil
	}
	handle, invokeErr := control.BeginDrain(ctx, drt.DrainRequest{OperationID: receipt.WorkerDrain.OperationID, Deadline: receipt.WorkerDrain.Deadline})
	out.DeadlineExpired = !receipt.WorkerDrain.Deadline.After(time.Now())
	if errors.Is(invokeErr, drt.ErrDrainDeadline) {
		// The exact handle proves this request was never accepted before expiry.
		out.Process = "incomplete"
		return out, nil
	}
	if invokeErr != nil || handle.RuntimeID != identity.RuntimeID || handle.OperationID != receipt.WorkerDrain.OperationID || !handle.Deadline.Equal(receipt.WorkerDrain.Deadline) {
		return out, nil
	}
	out.Process = "draining"
	// This observer may expire without changing the accepted operation deadline.
	result, waitErr := control.WaitDrain(ctx, handle)
	if result.Handle != handle {
		return out, nil
	}
	out.InFlight = strconv.FormatInt(result.InFlight, 10)
	out.UnknownClaims = strconv.FormatInt(result.UnknownClaims, 10)
	out.Quiescent = result.Quiescent
	out.DeadlineExpired = result.DeadlineExpired
	out.Complete = result.Complete && waitErr == nil && result.Quiescent && result.InFlight == 0 && result.UnknownClaims == 0
	if out.Complete {
		out.Process = "complete"
	} else if errors.Is(waitErr, drt.ErrDrainIncomplete) {
		out.Process = "incomplete"
	}
	return out, nil
}
