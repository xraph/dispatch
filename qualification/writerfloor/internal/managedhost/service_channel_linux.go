//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

const serviceWireLimit = 64 << 10

// ServiceCommand is a bounded, one-delivery command for one authenticated boot.
// Its nonce is independent of native mutation IDs and never substitutes for them.
type ServiceCommand struct {
	Nonce   string                       `json:"nonce"`
	Kind    string                       `json:"kind"`
	Binding *durable.QueryRuntimeBinding `json:"binding,omitempty"`
	Drain   *drt.DrainRequest            `json:"drain,omitempty"`
	Handle  *drt.DrainHandle             `json:"handle,omitempty"`
}

// ServiceResponse binds actual worker observations to the exact pending command.
type ServiceResponse struct {
	BootstrapID string                            `json:"bootstrap_id"`
	Nonce       string                            `json:"nonce"`
	Kind        string                            `json:"kind"`
	Failed      bool                              `json:"failed"`
	Readiness   *drt.WorkerReadiness              `json:"readiness,omitempty"`
	Status      *drt.WorkerStatus                 `json:"status,omitempty"`
	Proof       *durable.QueryRuntimeVerification `json:"proof,omitempty"`
	Handle      *drt.DrainHandle                  `json:"handle,omitempty"`
	Drain       *drt.DrainResult                  `json:"drain,omitempty"`
}

type servicePending struct {
	command   ServiceCommand
	delivered bool
	completed bool
	result    chan ServiceResponse
}

type serviceSlot struct {
	startup launcher.ServiceStartup
	binding *launcher.ServiceBinding
	pending *servicePending
	wake    chan struct{}
	refusal string
}

// RuntimeHost uses the configured launcher's real TLS, Authsome, Warden and
// signed creation checks. It never accepts a caller-selected authenticator.
// Mount Handler on the existing direct mutual-TLS role listener.
type RuntimeHost struct {
	enrollment *launcher.ServiceEnrollment
	mu         sync.Mutex
	slots      map[string]*serviceSlot
}

// NewRuntimeHost fixes at most sixteen service bootstraps from protected enrollment.
func NewRuntimeHost(ctx context.Context, enrollment *launcher.ServiceEnrollment, ids []string) (*RuntimeHost, error) {
	if enrollment == nil || len(ids) < 1 || len(ids) > 16 {
		return nil, durable.ErrInvalid
	}
	h := &RuntimeHost{enrollment: enrollment, slots: make(map[string]*serviceSlot, len(ids))}
	for _, id := range ids {
		if h.slots[id] != nil {
			return nil, durable.ErrInvalid
		}
		startup, err := enrollment.Startup(ctx, id)
		if err != nil {
			return nil, err
		}
		slot := &serviceSlot{startup: startup, wake: make(chan struct{}, 1)}
		binding, retained, err := enrollment.RetainedBinding(ctx, id)
		if err != nil {
			return nil, err
		}
		if retained {
			if _, err = ServiceIdentity(startup, binding); err != nil {
				return nil, err
			}
			slot.binding = &binding
		}
		h.slots[id] = slot
	}
	return h, nil
}

// ServiceIdentity combines the protected plan with its exact authenticated
// creation and boot binding. A public copy alone grants no command authority.
func ServiceIdentity(startup launcher.ServiceStartup, binding launcher.ServiceBinding) (durable.QueryRuntimeIdentity, error) {
	var empty durable.QueryRuntimeIdentity
	plan, err := launcher.ServicePlanDigest(startup.ServicePlan)
	if err != nil || binding.BootstrapID != startup.BootstrapID || binding.PlanDigest != plan || !objectID(binding.BootNonce) || !objectID(binding.CreationDigest) || !objectID(binding.Incarnation) || !objectID(binding.ResourceID) || binding.Account != startup.Account || binding.Key != startup.Key {
		return empty, durable.ErrQueryRetention
	}
	version, err := startup.ProbePolicyVersion.Uint64()
	if err != nil || version > math.MaxInt64 {
		return empty, durable.ErrInvalid
	}
	validity, err := startup.MaximumProofValidityNS.Uint64()
	if err != nil || validity > uint64(time.Minute) {
		return empty, durable.ErrInvalid
	}
	// Build enrollment is shared by runtimes of one protected build. Resource
	// custody and the boot nonce remain checked above and on every channel call;
	// putting them in BuildIdentity would prohibit an off-instance survivor.
	evidence, err := durable.Fingerprint("writerfloor.native.build-enrollment.v1", struct {
		Installation         string `json:"installation"`
		Namespace            string `json:"namespace"`
		Build                string `json:"build"`
		Artifact             string `json:"artifact"`
		Configuration        string `json:"configuration"`
		ConfigurationVersion string `json:"configuration_version"`
		ProbePolicy          string `json:"probe_policy"`
		ProbeVersion         uint64 `json:"probe_version"`
		Verifier             string `json:"verifier"`
		MaximumValidity      uint64 `json:"maximum_validity"`
	}{startup.Installation, startup.Namespace, startup.Build, startup.ArtifactDigest, startup.ConfigurationDigest, startup.ConfigurationVersion, startup.ProbePolicyID, version, startup.VerifierID, validity})
	if err != nil {
		return empty, err
	}
	identity := durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: startup.Installation, Namespace: startup.Namespace}, BuildID: startup.Build}, RuntimeID: startup.RuntimeID}, InstanceID: startup.InstanceID, IdentityVersion: 1, BuildIdentity: durable.BuildQueryIdentity{ArtifactDigest: startup.ArtifactDigest, ConfigurationDigest: startup.ConfigurationDigest, ConfigurationVersion: startup.ConfigurationVersion, EnrollmentEvidenceDigest: evidence, ProbePolicyID: startup.ProbePolicyID, ProbePolicyVersion: int64(version), VerifierID: startup.VerifierID, MaximumProofValidity: time.Duration(validity)}}
	return identity, identity.Validate()
}

func decodeService(reader io.Reader, out any) error {
	raw, err := io.ReadAll(io.LimitReader(reader, serviceWireLimit+1))
	if err != nil {
		return err
	}
	if len(raw) > serviceWireLimit {
		return durable.ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.InputOffset() > serviceWireLimit {
		return durable.ErrInvalid
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return durable.ErrInvalid
	}
	return nil
}
func serviceJSON(w http.ResponseWriter, value any) {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > serviceWireLimit {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(raw); err != nil {
		return
	}
}

// Handler accepts only the fixed readiness, single-delivery poll and result paths.
func (h *RuntimeHost) Handler() http.Handler { return http.HandlerFunc(h.serveHTTP) }
func (h *RuntimeHost) recordRefusal(id, phase string, err error) {
	var classified interface{ DiagnosticClass() string }
	if errors.As(err, &classified) {
		phase += "_" + classified.DiagnosticClass()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if slot := h.slots[id]; slot != nil {
		slot.refusal = phase
	}
}

func (h *RuntimeHost) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "invalid", http.StatusBadRequest)
		return
	}
	if r.URL.Path == "/managed-service/ready" {
		var ready launcher.ServiceReady
		if decodeService(r.Body, &ready) != nil {
			http.Error(w, "invalid", http.StatusBadRequest)
			return
		}
		h.mu.Lock()
		slot := h.slots[ready.BootstrapID]
		h.mu.Unlock()
		if slot == nil {
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		binding, err := h.enrollment.AuthenticateReady(r.Context(), r, ready)
		if err != nil {
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		if _, err = ServiceIdentity(slot.startup, binding); err != nil {
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		h.mu.Lock()
		if slot.binding != nil && *slot.binding != binding {
			h.mu.Unlock()
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		saved := binding
		slot.binding = &saved
		h.mu.Unlock()
		serviceJSON(w, binding)
		return
	}
	if r.URL.Path != "/managed-service/next" && r.URL.Path != "/managed-service/result" {
		http.NotFound(w, r)
		return
	}
	var response ServiceResponse
	if decodeService(r.Body, &response) != nil {
		http.Error(w, "invalid", http.StatusBadRequest)
		return
	}
	binding, err := h.enrollment.AuthenticateChannel(r.Context(), r, response.BootstrapID)
	if err != nil {
		h.recordRefusal(response.BootstrapID, "initial_auth", err)
		http.Error(w, "refused", http.StatusForbidden)
		return
	}
	h.mu.Lock()
	slot := h.slots[response.BootstrapID]
	if slot == nil || slot.binding == nil || *slot.binding != binding {
		h.mu.Unlock()
		http.Error(w, "refused", http.StatusForbidden)
		return
	}
	if r.URL.Path == "/managed-service/result" {
		accepted := acceptServiceResult(slot, response)
		h.mu.Unlock()
		if !accepted {
			http.Error(w, "changed", http.StatusConflict)
			return
		}
		serviceJSON(w, struct {
			Accepted bool `json:"accepted"`
		}{true})
		return
	}
	if response.Nonce != "" || response.Kind != "" || response.Failed || response.Readiness != nil || response.Status != nil || response.Proof != nil || response.Handle != nil || response.Drain != nil {
		h.mu.Unlock()
		http.Error(w, "invalid", http.StatusBadRequest)
		return
	}
	command := takeServiceCommand(slot)
	h.mu.Unlock()
	if command == nil {
		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		for command == nil {
			select {
			case <-r.Context().Done():
				return
			case <-timer.C:
				w.WriteHeader(http.StatusNoContent)
				return
			case <-slot.wake:
			}
			current, authErr := h.enrollment.AuthenticateChannel(r.Context(), r, response.BootstrapID)
			if authErr != nil || r.Context().Err() != nil || current != binding {
				h.recordRefusal(response.BootstrapID, "poll_auth", authErr)
				http.Error(w, "refused", http.StatusForbidden)
				return
			}
			h.mu.Lock()
			if slot.binding == nil || *slot.binding != current {
				h.mu.Unlock()
				http.Error(w, "refused", http.StatusForbidden)
				return
			}
			command = takeServiceCommand(slot)
			h.mu.Unlock()
		}
	}
	// Delivery is consumed before writing. An ambiguous response never redelivers.
	serviceJSON(w, command)
}
func takeServiceCommand(slot *serviceSlot) *ServiceCommand {
	if slot.pending == nil || slot.pending.delivered {
		return nil
	}
	slot.pending.delivered = true
	command := slot.pending.command
	return &command
}
func acceptServiceResult(slot *serviceSlot, response ServiceResponse) bool {
	pending := slot.pending
	if !validServiceResponse(response) || pending == nil || !pending.delivered || pending.completed || pending.command.Nonce != response.Nonce || pending.command.Kind != response.Kind {
		return false
	}
	select {
	case pending.result <- response:
		pending.completed = true
		return true
	default:
		return false
	}
}

func validServiceResponse(r ServiceResponse) bool {
	fields := 0
	for _, present := range []bool{r.Readiness != nil, r.Status != nil, r.Proof != nil, r.Handle != nil, r.Drain != nil} {
		if present {
			fields++
		}
	}
	if r.Failed {
		return fields == 0
	}
	if fields != 1 {
		return false
	}
	switch r.Kind {
	case "status", "start":
		return r.Status != nil
	case "readiness":
		return r.Readiness != nil
	case "verify":
		return r.Proof != nil
	case "begin_drain":
		return r.Handle != nil
	case "wait_drain":
		return r.Drain != nil
	default:
		return false
	}
}

func (h *RuntimeHost) call(ctx context.Context, id string, command ServiceCommand) (ServiceResponse, error) {
	var empty ServiceResponse
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return empty, err
	}
	command.Nonce = hex.EncodeToString(raw)
	pending := &servicePending{command: command, result: make(chan ServiceResponse, 1)}
	h.mu.Lock()
	slot := h.slots[id]
	if slot == nil || slot.binding == nil || slot.pending != nil {
		h.mu.Unlock()
		return empty, durable.ErrQueryRetention
	}
	slot.pending = pending
	select {
	case slot.wake <- struct{}{}:
	default:
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		if slot.pending == pending {
			slot.pending = nil
		}
		h.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		h.mu.Lock()
		delivered := pending.delivered
		h.mu.Unlock()
		return empty, fmt.Errorf("managed service %s (delivered=%t): %w", command.Kind, delivered, ctx.Err())
	case response := <-pending.result:
		if response.Failed {
			return empty, durable.ErrQueryRetention
		}
		return response, nil
	}
}
func (h *RuntimeHost) fixed(target durable.QueryRuntimeTarget) (string, durable.QueryRuntimeIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, slot := range h.slots {
		if slot.binding == nil {
			continue
		}
		identity, err := ServiceIdentity(slot.startup, *slot.binding)
		if err == nil && identity.QueryRuntimeTarget == target {
			return id, identity, nil
		}
	}
	return "", durable.QueryRuntimeIdentity{}, durable.ErrQueryRetention
}

// EnrolledIdentity reads the fixed creation and boot association so the
// controller can enroll the native build before enabling claims. It is not a
// runtime readiness proof and cannot authorize removal or provider transport.
func (h *RuntimeHost) EnrolledIdentity(target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	_, identity, err := h.fixed(target)
	return identity, err
}

// StartWorker starts one real polling loop after native build enrollment. An
// uncertain reply is reconciled with ResolveBinding, never another worker ID.
func (h *RuntimeHost) StartWorker(ctx context.Context, target durable.QueryRuntimeTarget) (drt.WorkerStatus, error) {
	id, identity, err := h.fixed(target)
	if err != nil {
		return drt.WorkerStatus{}, err
	}
	response, err := h.call(ctx, id, ServiceCommand{Kind: "start"})
	if err != nil {
		return drt.WorkerStatus{}, err
	}
	if response.Status == nil || !workerMatches(*response.Status, identity) || !response.Status.Ready || response.Status.AdmissionClosed {
		return drt.WorkerStatus{}, durable.ErrQueryRetention
	}
	return *response.Status, nil
}

// ResolveBinding challenges the enrolled boot and checks the real worker IDs.
func (h *RuntimeHost) ResolveBinding(ctx context.Context, target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	id, identity, err := h.fixed(target)
	if err != nil {
		return identity, err
	}
	response, err := h.call(ctx, id, ServiceCommand{Kind: "status"})
	if err != nil {
		return durable.QueryRuntimeIdentity{}, err
	}
	if response.Status == nil || !workerMatches(*response.Status, identity) {
		return durable.QueryRuntimeIdentity{}, durable.ErrQueryRetention
	}
	h.mu.Lock()
	queue := h.slots[id].startup.Queue
	h.mu.Unlock()
	if response.Status.Queue != queue || response.Status.State == drt.WorkerFailed || (!response.Status.Ready && !response.Status.AdmissionClosed) {
		return durable.QueryRuntimeIdentity{}, durable.ErrQueryRetention
	}
	return identity, nil
}

// Verify requests actual retained-query execution from the same authenticated boot.
func (h *RuntimeHost) Verify(ctx context.Context, binding durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error) {
	id, identity, err := h.fixed(binding.QueryRuntimeTarget)
	if err != nil {
		return durable.QueryRuntimeVerification{}, err
	}
	if identity != binding.QueryRuntimeIdentity {
		return durable.QueryRuntimeVerification{}, durable.ErrQueryRetention
	}
	response, err := h.call(ctx, id, ServiceCommand{Kind: "verify", Binding: &binding})
	if err != nil {
		return durable.QueryRuntimeVerification{}, err
	}
	if response.Proof == nil || response.Proof.Validate(identity, time.Now().UTC()) != nil {
		return durable.QueryRuntimeVerification{}, durable.ErrQueryRetention
	}
	return *response.Proof, nil
}

// BeginDrain forwards the exact original operation and deadline once per call.
// The native worker owns replay and cannot reopen closed admission.
func (h *RuntimeHost) BeginDrain(ctx context.Context, target durable.QueryRuntimeTarget, request drt.DrainRequest) (drt.DrainHandle, error) {
	id, _, err := h.fixed(target)
	if err != nil {
		return drt.DrainHandle{}, err
	}
	response, err := h.call(ctx, id, ServiceCommand{Kind: "begin_drain", Drain: &request})
	if err != nil {
		return drt.DrainHandle{}, err
	}
	if response.Handle == nil || response.Handle.RuntimeID != target.RuntimeID || !sameDrainRequest(response.Handle.DrainRequest, request) {
		return drt.DrainHandle{}, durable.ErrQueryRetention
	}
	return *response.Handle, nil
}

// WaitDrain observes the exact native drain operation without issuing another.
func (h *RuntimeHost) WaitDrain(ctx context.Context, target durable.QueryRuntimeTarget, handle drt.DrainHandle) (drt.DrainResult, error) {
	id, _, err := h.fixed(target)
	if err != nil || handle.RuntimeID != target.RuntimeID {
		return drt.DrainResult{}, durable.ErrQueryRetention
	}
	response, err := h.call(ctx, id, ServiceCommand{Kind: "wait_drain", Handle: &handle})
	if err != nil {
		return drt.DrainResult{}, err
	}
	if response.Drain == nil || !sameDrainHandle(response.Drain.Handle, handle) {
		return drt.DrainResult{}, durable.ErrQueryRetention
	}
	return *response.Drain, nil
}

var _ operator.QueryRuntimeHost = (*RuntimeHost)(nil)

func sameDrainRequest(a, b drt.DrainRequest) bool {
	a.Deadline = a.Deadline.UTC().Round(0)
	b.Deadline = b.Deadline.UTC().Round(0)
	return a == b
}
func sameDrainHandle(a, b drt.DrainHandle) bool {
	a.Deadline = a.Deadline.UTC().Round(0)
	b.Deadline = b.Deadline.UTC().Round(0)
	return a == b
}
