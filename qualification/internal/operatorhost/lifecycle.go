package operatorhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
)

// LifecycleOptions comes from trusted local configuration, never request fields.
// StartupPolicy is the physical-instance enrollment hook for a deployment host.
type LifecycleOptions struct {
	InstanceID         string
	RegistrationPolicy operator.QueryRegistrationPolicy
	StartupPolicy      func(context.Context, durable.WorkerProcessIdentity) error
	Probes             map[string][]HistoricalProbe
}

// HistoricalProbe names an explicit retained run and the expected query output.
type HistoricalProbe struct {
	Name           string
	Key            durable.Key
	Query          string
	ExpectedDigest string
}

type probeSample struct {
	Name         string
	Key          durable.Key
	Query        string
	Revision     int64
	LastSequence int64
	OutputDigest string
}

type lifecycleHost struct {
	host              *Host
	options           LifecycleOptions
	identities        map[string]durable.BuildQueryIdentity
	mu                sync.Mutex
	removedRuntimes   map[string]string
	revokedOperations map[string]bool
}

func lifecycleActions() []string {
	return []string{operator.ReadBuild, operator.EnrollRetirement, operator.RegisterBuild, operator.RetireBuild, operator.FinalizeBuild, operator.ResumeBuild, operator.ReadWorker, operator.DrainWorker, operator.ReadQueryRuntime, operator.RegisterQueryRuntime, operator.VerifyQueryRuntime, operator.RemoveQueryRuntime, operator.FinishQueryRuntime, operator.AbortQueryRuntime}
}

func defaultProbes(build string) []HistoricalProbe {
	sum := sha256.Sum256([]byte("history"))
	return []HistoricalProbe{{Name: "retained-status", Key: durable.Key{Namespace: "production", WorkflowID: "lifecycle-history-" + build, RunID: "run-1"}, Query: "status", ExpectedDigest: hex.EncodeToString(sum[:])}}
}

func newLifecycleHost(host *Host, options LifecycleOptions) (*lifecycleHost, error) {
	if !durable.DeliveryIdentifier(options.InstanceID) {
		return nil, durable.ErrInvalid
	}
	path, err := os.Executable()
	if err != nil {
		return nil, err
	}
	executable, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, executable)
	closeErr := executable.Close()
	if closeCopyErr := errors.Join(copyErr, closeErr); closeCopyErr != nil {
		return nil, closeCopyErr
	}
	artifact := hex.EncodeToString(hash.Sum(nil))
	configured := options.Probes
	options.Probes = map[string][]HistoricalProbe{}
	h := &lifecycleHost{host: host, options: options, identities: map[string]durable.BuildQueryIdentity{}, removedRuntimes: map[string]string{}, revokedOperations: map[string]bool{}}
	for _, build := range []string{"operator-v1", "operator-v2"} {
		probes, ok := configured[build]
		if !ok {
			probes = defaultProbes(build)
		}
		if len(probes) == 0 {
			return nil, durable.ErrInvalid
		}
		names := map[string]bool{}
		for _, probe := range probes {
			if !durable.DeliveryIdentifier(probe.Name) || names[probe.Name] || probe.Key.Namespace != "production" || len(probe.ExpectedDigest) != 64 {
				return nil, durable.ErrInvalid
			}
			if _, decodeErr := hex.DecodeString(probe.ExpectedDigest); decodeErr != nil {
				return nil, durable.ErrInvalid
			}
			if validateErr := (drt.QueryRequest{Key: probe.Key, Selection: durable.RunExplicit, BuildID: build, Name: probe.Query}).Validate(); validateErr != nil {
				return nil, validateErr
			}
			names[probe.Name] = true
		}
		h.options.Probes[build] = slices.Clone(probes)
		configuration := struct {
			Build, Namespace, Queue, Profile string
			Probes                           []HistoricalProbe
		}{build, "production", "operator", "operator-lifecycle-v1", probes}
		digest, digestErr := durable.Fingerprint("operator_host.configuration.v1", configuration)
		if digestErr != nil {
			return nil, digestErr
		}
		enrollment, enrollErr := durable.Fingerprint("operator_host.executable_configuration.v1", struct{ Artifact, Configuration string }{artifact, digest})
		if enrollErr != nil {
			return nil, enrollErr
		}
		h.identities[build] = durable.BuildQueryIdentity{ArtifactDigest: artifact, ConfigurationDigest: digest, ConfigurationVersion: configuration.Profile, EnrollmentEvidenceDigest: enrollment, ProbePolicyID: "retained-status", ProbePolicyVersion: 1, VerifierID: "operator-host-native", MaximumProofValidity: time.Minute}
	}
	return h, nil
}

func (h *lifecycleHost) buildIdentity(ctx context.Context, target durable.BuildTarget) (durable.BuildQueryIdentity, error) {
	if err := ctx.Err(); err != nil {
		return durable.BuildQueryIdentity{}, err
	}
	identity, ok := h.identities[target.BuildID]
	if target.Validate() != nil || target.InstallationID != "operator-host" || target.Namespace != "production" || !ok {
		return durable.BuildQueryIdentity{}, operator.ErrRuntimeUnavailable
	}
	return identity, nil
}

func (h *lifecycleHost) workerControl(ctx context.Context, target durable.QueryRuntimeTarget) (operator.WorkerControl, error) {
	if _, err := h.buildIdentity(ctx, target.BuildTarget); err != nil {
		return nil, err
	}
	worker := h.host.runtime.workers[target.BuildID]
	if worker == nil || worker.Status().RuntimeID != target.RuntimeID {
		return nil, operator.ErrRuntimeUnavailable
	}
	return operator.LocalWorkerControl{Worker: worker}, nil
}

func (h *lifecycleHost) ResolveBinding(ctx context.Context, target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	control, err := h.workerControl(ctx, target)
	if err != nil {
		return durable.QueryRuntimeIdentity{}, err
	}
	status, err := control.Status(ctx)
	if err != nil {
		return durable.QueryRuntimeIdentity{}, err
	}
	return durable.QueryRuntimeIdentity{QueryRuntimeTarget: target, InstanceID: status.InstanceID, IdentityVersion: 1, BuildIdentity: h.identities[target.BuildID]}, nil
}

func (h *lifecycleHost) authorizeStartup(ctx context.Context, worker *drt.Worker) error {
	status := worker.Status()
	target := durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "operator-host", Namespace: status.Namespace}, BuildID: status.BuildID}, RuntimeID: status.RuntimeID}
	identity, err := h.ResolveBinding(ctx, target)
	if err != nil {
		return err
	}
	if h.options.StartupPolicy != nil {
		if policyErr := h.options.StartupPolicy(ctx, durable.WorkerProcessIdentity{BuildTarget: target.BuildTarget, Queue: status.Queue, RuntimeID: status.RuntimeID, InstanceID: status.InstanceID}); policyErr != nil {
			return policyErr
		}
	}
	store, ok := h.host.Store.(durable.QueryRuntimeStore)
	if !ok {
		return durable.ErrQueryRetention
	}
	after := ""
	for ctx.Err() == nil {
		page, readErr := store.ListQueryRuntimes(ctx, durable.QueryRuntimeList{NamespaceTarget: target.NamespaceTarget, BuildID: target.BuildID, InstanceID: identity.InstanceID, After: after, Limit: durable.MaxReadPage})
		if readErr != nil {
			return readErr
		}
		for _, binding := range page.Items {
			if binding.QueryRuntimeIdentity == identity && binding.State == durable.QueryRuntimeActive && binding.Validate() == nil {
				return nil
			}
		}
		if page.Next == "" || page.Next <= after {
			break
		}
		after = page.Next
	}
	return durable.ErrQueryRetention
}

func (h *lifecycleHost) removed(runtimeID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.removedRuntimes[runtimeID] != ""
}

func (h *lifecycleHost) Verify(ctx context.Context, binding durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.verify(ctx, binding)
}

func (h *lifecycleHost) verify(ctx context.Context, binding durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error) {
	identity, err := h.ResolveBinding(ctx, binding.QueryRuntimeTarget)
	if err != nil || identity != binding.QueryRuntimeIdentity || h.removedRuntimes[identity.RuntimeID] != "" {
		return durable.QueryRuntimeVerification{}, durable.ErrQueryRetention
	}
	worker := h.host.runtime.workers[identity.BuildID]
	status := worker.Status()
	if !status.AdmissionClosed || status.InFlight != 0 || status.UnknownClaims != 0 {
		return durable.QueryRuntimeVerification{}, durable.ErrQueryRetention
	}
	samples := make([]probeSample, 0, len(h.options.Probes[identity.BuildID]))
	for _, probe := range h.options.Probes[identity.BuildID] {
		result, queryErr := worker.QueryExecution(ctx, drt.QueryRequest{Key: probe.Key, Selection: durable.RunExplicit, BuildID: identity.BuildID, Name: probe.Query})
		if queryErr != nil {
			return durable.QueryRuntimeVerification{}, queryErr
		}
		sum := sha256.Sum256(result.Output)
		digest := hex.EncodeToString(sum[:])
		if digest != probe.ExpectedDigest {
			return durable.QueryRuntimeVerification{}, durable.ErrQueryRetention
		}
		samples = append(samples, probeSample{Name: probe.Name, Key: result.Key, Query: probe.Query, Revision: result.Revision, LastSequence: result.LastSequence, OutputDigest: digest})
	}
	digest, err := durable.Fingerprint("operator_host.historical_probes.v1", struct {
		Identity durable.QueryRuntimeIdentity
		Samples  []probeSample
	}{identity, samples})
	if err != nil {
		return durable.QueryRuntimeVerification{}, err
	}
	now := durable.Timestamp(time.Now())
	proofID, err := durable.Fingerprint("operator_host.proof.v1", struct {
		Digest string
		At     time.Time
	}{digest, now})
	if err != nil {
		return durable.QueryRuntimeVerification{}, err
	}
	return durable.QueryRuntimeVerification{Identity: identity, ProofID: proofID, VerifierID: identity.BuildIdentity.VerifierID, EvidenceDigest: digest, ProbePolicyID: identity.BuildIdentity.ProbePolicyID, ProbePolicyVersion: identity.BuildIdentity.ProbePolicyVersion, VerifiedAt: now, ValidUntil: now.Add(identity.BuildIdentity.MaximumProofValidity)}, nil
}

// RuntimeIdentities returns constructed process identities for the private state file.
func (h *Host) RuntimeIdentities() []durable.QueryRuntimeIdentity {
	if h.lifecycle == nil {
		return nil
	}
	result := make([]durable.QueryRuntimeIdentity, 0, len(h.runtime.workers))
	for _, build := range []string{"operator-v1", "operator-v2"} {
		w := h.runtime.workers[build]
		result = append(result, durable.QueryRuntimeIdentity{QueryRuntimeTarget: durable.QueryRuntimeTarget{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "operator-host", Namespace: "production"}, BuildID: build}, RuntimeID: w.Status().RuntimeID}, InstanceID: h.lifecycle.options.InstanceID, IdentityVersion: 1, BuildIdentity: h.lifecycle.identities[build]})
	}
	return result
}
