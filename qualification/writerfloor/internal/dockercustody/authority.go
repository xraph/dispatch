//go:build linux

package dockercustody

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	provider "github.com/xraph/ctrlplane/provider/docker"
)

// fixtureAuthority is deliberately test enrollment. It is not the outer admission
// service, authenticated process enrollment, or evidence of native effect closure.
type fixtureAuthority struct {
	request  provider.ManagedDockerRequest
	key      []byte
	observer *observer
}

func (a *fixtureAuthority) VerifyRequest(ctx context.Context, r provider.ManagedDockerRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !reflect.DeepEqual(a.request, r) {
		return managed.ErrAuthority
	}
	_, err := a.observer.engine(ctx, r.BackendAuthority)
	return err
}
func (a *fixtureAuthority) Admit(ctx context.Context, r provider.ManagedDockerRequest) error {
	return a.VerifyRequest(ctx, r)
}
func (a *fixtureAuthority) mac(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	h := hmac.New(sha256.New, a.key)
	_, _ = h.Write(raw)
	return h.Sum(nil)
}
func fixtureIncarnation(engine, resource string) string {
	return "qualification:" + managed.Digest([]byte(engine+":"+resource))
}
func (a *fixtureAuthority) SealResponse(ctx context.Context, r provider.ManagedDockerRequest, response provider.ManagedDockerResponse) (provider.ManagedDockerCustody, error) {
	if err := a.VerifyRequest(ctx, r); err != nil {
		return provider.ManagedDockerCustody{}, err
	}
	custody := provider.ManagedDockerCustody{ResourceID: response.ResourceID, Incarnation: fixtureIncarnation(r.BackendAuthority, response.ResourceID)}
	if r.Command.Kind == managed.StepStartContainer {
		actual, err := a.observer.container(ctx, response.ResourceID)
		if err != nil {
			return custody, err
		}
		if actual.ID != response.ResourceID || !actual.State.Running || actual.State.Pid <= 0 || actual.State.StartedAt == "" {
			return custody, errors.New("started process not inspectable")
		}
		custody.OriginalProcess = &managed.OriginalProcessFacts{RuntimeID: "qualification:" + managed.Digest([]byte(fmt.Sprintf("%s:%d:%s", actual.ID, actual.State.Pid, actual.State.StartedAt)))}
	}
	custody.Attestation = a.mac(struct {
		Request  provider.ManagedDockerRequest
		Response provider.ManagedDockerResponse
		Custody  provider.ManagedDockerCustody
	}{r, response, custody})
	if len(custody.Attestation) == 0 {
		return custody, managed.ErrInvalid
	}
	return custody, nil
}
func (a *fixtureAuthority) VerifyCustody(ctx context.Context, r provider.ManagedDockerRequest, response provider.ManagedDockerResponse, c provider.ManagedDockerCustody) error {
	expected, err := a.SealResponse(ctx, r, response)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, c) {
		return managed.ErrAuthority
	}
	return nil
}
func (a *fixtureAuthority) VerifyProviderEffectBinding(ctx context.Context, o managed.ProviderEffectOriginal, f managed.ProviderEffectBindingFacts) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.request.Binding == nil || !reflect.DeepEqual(a.request.Binding.Original, o) || !reflect.DeepEqual(*a.request.Binding, f) {
		return managed.ErrAuthority
	}
	return nil
}
func (a *fixtureAuthority) NoDispatch(context.Context, provider.ManagedDockerRequest, string) (managed.ProviderInvocationFailure, error) {
	return managed.ProviderInvocationFailure{}, managed.ErrUnsupported
}
func (a *fixtureAuthority) VerifyNoDispatch(context.Context, provider.ManagedDockerRequest, string, managed.ProviderInvocationFailure) error {
	return managed.ErrUnsupported
}

type fixtureStartup struct{}

func (*fixtureStartup) ResolveManagedDockerStartup(_ context.Context, s managed.ProvisionService) (provider.ManagedDockerStartup, error) {
	if s.Startup.BindingID != "qualification-sleep" || s.Startup.BindingGeneration != "1" || s.Startup.BindingDigest != managed.Digest([]byte("/bin/sleep 600")) {
		return provider.ManagedDockerStartup{}, managed.ErrAuthority
	}
	return provider.ManagedDockerStartup{Entrypoint: []string{"/bin/sleep"}, Arguments: []string{"600"}}, nil
}

func requestFor(kind managed.ProviderStepKind, engine, image, name, network, resource string) provider.ManagedDockerRequest {
	now := time.Now().UTC().Add(-time.Second)
	step := managed.ProviderStepKey{OperationKey: managed.OperationKey{Key: managed.Key{TenantID: "qualification-only", InstanceID: id.New(id.PrefixInstance)}, ActionID: id.New(managed.PrefixOperation)}, ProviderOperationID: id.New(managed.PrefixOperation), Kind: kind}
	if kind == managed.StepCreateContainer || kind == managed.StepStartContainer || kind == managed.StepRemoveContainer {
		step.Service = "qualification-sleep"
	}
	p := managed.ProviderStepPermission{Step: step, CommandID: id.New(managed.PrefixOperation), Digest: managed.Digest([]byte(name + string(kind))), SpecDigest: managed.Digest([]byte(image)), CreateProfile: "ctrlplane.managed.create.v1", Physical: managed.Physical{Provider: "docker", Placement: "qualification", Project: "qualification", Generation: "1"}, IssuedAt: now, Revision: 1}
	c := provider.ManagedDockerCommand{Kind: kind}
	switch kind {
	case managed.StepCreateNetwork:
		c.Name = name
	case managed.StepCreateContainer:
		c.Name = name
		c.NetworkID = network
		c.Service = &managed.ProvisionService{Name: "qualification-sleep", Image: image, Role: "main", CPUMillis: "100", MemoryMiB: "32", PIDLimit: "16", Startup: managed.StartupBindingRef{BindingID: "qualification-sleep", BindingGeneration: "1", BindingDigest: managed.Digest([]byte("/bin/sleep 600"))}}
	default:
		p.ResourceID = resource
		c.ResourceID = resource
	}
	if kind == managed.StepRemoveContainer || kind == managed.StepRemoveNetwork {
		p.Seal = 1
		p.FenceDigest = strings.Repeat("d", 64)
	}
	r := provider.ManagedDockerRequest{BackendAuthority: engine, Permission: managed.ProviderPermissionWire(p), Receipt: managed.ReceiptWire(managed.Receipt{ID: p.CommandID, Digest: p.Digest, Revision: 1, LeaseEpoch: 1, AcceptedAt: now, LeaseUntil: now.Add(time.Hour)}), Command: c}
	if kind == managed.StepRemoveContainer || kind == managed.StepRemoveNetwork {
		attempt := managed.AttemptKey{Key: step.Key, AttemptID: id.New(managed.PrefixOperation), Generation: "1"}
		original := managed.ProviderEffectOriginal{Key: managed.ProviderEffectBindingKey{AttemptKey: attempt, Step: step, OriginalCommandID: p.CommandID}, AttemptRef: managed.AttemptRef{AttemptID: attempt.AttemptID, Generation: "1", DescriptorDigest: strings.Repeat("e", 64)}, Permission: r.Permission, Receipt: r.Receipt}
		resourceKind, method := "container", "container_remove"
		options := []managed.ProviderEffectOption{{Name: "force", Value: "true"}, {Name: "remove_volumes", Value: "false"}}
		if kind == managed.StepRemoveNetwork {
			resourceKind, method = "network", "network_remove"
			options = []managed.ProviderEffectOption{}
		}
		raw, err := managed.EncodeProviderEffectCommand(managed.ProviderEffectCommand{SchemaVersion: "1", Profile: provider.ManagedDockerProfile, BackendAuthority: engine, ResourceKind: resourceKind, ResourceID: resource, Incarnation: fixtureIncarnation(engine, resource), Method: method, Options: options})
		if err != nil {
			return provider.ManagedDockerRequest{}
		}
		digest, err := managed.ProviderEffectCommandDigest(raw)
		if err != nil {
			return provider.ManagedDockerRequest{}
		}
		r.Binding = &managed.ProviderEffectBindingFacts{Original: original, Producer: managed.RecoveryProducerIdentity{ProducerID: "qualification-only", Profile: provider.ManagedDockerProfile, Version: "1", VerifierID: "qualification-only"}, Command: raw, ProviderCommandDigest: digest, Attestation: []byte("qualification-only-fixture-binding")}
		r.Invocation = &managed.ProviderInvocationPermission{Key: managed.ProviderInvocationKey{AttemptKey: attempt, Step: step, CommandID: p.CommandID}, RootDigest: strings.Repeat("f", 64), Ordinal: "0", Permission: r.Permission, ProviderCommandDigest: digest, PermissionProofSetDigest: strings.Repeat("e", 64), PermissionProofCount: "1"}
	}
	return r
}
