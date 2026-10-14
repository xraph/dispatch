//go:build linux

package launcher

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	cid "github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
)

func servicePlanFixture(t *testing.T) ServicePlan {
	t.Helper()
	p := ServicePlan{BootstrapID: "service-one", Target: managed.Key{TenantID: "tenant", InstanceID: cid.New(cid.PrefixInstance)}, ProviderOperationID: cid.New(managed.PrefixOperation), Service: managed.ProvisionService{Name: "worker", Image: "registry.invalid/worker@sha256:" + strings.Repeat("a", 64), Role: "main", CPUMillis: "500", MemoryMiB: "256", PIDLimit: "64", Startup: managed.StartupBindingRef{BindingID: "startup-one", BindingGeneration: "1"}}, Endpoint: "https://host.invalid:9443", ServerName: "host.invalid", RuntimeID: strings.Repeat("a", 64), InstanceID: strings.Repeat("b", 64), HostID: strings.Repeat("c", 64), Installation: "installation", Namespace: "namespace", Build: "build", Queue: "queue", ArtifactDigest: strings.Repeat("d", 64), ConfigurationDigest: managed.Digest([]byte(`{"dsn":"private"}`)), ConfigurationVersion: "v1", ProbePolicyID: "probe", ProbePolicyVersion: "1", MaximumProofValidityNS: "30000000000", VerifierID: "verifier", ProducerVerifier: "producer", Configuration: json.RawMessage(`{"dsn":"private"}`)}
	digest, e := ServicePlanDigest(p)
	if e != nil {
		t.Fatal(e)
	}
	p.Service.Startup.BindingDigest = digest
	return p
}

func TestServicePlanRejectsSubstitutionAndUnboundedInput(t *testing.T) {
	p := servicePlanFixture(t)
	if e := validateServicePlan(p); e != nil {
		t.Fatal(e)
	}
	for name, change := range map[string]func(*ServicePlan){
		"target":             func(p *ServicePlan) { p.Target.InstanceID = cid.New(cid.PrefixInstance) },
		"provider-operation": func(p *ServicePlan) { p.ProviderOperationID = cid.New(managed.PrefixOperation) },
		"runtime":            func(p *ServicePlan) { p.RuntimeID = strings.Repeat("e", 64) },
		"configuration":      func(p *ServicePlan) { p.Configuration = json.RawMessage(`{ "dsn":"private"}`) },
		"http":               func(p *ServicePlan) { p.Endpoint = "http://host.invalid" },
		"endpoint-path":      func(p *ServicePlan) { p.Endpoint += "/caller-path" },
		"image-tag":          func(p *ServicePlan) { p.Service.Image = "worker:latest" },
		"memory":             func(p *ServicePlan) { p.Service.MemoryMiB = "513" },
		"cpu":                func(p *ServicePlan) { p.Service.CPUMillis = "2001" },
		"pid":                func(p *ServicePlan) { p.Service.PIDLimit = "129" },
		"proof-validity":     func(p *ServicePlan) { p.MaximumProofValidityNS = "60000000001" },
	} {
		t.Run(name, func(t *testing.T) {
			q := p
			change(&q)
			if validateServicePlan(q) == nil {
				t.Fatal("changed plan accepted")
			}
		})
	}
}

func TestServiceReadyRequiresExactProcessAndDescriptor(t *testing.T) {
	p := servicePlanFixture(t)
	ready := ServiceReady{BootNonce: strings.Repeat("f", 64), BootstrapID: p.BootstrapID, RuntimeID: p.RuntimeID, InstanceID: p.InstanceID, HostID: p.HostID, ArtifactDigest: p.ArtifactDigest, ConfigurationDigest: p.ConfigurationDigest, Descriptor: p.Service.Startup}
	if !serviceReadyMatches(p, ready) {
		t.Fatal("exact ready refused")
	}
	for name, change := range map[string]func(*ServiceReady){"missing-boot": func(r *ServiceReady) { r.BootNonce = "" }, "runtime": func(r *ServiceReady) { r.RuntimeID = strings.Repeat("1", 64) }, "instance": func(r *ServiceReady) { r.InstanceID = strings.Repeat("1", 64) }, "artifact": func(r *ServiceReady) { r.ArtifactDigest = strings.Repeat("1", 64) }, "config": func(r *ServiceReady) { r.ConfigurationDigest = strings.Repeat("1", 64) }, "descriptor": func(r *ServiceReady) { r.Descriptor.BindingGeneration = "2" }} {
		t.Run(name, func(t *testing.T) {
			q := ready
			change(&q)
			if serviceReadyMatches(p, q) {
				t.Fatal("substituted readiness accepted")
			}
		})
	}
}

type serviceCreationFixtureReader struct {
	value   ServiceCreation
	failure error
	calls   int
}

func (r *serviceCreationFixtureReader) ReadServiceCreation(_ context.Context, _ docker.ManagedDockerCommand) (ServiceCreation, error) {
	r.calls++
	return r.value, r.failure
}
func serviceFixture(t *testing.T) (*ServiceEnrollment, *serviceCreationFixtureReader, ServicePlan, docker.ManagedDockerCommand) {
	t.Helper()
	journal := rootJournal(t)
	p := servicePlanFixture(t)
	command := docker.ManagedDockerCommand{Kind: managed.StepCreateContainer, Name: "hint-only", NetworkID: strings.Repeat("1", 64), Service: &p.Service}
	reader := &serviceCreationFixtureReader{value: ServiceCreation{Target: p.Target, ProviderOperationID: p.ProviderOperationID, Command: command, Custody: docker.ManagedDockerCustody{ResourceID: strings.Repeat("2", 64), Incarnation: strings.Repeat("3", 64), Attestation: []byte("fixture-configured-reader-authenticated")}}}
	s := &ServiceEnrollment{host: &Host{}, root: journal.root, reader: reader, plans: map[string]ServicePlan{p.BootstrapID: p}}
	digest, e := ServicePlanDigest(p)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.save(serviceRecord{ID: p.BootstrapID, PlanDigest: digest, Phase: "prepared", Account: "account", Key: "key", Peer: strings.Repeat("4", 64)}, true); e != nil {
		t.Fatal(e)
	}
	return s, reader, p, command
}

func TestServiceBindOriginalCreationReplayAndReopen(t *testing.T) {
	s, reader, p, command := serviceFixture(t)
	first, e := s.BindCreated(context.Background(), p.BootstrapID, command)
	if e != nil {
		t.Fatal(e)
	}
	if first.ResourceID != reader.value.Custody.ResourceID || first.BootNonce != "" {
		t.Fatal("incorrect original association")
	}
	reopened := *s
	second, e := reopened.BindCreated(context.Background(), p.BootstrapID, command)
	if e != nil || first != second {
		t.Fatalf("replay: %+v %v", second, e)
	}
	reader.value.Custody.ResourceID = strings.Repeat("5", 64)
	if _, e = s.BindCreated(context.Background(), p.BootstrapID, command); !errors.Is(e, ErrRefused) {
		t.Fatalf("replacement adopted: %v", e)
	}
	saved, e := s.read(p.BootstrapID)
	if e != nil || saved.Creation.Custody.ResourceID != first.ResourceID {
		t.Fatal("retained original changed")
	}
	reader.failure = ErrUncertain
	if _, e = s.BindCreated(context.Background(), p.BootstrapID, command); !errors.Is(e, ErrUncertain) {
		t.Fatalf("lost authority accepted: %v", e)
	}
}

func TestServiceBindRejectsParentCommandAndCustodySubstitution(t *testing.T) {
	for name, change := range map[string]func(*ServiceCreation){"target": func(v *ServiceCreation) { v.Target.InstanceID = cid.New(cid.PrefixInstance) }, "operation": func(v *ServiceCreation) { v.ProviderOperationID = cid.New(managed.PrefixOperation) }, "network": func(v *ServiceCreation) { v.Command.NetworkID = strings.Repeat("4", 64) }, "resource": func(v *ServiceCreation) { v.Custody.ResourceID = "" }, "incarnation": func(v *ServiceCreation) { v.Custody.Incarnation = "" }, "attestation": func(v *ServiceCreation) { v.Custody.Attestation = nil }} {
		t.Run(name, func(t *testing.T) {
			s, reader, p, command := serviceFixture(t)
			change(&reader.value)
			if _, e := s.BindCreated(context.Background(), p.BootstrapID, command); !errors.Is(e, ErrRefused) {
				t.Fatalf("accepted: %v", e)
			}
			saved, e := s.read(p.BootstrapID)
			if e != nil || saved.Phase != "prepared" || saved.Creation != nil {
				t.Fatal("refusal changed enrollment")
			}
		})
	}
}

func TestServiceCanceledBindingAndBootHeaderCannotAuthorize(t *testing.T) {
	s, reader, p, command := serviceFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := s.BindCreated(ctx, p.BootstrapID, command); !errors.Is(e, context.Canceled) || reader.calls != 0 {
		t.Fatalf("cancel crossed reader: %v calls %d", e, reader.calls)
	}
	if _, e := s.BindCreated(context.Background(), p.BootstrapID, command); e != nil {
		t.Fatal(e)
	}
	record, e := s.read(p.BootstrapID)
	if e != nil {
		t.Fatal(e)
	}
	record.Ready = &ServiceReady{BootNonce: strings.Repeat("f", 64), BootstrapID: p.BootstrapID, RuntimeID: p.RuntimeID, InstanceID: p.InstanceID, HostID: p.HostID, ArtifactDigest: p.ArtifactDigest, ConfigurationDigest: p.ConfigurationDigest, Descriptor: p.Service.Startup}
	if e = s.save(record, false); e != nil {
		t.Fatal(e)
	}
	for _, nonce := range []string{"", strings.Repeat("e", 64), strings.Repeat("f", 64)} {
		request := httptest.NewRequestWithContext(t.Context(), "POST", "https://host.invalid/managed-service/next", nil)
		request.Header.Set("X-Writerfloor-Boot", nonce)
		if _, e = s.AuthenticateChannel(context.Background(), request, p.BootstrapID); !errors.Is(e, ErrRefused) {
			t.Fatalf("boot header without TLS became authority: %v", e)
		}
	}
}

func TestServiceResolverUsesRetainedStartupWithoutCreation(t *testing.T) {
	s, reader, p, _ := serviceFixture(t)
	record, e := s.read(p.BootstrapID)
	if e != nil {
		t.Fatal(e)
	}
	startup := ServiceStartup{ServicePlan: p, App: s.host.app.String(), Environment: s.host.environment.String(), Tenant: s.host.tenant, Account: record.Account, Key: record.Key, Bearer: "private-fixture", ClientKeyPEM: []byte("private-key")}
	raw, e := json.Marshal(startup)
	if e != nil {
		t.Fatal(e)
	}
	if e = writePrivate(s.root, p.BootstrapID+"-service-startup", raw, true); e != nil {
		t.Fatal(e)
	}
	record.StartupDigest = managed.Digest(raw)
	if e = s.save(record, false); e != nil {
		t.Fatal(e)
	}
	resolved, e := s.ResolveManagedDockerStartup(context.Background(), p.Service)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(resolved.Entrypoint, []string{"/app/workload"}) || !reflect.DeepEqual(resolved.Arguments, []string{"service"}) || len(resolved.Environment) != 1 || reader.calls != 0 {
		t.Fatal("resolver escaped fixed mode")
	}
	decoded, e := base64.StdEncoding.DecodeString(strings.TrimPrefix(resolved.Environment[0], "WRITERFLOOR_SERVICE_STARTUP="))
	if e != nil || string(decoded) != string(raw) {
		t.Fatal("private startup changed")
	}
	altered := p.Service
	altered.Startup.BindingGeneration = "2"
	if _, e = s.ResolveManagedDockerStartup(context.Background(), altered); !errors.Is(e, ErrRefused) {
		t.Fatal("unknown descriptor resolved")
	}
	record.Phase = "closed"
	if e = s.save(record, false); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ResolveManagedDockerStartup(context.Background(), p.Service); !errors.Is(e, ErrUncertain) {
		t.Fatal("closed enrollment reopened")
	}
}

func TestServiceReaderRejectsTypedNil(t *testing.T) {
	var reader *serviceCreationFixtureReader
	if !serviceReaderAbsent(reader) || !serviceReaderAbsent(nil) || serviceReaderAbsent(&serviceCreationFixtureReader{}) {
		t.Fatal("typed nil boundary")
	}
}

func TestServiceJournalRejectsCorruptReadyAndPlan(t *testing.T) {
	s, _, p, command := serviceFixture(t)
	if _, e := s.BindCreated(context.Background(), p.BootstrapID, command); e != nil {
		t.Fatal(e)
	}
	record, e := s.read(p.BootstrapID)
	if e != nil {
		t.Fatal(e)
	}
	record.Ready = &ServiceReady{BootNonce: strings.Repeat("f", 64), BootstrapID: p.BootstrapID, RuntimeID: p.RuntimeID, InstanceID: p.InstanceID, HostID: p.HostID, ArtifactDigest: p.ArtifactDigest, ConfigurationDigest: p.ConfigurationDigest, Descriptor: p.Service.Startup}
	if e = s.save(record, false); e != nil {
		t.Fatal(e)
	}
	saved, e := s.read(p.BootstrapID)
	if e != nil {
		t.Fatal(e)
	}
	binding, e := serviceBinding(saved)
	if e != nil || binding.BootNonce != record.Ready.BootNonce {
		t.Fatal("boot binding not retained")
	}
	changed := *record.Ready
	changed.Descriptor.BindingGeneration = "2"
	record.Ready = &changed
	if e = s.save(record, false); e != nil {
		t.Fatal(e)
	}
	if _, e = s.read(p.BootstrapID); !errors.Is(e, ErrRefused) {
		t.Fatalf("corrupt descriptor accepted: %v", e)
	}
	record.Ready = nil
	record.PlanDigest = strings.Repeat("0", 64)
	if e = s.save(record, false); e != nil {
		t.Fatal(e)
	}
	if _, e = s.read(p.BootstrapID); !errors.Is(e, ErrRefused) {
		t.Fatalf("changed plan accepted: %v", e)
	}
}

func TestServiceChannelJournalContentionIsBounded(t *testing.T) {
	j := rootJournal(t)
	held, err := openJournal(j.root, "channel")
	if err != nil {
		t.Fatal(err)
	}
	defer held.close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if lock, e := openServiceJournal(ctx, j.root, "channel"); !errors.Is(e, context.DeadlineExceeded) || lock != nil {
		t.Fatalf("busy journal did not obey deadline: %v", e)
	}
	if err = held.close(); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(t.Context(), time.Second)
	defer cancel2()
	lock, err := openServiceJournal(ctx2, j.root, "channel")
	if err != nil {
		t.Fatal(err)
	}
	if err = lock.close(); err != nil {
		t.Fatal(err)
	}
}
