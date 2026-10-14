//go:build linux

package launcher

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	"github.com/xraph/warden/wardenerr"
)

type serviceRecord struct {
	ID            string           `json:"id"`
	PlanDigest    string           `json:"plan_digest"`
	Phase         string           `json:"phase"`
	Account       string           `json:"account"`
	Key           string           `json:"key"`
	Peer          string           `json:"peer"`
	StartupDigest string           `json:"startup_digest"`
	Policy        *policy.Policy   `json:"policy"`
	Ready         *ServiceReady    `json:"ready"`
	Creation      *ServiceCreation `json:"creation"`
}

// ServiceEnrollment owns only a finite configured service set and real machine
// enrollment. It never creates, starts, removes or discovers a Docker resource.
type ServiceEnrollment struct {
	host   *Host
	root   string
	reader ServiceCreationReader
	plans  map[string]ServicePlan
	policy *warden.Engine
}

// NewServiceEnrollment requires a separate protected local service directory and
// a configured authenticated creation reader. No request can select a service plan.
func NewServiceEnrollment(host *Host, directory string, reader ServiceCreationReader, plans []ServicePlan) (*ServiceEnrollment, error) {
	if host == nil || host.engine == nil || host.policies == nil || serviceReaderAbsent(reader) || directory == host.root || len(plans) == 0 || len(plans) > MaxLaunches {
		return nil, ErrRefused
	}
	if e := secureDirectory(directory); e != nil {
		return nil, e
	}
	engine, e := warden.NewEngine(warden.WithStore(host.policies))
	if e != nil {
		return nil, e
	}
	out := &ServiceEnrollment{host: host, root: directory, reader: reader, plans: make(map[string]ServicePlan, len(plans)), policy: engine}
	descriptors := map[string]bool{}
	runtimes := map[string]bool{}
	instances := map[string]bool{}
	for _, plan := range plans {
		if operationErr := validateServicePlan(plan); operationErr != nil {
			return nil, operationErr
		}
		if _, exists := out.plans[plan.BootstrapID]; exists || descriptors[plan.Service.Startup.BindingID] || runtimes[plan.RuntimeID] || instances[plan.InstanceID] {
			return nil, ErrRefused
		}
		raw, err := json.Marshal(plan)
		if err != nil {
			return nil, err
		}
		var owned ServicePlan
		if operationErr := json.Unmarshal(raw, &owned); operationErr != nil {
			return nil, operationErr
		}
		out.plans[plan.BootstrapID] = owned
		descriptors[plan.Service.Startup.BindingID] = true
		runtimes[plan.RuntimeID] = true
		instances[plan.InstanceID] = true
	}
	return out, nil
}
func (s *ServiceEnrollment) plan(id string) (ServicePlan, error) {
	p, ok := s.plans[id]
	if !ok {
		return p, ErrRefused
	}
	return p, nil
}
func (s *ServiceEnrollment) read(id string) (serviceRecord, error) {
	p, e := s.plan(id)
	if e != nil {
		return serviceRecord{}, e
	}
	raw, e := readPrivate(filepath.Join(s.root, id+".json"), MaxRecord)
	if e != nil {
		return serviceRecord{}, e
	}
	var record serviceRecord
	digest, e := ServicePlanDigest(p)
	if e != nil {
		return record, e
	}
	if canonical(raw, &record, MaxRecord) != nil || record.ID != id || record.PlanDigest != digest {
		return record, ErrRefused
	}
	switch record.Phase {
	case "enrolling", "account", "key", "policy", "prepared", "bound", "closed":
	default:
		return record, ErrRefused
	}
	if record.Creation != nil && !serviceCreationMatches(p, record.Creation.Command, *record.Creation) {
		return record, ErrRefused
	}
	if record.Phase == "bound" && record.Creation == nil || record.Ready != nil && (record.Creation == nil || !serviceReadyMatches(p, *record.Ready)) {
		return record, ErrRefused
	}
	return record, nil
}
func (s *ServiceEnrollment) save(record serviceRecord, exclusive bool) error {
	raw, e := json.Marshal(record)
	if e != nil || len(raw) > MaxRecord {
		return ErrRefused
	}
	return writePrivate(s.root, record.ID+".json", raw, exclusive)
}

// Prepare retains credentials and exact Warden policy before any provider create.
// Ambiguous credential issuance refuses replay rather than creating another account/key.
func (s *ServiceEnrollment) Prepare(ctx context.Context, id string) (ServiceStartup, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	p, e := s.plan(id)
	if e != nil {
		return ServiceStartup{}, e
	}
	lock, e := openJournal(s.root, id)
	if e != nil {
		return ServiceStartup{}, e
	}
	defer lock.close()
	record, e := s.read(id)
	if e == nil {
		if record.Phase == "prepared" || record.Phase == "bound" {
			return s.Startup(ctx, id)
		}
		return ServiceStartup{}, ErrUncertain
	}
	if !errors.Is(e, os.ErrNotExist) {
		return ServiceStartup{}, e
	}
	digest, e := ServicePlanDigest(p)
	if e != nil {
		return ServiceStartup{}, e
	}
	record = serviceRecord{ID: id, PlanDigest: digest, Phase: "enrolling"}
	if operationErr := s.save(record, true); operationErr != nil {
		return ServiceStartup{}, operationErr
	}
	scopes := []string{"managed-service.ready", "managed-service.channel"}
	account, e := s.host.engine.CreateServiceAccountInEnvironment(ctx, s.host.app, s.host.environment, "managed-service-"+id, "fixed provider service", scopes)
	if e != nil {
		return ServiceStartup{}, ErrUncertain
	}
	record.Account = account.ID.String()
	record.Phase = "account"
	if operationErr := s.save(record, false); operationErr != nil {
		return ServiceStartup{}, operationErr
	}
	expiry := time.Now().UTC().Add(time.Hour)
	key, bearer, e := s.host.engine.CreateServiceAccountAPIKey(ctx, account.ID, "managed-service-"+id, scopes, &expiry)
	if e != nil {
		return ServiceStartup{}, ErrUncertain
	}
	record.Key = key.ID.String()
	record.Phase = "key"
	if operationErr := s.save(record, false); operationErr != nil {
		return ServiceStartup{}, operationErr
	}
	ca, e := readPrivate(filepath.Join(s.host.configDirectory, "proxy-ca.pem"), MaxRecord)
	if e != nil {
		return ServiceStartup{}, e
	}
	caKey, e := readPrivate(filepath.Join(s.host.configDirectory, "proxy-ca-key.pem"), MaxRecord)
	if e != nil {
		return ServiceStartup{}, e
	}
	_, peer, e := issueProxy(s.root, id, ca, caKey)
	if e != nil {
		return ServiceStartup{}, e
	}
	record.Peer = peer
	cert, e := readPrivate(filepath.Join(s.root, id+"-proxy-cert.pem"), MaxRecord)
	if e != nil {
		return ServiceStartup{}, e
	}
	privateKey, e := readPrivate(filepath.Join(s.root, id+"-proxy-key.pem"), MaxRecord)
	if e != nil {
		return ServiceStartup{}, e
	}
	serverCA, e := readPrivate(filepath.Join(s.host.configDirectory, "upstream-ca.pem"), MaxRecord)
	if e != nil {
		return ServiceStartup{}, e
	}
	startup := ServiceStartup{ServicePlan: p, App: s.host.app.String(), Environment: s.host.environment.String(), Tenant: s.host.tenant, Account: record.Account, Key: record.Key, Bearer: bearer, CAPEM: serverCA, ClientCertificatePEM: cert, ClientKeyPEM: privateKey}
	raw, e := json.Marshal(startup)
	if e != nil || len(raw) > MaxManifest || len(base64.StdEncoding.EncodeToString(raw))+128 > MaxManifest {
		return ServiceStartup{}, ErrRefused
	}
	if operationErr := writePrivate(s.root, id+"-service-startup", raw, true); operationErr != nil {
		return ServiceStartup{}, operationErr
	}
	record.StartupDigest = managed.Digest(raw)
	now := time.Now().UTC().Truncate(time.Microsecond)
	record.Policy = &policy.Policy{ID: wid.NewPolicyID(), AppID: s.host.app.String(), TenantID: s.host.tenant, Name: "managed-service-" + id, Effect: policy.EffectAllow, IsActive: true, Version: 1, Subjects: []policy.SubjectMatch{{Kind: "service_acct", ID: record.Account}}, Actions: scopes, Resources: []string{"managed_service:" + id}, CreatedAt: now, UpdatedAt: now}
	record.Phase = "policy"
	if operationErr := s.save(record, false); operationErr != nil {
		return ServiceStartup{}, operationErr
	}
	if e = s.host.policies.CreatePolicy(ctx, record.Policy); e != nil {
		return ServiceStartup{}, ErrUncertain
	}
	saved, e := s.host.policies.GetPolicy(ctx, s.host.tenant, record.Policy.ID)
	if e != nil || saved == nil {
		return ServiceStartup{}, ErrUncertain
	}
	expected, e := json.Marshal(record.Policy)
	if e != nil {
		return ServiceStartup{}, e
	}
	actual, e := json.Marshal(saved)
	if e != nil || !bytes.Equal(expected, actual) {
		return ServiceStartup{}, ErrRefused
	}
	record.Phase = "prepared"
	if operationErr := s.save(record, false); operationErr != nil {
		return ServiceStartup{}, operationErr
	}
	return startup, nil
}

// ReconcilePolicy only inspects a previously attempted exact policy insert. It
// never reissues credentials or inserts a missing policy after an unknown outcome.
func (s *ServiceEnrollment) ReconcilePolicy(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lock, e := openJournal(s.root, id)
	if e != nil {
		return e
	}
	defer lock.close()
	record, e := s.read(id)
	if e != nil {
		return e
	}
	if record.Phase != "policy" || record.Policy == nil {
		return ErrRefused
	}
	saved, e := s.host.policies.GetPolicy(ctx, s.host.tenant, record.Policy.ID)
	if errors.Is(e, wardenerr.ErrPolicyNotFound) {
		return ErrUncertain
	}
	if e != nil {
		return e
	}
	expected, e := json.Marshal(record.Policy)
	if e != nil {
		return e
	}
	actual, e := json.Marshal(saved)
	if e != nil || !bytes.Equal(actual, expected) {
		return ErrRefused
	}
	if operationErr := ctx.Err(); operationErr != nil {
		return operationErr
	}
	record.Phase = "prepared"
	return s.save(record, false)
}

// Startup returns the exact retained private startup only for the configured service.
func (s *ServiceEnrollment) Startup(ctx context.Context, id string) (ServiceStartup, error) {
	if e := ctx.Err(); e != nil {
		return ServiceStartup{}, e
	}
	record, e := s.read(id)
	if e != nil {
		return ServiceStartup{}, e
	}
	if record.Phase != "prepared" && record.Phase != "bound" {
		return ServiceStartup{}, ErrUncertain
	}
	raw, e := readPrivate(filepath.Join(s.root, id+"-service-startup"), MaxManifest)
	if e != nil {
		return ServiceStartup{}, e
	}
	var v ServiceStartup
	p, e := s.plan(id)
	if e != nil {
		return v, e
	}
	if canonical(raw, &v, MaxManifest) != nil || managed.Digest(raw) != record.StartupDigest || !reflect.DeepEqual(v.ServicePlan, p) || v.Account != record.Account || v.Key != record.Key || v.App != s.host.app.String() || v.Environment != s.host.environment.String() || v.Tenant != s.host.tenant {
		return v, ErrRefused
	}
	return v, nil
}

// ResolveManagedDockerStartup implements the existing SDK port with a fixed
// executable/mode and retained private startup. It issues no credentials or Docker calls.
func (s *ServiceEnrollment) ResolveManagedDockerStartup(ctx context.Context, service managed.ProvisionService) (docker.ManagedDockerStartup, error) {
	for id, p := range s.plans {
		if !reflect.DeepEqual(p.Service, service) {
			continue
		}
		startup, e := s.Startup(ctx, id)
		if e != nil {
			return docker.ManagedDockerStartup{}, e
		}
		raw, e := json.Marshal(startup)
		if e != nil {
			return docker.ManagedDockerStartup{}, e
		}
		out := docker.ManagedDockerStartup{Entrypoint: []string{"/app/workload"}, Arguments: []string{"service"}, Environment: []string{"WRITERFLOOR_SERVICE_STARTUP=" + base64.StdEncoding.EncodeToString(raw)}}
		encoded, e := json.Marshal(out)
		if e != nil || len(encoded) > 32768 {
			return docker.ManagedDockerStartup{}, ErrRefused
		}
		return out, nil
	}
	return docker.ManagedDockerStartup{}, ErrRefused
}

var _ docker.ManagedDockerStartupResolver = (*ServiceEnrollment)(nil)

// RetainedBinding restores the exact previously authenticated boot association.
// It proves retained custody only. A fresh authenticated channel challenge is
// required before reporting readiness, quiescence or retained-query capability.
func (s *ServiceEnrollment) RetainedBinding(ctx context.Context, id string) (ServiceBinding, bool, error) {
	if err := ctx.Err(); err != nil {
		return ServiceBinding{}, false, err
	}
	record, err := s.read(id)
	if err != nil {
		return ServiceBinding{}, false, err
	}
	if record.Phase != "prepared" && record.Phase != "bound" {
		return ServiceBinding{}, false, ErrRefused
	}
	if record.Ready == nil {
		return ServiceBinding{}, false, nil
	}
	if record.Creation == nil {
		return ServiceBinding{}, false, ErrRefused
	}
	plan, err := s.plan(id)
	if err != nil {
		return ServiceBinding{}, false, err
	}
	created, err := s.reader.ReadServiceCreation(ctx, record.Creation.Command)
	if err != nil {
		return ServiceBinding{}, false, err
	}
	if !serviceCreationMatches(plan, record.Creation.Command, created) || !reflect.DeepEqual(created, *record.Creation) {
		return ServiceBinding{}, false, ErrRefused
	}
	binding, err := serviceBinding(record)
	if err != nil {
		return ServiceBinding{}, false, err
	}
	return binding, true, ctx.Err()
}
