//go:build linux

package launcher

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"reflect"
	"slices"
	"time"

	cid "github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
)

// ServiceCreationReader is the configured signed creation-custody reader. The
// concrete managedhost CreationTransport implements it; requests cannot select it.
type ServiceCreationReader interface {
	ReadServiceCreation(context.Context, docker.ManagedDockerCommand) (ServiceCreation, error)
}

// ServiceCreation preserves the authenticated parent alongside signed creation custody.
type ServiceCreation struct {
	Target              managed.Key                 `json:"target"`
	ProviderOperationID cid.ID                      `json:"provider_operation_id"`
	Command             docker.ManagedDockerCommand `json:"command"`
	Custody             docker.ManagedDockerCustody `json:"custody"`
}

// ServicePlan fixes one provider service before its original create command. The
// protected administrator supplies it, including the exact private configuration.
type ServicePlan struct {
	BootstrapID            string                   `json:"bootstrap_id"`
	Target                 managed.Key              `json:"target"`
	ProviderOperationID    cid.ID                   `json:"provider_operation_id"`
	Service                managed.ProvisionService `json:"service"`
	Endpoint               string                   `json:"endpoint"`
	ServerName             string                   `json:"server_name"`
	RuntimeID              string                   `json:"runtime_id"`
	InstanceID             string                   `json:"instance_id"`
	HostID                 string                   `json:"host_id"`
	Installation           string                   `json:"installation"`
	Namespace              string                   `json:"namespace"`
	Build                  string                   `json:"build"`
	Queue                  string                   `json:"queue"`
	ArtifactDigest         string                   `json:"artifact_digest"`
	ConfigurationDigest    string                   `json:"configuration_digest"`
	ConfigurationVersion   string                   `json:"configuration_version"`
	ProbePolicyID          string                   `json:"probe_policy_id"`
	ProbePolicyVersion     managed.Decimal          `json:"probe_policy_version"`
	MaximumProofValidityNS managed.Decimal          `json:"maximum_proof_validity_ns"`
	VerifierID             string                   `json:"verifier_id"`
	ProducerVerifier       string                   `json:"producer_verifier"`
	Configuration          json.RawMessage          `json:"configuration"`
}

// ServiceStartup is private startup material encoded into the existing SDK's
// fixed environment resolver. It must never be returned in operator evidence.
type ServiceStartup struct {
	ServicePlan
	App                  string `json:"app"`
	Environment          string `json:"environment"`
	Tenant               string `json:"tenant"`
	Account              string `json:"account"`
	Key                  string `json:"key"`
	Bearer               string `json:"bearer"`
	CAPEM                []byte `json:"ca_pem"`
	ClientCertificatePEM []byte `json:"client_certificate_pem"`
	ClientKeyPEM         []byte `json:"client_key_pem"`
}

// ServiceReady is measured by the real worker process. Authentication confirms
// its configured identity; a separate live native probe must confirm execution.
type ServiceReady struct {
	BootNonce           string                    `json:"boot_nonce"`
	BootstrapID         string                    `json:"bootstrap_id"`
	RuntimeID           string                    `json:"runtime_id"`
	InstanceID          string                    `json:"instance_id"`
	HostID              string                    `json:"host_id"`
	ArtifactDigest      string                    `json:"artifact_digest"`
	ConfigurationDigest string                    `json:"configuration_digest"`
	Descriptor          managed.StartupBindingRef `json:"descriptor"`
}

// ServiceBinding authenticates the original resource and channel only. It is not
// proof of a live worker, completed effect, guard closure or new send permission.
type ServiceBinding struct {
	BootNonce      string `json:"boot_nonce"`
	BootstrapID    string `json:"bootstrap_id"`
	PlanDigest     string `json:"plan_digest"`
	ResourceID     string `json:"resource_id"`
	Incarnation    string `json:"incarnation"`
	CreationDigest string `json:"creation_digest"`
	Account        string `json:"account"`
	Key            string `json:"key"`
	PeerDigest     string `json:"peer_digest"`
}

// NewServiceProcessIDs produces immutable random IDs before provisioning. A new
// process or replacement must use a new plan and bootstrap, never these IDs again.
func NewServiceProcessIDs() (runtimeID, instanceID, hostID string, err error) {
	values := make([]string, 3)
	for i := range values {
		raw := make([]byte, 32)
		if _, err = rand.Read(raw); err != nil {
			return "", "", "", err
		}
		values[i] = hex.EncodeToString(raw)
	}
	return values[0], values[1], values[2], nil
}

// ServicePlanDigest commits the public descriptor and exact configuration digest.
// Clearing its own digest avoids a self-referential startup descriptor.
func ServicePlanDigest(plan ServicePlan) (string, error) {
	plan.Service.Startup.BindingDigest = ""
	plan.Configuration = nil
	raw, err := json.Marshal(plan)
	if err != nil || len(raw) > MaxRecord {
		return "", ErrRefused
	}
	return managed.Digest(raw), nil
}
func validateServicePlan(p ServicePlan) error {
	if p.ProviderOperationID.Prefix() != managed.PrefixOperation || !token(p.Target.TenantID) || p.Target.InstanceID.Prefix() != cid.PrefixInstance || !token(p.BootstrapID) || len(p.BootstrapID) > 32 || !hexadecimal(p.RuntimeID) || !hexadecimal(p.InstanceID) || !hexadecimal(p.HostID) || !hexadecimal(p.ArtifactDigest) || !hexadecimal(p.ConfigurationDigest) || len(p.Configuration) > 4096 || !canonicalConfiguration(p.Configuration) || managed.Digest(p.Configuration) != p.ConfigurationDigest {
		return ErrRefused
	}
	for _, value := range []string{p.Installation, p.Namespace, p.Build, p.Queue, p.ConfigurationVersion, p.ProbePolicyID, p.VerifierID, p.ProducerVerifier, p.Service.Name, p.Service.Startup.BindingID} {
		if !token(value) {
			return ErrRefused
		}
	}
	for _, value := range []managed.Decimal{p.ProbePolicyVersion, p.MaximumProofValidityNS, p.Service.Startup.BindingGeneration, p.Service.CPUMillis, p.Service.MemoryMiB, p.Service.PIDLimit} {
		v, e := value.Uint64()
		if e != nil || v == 0 {
			return ErrRefused
		}
	}
	for _, bound := range []struct {
		value   managed.Decimal
		maximum uint64
	}{{p.Service.CPUMillis, 2000}, {p.Service.MemoryMiB, 512}, {p.Service.PIDLimit, 128}, {p.MaximumProofValidityNS, uint64(time.Minute)}} {
		value, e := bound.value.Uint64()
		if e != nil || value > bound.maximum {
			return ErrRefused
		}
	}
	u, e := url.Parse(p.Endpoint)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || p.ServerName == "" || len(p.ServerName) > 253 {
		return ErrRefused
	}
	if !slices.Contains([]string{"main", "sidecar"}, p.Service.Role) {
		return ErrRefused
	}
	// The existing SDK owns the concrete immutable-image and resource bounds too.
	if len(p.Service.Image) > 256 || len(p.Service.Image) < 72 || p.Service.Image[len(p.Service.Image)-72:len(p.Service.Image)-64] != "@sha256:" || !hexadecimal(p.Service.Image[len(p.Service.Image)-64:]) {
		return ErrRefused
	}
	digest, e := ServicePlanDigest(p)
	if e != nil || p.Service.Startup.BindingDigest != digest {
		return ErrRefused
	}
	return nil
}
func serviceReadyMatches(p ServicePlan, r ServiceReady) bool {
	return hexadecimal(r.BootNonce) && r == (ServiceReady{r.BootNonce, p.BootstrapID, p.RuntimeID, p.InstanceID, p.HostID, p.ArtifactDigest, p.ConfigurationDigest, p.Service.Startup})
}
func serviceReaderAbsent(value ServiceCreationReader) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return v.IsNil()
	}
	return false
}
