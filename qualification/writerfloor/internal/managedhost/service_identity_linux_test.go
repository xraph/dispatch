//go:build linux

package managedhost

import (
	"strings"
	"testing"

	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

// This fixture tests normalization only. It does not impersonate an authenticated
// enrollment or assert that an arbitrary submitted binding grants authority.
func serviceIdentityFixture(t *testing.T) (launcher.ServiceStartup, launcher.ServiceBinding) {
	t.Helper()
	startup := launcher.ServiceStartup{ServicePlan: launcher.ServicePlan{BootstrapID: "first", RuntimeID: "runtime-first", InstanceID: "instance-first", Installation: "fixed", Namespace: "native", Build: "one-build", ArtifactDigest: strings.Repeat("a", 64), ConfigurationDigest: strings.Repeat("b", 64), ConfigurationVersion: "one", ProbePolicyID: "retained", ProbePolicyVersion: "1", MaximumProofValidityNS: "60000000000", VerifierID: "worker"}, Account: "account-first", Key: "key-first"}
	digest, err := launcher.ServicePlanDigest(startup.ServicePlan)
	if err != nil {
		t.Fatal(err)
	}
	binding := launcher.ServiceBinding{BootstrapID: startup.BootstrapID, PlanDigest: digest, BootNonce: strings.Repeat("c", 64), ResourceID: strings.Repeat("d", 64), Incarnation: strings.Repeat("e", 64), CreationDigest: strings.Repeat("f", 64), Account: startup.Account, Key: startup.Key}
	return startup, binding
}
func TestServiceBuildIdentitySharedWithoutAdoptingAnotherProcess(t *testing.T) {
	first, firstBinding := serviceIdentityFixture(t)
	candidate, err := ServiceIdentity(first, firstBinding)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.BootstrapID = "second"
	second.RuntimeID = "runtime-second"
	second.InstanceID = "instance-second"
	second.Account = "account-second"
	second.Key = "key-second"
	secondBinding := firstBinding
	secondBinding.BootstrapID = second.BootstrapID
	secondBinding.Account = second.Account
	secondBinding.Key = second.Key
	secondBinding.BootNonce = strings.Repeat("1", 64)
	secondBinding.ResourceID = strings.Repeat("2", 64)
	secondBinding.Incarnation = strings.Repeat("3", 64)
	secondBinding.CreationDigest = strings.Repeat("4", 64)
	secondBinding.PlanDigest, err = launcher.ServicePlanDigest(second.ServicePlan)
	if err != nil {
		t.Fatal(err)
	}
	survivor, err := ServiceIdentity(second, secondBinding)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.BuildIdentity != survivor.BuildIdentity {
		t.Fatal("same protected build cannot retain a survivor")
	}
	if candidate.RuntimeID == survivor.RuntimeID || candidate.InstanceID == survivor.InstanceID {
		t.Fatal("distinct process identities collapsed")
	}
	if _, err = ServiceIdentity(second, firstBinding); err == nil {
		t.Fatal("another process binding adopted")
	}
	secondBinding.BootNonce = ""
	if _, err = ServiceIdentity(second, secondBinding); err == nil {
		t.Fatal("missing boot binding accepted")
	}
}
func TestServiceBuildIdentityCommitsEverySharedBuildField(t *testing.T) {
	original, binding := serviceIdentityFixture(t)
	expected, err := ServiceIdentity(original, binding)
	if err != nil {
		t.Fatal(err)
	}
	changes := []func(*launcher.ServiceStartup){
		func(s *launcher.ServiceStartup) { s.Installation = "another" },
		func(s *launcher.ServiceStartup) { s.Namespace = "another" },
		func(s *launcher.ServiceStartup) { s.Build = "another" },
		func(s *launcher.ServiceStartup) { s.ArtifactDigest = strings.Repeat("1", 64) },
		func(s *launcher.ServiceStartup) { s.ConfigurationDigest = strings.Repeat("2", 64) },
		func(s *launcher.ServiceStartup) { s.ConfigurationVersion = "another" },
		func(s *launcher.ServiceStartup) { s.ProbePolicyID = "another" },
		func(s *launcher.ServiceStartup) { s.ProbePolicyVersion = "2" },
		func(s *launcher.ServiceStartup) { s.VerifierID = "another" },
		func(s *launcher.ServiceStartup) { s.MaximumProofValidityNS = "30000000000" },
	}
	for i, change := range changes {
		changed := original
		change(&changed)
		changedBinding := binding
		changedBinding.PlanDigest, err = launcher.ServicePlanDigest(changed.ServicePlan)
		if err != nil {
			t.Fatal(err)
		}
		result, identityErr := ServiceIdentity(changed, changedBinding)
		if identityErr != nil {
			t.Fatal(identityErr)
		}
		if result.BuildIdentity.EnrollmentEvidenceDigest == expected.BuildIdentity.EnrollmentEvidenceDigest {
			t.Fatalf("shared field %d omitted", i)
		}
	}
}
