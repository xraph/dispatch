//go:build linux

package managedhost

import (
	"strconv"
	"testing"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
)

func TestNativeOriginalIdentityChecksEveryNativeField(t *testing.T) {
	_, binding := nativeWorkerFixture(t)
	i := binding.QueryRuntimeIdentity
	b := i.BuildIdentity
	process := managed.OriginalProcessFacts{Installation: i.InstallationID, Namespace: i.Namespace, Build: i.BuildID, RuntimeID: i.RuntimeID, InstanceID: i.InstanceID, IdentityVersion: managed.Decimal(strconv.FormatInt(i.IdentityVersion, 10)), ArtifactDigest: b.ArtifactDigest, ConfigurationDigest: b.ConfigurationDigest, ConfigurationVersion: b.ConfigurationVersion, EnrollmentEvidenceDigest: b.EnrollmentEvidenceDigest, ProbePolicyID: b.ProbePolicyID, ProbePolicyVersion: managed.Decimal(strconv.FormatInt(b.ProbePolicyVersion, 10)), MaximumProofValidityNS: managed.Decimal(strconv.FormatInt(int64(b.MaximumProofValidity), 10)), VerifierID: b.VerifierID}
	if !nativeIdentityMatchesOriginal(i, process) {
		t.Fatal("exact original refused")
	}
	changes := []func(*durable.QueryRuntimeIdentity){
		func(v *durable.QueryRuntimeIdentity) { v.InstallationID += "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.Namespace += "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.BuildID += "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.RuntimeID += "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.InstanceID += "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.IdentityVersion++ },
		func(v *durable.QueryRuntimeIdentity) { v.BuildIdentity.ArtifactDigest = "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.BuildIdentity.ConfigurationDigest = "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.BuildIdentity.ConfigurationVersion += "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.BuildIdentity.EnrollmentEvidenceDigest = "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.BuildIdentity.ProbePolicyID += "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.BuildIdentity.ProbePolicyVersion++ },
		func(v *durable.QueryRuntimeIdentity) { v.BuildIdentity.VerifierID += "changed" },
		func(v *durable.QueryRuntimeIdentity) { v.BuildIdentity.MaximumProofValidity++ },
	}
	for n, change := range changes {
		changed := i
		change(&changed)
		if nativeIdentityMatchesOriginal(changed, process) {
			t.Fatalf("identity change %d accepted", n)
		}
	}
}
