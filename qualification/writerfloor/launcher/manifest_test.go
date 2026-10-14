package launcher

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xraph/ctrlplane/managed"
)

func fixtureManifest() Manifest {
	digest := managed.Digest([]byte("fixture"))
	spec := Specification{Role: "client", Executable: "/app/workload", Arguments: []string{"--fixed"}, Networks: []string{}, Upstream: "https://executor.internal/execute", ServerName: "executor.internal", ConfigurationDigest: digest}
	raw, _ := json.Marshal(spec)
	return Manifest{Authority: "authority", Epoch: 1, LauncherDaemonID: "launcher-engine", ProviderDaemonID: "provider-engine", API: "1.56", Artifacts: []Artifact{{Certificate: "candidate", ImageID: "sha256:" + digest, RepositoryDigest: "registry.local/candidate@sha256:" + digest, ExecutableDigest: digest, ModulesDigest: digest, EvidenceDigest: digest, SpecificationDigest: managed.Digest(raw), Specification: spec}}}
}
func TestFixedManifestSubstitutions(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Manifest)
	}{
		{"entrypoint", func(m *Manifest) { m.Artifacts[0].Specification.Executable = "/bin/sh" }},
		{"host-network", func(m *Manifest) { m.Artifacts[0].Specification.Networks = []string{"host"} }},
		{"credential-upstream", func(m *Manifest) {
			m.Artifacts[0].Specification.Upstream = "https://user:secret@executor.internal/execute"
		}},
		{"provider-socket", func(m *Manifest) { m.ProviderDaemonID = m.LauncherDaemonID }},
		{"changed-config", func(m *Manifest) { m.Artifacts[0].Specification.ConfigurationDigest = managed.Digest([]byte("new")) }},
		{"command-change", func(m *Manifest) { m.Artifacts[0].Specification.Arguments = []string{"--arbitrary"} }},
		{"tag", func(m *Manifest) { m.Artifacts[0].ImageID = "candidate:latest" }},
		{"uncertified", func(m *Manifest) { m.Artifacts = append(m.Artifacts, m.Artifacts[0]) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := fixtureManifest()
			test.change(&m)
			if m.Validate() == nil {
				t.Fatal("substitution accepted")
			}
		})
	}
	m := fixtureManifest()
	raw, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = DecodeManifest(raw); e != nil {
		t.Fatal(e)
	}
	if _, e = DecodeManifest(append(raw, ' ')); e == nil {
		t.Fatal("noncanonical manifest accepted")
	}
	if _, e = DecodeManifest([]byte(strings.Repeat("x", MaxManifest+1))); e == nil {
		t.Fatal("unbounded manifest accepted")
	}
}
