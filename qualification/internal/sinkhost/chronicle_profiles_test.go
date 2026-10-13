package sinkhost

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	aid "github.com/xraph/authsome/id"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/authority"
)

func profileFixture() ChronicleProfiles {
	p := ChronicleProfiles{Version: 1}
	app := aid.NewAppID().String()
	for i, namespace := range []string{"production", "audit", "foreign"} {
		p.Routes = append(p.Routes, ChronicleRoute{Binding: ecosystem.Binding{Producer: "dispatch-lifecycle", InstallationID: "operator-host", Namespace: namespace, AppID: app, TenantID: "tenant-" + namespace}, Address: "127.0.0.1:" + []string{"12001", "12002", "12003"}[i], Credential: authority.Credential{KeyID: aid.NewAPIKeyID(), AccountID: aid.NewServiceAccountID(), Secret: "private-profile-" + namespace}})
	}
	return p
}

func TestChronicleProfilesPrivateStrictBundle(t *testing.T) {
	p := profileFixture()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "routes.json")
	if err := SaveChronicleProfiles(path, p); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadChronicleProfiles(path)
	if err != nil || len(loaded.Routes) != 3 {
		t.Fatal("private bundle round trip failed")
	}
	if err = SaveChronicleProfiles(path, p); err == nil {
		t.Fatal("bundle overwrite accepted")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"unknown":        []byte(strings.Replace(string(raw), `"version":1`, `"unknown":1,"version":1`, 1)),
		"extra_document": append(append([]byte{}, raw...), []byte(` {}`)...),
		"oversized":      []byte(strings.Repeat(" ", maxChronicleBundleBytes+1)),
		"truncated":      raw[:len(raw)-1],
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			file := filepath.Join(directory, name)
			if writeErr := os.WriteFile(file, body, 0600); writeErr != nil {
				t.Fatal(writeErr)
			}
			if _, loadErr := LoadChronicleProfiles(file); loadErr == nil {
				t.Fatal("invalid bundle accepted")
			}
		})
	}
	link := filepath.Join(directory, "link")
	if err = os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadChronicleProfiles(link); err == nil {
		t.Fatal("symlink accepted")
	}
	if _, err = LoadChronicleProfiles(directory); err == nil {
		t.Fatal("directory accepted")
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadChronicleProfiles(path); err == nil {
		t.Fatal("public bundle accepted")
	}
}

func TestChronicleProfilesRejectAmbiguousOwnership(t *testing.T) {
	cases := map[string]func(*ChronicleProfiles){
		"version":     func(p *ChronicleProfiles) { p.Version = 2 },
		"missing":     func(p *ChronicleProfiles) { p.Routes = p.Routes[:2] },
		"namespace":   func(p *ChronicleProfiles) { p.Routes[1].Binding.Namespace = p.Routes[0].Binding.Namespace },
		"key":         func(p *ChronicleProfiles) { p.Routes[1].Credential.KeyID = p.Routes[0].Credential.KeyID },
		"account":     func(p *ChronicleProfiles) { p.Routes[1].Credential.AccountID = p.Routes[0].Credential.AccountID },
		"endpoint":    func(p *ChronicleProfiles) { p.Routes[1].Address = p.Routes[0].Address },
		"nonloopback": func(p *ChronicleProfiles) { p.Routes[1].Address = "192.0.2.1:80" },
		"hostname":    func(p *ChronicleProfiles) { p.Routes[1].Address = "localhost:80" },
		"zero_port":   func(p *ChronicleProfiles) { p.Routes[1].Address = "127.0.0.1:0" },
		"path":        func(p *ChronicleProfiles) { p.Routes[1].Address = "127.0.0.1:80/accept" },
		"app":         func(p *ChronicleProfiles) { p.Routes[1].Binding.AppID = aid.NewAppID().String() },
		"producer":    func(p *ChronicleProfiles) { p.Routes[1].Binding.Producer = "another" },
		"secret":      func(p *ChronicleProfiles) { p.Routes[1].Credential.Secret = "" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			p := profileFixture()
			change(&p)
			if p.Validate() == nil {
				t.Fatal("ambiguous profile accepted")
			}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "routes")
			if err = os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err = LoadChronicleProfiles(path); err == nil {
				t.Fatal("invalid on-disk profile accepted")
			}
		})
	}
}
