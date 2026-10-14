//go:build linux

package launcher

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

func TestFixedDockerRequestHasNoClientNetworkOrPrivilege(t *testing.T) {
	a := fixtureManifest().Artifacts[0]
	request := fixedContainer(a, "/private/state", "launch-one")
	if request.Name != "" || request.Config.Image != a.ImageID || request.Config.User != "10001:10001" || request.HostConfig.NetworkMode != "none" || !request.Config.NetworkDisabled || request.HostConfig.Privileged || !request.HostConfig.ReadonlyRootfs || len(request.HostConfig.CapAdd) != 0 || request.HostConfig.CapDrop[0] != "ALL" {
		t.Fatal("fixed isolation changed")
	}
	if len(request.HostConfig.Mounts) != 2 {
		t.Fatal("unexpected mounts")
	}
	for _, mount := range request.HostConfig.Mounts {
		if !mount.ReadOnly || strings.Contains(mount.Source, "proxy-key") || strings.Contains(mount.Source, "docker.sock") {
			t.Fatal("private authority mounted")
		}
	}
	if request.HostConfig.PidsLimit == nil || *request.HostConfig.PidsLimit != 64 || request.HostConfig.Memory != 256<<20 || request.HostConfig.NanoCPUs != 500000000 {
		t.Fatal("resource cap changed")
	}
}
func rootJournal(t *testing.T) *journal {
	t.Helper()
	if os.Geteuid() != 0 {
		if os.Getenv("WRITERFLOOR_LAUNCHER_REQUIRED") == "1" {
			t.Fatal("isolated Linux root qualification required")
		}
		t.Skip("journal/Unix mount ownership requires isolated Linux root")
	}
	root, tempErr := os.MkdirTemp("", "wf-launcher-")
	if tempErr != nil {
		t.Fatal(tempErr)
	}
	t.Cleanup(func() {
		if cleanupErr := os.RemoveAll(root); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	if operationErr := os.Chmod(root, 0o700); operationErr != nil {
		t.Fatal(operationErr)
	}
	j, e := openJournal(root, "launch")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = j.close() })
	return j
}
func TestJournalRestartNeverRepeatsCreate(t *testing.T) {
	j := rootJournal(t)
	m := fixtureManifest()
	record, e := j.create("launch", m, m.Artifacts[0])
	if e != nil {
		t.Fatal(e)
	}
	if e = j.save(record, "creating"); e != nil {
		t.Fatal(e)
	}
	if _, e = j.create("launch", m, m.Artifacts[0]); e == nil {
		t.Fatal("replayed create accepted")
	}
	saved, e := j.read("launch")
	if e != nil || saved.Phase != "creating" || saved.Container != "" {
		t.Fatalf("%+v %v", saved, e)
	}
	if e = j.close(); e != nil {
		t.Fatal(e)
	}
	reopened, e := openJournal(j.root, "launch")
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.close()
	if _, e = reopened.create("launch", m, m.Artifacts[0]); e == nil {
		t.Fatal("reopen allowed create")
	}
	saved, e = reopened.read("launch")
	if e != nil || saved.Phase != "creating" {
		t.Fatal("uncertainty lost")
	}
}
func TestJournalRejectsSymlinkAndCapacity(t *testing.T) {
	j := rootJournal(t)
	m := fixtureManifest()
	for n := 0; n < MaxLaunches; n++ {
		id := string(rune('a' + n))
		if _, e := j.create(id, m, m.Artifacts[0]); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := j.create("overflow", m, m.Artifacts[0]); e == nil {
		t.Fatal("capacity grew")
	}
	if operationErr := os.Symlink(filepath.Join(j.root, "a.json"), filepath.Join(j.root, "forged.json")); operationErr != nil {
		t.Fatal(operationErr)
	}
	if _, e := j.read("forged"); e == nil {
		t.Fatal("symlink read accepted")
	}
}

func testCA(t *testing.T) ([]byte, []byte, tls.Certificate, *x509.CertPool) {
	t.Helper()
	public, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, e := x509.CreateCertificate(rand.Reader, ca, ca, public, key)
	if e != nil {
		t.Fatal(e)
	}
	ca, e = x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	private, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"localhost"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	serverDER, e := x509.CreateCertificate(rand.Reader, leaf, ca, public, key)
	if e != nil {
		t.Fatal(e)
	}
	server, e := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}), keyPEM)
	if e != nil {
		t.Fatal(e)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return caPEM, keyPEM, server, pool
}
func TestProxyUsesItsOwnMTLSIdentityAndFixedRoute(t *testing.T) {
	ca, key, serverCertificate, pool := testCA(t)
	root, tempErr := os.MkdirTemp("", "wf-launcher-")
	if tempErr != nil {
		t.Fatal(tempErr)
	}
	t.Cleanup(func() {
		if cleanupErr := os.RemoveAll(root); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	pair, fingerprint, e := issueProxy(root, "launch", ca, key)
	if e != nil {
		t.Fatal(e)
	}
	var calls atomic.Int64
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || managed.Digest(r.TLS.PeerCertificates[0].Raw) != fingerprint {
			t.Error("wrong proxy identity")
		}
		if r.URL.Path != "/execute" || r.Header.Get("X-Workload") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "Bearer copied-machine-key" {
			t.Error("caller selected forwarding authority")
		}
		_, _ = w.Write([]byte(`{"historical":true}`))
	}))
	target.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCertificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	target.StartTLS()
	defer target.Close()
	upstream, e := url.Parse(target.URL + "/execute")
	if e != nil {
		t.Fatal(e)
	}
	transport := &http.Transport{DisableKeepAlives: true, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ServerName: "localhost", Certificates: []tls.Certificate{pair}}}
	defer transport.CloseIdleConnections()
	forwarder := &http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrRefused }}
	proxy := httptest.NewServer(proxyHandler(forwarder, upstream))
	defer proxy.Close()
	request, e := http.NewRequestWithContext(t.Context(), "POST", proxy.URL+"/execute", strings.NewReader(`{}`))
	if e != nil {
		t.Fatal(e)
	}
	request.Header.Set("Authorization", "Bearer copied-machine-key")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Workload", "another-workload")
	request.Header.Set("Cookie", "credential=stolen")
	response, e := http.DefaultClient.Do(request)
	if e != nil {
		t.Fatal(e)
	}
	body, e := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if e != nil || response.StatusCode != 200 || string(body) != `{"historical":true}` {
		t.Fatalf("%s %v", body, e)
	}
	request, e = http.NewRequestWithContext(t.Context(), "POST", proxy.URL+"/arbitrary", strings.NewReader(`{}`))
	if e != nil {
		t.Fatal(e)
	}
	request.Header.Set("Authorization", "Bearer copied-machine-key")
	request.Header.Set("Content-Type", "application/json")
	response, e = http.DefaultClient.Do(request)
	if e != nil {
		t.Fatal(e)
	}
	_ = response.Body.Close()
	if response.StatusCode != 403 || calls.Load() != 1 {
		t.Fatal("arbitrary route forwarded")
	}
	if _, e = os.Stat(filepath.Join(root, "launch-proxy-key.pem")); e != nil {
		t.Fatal(e)
	}
}
func TestStaticExecutableRejectsForgery(t *testing.T) {
	raw := []byte("not an ELF executable")
	if verifyStaticExecutable(raw, managed.Digest(raw)) == nil {
		t.Fatal("non-executable accepted")
	}
	if verifyStaticExecutable(raw, managed.Digest([]byte("different"))) == nil {
		t.Fatal("digest substitution accepted")
	}
}
func TestPrivateControlCancelsExactSupervisor(t *testing.T) {
	j := rootJournal(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	control, e := startControl(j.root, "launch", cancel)
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, stopErr := requestStop(t.Context(), j.root, "launch"); done <- stopErr }()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("supervisor not cancelled")
	}
	control.finish(nil)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}

func TestSDKCreationAndRemovalUseRecordedIDs(t *testing.T) {
	id := strings.Repeat("a", 64)
	var creates, stops, removes atomic.Int64
	var running atomic.Bool
	running.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1.56/containers/create":
			creates.Add(1)
			var request map[string]json.RawMessage
			if json.NewDecoder(r.Body).Decode(&request) != nil {
				t.Error("bad SDK request")
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(201)
			_, _ = w.Write([]byte(`{"Id":"` + id + `","Warnings":[]}`))
		case r.Method == "GET" && r.URL.Path == "/v1.56/containers/"+id+"/json":
			w.Header().Set("Content-Type", "application/json")
			state := "false"
			if running.Load() {
				state = "true"
			}
			_, _ = w.Write([]byte(`{"Id":"` + id + `","State":{"Running":` + state + `}}`))
		case r.Method == "POST" && r.URL.Path == "/v1.56/containers/"+id+"/stop":
			stops.Add(1)
			running.Store(false)
			w.WriteHeader(204)
		case r.Method == "DELETE" && r.URL.Path == "/v1.56/containers/"+id:
			removes.Add(1)
			if r.URL.Query().Get("force") == "1" || r.URL.Query().Get("v") == "1" {
				t.Error("broad teardown")
			}
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected Docker route %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	transport := &http.Transport{DisableKeepAlives: true}
	api, e := client.New(client.WithHost(server.URL), client.WithAPIVersion("1.56"), client.WithHTTPClient(&http.Client{Transport: strictTransport{transport}}))
	if e != nil {
		t.Fatal(e)
	}
	d := &dockerRuntime{api, transport}
	defer d.close()
	got, e := d.create(t.Context(), fixtureManifest().Artifacts[0], "/private/state", "launch")
	if e != nil || got != id {
		t.Fatalf("%q %v", got, e)
	}
	if e = d.remove(t.Context(), got); e != nil {
		t.Fatal(e)
	}
	if creates.Load() != 1 || stops.Load() != 1 || removes.Load() != 1 {
		t.Fatal("unexpected mutation count")
	}
	if e = d.remove(t.Context(), "container-name"); e == nil {
		t.Fatal("name-based teardown accepted")
	}
}
func TestSDKRedirectNeverRepeatsCreate(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, "/again", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	transport := &http.Transport{DisableKeepAlives: true}
	api, e := client.New(client.WithHost(server.URL), client.WithAPIVersion("1.56"), client.WithHTTPClient(&http.Client{Transport: strictTransport{transport}}))
	if e != nil {
		t.Fatal(e)
	}
	d := &dockerRuntime{api, transport}
	defer d.close()
	if _, e = d.create(t.Context(), fixtureManifest().Artifacts[0], "/private/state", "launch"); e == nil {
		t.Fatal("redirect accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("create repeated")
	}
}

func TestStartNeverResumesAnUncertainRecord(t *testing.T) {
	j := rootJournal(t)
	m := fixtureManifest()
	record, e := j.create("launch", m, m.Artifacts[0])
	if e != nil {
		t.Fatal(e)
	}
	if e = j.save(record, "starting"); e != nil {
		t.Fatal(e)
	}
	if e = j.close(); e != nil {
		t.Fatal(e)
	}
	h := &Host{root: j.root, manifest: m}
	if e = h.Start(t.Context(), "launch"); !errors.Is(e, ErrUncertain) {
		t.Fatalf("uncertain start reached runtime: %v", e)
	}
	saved, e := h.Inspect("launch")
	if e != nil || saved.Phase != "starting" {
		t.Fatal("retained uncertainty changed")
	}
}
func TestReadinessRefusesForgedChannelBeforeLedger(t *testing.T) {
	b := &Readiness{enrolled: map[string]Enrollment{"launch": {Binding: admission.Binding{ID: "launch", PeerDigest: strings.Repeat("a", 64)}}}}
	request := httptest.NewRequestWithContext(t.Context(), "POST", "https://example/writerfloor/ready", nil)
	request.Header.Set("X-Client-Certificate", strings.Repeat("a", 64))
	if _, e := b.check(t.Context(), request, readyRequest{"launch", strings.Repeat("b", 64)}); e == nil {
		t.Fatal("header authority accepted")
	}
	request.TLS = &tls.ConnectionState{HandshakeComplete: true}
	if _, e := b.check(t.Context(), request, readyRequest{"launch", strings.Repeat("b", 64)}); e == nil {
		t.Fatal("unverified TLS accepted")
	}
}
func TestReadinessProbeBindsExactEnrollmentAndFreshChallenge(t *testing.T) {
	ca, key, serverCertificate, pool := testCA(t)
	root, tempErr := os.MkdirTemp("", "wf-launcher-")
	if tempErr != nil {
		t.Fatal(tempErr)
	}
	t.Cleanup(func() {
		if cleanupErr := os.RemoveAll(root); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	pair, peer, e := issueProxy(root, "launch", ca, key)
	if e != nil {
		t.Fatal(e)
	}
	enrollment := Enrollment{Authority: "authority", Key: "key", Binding: admission.Binding{ID: "launch", Principal: "account", Certificate: "certificate", PeerDigest: peer}}
	encoded, e := json.Marshal(enrollment)
	if e != nil {
		t.Fatal(e)
	}
	digest := managed.Digest(encoded)
	var substitution atomic.Bool
	var calls atomic.Int64
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var request readyRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			t.Error("bad probe")
		}
		if r.URL.Path != "/writerfloor/ready" || r.Header.Get("Authorization") != "Bearer fixture" || !hexadecimal(request.Challenge) || managed.Digest(r.TLS.PeerCertificates[0].Raw) != peer {
			t.Error("probe identity missing")
		}
		result := readyResponse{"authority", "launch", "account", "key", peer, "certificate", digest, request.Challenge}
		if substitution.Load() {
			result.Enrollment = strings.Repeat("0", 64)
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	target.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCertificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	target.StartTLS()
	defer target.Close()
	spec := Specification{Upstream: target.URL + "/execute", ServerName: "localhost"}
	if witness, e := probeReadiness(t.Context(), spec, pair, ca, "fixture", enrollment); e != nil || !hexadecimal(witness) {
		t.Fatalf("probe: %q %v", witness, e)
	}
	substitution.Store(true)
	if _, e := probeReadiness(t.Context(), spec, pair, ca, "fixture", enrollment); e == nil {
		t.Fatal("different enrollment accepted")
	}
	if calls.Load() != 2 {
		t.Fatal("probe retried")
	}
}
