//go:build linux

package launcher

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/warden"

	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

type readyRequest struct {
	Launch    string `json:"launch"`
	Challenge string `json:"challenge"`
}
type readyResponse struct {
	Authority   string `json:"authority"`
	Launch      string `json:"launch"`
	Account     string `json:"account"`
	Key         string `json:"key"`
	Peer        string `json:"peer"`
	Certificate string `json:"certificate"`
	Enrollment  string `json:"enrollment"`
	Challenge   string `json:"challenge"`
}

// Readiness is a read-only route on the fixed admission or executor mTLS host.
// It authenticates actual Authsome/Warden state and immutable ledger bindings.
// It does not grant mutation authority or consume a delegation.
type Readiness struct {
	provider  authority.Provider
	policy    *warden.Engine
	ledger    *sql.DB
	authority string
	enrolled  map[string]Enrollment
}

// NewReadiness accepts the bounded prepared startup records loaded before serving.
// Each account has exactly one key, so Authsome's returned subject selects one saved pair.
func NewReadiness(provider *authority.Provider, policy *warden.Engine, ledger *sql.DB, authorityID string, enrolled []Enrollment) (*Readiness, error) {
	if provider == nil || provider.Engine == nil || policy == nil || ledger == nil || !token(authorityID) || len(enrolled) == 0 || len(enrolled) > MaxLaunches {
		return nil, ErrRefused
	}
	b := &Readiness{provider: *provider, policy: policy, ledger: ledger, authority: authorityID, enrolled: make(map[string]Enrollment, len(enrolled))}
	b.provider.Credentials = nil
	accounts := map[string]bool{}
	for _, v := range enrolled {
		if v.Authority != authorityID || v.App != provider.Binding.AppID || v.Tenant != provider.Binding.TenantID || v.Environment != provider.EnvironmentID || !token(v.Binding.ID) || !hexadecimal(v.Binding.RuntimeID) || !hexadecimal(v.Binding.PeerDigest) || accounts[v.Binding.Principal] || b.enrolled[v.Binding.ID].Key != "" {
			return nil, ErrRefused
		}
		raw, e := json.Marshal(v)
		if e != nil || len(raw) > MaxRecord {
			return nil, ErrRefused
		}
		var owned Enrollment
		if json.Unmarshal(raw, &owned) != nil {
			return nil, ErrRefused
		}
		credential, e := owned.Credential()
		if e != nil {
			return nil, e
		}
		b.provider.Credentials = append(b.provider.Credentials, credential)
		b.enrolled[v.Binding.ID] = owned
		accounts[v.Binding.Principal] = true
	}
	return b, nil
}
func readyPeer(r *http.Request) (string, error) {
	if r.TLS == nil || !r.TLS.HandshakeComplete || len(r.TLS.PeerCertificates) == 0 || r.TLS.PeerCertificates[0] == nil {
		return "", ErrRefused
	}
	leaf := r.TLS.PeerCertificates[0]
	for _, chain := range r.TLS.VerifiedChains {
		if len(chain) > 0 && chain[0] != nil && chain[0].Equal(leaf) && len(leaf.Raw) > 0 {
			return managed.Digest(leaf.Raw), nil
		}
	}
	return "", ErrRefused
}
func (b *Readiness) check(ctx context.Context, r *http.Request, request readyRequest) (readyResponse, error) {
	v, ok := b.enrolled[request.Launch]
	if !ok || !hexadecimal(request.Challenge) {
		return readyResponse{}, ErrRefused
	}
	peer, e := readyPeer(r)
	if e != nil || peer != v.Binding.PeerDigest {
		return readyResponse{}, ErrRefused
	}
	identity, e := b.provider.Authenticate(ctx, r)
	if e != nil || identity == nil || identity.Subject != v.Binding.Principal || !slices.Contains(identity.Scopes, "writerfloor.ready") {
		return readyResponse{}, ErrRefused
	}
	var raw []byte
	if e = b.ledger.QueryRowContext(ctx, `SELECT payload FROM writerfloor_facts WHERE authority=$1 AND kind='binding' AND id=$2`, b.authority, request.Launch).Scan(&raw); e != nil {
		return readyResponse{}, ErrRefused
	}
	var binding admission.Binding
	if canonical(raw, &binding, 4096) != nil || binding != v.Binding {
		return readyResponse{}, ErrRefused
	}
	var revoked bool
	if e = b.ledger.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM writerfloor_facts WHERE authority=$1 AND kind='revocation' AND (id=$2 OR id=$3))`, b.authority, "binding:"+binding.ID, "certificate:"+binding.Certificate).Scan(&revoked); e != nil || revoked {
		return readyResponse{}, ErrRefused
	}
	checkCtx := warden.WithNamespace(warden.WithTenant(ctx, v.App, v.Tenant), "")
	result, e := b.policy.Check(checkCtx, &warden.CheckRequest{Subject: warden.Subject{Kind: warden.SubjectServiceAcct, ID: identity.Subject}, Action: warden.Action{Name: "writerfloor.ready"}, Resource: warden.Resource{Type: "writerfloor_authority", ID: b.authority}, TenantID: v.Tenant})
	if e != nil || result == nil || !result.Allowed || len(result.Obligations) != 0 {
		return readyResponse{}, ErrRefused
	}
	encoded, e := json.Marshal(v)
	if e != nil {
		return readyResponse{}, e
	}
	return readyResponse{b.authority, binding.ID, binding.Principal, v.Key, peer, binding.Certificate, managed.Digest(encoded), request.Challenge}, nil
}

// Handler serves only POST /writerfloor/ready on the trusted mTLS listener.
func (b *Readiness) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/writerfloor/ready" || r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		raw, e := io.ReadAll(io.LimitReader(r.Body, 1025))
		var request readyRequest
		if e != nil || canonical(raw, &request, 1024) != nil {
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		response, e := b.check(ctx, r, request)
		if e != nil {
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if encodeErr := json.NewEncoder(w).Encode(response); encodeErr != nil {
			return
		}
	})
}
func probeReadiness(ctx context.Context, s Specification, pair tls.Certificate, serverCA []byte, bearer string, enrollment Enrollment) (string, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(serverCA) {
		return "", ErrRefused
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ForceAttemptHTTP2: false, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ServerName: s.ServerName, Certificates: []tls.Certificate{pair}}, TLSHandshakeTimeout: 2 * time.Second, ResponseHeaderTimeout: 3 * time.Second}
	defer transport.CloseIdleConnections()
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrRefused }}
	nonce := make([]byte, 32)
	if _, e := rand.Read(nonce); e != nil {
		return "", e
	}
	challenge := hex.EncodeToString(nonce)
	raw, e := json.Marshal(readyRequest{enrollment.Binding.ID, challenge})
	if e != nil {
		return "", e
	}
	u, e := url.Parse(s.Upstream)
	if e != nil {
		return "", e
	}
	u.Path = "/writerfloor/ready"
	request, e := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(raw))
	if e != nil {
		return "", e
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+bearer)
	response, e := client.Do(request)
	if e != nil {
		return "", ErrRefused
	}
	defer response.Body.Close()
	body, e := io.ReadAll(io.LimitReader(response.Body, MaxRecord+1))
	if e != nil || response.StatusCode != 200 || len(body) > MaxRecord {
		return "", ErrRefused
	}
	// Encoder adds one newline; the remaining payload must be canonical.
	body = bytes.TrimSuffix(body, []byte("\n"))
	var got readyResponse
	if canonical(body, &got, MaxRecord) != nil {
		return "", ErrRefused
	}
	encoded, e := json.Marshal(enrollment)
	if e != nil {
		return "", e
	}
	binding := enrollment.Binding
	want := readyResponse{enrollment.Authority, binding.ID, binding.Principal, enrollment.Key, binding.PeerDigest, binding.Certificate, managed.Digest(encoded), challenge}
	if got != want {
		return "", ErrRefused
	}
	return managed.Digest(body), nil
}
