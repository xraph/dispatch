//go:build linux

package launcher

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/xraph/ctrlplane/managed"
)

type privateProxy struct {
	server    *http.Server
	transport *http.Transport
	listener  net.Listener
	digest    string
	directory string
	done      chan error
}

func issueProxy(root, id string, caPEM, keyPEM []byte) (tls.Certificate, string, error) {
	block, _ := pem.Decode(caPEM)
	if block == nil {
		return tls.Certificate{}, "", ErrRefused
	}
	ca, e := x509.ParseCertificate(block.Bytes)
	if e != nil || !ca.IsCA {
		return tls.Certificate{}, "", ErrRefused
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return tls.Certificate{}, "", ErrRefused
	}
	key, e := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if e != nil {
		return tls.Certificate{}, "", e
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return tls.Certificate{}, "", ErrRefused
	}
	public, private, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		return tls.Certificate{}, "", e
	}
	serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if e != nil {
		return tls.Certificate{}, "", e
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "writerfloor-" + id}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if template.NotAfter.After(ca.NotAfter) {
		return tls.Certificate{}, "", ErrRefused
	}
	der, e := x509.CreateCertificate(rand.Reader, template, ca, public, signer)
	if e != nil {
		return tls.Certificate{}, "", e
	}
	if operationErr := templateValidity(der, ca); operationErr != nil {
		return tls.Certificate{}, "", operationErr
	}
	encoded, e := x509.MarshalPKCS8PrivateKey(private)
	if e != nil {
		return tls.Certificate{}, "", e
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
	if operationErr := writePrivate(root, id+"-proxy-cert.pem", certPEM, true); operationErr != nil {
		return tls.Certificate{}, "", operationErr
	}
	if operationErr := writePrivate(root, id+"-proxy-key.pem", privatePEM, true); operationErr != nil {
		return tls.Certificate{}, "", operationErr
	}
	pair, e := tls.X509KeyPair(append(certPEM, caPEM...), privatePEM)
	return pair, managed.Digest(der), e
}
func templateValidity(der []byte, ca *x509.Certificate) error {
	cert, e := x509.ParseCertificate(der)
	if e != nil {
		return e
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	_, e = cert.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	return e
}
func newProxy(root, id string, s Specification, pair tls.Certificate, fingerprint string, serverCA []byte) (*privateProxy, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(serverCA) {
		return nil, ErrRefused
	}
	u, e := url.Parse(s.Upstream)
	if e != nil {
		return nil, e
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ForceAttemptHTTP2: false, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ServerName: s.ServerName, Certificates: []tls.Certificate{pair}}, TLSHandshakeTimeout: 2 * time.Second, ResponseHeaderTimeout: 3 * time.Second}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrRefused }}
	directory := filepath.Join(root, id+"-socket")
	if operationErr := checkSocketDirectory(directory); operationErr != nil {
		return nil, operationErr
	}
	socket := filepath.Join(directory, "admission.sock")
	listener, e := (&net.ListenConfig{}).Listen(context.Background(), "unix", socket)
	if e != nil {
		return nil, e
	}
	if e = os.Chmod(socket, 0o660); e != nil { //nolint:gosec // Private socket directory grants access only to the fixed workload group.
		_ = listener.Close()
		return nil, e
	}
	if e = os.Chown(socket, 0, 10001); e != nil {
		_ = listener.Close()
		return nil, e
	}
	server := &http.Server{Handler: proxyHandler(client, u), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 6 * time.Second, IdleTimeout: time.Second, MaxHeaderBytes: 8192}
	return &privateProxy{server, transport, listener, fingerprint, directory, make(chan error, 1)}, nil
}
func proxyHandler(client *http.Client, upstream *url.URL) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != upstream.Path || r.URL.RawQuery != "" || r.URL.RawPath != "" || len(r.Header.Values("Authorization")) != 1 || r.Header.Get("Authorization") == "" || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "fixed request refused", http.StatusForbidden)
			return
		}
		raw, e := io.ReadAll(io.LimitReader(r.Body, MaxRecord+1))
		if e != nil || len(raw) > MaxRecord {
			http.Error(w, "bounded request required", http.StatusBadRequest)
			return
		}
		request, e := http.NewRequestWithContext(r.Context(), "POST", upstream.String(), bytes.NewReader(raw))
		if e != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		// Construct from nothing. No identity, forwarding, proxy, cookie or routing header survives.
		request.Header.Set("Authorization", r.Header.Get("Authorization"))
		request.Header.Set("Content-Type", "application/json")
		response, e := client.Do(request)
		if e != nil {
			http.Error(w, "uncertain response; inspect only", http.StatusServiceUnavailable)
			return
		}
		defer response.Body.Close()
		body, e := io.ReadAll(io.LimitReader(response.Body, MaxRecord*2+1))
		if e != nil || len(body) > MaxRecord*2 || response.StatusCode >= 300 && response.StatusCode < 400 {
			http.Error(w, "uncertain response; inspect only", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.StatusCode)
		if _, writeErr := w.Write(body); writeErr != nil {
			return
		}
	})
}
func (p *privateProxy) start() { go func() { p.done <- p.server.Serve(p.listener); close(p.done) }() }
func (p *privateProxy) close(ctx context.Context) error {
	// Close cancels active incoming requests, which also cancel their outbound context.
	e := p.server.Close()
	p.transport.CloseIdleConnections()
	select {
	case serveErr := <-p.done:
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			return errors.Join(e, serveErr)
		}
		return e
	case <-ctx.Done():
		return errors.Join(e, ctx.Err())
	}
}

func prepareSocketDirectory(root, id string) error {
	path := filepath.Join(root, id+"-socket")
	if operationErr := os.Mkdir(path, 0o750); operationErr != nil {
		return operationErr
	}
	if operationErr := os.Chown(path, 0, 10001); operationErr != nil {
		return operationErr
	}
	return checkSocketDirectory(path)
}
func checkSocketDirectory(path string) error {
	info, e := os.Lstat(path)
	if e != nil {
		return e
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm() != 0o750 || st.Uid != 0 || st.Gid != 10001 {
		return ErrRefused
	}
	return nil
}
