package ecosystem

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	ca "github.com/xraph/chronicle/acceptance"
	ra "github.com/xraph/relay/acceptance"
)

const MaxResponseBytes = 256 << 10

var ErrTransport = errors.New("dispatch: sink transport unavailable or invalid response")

// ConflictResponse is emitted only after the reliable API confirms a conflict.
// Its binding prevents an unrelated proxy error from permanently blocking work.
type ConflictResponse struct {
	Destination       string `json:"destination"`
	SourceKey         string `json:"source_key"`
	SourceFingerprint string `json:"source_fingerprint"`
	Fingerprint       string `json:"fingerprint"`
}

// Remote owns a fixed destination and never accepts URLs from delivery content.
// Credentials are private and error messages contain no request or response data.
type Remote struct {
	endpoint string
	bearer   string
	client   *http.Client
}

func NewRemote(endpoint, bearer string, timeout time.Duration, localLoopbackHTTP bool) (*Remote, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || bearer == "" || timeout <= 0 || timeout > time.Minute {
		return nil, ErrTransport
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if !localLoopbackHTTP || u.Scheme != "http" || ip == nil || !ip.IsLoopback() {
			return nil, ErrTransport
		}
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: timeout}).DialContext, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: timeout, ResponseHeaderTimeout: timeout, MaxIdleConnsPerHost: 4, IdleConnTimeout: time.Minute}
	client := &http.Client{Timeout: timeout, Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	return &Remote{endpoint: endpoint, bearer: bearer, client: client}, nil
}
func (r *Remote) Close() { r.client.CloseIdleConnections() }
func (r *Remote) call(ctx context.Context, request any, expected ConflictResponse, out any) (bool, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return false, ErrTransport
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(raw))
	if err != nil {
		return false, ErrTransport
	}
	req.Header.Set("Authorization", "Bearer "+r.bearer)
	req.Header.Set("Content-Type", "application/json")
	response, err := r.client.Do(req)
	if err != nil {
		return false, ErrTransport
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if err != nil || len(body) > MaxResponseBytes {
		return false, ErrTransport
	}
	// Validate strict JSON without rewriting integer tokens into exponent form.
	// Chronicle receipts decode Sequence directly as uint64.
	if _, err = ra.CanonicalJSON(body); err != nil {
		return false, ErrTransport
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if response.StatusCode == http.StatusConflict {
		var conflict ConflictResponse
		if decoder.Decode(&conflict) != nil || conflict != expected {
			return false, ErrTransport
		}
		return true, nil
	}
	if response.StatusCode != http.StatusOK || decoder.Decode(out) != nil {
		return false, ErrTransport
	}
	return false, nil
}
func (r *Remote) RecordOnce(ctx context.Context, request ca.Request) (*ca.Receipt, error) {
	fp, err := ca.Fingerprint(request)
	if err != nil {
		return nil, err
	}
	var receipt ca.Receipt
	conflict, err := r.call(ctx, request, ConflictResponse{Destination: "chronicle", SourceKey: request.SourceKey, SourceFingerprint: request.SourceFingerprint, Fingerprint: fp}, &receipt)
	if conflict {
		return nil, ca.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	return &receipt, nil
}
func (r *Remote) SendReliable(ctx context.Context, request ra.Request) (*ra.Receipt, error) {
	fp, err := ra.Fingerprint(request)
	if err != nil {
		return nil, err
	}
	var receipt ra.Receipt
	conflict, err := r.call(ctx, request, ConflictResponse{Destination: "relay", SourceKey: request.SourceKey, SourceFingerprint: request.SourceFingerprint, Fingerprint: fp}, &receipt)
	if conflict {
		return nil, ra.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	return &receipt, nil
}
