package managedhost

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	ft "github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

// NativeEndpoint is private, protected controller configuration for the existing
// Forge dashboard contract. The server's real Authsome and Warden layers decide
// every request. Neither bearer nor client key belongs in retained evidence.
type NativeEndpoint struct {
	URL            string
	ServerName     string
	CAPEM          []byte
	CertificatePEM []byte
	KeyPEM         []byte
	Bearer         string
}

// NativeClient calls the existing Dispatch contributor over authenticated TLS.
// It performs no automatic application retry and exposes no arbitrary intent API.
type NativeClient struct {
	endpoint string
	bearer   string
	client   *http.Client
}

// NewNativeClient fixes one trusted endpoint, client identity and server CA.
func NewNativeClient(configuration NativeEndpoint) (*NativeClient, error) {
	endpoint, err := url.Parse(configuration.URL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.RawPath != "" || endpoint.ForceQuery || endpoint.Fragment != "" || configuration.ServerName == "" || configuration.Bearer == "" {
		return nil, durable.ErrInvalid
	}
	pair, err := tls.X509KeyPair(configuration.CertificatePEM, configuration.KeyPEM)
	if err != nil {
		return nil, durable.ErrInvalid
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(configuration.CAPEM) {
		return nil, durable.ErrInvalid
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: configuration.ServerName, Certificates: []tls.Certificate{pair}}, MaxConnsPerHost: 4, MaxIdleConnsPerHost: 4, ResponseHeaderTimeout: 15 * time.Second}
	return &NativeClient{endpoint: configuration.URL, bearer: configuration.Bearer, client: &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Close releases connections. It does not revoke a retained native operation.
func (c *NativeClient) Close() { c.client.CloseIdleConnections() }
func (c *NativeClient) call(ctx context.Context, kind fc.Kind, intent string, input, output any) error {
	payload, err := json.Marshal(input)
	if err != nil || len(payload) > 64<<10 {
		return durable.ErrInvalid
	}
	request := fc.Request{Envelope: "v1", Kind: kind, Contributor: "dispatch", Intent: intent, IntentVersion: 1, Payload: payload}
	if kind == fc.KindCommand {
		request.CSRF, err = c.csrf(ctx)
		if err != nil {
			return err
		}
		request.IdempotencyKey, err = durable.Fingerprint("writerfloor.native.transport.v1", struct {
			Intent  string
			Payload json.RawMessage
		}{intent, payload})
		if err != nil {
			return err
		}
	}
	raw, err := json.Marshal(request)
	if err != nil || len(raw) > 64<<10 {
		return durable.ErrInvalid
	}
	wire, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/api/dashboard/v1", bytes.NewReader(raw))
	if err != nil {
		return durable.ErrInvalid
	}
	wire.Header.Set("Authorization", "Bearer "+c.bearer)
	wire.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(wire)
	if err != nil {
		return durable.ErrQueryRetention
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return durable.ErrQueryRetention
	}
	var envelope fc.Response
	if decodeNativeResponse(response.Body, &envelope) != nil || !envelope.OK || envelope.Envelope != "v1" || envelope.Kind != kind || envelope.Meta.IntentVersion != 1 {
		return durable.ErrQueryRetention
	}
	return decodeNativeResponse(bytes.NewReader(envelope.Data), output)
}

func decodeNativeResponse(reader io.Reader, out any) error {
	raw, err := io.ReadAll(io.LimitReader(reader, (64<<10)+1))
	if err != nil || len(raw) > 64<<10 {
		return durable.ErrQueryRetention
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(out) != nil {
		return durable.ErrQueryRetention
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return durable.ErrQueryRetention
	}
	return nil
}

// RegisterBuild calls the exact published durable.buildRegister contract.
func (c *NativeClient) RegisterBuild(ctx context.Context, input operator.RegisterBuildInput) (operator.LifecycleAcceptance, error) {
	var result operator.LifecycleAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.buildRegister", input, &result)
	return result, err
}

// RetireBuild calls the exact published durable.buildRetire contract.
func (c *NativeClient) RetireBuild(ctx context.Context, input operator.BuildRetirementInput) (operator.LifecycleAcceptance, error) {
	var result operator.LifecycleAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.buildRetire", input, &result)
	return result, err
}

// RegisterRuntime calls the exact published durable.queryRuntimeRegister contract.
func (c *NativeClient) RegisterRuntime(ctx context.Context, input operator.QueryRuntimeCommand) (operator.QueryRuntimeAcceptance, error) {
	var result operator.QueryRuntimeAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.queryRuntimeRegister", input, &result)
	return result, err
}

// VerifyRuntime calls the exact published durable.queryRuntimeVerify contract.
func (c *NativeClient) VerifyRuntime(ctx context.Context, input operator.QueryRuntimeCommand) (operator.QueryRuntimeAcceptance, error) {
	var result operator.QueryRuntimeAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.queryRuntimeVerify", input, &result)
	return result, err
}

// BeginRemoval calls the exact published durable.queryRuntimeRemove contract.
func (c *NativeClient) BeginRemoval(ctx context.Context, input operator.QueryRuntimeCommand) (operator.QueryRemovalAcceptance, error) {
	var result operator.QueryRemovalAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.queryRuntimeRemove", input, &result)
	return result, err
}

// FinishRemoval calls the exact published durable.queryRuntimeFinish contract.
func (c *NativeClient) FinishRemoval(ctx context.Context, input operator.QueryRemovalInput) (operator.QueryRuntimeAcceptance, error) {
	var result operator.QueryRuntimeAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.queryRuntimeFinish", input, &result)
	return result, err
}

// AbortRemoval calls the exact published durable.queryRuntimeAbort contract.
func (c *NativeClient) AbortRemoval(ctx context.Context, input operator.QueryRemovalInput) (operator.QueryRuntimeAcceptance, error) {
	var result operator.QueryRuntimeAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.queryRuntimeAbort", input, &result)
	return result, err
}

// DrainWorker calls the exact published durable.workerDrain contract.
func (c *NativeClient) DrainWorker(ctx context.Context, input operator.WorkerDrainInput) (operator.WorkerDrainAcceptance, error) {
	var result operator.WorkerDrainAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.workerDrain", input, &result)
	return result, err
}

// ReadDrain calls the exact published durable.workerDrainReceipt contract.
func (c *NativeClient) ReadDrain(ctx context.Context, input operator.WorkerDrainInput) (operator.WorkerDrainAcceptance, error) {
	var result operator.WorkerDrainAcceptance
	err := c.call(ctx, fc.KindQuery, "durable.workerDrainReceipt", input, &result)
	return result, err
}

// Revalidate calls the exact published durable.queryRemovalRevalidate contract.
func (c *NativeClient) Revalidate(ctx context.Context, input operator.QueryRevalidationInput) (durable.QueryRemovalRevalidationReceipt, error) {
	var result durable.QueryRemovalRevalidationReceipt
	err := c.call(ctx, fc.KindCommand, "durable.queryRemovalRevalidate", input, &result)
	return result, err
}

// ReadRevalidation calls the exact published durable.queryRemovalRevalidation contract.
func (c *NativeClient) ReadRevalidation(ctx context.Context, input operator.QueryGuardReadInput) (durable.QueryRemovalRevalidationReceipt, error) {
	var result durable.QueryRemovalRevalidationReceipt
	err := c.call(ctx, fc.KindQuery, "durable.queryRemovalRevalidation", input, &result)
	return result, err
}

// BindCommit calls the exact published durable.queryRemovalCommit contract.
func (c *NativeClient) BindCommit(ctx context.Context, input operator.QueryCommitInput) (operator.QueryGuardAcceptance, error) {
	var result operator.QueryGuardAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.queryRemovalCommit", input, &result)
	return result, err
}

// CheckCommit calls the exact published durable.queryRemovalCommitStatus contract.
func (c *NativeClient) CheckCommit(ctx context.Context, input operator.QueryGuardReadInput) (operator.QueryCommitStatus, error) {
	var result operator.QueryCommitStatus
	err := c.call(ctx, fc.KindQuery, "durable.queryRemovalCommitStatus", input, &result)
	return result, err
}

// CloseGuard calls the exact published durable.queryRemovalGuardClose contract.
func (c *NativeClient) CloseGuard(ctx context.Context, input operator.QueryGuardCloseInput) (operator.QueryGuardAcceptance, error) {
	var result operator.QueryGuardAcceptance
	err := c.call(ctx, fc.KindCommand, "durable.queryRemovalGuardClose", input, &result)
	return result, err
}

// ReadGuardReceipt calls the exact published durable.queryRemovalGuardReceipt contract.
func (c *NativeClient) ReadGuardReceipt(ctx context.Context, input operator.QueryGuardReceiptInput) (operator.QueryGuardAcceptance, error) {
	var result operator.QueryGuardAcceptance
	err := c.call(ctx, fc.KindQuery, "durable.queryRemovalGuardReceipt", input, &result)
	return result, err
}

func (c *NativeClient) csrf(ctx context.Context) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/api/dashboard/v1/csrf", http.NoBody)
	if err != nil {
		return "", durable.ErrInvalid
	}
	request.Header.Set("Authorization", "Bearer "+c.bearer)
	response, err := c.client.Do(request)
	if err != nil {
		return "", durable.ErrQueryRetention
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", durable.ErrQueryRetention
	}
	var token ft.CSRFTokenResponse
	if decodeNativeResponse(response.Body, &token) != nil || token.Token == "" || len(token.Token) > 4096 || !token.ExpiresAt.After(time.Now()) {
		return "", durable.ErrQueryRetention
	}
	return token.Token, nil
}
