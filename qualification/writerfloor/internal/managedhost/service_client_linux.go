//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

// DecodeServiceStartup accepts only the bounded private descriptor installed by
// the existing SDK startup resolver. Callers must never log this value or errors
// containing the input, since it includes machine and database credentials.
func DecodeServiceStartup(encoded string) (launcher.ServiceStartup, error) {
	var startup launcher.ServiceStartup
	if len(encoded) > base64.StdEncoding.EncodedLen(serviceWireLimit) {
		return startup, durable.ErrInvalid
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return startup, durable.ErrInvalid
	}
	if err = decodeService(bytes.NewReader(raw), &startup); err != nil {
		return launcher.ServiceStartup{}, durable.ErrInvalid
	}
	plan, err := launcher.ServicePlanDigest(startup.ServicePlan)
	if err != nil || plan != startup.Service.Startup.BindingDigest || len(startup.Configuration) > 4096 {
		return launcher.ServiceStartup{}, durable.ErrInvalid
	}
	hash := sha256.Sum256(startup.Configuration)
	if hex.EncodeToString(hash[:]) != startup.ConfigurationDigest {
		return launcher.ServiceStartup{}, durable.ErrInvalid
	}
	return startup, nil
}

type serviceClient struct {
	startup launcher.ServiceStartup
	boot    string
	client  *http.Client
}

func newServiceClient(startup launcher.ServiceStartup) (*serviceClient, error) {
	endpoint, err := url.Parse(startup.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" || startup.ServerName == "" || startup.Bearer == "" {
		return nil, durable.ErrInvalid
	}
	cert, err := tls.X509KeyPair(startup.ClientCertificatePEM, startup.ClientKeyPEM)
	if err != nil {
		return nil, durable.ErrInvalid
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(startup.CAPEM) {
		return nil, durable.ErrInvalid
	}
	nonce := make([]byte, 32)
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: startup.ServerName, Certificates: []tls.Certificate{cert}}, MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1, ResponseHeaderTimeout: 25 * time.Second}
	return &serviceClient{startup: startup, boot: hex.EncodeToString(nonce), client: &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

var errServiceChannelUnavailable = errors.New("managed service channel temporarily unavailable")

type serviceChannelError struct{ class string }

func (e serviceChannelError) Error() string           { return "managed service channel refused" }
func (e serviceChannelError) Unwrap() error           { return durable.ErrQueryRetention }
func (e serviceChannelError) DiagnosticClass() string { return e.class }

func (c *serviceClient) post(ctx context.Context, path string, in, out any) (bool, error) {
	raw, err := json.Marshal(in)
	if err != nil || len(raw) > serviceWireLimit {
		return false, durable.ErrInvalid
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.startup.Endpoint+path, bytes.NewReader(raw))
	if err != nil {
		return false, durable.ErrInvalid
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.startup.Bearer)
	request.Header.Set("X-Writerfloor-Boot", c.boot)
	response, err := c.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return false, context.Cause(ctx)
		}
		return false, errServiceChannelUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return false, nil
	}
	if response.StatusCode != http.StatusOK {
		phase := "unknown"
		switch path {
		case "/managed-service/ready":
			phase = "ready"
		case "/managed-service/next":
			phase = "next"
		case "/managed-service/result":
			phase = "result"
		}
		return false, serviceChannelError{class: fmt.Sprintf("channel_%s_http_%d", phase, response.StatusCode)}
	}
	if err = decodeService(response.Body, out); err != nil {
		return false, durable.ErrQueryRetention
	}
	return true, nil
}
func measuredServiceArtifact() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	size, readErr := io.Copy(hash, io.LimitReader(file, (512<<20)+1))
	closeErr := file.Close()
	if readErr != nil {
		return "", readErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if size > 512<<20 {
		return "", durable.ErrInvalid
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ServeWorkerChannel runs the actual process-owned worker behind its enrolled
// outbound channel. No listener, second deployer or request-selected worker is
// created here. Loss after a command delivery returns uncertainty and stops.
func ServeWorkerChannel(ctx context.Context, startup launcher.ServiceStartup, worker *drt.Worker, store durable.QueryRuntimeStore, probes []RetainedProbe) error {
	if worker == nil {
		return durable.ErrInvalid
	}
	status := worker.Status()
	if status.RuntimeID != startup.RuntimeID || status.InstanceID != startup.InstanceID || status.Namespace != startup.Namespace || status.BuildID != startup.Build || status.Queue != startup.Queue {
		return durable.ErrQueryRetention
	}
	artifact, err := measuredServiceArtifact()
	if err != nil {
		return err
	}
	if artifact != startup.ArtifactDigest {
		return durable.ErrQueryRetention
	}
	config := sha256.Sum256(startup.Configuration)
	if hex.EncodeToString(config[:]) != startup.ConfigurationDigest {
		return durable.ErrQueryRetention
	}
	client, err := newServiceClient(startup)
	if err != nil {
		return err
	}
	defer client.client.CloseIdleConnections()
	ready := launcher.ServiceReady{BootNonce: client.boot, BootstrapID: startup.BootstrapID, RuntimeID: status.RuntimeID, InstanceID: status.InstanceID, HostID: startup.HostID, ArtifactDigest: artifact, ConfigurationDigest: startup.ConfigurationDigest, Descriptor: startup.Service.Startup}
	var binding launcher.ServiceBinding
	accepted, err := client.post(ctx, "/managed-service/ready", ready, &binding)
	if err != nil || !accepted {
		return durable.ErrQueryRetention
	}
	if binding.BootNonce != client.boot {
		return durable.ErrQueryRetention
	}
	identity, err := ServiceIdentity(startup, binding)
	if err != nil {
		return err
	}
	runtime, err := NewWorkerRuntime(worker, store, identity, probes)
	if err != nil {
		return err
	}
	process := newServiceProcess(ctx, runtime)
	defer process.stop()
	ctx = process.ctx
	// The host creates random one-use challenges. A finite remembered set also
	// refuses redelivery from a compromised or buggy channel during this process.
	seen := make(map[string]bool)
	var reconnectDeadline time.Time
	for {
		if operationErr := ctx.Err(); operationErr != nil {
			return context.Cause(ctx)
		}
		var command ServiceCommand
		pollContext := ctx
		stopPoll := func() {}
		if !reconnectDeadline.IsZero() {
			pollContext, stopPoll = context.WithDeadline(ctx, reconnectDeadline)
		}
		available, pollErr := client.post(pollContext, "/managed-service/next", ServiceResponse{BootstrapID: startup.BootstrapID}, &command)
		stopPoll()
		if errors.Is(pollErr, errServiceChannelUnavailable) {
			if reconnectDeadline.IsZero() {
				reconnectDeadline = time.Now().Add(time.Minute)
			}
			if !time.Now().Before(reconnectDeadline) {
				return pollErr
			}
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if pollErr != nil {
			return pollErr
		}
		reconnectDeadline = time.Time{}
		if !available {
			continue
		}
		if !objectID(command.Nonce) || seen[command.Nonce] || len(seen) >= 4096 {
			return durable.ErrQueryRetention
		}
		seen[command.Nonce] = true
		observer, stopObserver := context.WithTimeout(ctx, 25*time.Second)
		result := process.execute(observer, command)
		stopObserver()
		result.BootstrapID = startup.BootstrapID
		var receipt struct {
			Accepted bool `json:"accepted"`
		}
		accepted, err = client.post(ctx, "/managed-service/result", result, &receipt)
		if err != nil {
			return err
		}
		if !accepted || !receipt.Accepted {
			return serviceChannelError{class: "result_unconfirmed"}
		}
	}
}
func executeServiceCommand(ctx context.Context, runtime *WorkerRuntime, command ServiceCommand) ServiceResponse {
	result := ServiceResponse{Nonce: command.Nonce, Kind: command.Kind}
	var err error
	switch command.Kind {
	case "status":
		if command.Binding != nil || command.Drain != nil || command.Handle != nil {
			result.Failed = true
			return result
		}
		var value drt.WorkerStatus
		value, err = runtime.Status(ctx)
		if err == nil {
			result.Status = &value
		}
	case "readiness":
		if command.Binding != nil || command.Drain != nil || command.Handle != nil {
			result.Failed = true
			return result
		}
		var value drt.WorkerReadiness
		value, err = runtime.worker.Readiness(ctx)
		if err == nil {
			result.Readiness = &value
		}
	case "verify":
		if command.Binding == nil || command.Drain != nil || command.Handle != nil {
			result.Failed = true
			return result
		}
		var value durable.QueryRuntimeVerification
		value, err = runtime.Verify(ctx, *command.Binding)
		if err == nil {
			result.Proof = &value
		}
	case "begin_drain":
		if command.Binding != nil || command.Drain == nil || command.Handle != nil {
			result.Failed = true
			return result
		}
		var value drt.DrainHandle
		value, err = runtime.BeginDrain(ctx, *command.Drain)
		if err == nil {
			result.Handle = &value
		}
	case "wait_drain":
		if command.Binding != nil || command.Drain != nil || command.Handle == nil {
			result.Failed = true
			return result
		}
		var value drt.DrainResult
		value, err = runtime.WaitDrain(ctx, *command.Handle)
		if err == nil {
			result.Drain = &value
		}
	default:
		err = durable.ErrInvalid
	}
	result.Failed = err != nil
	return result
}

// serviceProcess is owned by the one serial authenticated channel loop. Native
// build enrollment happens after Ready, before the first explicit start command.
type serviceProcess struct {
	runtime  *WorkerRuntime
	ctx      context.Context
	cancel   context.CancelCauseFunc
	finished chan struct{}
	started  bool
}

func newServiceProcess(ctx context.Context, runtime *WorkerRuntime) *serviceProcess {
	bounded, cancel := context.WithCancelCause(ctx)
	return &serviceProcess{runtime: runtime, ctx: bounded, cancel: cancel}
}
func (p *serviceProcess) stop() {
	p.cancel(context.Canceled)
	if p.started {
		<-p.finished
	}
}
func (p *serviceProcess) execute(ctx context.Context, command ServiceCommand) ServiceResponse {
	if command.Kind != "start" {
		return executeServiceCommand(ctx, p.runtime, command)
	}
	result := ServiceResponse{Nonce: command.Nonce, Kind: command.Kind}
	if command.Binding != nil || command.Drain != nil || command.Handle != nil || p.ctx.Err() != nil {
		result.Failed = true
		return result
	}
	if !p.started {
		p.started = true
		p.finished = make(chan struct{})
		go func() {
			defer close(p.finished)
			if err := p.runtime.worker.Run(p.ctx); err != nil {
				p.cancel(err)
			}
		}()
	}
	// Real worker status, rather than the goroutine launch, establishes readiness.
	readyCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	timer := time.NewTicker(time.Millisecond)
	defer timer.Stop()
	for {
		status := p.runtime.worker.Status()
		if status.Ready && !status.AdmissionClosed && status.State != drt.WorkerFailed {
			result.Status = &status
			return result
		}
		if status.AdmissionClosed || status.State == drt.WorkerFailed {
			result.Failed = true
			return result
		}
		select {
		case <-readyCtx.Done():
			result.Failed = true
			return result
		case <-p.ctx.Done():
			result.Failed = true
			return result
		case <-timer.C:
		}
	}
}
