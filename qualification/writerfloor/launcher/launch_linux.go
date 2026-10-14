//go:build linux

package launcher

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	wpg "github.com/xraph/warden/store/postgres"

	"github.com/xraph/authsome"
	aid "github.com/xraph/authsome/id"
	"github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

// Startup is the fixed file contract consumed by a reviewed workload executable.
// The proxy identity/private key is deliberately absent from the client file.
type Startup struct {
	Account       string          `json:"account"`
	Key           string          `json:"key"`
	Bearer        string          `json:"bearer"`
	App           string          `json:"app"`
	Environment   string          `json:"environment"`
	Socket        string          `json:"socket"`
	Configuration json.RawMessage `json:"configuration"`
}

// Host owns only administration handles. None is exposed to launched workloads.
type Host struct {
	root            string
	configDirectory string
	manifest        Manifest
	engine          *authsome.Engine
	ledger          *sql.DB
	admin           *admission.Admin
	app             aid.AppID
	environment     aid.EnvironmentID
	tenant          string
	policies        *wpg.Store
}

func (h *Host) certificate(ctx context.Context, a Artifact) error {
	var raw []byte
	if operationErr := h.ledger.QueryRowContext(ctx, `SELECT payload FROM writerfloor_facts WHERE authority=$1 AND kind='certificate' AND id=$2`, h.manifest.Authority, a.Certificate).Scan(&raw); operationErr != nil {
		return operationErr
	}
	var c admission.Certificate
	if canonical(raw, &c, 4096) != nil {
		return ErrRefused
	}
	_, imageDigest, _ := strings.Cut(a.RepositoryDigest, "@sha256:")
	if c.ID != a.Certificate || c.Artifact != imageDigest || c.Executable != a.ExecutableDigest || c.Modules != a.ModulesDigest || c.Specification != a.SpecificationDigest || c.Evidence != a.EvidenceDigest {
		return ErrRefused
	}
	var revoked bool
	if operationErr := h.ledger.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM writerfloor_facts WHERE authority=$1 AND kind='revocation' AND id=$2)`, h.manifest.Authority, "certificate:"+c.ID).Scan(&revoked); operationErr != nil {
		return operationErr
	}
	if revoked {
		return ErrRefused
	}
	if a.Specification.Role == "executor" {
		state, e := h.admin.Read(ctx)
		if e != nil {
			return e
		}
		var protocol string
		switch {
		case state.Pending != nil:
			protocol = string(state.Pending.MinimumProtocol)
		case state.Current != nil:
			protocol = string(state.Current.MinimumProtocol)
		default:
			return ErrRefused
		}
		epoch := ""
		if state.Pending != nil {
			epoch = string(state.Pending.Epoch)
		} else if state.Current != nil {
			epoch = string(state.Current.Epoch)
		}
		if epoch != strconv.FormatUint(h.manifest.Epoch, 10) {
			return ErrRefused
		}
		floor, e := strconv.ParseUint(protocol, 10, 64)
		if e != nil || floor == 0 || c.Protocol < floor {
			return ErrRefused
		}
	}
	return nil
}

// Prepare enrolls one machine and creates a verified stopped container. It never starts a workload.
func (h *Host) Prepare(ctx context.Context, launchID, certificate string) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	a, e := h.manifest.artifact(certificate)
	if e != nil {
		return e
	}
	if operationErr := h.certificate(ctx, a); operationErr != nil {
		return operationErr
	}
	j, e := openJournal(h.root, launchID)
	if e != nil {
		return e
	}
	defer j.close()
	if _, e = j.read(launchID); e == nil {
		return ErrUncertain
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	r, e := j.create(launchID, h.manifest, a)
	if e != nil {
		return e
	}
	docker, e := newDocker(ctx, h.manifest)
	if e != nil {
		return e
	}
	defer docker.close()
	if operationErr := docker.verifyImage(ctx, a); operationErr != nil {
		return operationErr
	}
	private, e := readPrivate(filepath.Join(h.configDirectory, certificate+"-config"), 4096)
	if e != nil || managed.Digest(private) != a.Specification.ConfigurationDigest || !canonicalConfiguration(private) {
		return ErrRefused
	}
	if operationErr := j.save(r, "enrolling"); operationErr != nil {
		return operationErr
	}
	scopes := []string{"writerfloor.ready", "writerfloor.request"}
	if a.Specification.Role == "executor" {
		scopes = []string{"writerfloor.ready", "writerfloor.delegate"}
	}
	account, e := h.engine.CreateServiceAccountInEnvironment(ctx, h.app, h.environment, "writerfloor-"+launchID, "fixed qualification workload", scopes)
	if e != nil {
		return ErrUncertain
	}
	r.Account = account.ID.String()
	if operationErr := j.save(r, "account"); operationErr != nil {
		return operationErr
	}
	expires := time.Now().UTC().Add(time.Hour)
	key, secret, e := h.engine.CreateServiceAccountAPIKey(ctx, account.ID, "writerfloor-"+launchID, scopes, &expires)
	if e != nil {
		return ErrUncertain
	}
	r.Key = key.ID.String()
	if operationErr := j.save(r, "key"); operationErr != nil {
		return operationErr
	}
	startup := Startup{r.Account, r.Key, secret, h.app.String(), h.environment.String(), "/run/writerfloor/admission.sock", private}
	raw, e := json.Marshal(startup)
	if e != nil || len(raw) > MaxRecord {
		return ErrRefused
	}
	if operationErr := writePrivate(h.root, launchID+"-machine", raw, true); operationErr != nil {
		return operationErr
	}
	if operationErr := writePrivate(h.root, launchID+"-startup", raw, true); operationErr != nil {
		return operationErr
	}
	if operationErr := os.Chown(filepath.Join(h.root, launchID+"-startup"), 10001, 10001); operationErr != nil {
		return operationErr
	}
	if modeErr := os.Chmod(filepath.Join(h.root, launchID+"-startup"), 0o400); modeErr != nil {
		return modeErr
	}
	ca, e := readPrivate(filepath.Join(h.configDirectory, "proxy-ca.pem"), MaxRecord)
	if e != nil {
		return e
	}
	caKey, e := readPrivate(filepath.Join(h.configDirectory, "proxy-ca-key.pem"), MaxRecord)
	if e != nil {
		return e
	}
	_, peer, e := issueProxy(h.root, launchID, ca, caKey)
	if e != nil {
		return e
	}
	r.Peer = peer
	if operationErr := prepareSocketDirectory(h.root, launchID); operationErr != nil {
		return operationErr
	}
	if operationErr := j.save(r, "creating"); operationErr != nil {
		return operationErr
	}
	containerID, createErr := docker.create(ctx, a, h.root, launchID)
	if hexadecimal(containerID) {
		r.Container = containerID
		if operationErr := j.save(r, "created"); operationErr != nil {
			return operationErr
		}
	}
	if createErr != nil || r.Container == "" {
		return ErrUncertain
	}
	if operationErr := docker.verifyStopped(ctx, r.Container, a, h.root, launchID); operationErr != nil {
		return operationErr
	}
	if operationErr := docker.verifyExecutable(ctx, r.Container, a); operationErr != nil {
		return operationErr
	}
	if operationErr := j.save(r, "verified"); operationErr != nil {
		return operationErr
	}
	// Recheck the exact certificate immediately before registering and starting.
	if operationErr := h.certificate(ctx, a); operationErr != nil {
		return operationErr
	}
	state, e := h.admin.Read(ctx)
	if e != nil {
		return e
	}
	r.BindOperation = id.New(managed.PrefixOperation).String()
	if operationErr := j.save(r, "binding"); operationErr != nil {
		return operationErr
	}
	binding := admission.Binding{ID: launchID, Certificate: certificate, RuntimeID: r.Container, Principal: r.Account, PeerDigest: r.Peer, Epoch: h.manifest.Epoch, Role: a.Specification.Role}
	if _, e = h.admin.Apply(ctx, admission.Command{ID: r.BindOperation, ExpectedRevision: state.Revision, Action: "bind", Binding: &binding}); e != nil {
		return ErrUncertain
	}
	return j.save(r, "bound")
}

// Start requires an enrolled immutable machine and a live authenticated readiness proof.
// Starting/running records are never resumed: any uncertain start is inspect/stop only.
func (h *Host) Start(ctx context.Context, launchID string) (result error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	j, e := openJournal(h.root, launchID)
	if e != nil {
		return e
	}
	defer j.close()
	r, e := j.read(launchID)
	if e != nil {
		return e
	}
	if r.Phase != "enrolled" {
		return ErrUncertain
	}
	enrollment, e := h.readEnrollment(r)
	if e != nil {
		return e
	}
	a, e := h.manifest.artifact(r.Certificate)
	if e != nil {
		return e
	}
	if operationErr := h.certificate(ctx, a); operationErr != nil {
		return operationErr
	}
	docker, e := newDocker(ctx, h.manifest)
	if e != nil {
		return e
	}
	defer docker.close()
	if operationErr := docker.verifyImage(ctx, a); operationErr != nil {
		return operationErr
	}
	if operationErr := docker.verifyStopped(ctx, r.Container, a, h.root, launchID); operationErr != nil {
		return operationErr
	}
	if operationErr := docker.verifyExecutable(ctx, r.Container, a); operationErr != nil {
		return operationErr
	}
	machine, e := readPrivate(filepath.Join(h.root, launchID+"-machine"), MaxRecord)
	if e != nil {
		return e
	}
	var startup Startup
	if canonical(machine, &startup, MaxRecord) != nil || startup.Account != r.Account || startup.Key != r.Key || startup.App != h.app.String() || startup.Environment != h.environment.String() || managed.Digest(startup.Configuration) != a.Specification.ConfigurationDigest {
		return ErrRefused
	}
	if startupErr := verifyStartup(filepath.Join(h.root, launchID+"-startup"), machine); startupErr != nil {
		return startupErr
	}
	cert, e := readPrivate(filepath.Join(h.root, launchID+"-proxy-cert.pem"), MaxRecord)
	if e != nil {
		return e
	}
	key, e := readPrivate(filepath.Join(h.root, launchID+"-proxy-key.pem"), MaxRecord)
	if e != nil {
		return e
	}
	pair, e := tls.X509KeyPair(cert, key)
	if e != nil || len(pair.Certificate) == 0 || managed.Digest(pair.Certificate[0]) != r.Peer {
		return ErrRefused
	}
	upstreamCA, e := readPrivate(filepath.Join(h.configDirectory, "upstream-ca.pem"), MaxRecord)
	if e != nil {
		return e
	}
	witness, e := probeReadiness(ctx, a.Specification, pair, upstreamCA, startup.Bearer, enrollment)
	if e != nil {
		return e
	}
	r.Readiness = witness
	control, e := startControl(h.root, launchID, cancel)
	if e != nil {
		return e
	}
	defer func() { control.finish(result) }()
	proxy, e := newProxy(h.root, launchID, a.Specification, pair, r.Peer, upstreamCA)
	if e != nil {
		return e
	}
	proxy.start()
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = proxy.close(cleanup)
	}()
	if operationErr := j.save(r, "starting"); operationErr != nil {
		return operationErr
	}
	return runStarted(ctx,
		func(ctx context.Context) error { return docker.start(ctx, r.Container) },
		func() error { return j.save(r, "running") },
		func() error {
			select {
			case <-ctx.Done():
				return nil
			case proxyErr := <-proxy.done:
				if errors.Is(proxyErr, http.ErrServerClosed) {
					return nil
				}
				return proxyErr
			}
		},
		func(cleanup context.Context) error {
			proxyCloseErr := proxy.server.Close()
			proxy.transport.CloseIdleConnections()
			return errors.Join(proxyCloseErr, h.stopHeld(cleanup, j, r, docker))
		})
}

// Inspect reads retained references without taking the active supervisor lock.
func (h *Host) Inspect(launchID string) (Record, error) {
	if !token(launchID) || len(launchID) > 32 {
		return Record{}, ErrRefused
	}
	if operationErr := secureDirectory(h.root); operationErr != nil {
		return Record{}, operationErr
	}
	return (&journal{root: h.root}).read(launchID)
}

// Stop closes an active proxy and tears down only the retained Docker identity.
func (h *Host) Stop(ctx context.Context, launchID string) error {
	if handled, e := requestStop(ctx, h.root, launchID); handled {
		return e
	}
	j, e := openJournal(h.root, launchID)
	if e != nil {
		return e
	}
	defer j.close()
	r, e := j.read(launchID)
	if e != nil {
		return e
	}
	raw, e := json.Marshal(h.manifest)
	if e != nil || managed.Digest(raw) != r.Manifest {
		return ErrRefused
	}
	docker, e := newDocker(ctx, h.manifest)
	if e != nil {
		return e
	}
	defer docker.close()
	return h.stopHeld(ctx, j, r, docker)
}
func (h *Host) stopHeld(ctx context.Context, j *journal, r Record, docker *dockerRuntime) error {
	if r.Phase == "stopped" {
		return nil
	}
	if r.Container == "" {
		return ErrUncertain
	}
	if r.Phase != "removed" {
		if operationErr := j.save(r, "stopping"); operationErr != nil {
			return errors.Join(ErrUncertain, operationErr, docker.remove(ctx, r.Container))
		}
		if operationErr := docker.remove(ctx, r.Container); operationErr != nil {
			return ErrUncertain
		}
		if operationErr := j.save(r, "removed"); operationErr != nil {
			return operationErr
		}
		r.Phase = "removed"
	}
	if r.Key != "" {
		keyID, e := aid.ParseAPIKeyID(r.Key)
		if e != nil {
			return e
		}
		key, e := h.engine.APIKeyStore().GetAPIKey(ctx, keyID)
		if e != nil || key == nil || key.ServiceAccountID.String() != r.Account || key.AppID != h.app || key.EnvID != h.environment {
			return ErrRefused
		}
		if !key.Revoked {
			key.Revoked = true
			if operationErr := h.engine.APIKeyStore().UpdateAPIKey(ctx, key); operationErr != nil {
				return operationErr
			}
		}
	}
	var bound bool
	if operationErr := h.ledger.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM writerfloor_facts WHERE authority=$1 AND kind='binding' AND id=$2)`, h.manifest.Authority, r.ID).Scan(&bound); operationErr != nil {
		return operationErr
	}
	if bound {
		if r.RevokeOperation == "" {
			state, e := h.admin.Read(ctx)
			if e != nil {
				return e
			}
			r.RevokeOperation = id.New(managed.PrefixOperation).String()
			r.RevokeRevision = state.Revision
			if operationErr := j.save(r, "removed"); operationErr != nil {
				return operationErr
			}
		}
		if _, e := h.admin.Apply(ctx, admission.Command{ID: r.RevokeOperation, ExpectedRevision: r.RevokeRevision, Action: "revoke", Revoke: "binding:" + r.ID}); e != nil {
			return e
		}
	}
	return j.save(r, "stopped")
}

// canonicalConfiguration preserves the bytes embedded by json.Marshal(Startup).
// Validate this before enrolling a machine or issuing its API key.
func canonicalConfiguration(raw []byte) bool {
	if !json.Valid(raw) {
		return false
	}
	encoded, err := json.Marshal(json.RawMessage(raw))
	return err == nil && bytes.Equal(encoded, raw)
}

// runStarted always closes the request channel and bounds cleanup after a retained start attempt.
func runStarted(ctx context.Context, start func(context.Context) error, saveRunning, wait func() error, cleanup func(context.Context) error) (result error) {
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		result = errors.Join(result, cleanup(cleanupContext))
	}()
	if err := start(ctx); err != nil {
		return errors.Join(ErrUncertain, err)
	}
	if err := saveRunning(); err != nil {
		return err
	}
	return wait()
}

func (m Manifest) artifact(key string) (Artifact, error) {
	if operationErr := m.Validate(); operationErr != nil {
		return Artifact{}, operationErr
	}
	for _, a := range m.Artifacts {
		if a.Certificate == key {
			return a, nil
		}
	}
	return Artifact{}, ErrRefused
}
