package operatorhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
)

type nativeChronicle struct {
	command           *exec.Cmd
	done              chan error
	stopped           bool
	exit              error
	logs              *bytes.Buffer
	config            sinkhost.Config
	additionalSecrets []string
}

func startNativeChronicle(t *testing.T, config sinkhost.Config, path string, owners ...*auditSupervisor) *nativeChronicle {
	t.Helper()
	binary := os.Getenv("DISPATCH_SINK_HOST_BINARY")
	if binary == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("native Chronicle binary required")
		}
		t.Skip("native Chronicle binary required")
	}
	// Cleanup joins the operator before sinks; test-context cancellation must not
	// terminate all children concurrently.
	command := exec.CommandContext(context.Background(), binary, "--role", "chronicle", "--config", path)
	logs := new(bytes.Buffer)
	command.Stdout = logs
	command.Stderr = logs
	n := &nativeChronicle{command: command, done: make(chan error, 1), logs: logs, config: config}
	if len(owners) != 0 && owners[0] != nil {
		owners[0].sinks = append(owners[0].sinks, n)
	} else {
		t.Cleanup(func() { _ = n.cleanup(t) })
	}

	if err := command.Start(); err != nil {
		n.stopped, n.exit = true, err
		t.Fatal(n.diagnostic("start", err))
	}
	go func() { n.done <- command.Wait() }()
	client := &http.Client{Timeout: time.Second}
	until := time.Now().Add(10 * time.Second)
	for time.Now().Before(until) {
		select {
		case err := <-n.done:
			n.stopped, n.exit = true, err
			t.Fatal(n.diagnostic("startup", errors.New("native Chronicle stopped before readiness")))
		default:
		}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+config.Addresses["chronicle"]+"/health", nil)
		if err != nil {
			stopErr := n.stop()
			t.Fatal(n.diagnostic("request", errors.Join(err, stopErr)))
		}
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == 200 {
				return n
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	stopErr := n.stop()
	t.Fatal(n.diagnostic("readiness", errors.Join(errors.New("native Chronicle readiness unavailable"), stopErr)))
	return nil
}
func (n *nativeChronicle) kill(t *testing.T) {
	t.Helper()
	if err := n.stop(); err != nil {
		t.Fatal(n.diagnostic("stop", err))
	}
}

// Always attempt to reap the child, even when termination itself fails.
func (n *nativeChronicle) stop() error {
	if n.stopped {
		return nil
	}
	if n.command.Process == nil {
		n.stopped = true
		return nil
	}
	killErr := n.command.Process.Signal(syscall.SIGTERM)
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	select {
	case n.exit = <-n.done:
		n.stopped = true
		return errors.Join(killErr, n.exit)
	case <-timer.C:
		timeoutErr := errors.Join(killErr, errors.New("native Chronicle child join timed out"))
		forceErr := n.command.Process.Kill()
		select {
		case n.exit = <-n.done:
			n.stopped = true
		case <-time.After(5 * time.Second):
			return errors.Join(timeoutErr, forceErr, errors.New("native Chronicle forced join incomplete"))
		}
		return errors.Join(timeoutErr, forceErr)
	}
}

func lifecycleSinkConfig(t *testing.T) (sinkhost.Config, string, map[string]*pgx.Conn) {
	t.Helper()
	config := sinkhost.Config{Binding: ecosystem.Binding{Producer: "dispatch-lifecycle", InstallationID: "operator-host", Namespace: "production", TenantID: "tenant-production"}, PolicyTenant: "tenant-production", DSNs: map[string]string{}, Addresses: map[string]string{}}
	for _, role := range []string{"dispatch", "chronicle", "relay", "authsome", "warden"} {
		config.DSNs[role] = isolatedPostgresScenario(t)
	}
	for _, role := range []string{"dispatch", "chronicle", "relay", "receiver"} {
		listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		config.Addresses[role] = listener.Addr().String()
		if err = listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for role, dsn := range config.DSNs {
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatal("invalid fixture DSN")
		}
		q := parsed.Query()
		q.Set("pool_max_conns", "2")
		parsed.RawQuery = q.Encode()
		config.DSNs[role] = parsed.String()
	}
	if err := sinkhost.Bootstrap(t.Context(), &config); err != nil {
		t.Fatal("native lifecycle sink bootstrap failed")
	}
	configPath := filepath.Join(t.TempDir(), "sink.json")
	if err := sinkhost.Save(configPath, config); err != nil {
		t.Fatal(err)
	}
	connections := map[string]*pgx.Conn{}
	for _, role := range []string{"dispatch", "chronicle"} {
		parsed, _ := url.Parse(config.DSNs[role])
		q := parsed.Query()
		q.Del("pool_max_conns")
		parsed.RawQuery = q.Encode()
		connection, err := pgx.Connect(t.Context(), parsed.String())
		if err != nil {
			t.Fatal("fixture observation unavailable")
		}
		connections[role] = connection
		t.Cleanup(func() { _ = connection.Close(context.Background()) })
	}
	return config, configPath, connections
}

type nativeChronicleProfiles struct {
	supervisor *auditSupervisor
	configs    map[string]sinkhost.Config
	paths      map[string]string
	routes     sinkhost.ChronicleProfiles
	bundle     string
}

func lifecycleChronicleProfiles(t *testing.T) (nativeChronicleProfiles, map[string]*pgx.Conn) {
	t.Helper()
	base, path, db := lifecycleSinkConfig(t)
	profiles := nativeChronicleProfiles{configs: map[string]sinkhost.Config{"production": base}, paths: map[string]string{"production": path}, routes: sinkhost.ChronicleProfiles{Version: 1, Routes: []sinkhost.ChronicleRoute{sinkhost.ChronicleProfile(base)}}}
	original, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err = os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{"audit", "foreign"} {
		listener, listenErr := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		if listenErr != nil {
			t.Fatal(listenErr)
		}
		address := listener.Addr().String()
		if err = listener.Close(); err != nil {
			t.Fatal(err)
		}
		binding := base.Binding
		binding.Namespace, binding.TenantID = namespace, "tenant-"+namespace
		config, provisionErr := sinkhost.ProvisionChronicleProfile(t.Context(), base, binding, address)
		if provisionErr != nil {
			t.Fatal("additional Chronicle profile provisioning failed")
		}
		unchanged, marshalErr := json.Marshal(base)
		if marshalErr != nil || !bytes.Equal(original, unchanged) || config.EnvironmentID != base.EnvironmentID || !bytes.Equal(config.HMACKey, base.HMACKey) || !bytes.Equal(config.TokenKey, base.TokenKey) || len(config.Credentials) != 1 {
			t.Fatal("additional issuance changed original identity")
		}
		profiles.configs[namespace] = config
		profiles.paths[namespace] = filepath.Join(directory, namespace+".json")
		if err = sinkhost.Save(profiles.paths[namespace], config); err != nil {
			t.Fatal(err)
		}
		profiles.routes.Routes = append(profiles.routes.Routes, sinkhost.ChronicleProfile(config))
	}
	profiles.bundle = filepath.Join(directory, "routes.json")
	if err = sinkhost.SaveChronicleProfiles(profiles.bundle, profiles.routes); err != nil {
		t.Fatal(err)
	}
	profiles.supervisor = newAuditSupervisor(t, db)
	return profiles, db
}

func (p nativeChronicleProfiles) start(t *testing.T) map[string]*nativeChronicle {
	t.Helper()
	sinks := map[string]*nativeChronicle{}
	for _, namespace := range []string{"production", "audit", "foreign"} {
		sinks[namespace] = p.startSink(t, namespace)
	}
	return sinks
}

func (n *nativeChronicle) cleanup(t *testing.T) error {
	stopErr := n.stop()
	cleanupErr := stopErr
	if n.stopped {
		for _, credential := range n.config.Credentials {
			if credential.Secret != "" && bytes.Contains(n.logs.Bytes(), []byte(credential.Secret)) {
				t.Error("sink credential in logs")
				cleanupErr = errors.Join(cleanupErr, errors.New("sink credential in logs"))
			}
		}
	}
	if t.Failed() || stopErr != nil {
		t.Log(n.diagnostic("cleanup", stopErr))
	}
	if stopErr != nil {
		t.Error("native Chronicle cleanup failed")
	}
	return cleanupErr
}

func (p nativeChronicleProfiles) startSink(t *testing.T, namespace string) *nativeChronicle {
	t.Helper()
	n := startNativeChronicle(t, p.configs[namespace], p.paths[namespace], p.supervisor)
	for _, profile := range p.configs {
		for _, credential := range profile.Credentials {
			n.additionalSecrets = append(n.additionalSecrets, credential.Secret)
		}
	}
	return n
}
