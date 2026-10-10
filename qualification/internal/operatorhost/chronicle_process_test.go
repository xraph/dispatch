package operatorhost

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
)

type nativeChronicle struct {
	command *exec.Cmd
	done    chan error
	stopped bool
}

func startNativeChronicle(t *testing.T, config sinkhost.Config, path string) *nativeChronicle {
	t.Helper()
	binary := os.Getenv("DISPATCH_SINK_HOST_BINARY")
	if binary == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("native Chronicle binary required")
		}
		t.Skip("native Chronicle binary required")
	}
	command := exec.CommandContext(t.Context(), binary, "--role", "chronicle", "--config", path)
	logs := new(bytes.Buffer)
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	n := &nativeChronicle{command: command, done: make(chan error, 1)}
	go func() { n.done <- command.Wait() }()
	t.Cleanup(func() {
		if !n.stopped {
			n.kill(t)
		}
		for _, credential := range config.Credentials {
			if bytes.Contains(logs.Bytes(), []byte(credential.Secret)) {
				t.Error("sink credential in logs")
			}
		}
	})
	client := &http.Client{Timeout: time.Second}
	until := time.Now().Add(10 * time.Second)
	for time.Now().Before(until) {
		select {
		case <-n.done:
			n.stopped = true
			t.Fatal("native Chronicle stopped before readiness")
		default:
		}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+config.Addresses["chronicle"]+"/health", nil)
		if err != nil {
			t.Fatal(err)
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
	t.Fatal("native Chronicle readiness unavailable")
	return nil
}
func (n *nativeChronicle) kill(t *testing.T) {
	t.Helper()
	if err := n.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-n.done
	n.stopped = true
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
