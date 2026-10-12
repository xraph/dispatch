package operatorhost

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
)

const diagnosticSecret = "private-chronicle-credential-do-not-retain"

func diagnosticConfig() sinkhost.Config {
	return sinkhost.Config{Addresses: map[string]string{"chronicle": "127.0.0.1:1"}, DSNs: map[string]string{"chronicle": "postgres://private-user:private-password@127.0.0.1/private-db"}, Credentials: map[string]authority.Credential{"chronicle": {Secret: diagnosticSecret}}, TokenKey: bytes.Repeat([]byte("T"), 32), HMACKey: bytes.Repeat([]byte("H"), 32), WebhookSecret: "private-webhook-key"}
}

// The actual test executable doubles as a failing native child before flag parsing.
func init() {
	if os.Getenv("DISPATCH_DIAGNOSTIC_LOG_CHILD") != "1" {
		return
	}
	config := diagnosticConfig()
	raw, _ := json.Marshal(config)
	var output bytes.Buffer
	output.Write(append(raw, '\n'))
	for _, key := range [][]byte{config.TokenKey, config.HMACKey} {
		_, _ = output.WriteString(string(key) + " " + hex.EncodeToString(key) + " " + base64.StdEncoding.EncodeToString(key) + "\n")
	}
	_, _ = output.WriteString(strings.Repeat("x", 4068-output.Len()) + diagnosticSecret + strings.Repeat("z", 1000) + "\n")
	_, _ = os.Stdout.Write(output.Bytes())
	if os.Getenv("DISPATCH_DIAGNOSTIC_MODE") == "timeout" {
		time.Sleep(time.Hour)
	}
	os.Exit(17)
}

func TestNativeChronicleFailureDiagnostics(t *testing.T) {
	if os.Getenv("DISPATCH_DIAGNOSTIC_HARNESS") == "1" {
		directory := os.Getenv("DISPATCH_DIAGNOSTIC_DIR")
		private, err := os.MkdirTemp(directory, "private-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = os.RemoveAll(private)
			_ = os.WriteFile(filepath.Join(directory, "cleaned"), []byte("cleaned"), 0600)
		})
		config := diagnosticConfig()
		path := filepath.Join(private, "config.json")
		if err = sinkhost.Save(path, config); err != nil {
			t.Fatal(err)
		}
		t.Setenv("DISPATCH_DIAGNOSTIC_LOG_CHILD", "1")
		t.Setenv("DISPATCH_SINK_HOST_BINARY", os.Args[0])
		startNativeChronicle(t, config, path)
		t.Fatal("failing child unexpectedly became ready")
	}
	for _, mode := range []string{"exit", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestNativeChronicleFailureDiagnostics$")
			command.Env = append(os.Environ(), "DISPATCH_DIAGNOSTIC_HARNESS=1", "DISPATCH_DIAGNOSTIC_DIR="+directory, "DISPATCH_DIAGNOSTIC_MODE="+mode)
			raw, err := command.CombinedOutput()
			if err == nil {
				t.Fatal("expected native helper failure")
			}
			if !bytes.Contains(raw, []byte(`"role":"chronicle"`)) || !bytes.Contains(raw, []byte(`"phase":`)) || !bytes.Contains(raw, []byte(`"exit":`)) || !bytes.Contains(raw, []byte("[redacted]")) {
				t.Fatalf("structured sanitized native diagnostic missing: %s", raw)
			}
			if mode == "exit" && !bytes.Contains(raw, []byte("exit status 17")) {
				t.Fatal("child exit status discarded")
			}
			config := diagnosticConfig()
			secrets := make([]string, 0, 12)
			secrets = append(secrets, diagnosticSecret, diagnosticSecret[:20], config.DSNs["chronicle"], config.WebhookSecret, "private-user", "private-password")
			for _, key := range [][]byte{config.TokenKey, config.HMACKey} {
				secrets = append(secrets, string(key), hex.EncodeToString(key), base64.StdEncoding.EncodeToString(key))
			}
			for _, secret := range secrets {
				if bytes.Contains(raw, []byte(secret)) {
					t.Fatal("native diagnostic leaked private material")
				}
			}
			if len(raw) > 20000 {
				t.Fatalf("diagnostic output unbounded: %d", len(raw))
			}
			if _, err = os.Stat(filepath.Join(directory, "cleaned")); err != nil {
				t.Fatal("failure bypassed private cleanup")
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 1 {
				t.Fatal("private failure configuration survived cleanup")
			}
		})
	}
}

// The buffer belongs to exec's output copier until Wait has completed.
func (n *nativeChronicle) diagnostic(phase string, cause error) string {
	secrets := []string{n.config.WebhookSecret}
	for _, credential := range n.config.Credentials {
		secrets = append(secrets, credential.Secret)
	}
	for _, dsn := range n.config.DSNs {
		secrets = append(secrets, dsn)
		if parsed, err := url.Parse(dsn); err == nil && parsed.User != nil {
			secrets = append(secrets, parsed.User.String(), parsed.User.Username())
			if password, ok := parsed.User.Password(); ok {
				secrets = append(secrets, password)
			}
		}
	}
	for _, key := range [][]byte{n.config.TokenKey, n.config.HMACKey} {
		secrets = append(secrets, string(key), hex.EncodeToString(key), base64.StdEncoding.EncodeToString(key), base64.RawStdEncoding.EncodeToString(key), base64.URLEncoding.EncodeToString(key), base64.RawURLEncoding.EncodeToString(key))
	}
	// Replace longer values first so overlapping values cannot leave secret suffixes.
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	sanitize := func(text string, limit int) string {
		for _, secret := range secrets {
			if secret != "" {
				text = strings.ReplaceAll(text, secret, "[redacted]")
			}
		}
		if len(text) > limit {
			text = text[:limit]
			for !utf8.ValidString(text) && len(text) > 0 {
				text = text[:len(text)-1]
			}
			text += "[truncated]"
		}
		return text
	}
	output := "child output unavailable: not joined"
	if n.stopped {
		output = n.logs.String()
	}
	exit := "not joined"
	if n.stopped {
		exit = "success"
		if n.exit != nil {
			exit = n.exit.Error()
		}
	}
	failure := ""
	if cause != nil {
		failure = cause.Error()
	}
	record := struct {
		Role   string `json:"role"`
		Phase  string `json:"phase"`
		Exit   string `json:"exit"`
		Cause  string `json:"cause"`
		Output string `json:"output"`
	}{"chronicle", phase, sanitize(exit, 512), sanitize(failure, 512), sanitize(output, 4096)}
	raw, _ := json.Marshal(record)
	return string(raw)
}
