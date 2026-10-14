//go:build linux

package launcher

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/client"
)

func TestRunStartedAlwaysCleansKnownLaunch(t *testing.T) {
	for _, scenario := range []string{"ambiguous start", "running journal failure", "wait failure", "success", "cleanup failure", "cancelled caller"} {
		t.Run(scenario, func(t *testing.T) {
			cause := errors.New("retained original error")
			cleanupFailure := errors.New("retained cleanup error")
			caller, cancel := context.WithCancel(t.Context())
			defer cancel()
			starts, saves, waits, cleanups := 0, 0, 0, 0
			result := runStarted(caller, func(ctx context.Context) error {
				starts++
				if scenario == "cancelled caller" {
					cancel()
					return ctx.Err()
				}
				if scenario == "ambiguous start" {
					return cause
				}
				return nil
			}, func() error {
				saves++
				if scenario == "running journal failure" {
					return cause
				}
				return nil
			}, func() error {
				waits++
				if scenario == "wait failure" {
					return cause
				}
				return nil
			}, func(ctx context.Context) error {
				cleanups++
				if ctx.Err() != nil {
					t.Error("cleanup inherited cancelled caller")
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 20*time.Second {
					t.Error("cleanup lacks independent bounded deadline")
				}
				if scenario == "cleanup failure" {
					return cleanupFailure
				}
				return nil
			})
			if starts != 1 || cleanups != 1 {
				t.Fatalf("start/cleanup repeated or skipped: %d/%d", starts, cleanups)
			}
			switch scenario {
			case "ambiguous start":
				if !errors.Is(result, ErrUncertain) || !errors.Is(result, cause) || saves != 0 || waits != 0 {
					t.Fatal("ambiguous start was lost or advanced", result, saves, waits)
				}
			case "running journal failure":
				if !errors.Is(result, cause) || saves != 1 || waits != 0 {
					t.Fatal("failed journal advanced", result, saves, waits)
				}
			case "wait failure":
				if !errors.Is(result, cause) || saves != 1 || waits != 1 {
					t.Fatal("wait error lost", result)
				}
			case "success":
				if result != nil || saves != 1 || waits != 1 {
					t.Fatal("success did not clean up", result)
				}
			case "cleanup failure":
				if !errors.Is(result, cleanupFailure) {
					t.Fatal("cleanup failure hidden", result)
				}
			case "cancelled caller":
				if !errors.Is(result, context.Canceled) || saves != 0 || waits != 0 {
					t.Fatal("caller cancellation lost", result)
				}
			}
		})
	}
}

func TestRunStartedPreservesStartAndCleanupUncertainty(t *testing.T) {
	lost := errors.New("start response lost")
	cleanupLost := errors.New("remove response lost")
	result := runStarted(t.Context(), func(context.Context) error { return lost }, func() error { t.Error("saved running after unknown start"); return nil }, func() error { t.Error("waited after unknown start"); return nil }, func(context.Context) error { return cleanupLost })
	if !errors.Is(result, ErrUncertain) || !errors.Is(result, lost) || !errors.Is(result, cleanupLost) {
		t.Fatal("uncertainty association lost", result)
	}
}

func TestVerifyStartupRequiresExactMountedBytesAndOwnership(t *testing.T) {
	for _, scenario := range []string{"exact", "changed bytes", "extra bytes", "writable", "wrong owner", "wrong group", "symlink"} {
		t.Run(scenario, func(t *testing.T) {
			j := rootJournal(t)
			path := filepath.Join(j.root, "launch-startup")
			expected := []byte(`{"configuration":{"route":"fixed"}}`)
			body := expected
			if scenario == "changed bytes" {
				body = []byte(`{"configuration":{"route":"other"}}`)
			}
			if scenario == "extra bytes" {
				body = append(append([]byte(nil), expected...), '\n')
			}
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			owner, group := 10001, 10001
			if scenario == "wrong owner" {
				owner = 0
			}
			if scenario == "wrong group" {
				group = 0
			}
			if err := os.Chown(path, owner, group); err != nil {
				t.Fatal(err)
			}
			mode := os.FileMode(0400)
			if scenario == "writable" {
				mode = 0600
			}
			if err := os.Chmod(path, mode); err != nil {
				t.Fatal(err)
			}
			if scenario == "symlink" {
				link := filepath.Join(j.root, "launch-link")
				if err := os.Symlink(path, link); err != nil {
					t.Fatal(err)
				}
				path = link
			}
			err := verifyStartup(path, expected)
			if (err == nil) != (scenario == "exact") {
				t.Fatalf("startup comparison: %v", err)
			}
		})
	}
}

func TestStopHeldRemovesKnownIDWhenStoppingJournalFails(t *testing.T) {
	j := rootJournal(t)
	m := fixtureManifest()
	record, err := j.create("launch", m, m.Artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	record.Container = strings.Repeat("a", 64)
	if err = j.save(record, "starting"); err != nil {
		t.Fatal(err)
	}
	// Reject replacement through the journal's actual ownership/mode validation.
	if err = os.Chmod(filepath.Join(j.root, "launch.json"), 0400); err != nil {
		t.Fatal(err)
	}
	var removes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1.56/containers/"+record.Container+"/json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"Id":"` + record.Container + `","State":{"Running":false}}`))
		case r.Method == "DELETE" && r.URL.Path == "/v1.56/containers/"+record.Container:
			removes.Add(1)
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected cleanup request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	transport := &http.Transport{DisableKeepAlives: true}
	api, err := client.New(client.WithHost(server.URL), client.WithAPIVersion("1.56"), client.WithHTTPClient(&http.Client{Transport: strictTransport{transport}}))
	if err != nil {
		t.Fatal(err)
	}
	docker := &dockerRuntime{api, transport}
	defer docker.close()
	h := &Host{root: j.root, manifest: m}
	if err = h.stopHeld(t.Context(), j, record, docker); err == nil {
		t.Fatal("journal failure hidden")
	}
	if removes.Load() != 1 {
		t.Fatal("known container escaped cleanup on journal failure")
	}
}

func TestCanonicalConfigurationRefusesEncodingChangesBeforeEnrollment(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want bool
	}{
		{"compact", []byte(`{"route":"fixed"}`), true},
		{"escaped HTML", []byte(`{"value":"\u003c"}`), true},
		{"pretty JSON", []byte("{\n  \"route\": \"fixed\"\n}"), false},
		{"literal HTML", []byte(`{"value":"<"}`), false},
		{"trailing newline", []byte("{\"route\":\"fixed\"}\n"), false},
		{"invalid JSON", []byte(`{"route":}`), false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := canonicalConfiguration(test.raw); got != test.want {
				t.Fatalf("encoding preflight: got %v, want %v", got, test.want)
			}
		})
	}
}
