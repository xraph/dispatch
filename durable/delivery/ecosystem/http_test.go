package ecosystem_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ra "github.com/xraph/relay/acceptance"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

func TestRemoteTransportBoundaries(t *testing.T) {
	b, d := envelope(t, durable.DestinationRelay)
	req, _ := ecosystem.RelayRequest(b, d)
	for _, endpoint := range []string{"http://example.com/accept", "http://localhost/accept", "https://user:pass@example.com/accept", "https://example.com/accept?key=secret", "file:///tmp/sink"} {
		if _, err := ecosystem.NewRemote(endpoint, "private-bearer", time.Second, true); err == nil {
			t.Fatalf("unsafe endpoint admitted: %s", endpoint)
		}
	}
	var redirected atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { redirected.Add(1); w.WriteHeader(http.StatusOK) }))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client, err := ecosystem.NewRemote(redirect.URL, "private-bearer", time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err = client.SendReliable(t.Context(), req); err == nil || strings.Contains(err.Error(), "private-bearer") || redirected.Load() != 0 {
		t.Fatalf("redirect leaked request: %v %d", err, redirected.Load())
	}
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer tlsServer.Close()
	tlsClient, err := ecosystem.NewRemote(tlsServer.URL, "private-bearer", time.Second, false)
	if err != nil {
		t.Fatal(err)
	}
	defer tlsClient.Close()
	if _, err = tlsClient.SendReliable(t.Context(), req); err == nil {
		t.Fatal("untrusted TLS certificate accepted")
	}
	for _, response := range []string{`{"version":1,"version":2}`, strings.Repeat("x", ecosystem.MaxResponseBytes+1), `{"number":NaN}`, `{"version":1} {}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(response)) }))
		remote, e := ecosystem.NewRemote(server.URL, "private-bearer", time.Second, true)
		if e != nil {
			t.Fatal(e)
		}
		_, e = remote.SendReliable(t.Context(), req)
		remote.Close()
		server.Close()
		if e == nil {
			t.Fatal("ambiguous or excessive response admitted")
		}
	}
}

func TestRemoteConfirmedConflictBinding(t *testing.T) {
	b, d := envelope(t, durable.DestinationRelay)
	req, _ := ecosystem.RelayRequest(b, d)
	fp, _ := ra.Fingerprint(req)
	for _, matched := range []bool{false, true} {
		conflict := ecosystem.ConflictResponse{Destination: "relay", SourceKey: req.SourceKey, SourceFingerprint: req.SourceFingerprint, Fingerprint: fp}
		if !matched {
			conflict.SourceKey = "unrelated"
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer private-bearer" {
				t.Error("credential missing")
			}
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(conflict)
		}))
		client, e := ecosystem.NewRemote(server.URL, "private-bearer", time.Second, true)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.SendReliable(t.Context(), req)
		client.Close()
		server.Close()
		if errors.Is(e, ra.ErrConflict) != matched {
			t.Fatalf("conflict binding: %v %v", matched, e)
		}
	}
}

func TestRemoteRetryBytesRemainIdentical(t *testing.T) {
	b, d := envelope(t, durable.DestinationRelay)
	req, err := ecosystem.RelayRequest(b, d)
	if err != nil {
		t.Fatal(err)
	}
	bodies := make(chan []byte, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, e := io.ReadAll(r.Body)
		if e != nil {
			t.Error(e)
		}
		bodies <- body
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := ecosystem.NewRemote(server.URL, "private-bearer", time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for i := 0; i < 2; i++ {
		if _, err = client.SendReliable(t.Context(), req); err == nil || errors.Is(err, ra.ErrConflict) {
			t.Fatalf("uncertainty classified as conflict: %v", err)
		}
	}
	first, second := <-bodies, <-bodies
	if !bytes.Equal(first, second) {
		t.Fatal("retry changed request bytes")
	}
}
