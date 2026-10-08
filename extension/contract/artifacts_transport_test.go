package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

func TestArtifactTransportQueriesAndUncachedDownloads(t *testing.T) {
	s := memory.New()
	signer := newContractSigner()
	d := contractDeps(t, s, engine.WithArtifacts(artifact.NewService(s, signer), nil))
	a := seedArtifact(t, s, "transport", artifact.Durable, "", "")
	j := seedJob(t, d, "owner", job.StateCompleted, "", "", "default")
	callContract(t, d, "query", "artifacts.list", ArtifactsListInput{})
	callContract(t, d, "query", "artifacts.get", IDInput{ID: a.ID.String()})
	callContract(t, d, "query", "artifacts.forJob", IDInput{ID: j.ID.String()})
	reg, wreg := fc.NewRegistry(), fc.NewWardenRegistry()
	disp := dispatcher.New(nil)
	if err := Register(disp, reg, wreg, d); err != nil {
		t.Fatal(err)
	}
	handler := transport.NewHandler(reg, wreg, disp, nil)
	var urls []string
	for _, artifactID := range []string{a.ID.String(), a.ID.String(), id.NewArtifactID().String(), "broken"} {
		raw, err := json.Marshal(map[string]any{"envelope": "v1", "kind": "query", "contributor": "dispatch", "intent": "artifacts.presign", "payload": IDInput{ID: artifactID}})
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/dashboard/v1", bytes.NewReader(raw)))
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("download response can be cached: %v", response.Header())
		}
		var envelope fc.Response
		if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		if artifactID != a.ID.String() {
			if envelope.OK {
				t.Fatal("invalid artifact succeeded")
			}
			continue
		}
		if !envelope.OK {
			t.Fatalf("download failed: %s", response.Body)
		}
		var download ArtifactDownload
		if err := json.Unmarshal(envelope.Data, &download); err != nil || download.URL == nil {
			t.Fatalf("download=%+v, %v", download, err)
		}
		urls = append(urls, *download.URL)
	}
	if len(urls) != 2 || urls[0] == urls[1] || signer.calls != 2 {
		t.Fatalf("cached URLs=%v calls=%d", urls, signer.calls)
	}
}
