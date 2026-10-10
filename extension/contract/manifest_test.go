package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/loader"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

func loadManifest(t *testing.T) *fc.ContractManifest {
	t.Helper()
	m, err := loader.Load(bytes.NewReader(manifestYAML), "dispatch/contract/manifest.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestEveryDeclaredIntentIsBound(t *testing.T) {
	deps := contractDeps(t, memory.New())
	manifest := loadManifest(t)
	if manifest.Contributor.Name != ContributorName {
		t.Fatal("contributor mismatch")
	}
	bound := map[string]bool{}
	for _, b := range bindings(deps) {
		if bound[b.intent] {
			t.Fatalf("duplicate %s", b.intent)
		}
		bound[b.intent] = true
	}
	if len(bound) != len(manifest.Intents) {
		t.Fatal("manifest/binding count mismatch")
	}
	for _, in := range manifest.Intents {
		if !bound[in.Name] {
			t.Fatalf("unbound %s", in.Name)
		}
	}
	if err := Register(dispatcher.New(nil), fc.NewRegistry(), fc.NewWardenRegistry(), deps); err != nil {
		t.Fatal(err)
	}
}

func TestManifestCommandInvalidations(t *testing.T) {
	want := map[string][]string{
		"workflows.replayFrom": {"workflows.list", "workflows.get", "workflows.replayPreview", "overview.summary"},
		"crons.enable":         {"crons.list", "crons.get", "overview.summary"},
		"crons.disable":        {"crons.list", "crons.get", "overview.summary"},
		"crons.delete":         {"crons.list", "crons.get", "overview.summary"},
		"crons.runNow":         {"crons.get", "jobs.list", "jobs.counts", "queues.list", "queues.get", "overview.summary"},
		"dlq.replay":           {"dlq.list", "dlq.get", "dlq.counts", "jobs.list", "jobs.counts", "queues.list", "queues.get", "overview.summary"},
		"dlq.replayAll":        {"dlq.list", "dlq.get", "dlq.counts", "jobs.list", "jobs.counts", "queues.list", "queues.get", "overview.summary"},
		"dlq.delete":           {"dlq.list", "dlq.get", "dlq.counts", "dlq.purgePreview", "overview.summary"},
		"dlq.purge":            {"dlq.list", "dlq.get", "dlq.counts", "dlq.purgePreview", "overview.summary"},
		"jobs.cancel":          {"jobs.list", "jobs.get", "jobs.counts", "queues.list", "queues.get", "overview.summary"},
		"jobs.retry":           {"jobs.list", "jobs.get", "jobs.counts", "queues.list", "queues.get", "overview.summary", "dlq.list", "dlq.get", "dlq.counts"},
	}
	for _, name := range []string{"durable.start", "durable.signal", "durable.signalStart", "durable.cancel"} {
		want[name] = []string{"durable.executions", "durable.execution", "durable.history", "durable.tasks", "durable.chain", "durable.children", "durable.audit", "durable.hooks", "durable.capabilities", "durable.payload", "durable.query"}
	}
	found := 0
	for _, intent := range loadManifest(t).Intents {
		if intent.Kind != fc.IntentKindCommand {
			if len(intent.Invalidates) != 0 {
				t.Fatalf("query invalidates: %s", intent.Name)
			}
			continue
		}
		found++
		if !reflect.DeepEqual(intent.Invalidates, want[intent.Name]) {
			t.Fatalf("%s invalidates=%v", intent.Name, intent.Invalidates)
		}
	}
	if found != len(want) {
		t.Fatalf("commands = %d", found)
	}
}

func TestCommandInvalidatesReachTheClient(t *testing.T) {
	for _, intent := range []string{"jobs.cancel", "jobs.retry"} {
		t.Run(intent, func(t *testing.T) {
			deps := contractDeps(t, memory.New())
			state := job.StatePending
			if intent == "jobs.retry" {
				state = job.StateFailed
			}
			j := seedJob(t, deps, "transport", state, "", "", "mail")
			reg := fc.NewRegistry()
			wreg := fc.NewWardenRegistry()
			d := dispatcher.New(nil)
			if err := Register(d, reg, wreg, deps); err != nil {
				t.Fatal(err)
			}
			payload := `{"envelope":"v1","kind":"command","contributor":"dispatch","intent":"` + intent + `","csrf":"test","idempotencyKey":"test-` + intent + `","payload":{"id":"` + j.ID.String() + `"}}`
			req := httptest.NewRequestWithContext(testContext(), http.MethodPost, "/api/dashboard/v1", strings.NewReader(payload))
			recorder := httptest.NewRecorder()
			transport.NewHandler(reg, wreg, d, nil).ServeHTTP(recorder, req)
			var response fc.Response
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || !response.OK {
				t.Fatalf("response=%s, %v", recorder.Body, err)
			}
			var want []string
			for _, declared := range loadManifest(t).Intents {
				if declared.Name == intent {
					want = declared.Invalidates
				}
			}
			if !reflect.DeepEqual(response.Meta.Invalidates, want) {
				t.Fatalf("invalidates=%v, want %v", response.Meta.Invalidates, want)
			}
			stored, err := deps.Store.GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatal(err)
			}
			expected := job.StateCancelled
			if intent == "jobs.retry" {
				expected = job.StatePending
			}
			if stored.State != expected {
				t.Fatalf("stored state=%s, want %s", stored.State, expected)
			}
		})
	}
}

func TestRegisterRejectsMissingDependencies(t *testing.T) {
	if err := Register(dispatcher.New(nil), fc.NewRegistry(), fc.NewWardenRegistry(), Deps{}); err == nil {
		t.Fatal("accepted missing engine")
	}
	deps := contractDeps(t, memory.New())
	if err := Register(nil, fc.NewRegistry(), fc.NewWardenRegistry(), deps); err == nil {
		t.Fatal("accepted missing dispatcher")
	}
}
