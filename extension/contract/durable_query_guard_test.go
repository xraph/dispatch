package contract

import (
	"encoding/json"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"

	"github.com/xraph/dispatch/store/memory"
)

func TestQueryGuardStrictContracts(t *testing.T) {
	registry, wardens, d := fc.NewRegistry(), fc.NewWardenRegistry(), dispatcher.New(nil)
	if err := Register(d, registry, wardens, durableDeps(t, memory.New())); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"durable.queryRemovalRevalidate", "durable.queryRemovalRevalidation", "durable.queryRemovalCommit", "durable.queryRemovalCommitStatus", "durable.queryRemovalGuardClose", "durable.queryRemovalGuardReceipt"} {
		t.Run(name, func(t *testing.T) {
			intent, ok := registry.Intent(ContributorName, name, 1)
			if !ok || intent.Schema.Input["type"] != "object" || intent.Schema.Input["additionalProperties"] != false || intent.Schema.Output == nil {
				t.Fatal("guard schema missing or permissive")
			}
			if DurableAction(name) == "" || queryGuardDurableKind(name) == "" {
				t.Fatal("guard intent missing authorization or dispatch classification")
			}
			payload := map[string]any{"namespace": "allowed", "build_id": "historic", "runtime_id": "runtime", "reservation_request_id": "begin", "reservation_version": "01", "request_id": "guard", "command_digest": "invalid", "kind": "unknown", "evidence": map[string]any{"trusted": true}}
			data, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = d.Dispatch(t.Context(), fc.Request{Contributor: ContributorName, Intent: name, IntentVersion: 1, Kind: queryGuardDurableKind(name), Payload: data}, testPrincipal()); err == nil {
				t.Fatal("invalid enum, decimal or caller evidence accepted")
			}
		})
	}
}
