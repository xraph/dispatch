package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"

	"github.com/xraph/dispatch/store/memory"
)

func TestQueryGuardWireOutputSchemas(t *testing.T) {
	registry := fc.NewRegistry()
	if err := Register(dispatcher.New(nil), registry, fc.NewWardenRegistry(), durableDeps(t, memory.New())); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../operator/testdata/durable-wire.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures map[string]struct{ Data json.RawMessage }
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for name, intentName := range map[string]string{"query_guard_commit": "durable.queryRemovalCommit", "query_guard_close": "durable.queryRemovalGuardClose", "query_guard_commit_status": "durable.queryRemovalCommitStatus"} {
		t.Run(name, func(t *testing.T) {
			intent, ok := registry.Intent(ContributorName, intentName, 1)
			if !ok {
				t.Fatal("missing intent")
			}
			encoded, marshalErr := json.Marshal(intent.Schema.Output)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			document, decodeErr := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			compiler := jsonschema.NewCompiler()
			compiler.AssertFormat()
			if addErr := compiler.AddResource("guard-output.json", document); addErr != nil {
				t.Fatal(addErr)
			}
			schema, compileErr := compiler.Compile("guard-output.json")
			if compileErr != nil {
				t.Fatal(compileErr)
			}
			value, valueErr := jsonschema.UnmarshalJSON(bytes.NewReader(fixtures[name].Data))
			if valueErr != nil {
				t.Fatal(valueErr)
			}
			if validateErr := schema.Validate(value); validateErr != nil {
				t.Fatal(validateErr)
			}
		})
	}
}

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

func TestQueryGuardUnknownFieldsFailBeforeService(t *testing.T) {
	d := dispatcher.New(nil)
	for _, binding := range queryGuardDurableBindings(Deps{}) {
		if err := binding.bind(d); err != nil {
			t.Fatal(err)
		}
		for _, payload := range []string{`{"evidence":{"trusted":true}}`, `{"namespace":"allowed","extra":true}`, `{"scope":{"extra":true}}`} {
			var params map[string]any
			if err := json.Unmarshal([]byte(payload), &params); err != nil {
				t.Fatal(err)
			}
			for _, request := range []fc.Request{
				{Contributor: ContributorName, Intent: binding.intent, IntentVersion: 1, Kind: queryGuardDurableKind(binding.intent), Payload: json.RawMessage(payload)},
				{Contributor: ContributorName, Intent: binding.intent, IntentVersion: 1, Kind: queryGuardDurableKind(binding.intent), Params: params},
			} {
				_, _, err := d.Dispatch(t.Context(), request, testPrincipal())
				var contractError *fc.Error
				if !errors.As(err, &contractError) || contractError.Code != fc.CodeBadRequest {
					t.Fatalf("%s unknown fields reached unavailable service: %v", binding.intent, err)
				}
			}
		}
	}
}
