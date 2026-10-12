package contract

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

func TestOperationalTransportAllEightQueries(t *testing.T) {
	d := contractDeps(t, memory.New())
	if err := engine.RegisterChecked(d.Engine, job.NewDefinition("transport", func(context.Context, struct{}) error { return nil })); err != nil {
		t.Fatal(err)
	}
	inputs := map[string]any{
		"workers.list": EmptyInput{}, "workers.get": IDInput{ID: d.Engine.WorkerID().String()},
		"queues.list": EmptyInput{}, "queues.get": NameInput{Name: "default"},
		"handlers.list": EmptyInput{}, "handlers.get": HandlerInput{Kind: "job", Name: "transport"},
		"engine.config": EmptyInput{}, "overview.summary": EmptyInput{},
	}
	for intent, input := range inputs {
		t.Run(intent, func(t *testing.T) {
			response := callContract(t, d, "query", intent, input)
			var data map[string]json.RawMessage
			if err := json.Unmarshal(response.Data, &data); err != nil {
				t.Fatal(err)
			}
			if string(data["asOf"]) == "" || string(data["asOf"]) == "null" {
				t.Fatalf("missing asOf: %s", response.Data)
			}
			if intent == "queues.get" && (string(data["localActiveCount"]) != "null" || string(data["localSettings"]) != "null") {
				t.Fatalf("invented local settings: %s", response.Data)
			}
		})
	}
}
