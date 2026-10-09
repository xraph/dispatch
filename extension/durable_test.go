package extension_test

import (
	"context"
	"errors"
	"testing"
	"time"

	forgetesting "github.com/xraph/forge/testing"
	"gopkg.in/yaml.v3"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
)

type legacyStore struct{ store.Store }

func durableOptions(handler drt.WorkflowFunc) drt.Options {
	return drt.Options{Namespace: "operations", Queue: "durable", BuildID: "build1", Owner: "worker", PollInterval: time.Millisecond, Workflows: map[string]drt.WorkflowFunc{"order": handler}}
}
func TestExtensionDurableLifecycleAndCapturedHandlers(t *testing.T) {
	s := memory.New()
	handler := func(*drt.Workflow, []byte) ([]byte, error) { return []byte("done"), nil }
	options := durableOptions(handler)
	option := extension.WithDurableWorkflows(options)
	options.Workflows["order"] = nil
	e := extension.New(extension.WithStore(s), extension.WithDisableRoutes(), option)
	options.Workflows["order"] = func(*drt.Workflow, []byte) ([]byte, error) { panic("mutated") }
	if err := e.Register(forgetesting.NewTestApp("durable", "1")); err != nil {
		t.Fatal(err)
	}
	request := durable.StartRequest{Key: durable.Key{Namespace: "operations", WorkflowID: "order", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: "build1", Queue: "durable"}
	if _, err := e.Engine().StartDurableWorkflow(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		execution, err := s.GetExecution(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		if execution.State == durable.StateCompleted {
			if string(execution.Output) != "done" {
				t.Fatal(execution)
			}
			if err := e.Health(t.Context()); err != nil {
				t.Fatal(err)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("workflow did not complete")
}
func TestExtensionDurableConfigRejectsInvalidAndUnsupported(t *testing.T) {
	valid := durableOptions(func(*drt.Workflow, []byte) ([]byte, error) { return nil, nil })
	for _, field := range []string{"namespace", "queue", "build", "owner", "timing", "concurrency", "handler"} {
		t.Run(field, func(t *testing.T) {
			opts := durableOptions(valid.Workflows["order"])
			switch field {
			case "namespace":
				opts.Namespace = ""
			case "queue":
				opts.Queue = ""
			case "build":
				opts.BuildID = ""
			case "owner":
				opts.Owner = ""
			case "timing":
				opts.PollInterval = -1
			case "concurrency":
				opts.Concurrency = -1
			case "handler":
				opts.Workflows["order"] = nil
			}
			e := extension.New(extension.WithStore(memory.New()), extension.WithDisableRoutes(), extension.WithDurableWorkflows(opts))
			if err := e.Register(forgetesting.NewTestApp(field, "1")); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	e := extension.New(extension.WithStore(legacyStore{memory.New()}), extension.WithDisableRoutes(), extension.WithDurableWorkflows(valid))
	if err := e.Register(forgetesting.NewTestApp("legacy", "1")); !errors.Is(err, engine.ErrDurableUnsupported) {
		t.Fatal(err)
	}
}
func TestExtensionHealthIncludesWorkerFailure(t *testing.T) {
	s := memory.New()
	e := extension.New(extension.WithStore(s), extension.WithDisableRoutes(), extension.WithDurableWorkflows(durableOptions(func(*drt.Workflow, []byte) ([]byte, error) { panic("worker failed") })))
	if err := e.Register(forgetesting.NewTestApp("health", "1")); err != nil {
		t.Fatal(err)
	}
	_, err := e.Engine().StartDurableWorkflow(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: "operations", WorkflowID: "order", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: "build1", Queue: "durable"})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if errors.Is(e.Health(t.Context()), drt.ErrWorkflowPanic) {
			if err := s.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("health hid worker failure")
}

func TestSerializedDurableConfigWithGoHandlers(t *testing.T) {
	var cfg extension.Config
	if err := yaml.Unmarshal([]byte("durable:\n  enabled: true\n  namespace: operations\n  queue: durable\n  build_id: build1\n  owner: yaml-worker\n  poll_interval: 1ms\n"), &cfg); err != nil {
		t.Fatal(err)
	}
	handlers := map[string]drt.WorkflowFunc{"order": func(*drt.Workflow, []byte) ([]byte, error) { return []byte("yaml-completed"), nil }}
	option := extension.WithDurableHandlers(handlers, nil)
	handlers["order"] = nil
	s := memory.New()
	e := extension.New(extension.WithStore(s), extension.WithConfig(cfg), extension.WithDisableRoutes(), option)
	app := forgetesting.NewTestApp("yaml", "1")
	app.Config().Set("extensions.dispatch", map[string]any{"durable": map[string]any{"enabled": true, "namespace": "operations", "queue": "durable", "build_id": "build1", "owner": "yaml-worker", "poll_interval": "1ms"}})
	if err := e.Register(app); err != nil {
		t.Fatal(err)
	}
	request := durable.StartRequest{Key: durable.Key{Namespace: "operations", WorkflowID: "order", RunID: "yaml-run"}, RequestID: "yaml-start", WorkflowType: "order", BuildID: "build1", Queue: "durable"}
	if _, err := e.Engine().StartDurableWorkflow(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		execution, err := s.GetExecution(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		if execution.State == durable.StateCompleted {
			events, err := s.ReadHistory(t.Context(), request.Key, 0, 100)
			if err != nil || len(events) == 0 || events[len(events)-1].Type != drt.EventWorkflowCompleted {
				t.Fatalf("history %v %v", events, err)
			}
			if string(execution.Output) != "yaml-completed" {
				t.Fatal(execution)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("YAML worker never executed")
}
