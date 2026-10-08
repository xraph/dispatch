package contract

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/engine"
	execution "github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/subprocess"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

func inspectionContractDeps(t *testing.T) Deps {
	t.Helper()
	s := memory.New()
	backend := artifacttest.NewBackend()
	service := artifact.NewService(s, backend, artifact.WithDefaultBucket("dispatch-output"))
	c, err := cache.New(t.TempDir(), backend, cache.WithBudget(4096))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	process := subprocess.New(subprocess.WithBinary("secret-binary"), subprocess.WithArgs("secret-argument"), subprocess.WithEnv(map[string]string{"SECRET": "secret-value"}),
		subprocess.WithUser(1001, 1002), subprocess.WithAllowSameUser(), subprocess.WithRlimits(subprocess.Rlimits{NoFile: 128, Core: 1024}), subprocess.WithStrictRlimits(), subprocess.WithScratchDir("/scratch/process"))
	return contractDeps(t, s, engine.WithArtifacts(service, c), engine.WithExecutor(process),
		engine.WithResourceDefaults(resource.Set{resource.Memory: 10}, map[string]resource.Set{"batch": {resource.Memory: 20}}))
}
func TestHandlerContractDescribesDeclarationsWithoutExecutingThem(t *testing.T) {
	d := inspectionContractDeps(t)
	ctx := context.Background()
	p := fc.Principal{}
	def := job.NewDefinition("convert", func(context.Context, struct{}) error { return nil },
		job.WithArtifactInputs(artifact.Input("source", artifact.Required, artifact.MaxSize(1024), artifact.StageAsPath)),
		job.WithResources(resource.Set{resource.Memory: 128}), job.WithResourceLimits(resource.Set{resource.Memory: 256}), job.WithResourceClass("batch"),
		job.WithResourceFunc(func(context.Context, resource.Request) (resource.Set, error) {
			t.Fatal("inspection executed resource function")
			return nil, nil
		}),
		job.WithLeaseTTL(2*time.Minute), job.WithExecution(execution.Isolate(execution.LevelProcess), execution.GracePeriod(7*time.Second), execution.Image("worker:v1")))
	if err := engine.RegisterChecked(d.Engine, def); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterChecked(d.Engine, job.NewDefinition("plain", func(context.Context, struct{}) error { return nil })); err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{3, 1, 2} {
		engine.RegisterWorkflow(d.Engine, workflow.NewWorkflowV("convert", version, func(*workflow.Workflow, struct{}) error { return nil }))
	}
	page, err := handlersListHandler(d)(ctx, EmptyInput{}, p)
	if err != nil || len(page.Items) != 3 || page.Items[0].Kind != "job" || page.Items[0].Name != "convert" || page.Items[2].Kind != "workflow" || page.AsOf == "" {
		t.Fatalf("handlers=%+v, %v", page, err)
	}
	detail, err := handlersGetHandler(d)(ctx, HandlerInput{Kind: "job", Name: "convert"}, p)
	if err != nil || detail.Job == nil {
		t.Fatalf("job=%+v, %v", detail, err)
	}
	got := detail.Job
	if len(got.Inputs) != 1 || !got.Inputs[0].Required || got.Inputs[0].MaxSize != 1024 || got.Inputs[0].Mode != "path" || got.Resources[resource.Memory] != 128 ||
		got.ResourceLimits[resource.Memory] != 256 || got.ResourceClass == nil || *got.ResourceClass != "batch" || !got.ResourceFunction || got.LeaseTTL == nil || got.LeaseTTL.MS != 120000 ||
		got.EffectiveLeaseTTL.MS != 120000 || got.Execution.Level != "process" || got.Execution.GracePeriod.MS != 7000 || got.Execution.Image == nil {
		t.Fatalf("declaration=%+v", got)
	}
	got.Resources[resource.Memory] = 0
	got.Inputs[0].Name = "changed"
	again, err := handlersGetHandler(d)(ctx, HandlerInput{Kind: "job", Name: "convert"}, p)
	if err != nil || again.Job.Resources[resource.Memory] != 128 || again.Job.Inputs[0].Name != "source" {
		t.Fatalf("detached=%+v, %v", again, err)
	}
	plain, err := handlersGetHandler(d)(ctx, HandlerInput{Kind: "job", Name: "plain"}, p)
	if err != nil || plain.Job.LeaseTTL != nil || plain.Job.EffectiveLeaseTTL.MS != d.Engine.Inspect().Pool.DefaultLeaseTTL.Milliseconds() || plain.Job.Inputs == nil ||
		plain.Job.ResourceFunction || plain.Job.Execution.Image != nil {
		t.Fatalf("plain=%+v, %v", plain, err)
	}
	versions, err := handlersGetHandler(d)(ctx, HandlerInput{Kind: "workflow", Name: "convert"}, p)
	if err != nil || versions.Job != nil || !reflect.DeepEqual(versions.Versions, []int{1, 2, 3}) {
		t.Fatalf("workflow=%+v, %v", versions, err)
	}
	for _, input := range []HandlerInput{{Kind: "other", Name: "x"}, {Kind: "job", Name: " "}} {
		if _, readErr := handlersGetHandler(d)(ctx, input, p); !errors.Is(readErr, fc.ErrBadRequest) {
			t.Fatalf("invalid=%v", readErr)
		}
	}
	for _, kind := range []string{"job", "workflow"} {
		if _, readErr := handlersGetHandler(d)(ctx, HandlerInput{Kind: kind, Name: "missing"}, p); !errors.Is(readErr, fc.ErrNotFound) {
			t.Fatalf("missing=%v", readErr)
		}
	}
}
func TestEngineContractEffectiveSettingsAndPrivateConfiguration(t *testing.T) {
	d := inspectionContractDeps(t)
	got, err := engineConfigHandler(d)(context.Background(), EmptyInput{}, fc.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkerID != d.Engine.WorkerID().String() || got.AsOf == "" || got.Queues == nil || got.Pool.WorkerHeartbeatInterval.MS != 10000 || got.Pool.WorkerStaleThreshold.MS != 300000 ||
		!got.Artifacts.Enabled || got.Artifacts.Backend == nil || *got.Artifacts.Backend != "memory" || got.Artifacts.DefaultBucket == nil || *got.Artifacts.DefaultBucket != "dispatch-output" ||
		got.Artifacts.Cache == nil || got.Artifacts.Cache.BudgetBytes != 4096 || got.Artifacts.Cache.UsedBytes != 0 {
		t.Fatalf("settings=%+v", got)
	}
	found := false
	for _, executor := range got.Executors {
		if executor.Subprocess != nil {
			found = true
			process := executor.Subprocess
			if !process.UserConfigured || process.UID == nil || *process.UID != 1001 || process.GID == nil || *process.GID != 1002 || !process.StrictRlimits ||
				!process.AllowSameUser || !process.HasRlimits || process.RequestedLimits.Core != 0 || process.RequestedLimits.NoFile != 128 || process.ScratchDir == nil {
				t.Fatalf("process=%+v", process)
			}
		}
	}
	if !found {
		t.Fatal("subprocess missing")
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-binary", "secret-argument", "secret-value", "SECRET"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("private config leaked: %s", secret)
		}
	}
	got.Resources.Defaults[resource.Memory] = 0
	got.Resources.Queues["batch"][resource.Memory] = 0
	again, err := engineConfigHandler(d)(context.Background(), EmptyInput{}, fc.Principal{})
	if err != nil || again.Resources.Defaults[resource.Memory] != 10 || again.Resources.Queues["batch"][resource.Memory] != 20 {
		t.Fatalf("detached settings=%+v, %v", again, err)
	}
}
func TestEngineContractDisabledSubsystemsAndHeartbeatFallback(t *testing.T) {
	s := memory.New()
	base, err := dispatch.New(dispatch.WithStore(s), dispatch.WithHeartbeatInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Build(base, engine.WithExecutor(subprocess.New()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })
	got, err := engineConfigHandler(Deps{Engine: eng, Store: s})(context.Background(), EmptyInput{}, fc.Principal{})
	if err != nil || got.Artifacts.Enabled || got.Artifacts.Backend != nil || got.Artifacts.Cache != nil || got.Resources.Enabled || got.Resources.CustomKeys == nil ||
		got.Pool.JobHeartbeatInterval.MS != 0 || got.Pool.WorkerHeartbeatInterval.MS != 10000 || got.Queues == nil {
		t.Fatalf("defaults=%+v, %v", got, err)
	}
	for _, executor := range got.Executors {
		if executor.Subprocess != nil && (executor.Subprocess.UID != nil || executor.Subprocess.GID != nil) {
			t.Fatal("unset subprocess user invented")
		}
	}
}
