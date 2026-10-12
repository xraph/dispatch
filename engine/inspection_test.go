package engine_test

import (
	"os"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
)

func TestInspectionCopiesConfigurationAndReportsEffectiveScratch(t *testing.T) {
	d, err := dispatch.New(dispatch.WithStore(memory.New()), dispatch.WithQueues([]string{"mail"}))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Build(d,
		engine.WithResourceDefaults(resource.Set{resource.Memory: 100}, map[string]resource.Set{"mail": {resource.CPU: 2}}),
		engine.WithWorkerCapacity(resource.Set{resource.Memory: 1000}),
		engine.WithWorkerCustomKeys([]string{"gpu"}), engine.WithScratchRoot("/ignored-without-artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	got := eng.Inspect()
	if got.ScratchRoot != os.TempDir() || got.ResourceManagerEnabled || got.EstimatorConfigured {
		t.Fatalf("inspection = %+v", got)
	}
	got.Config.Queues[0] = "changed"
	got.ResourceDefaults[resource.Memory] = 0
	got.QueueResources["mail"][resource.CPU] = 0
	got.WorkerCapacity[resource.Memory] = 0
	got.WorkerCustomKeys[0] = "changed"
	after := eng.Inspect()
	if after.Config.Queues[0] != "mail" || after.ResourceDefaults[resource.Memory] != 100 ||
		after.QueueResources["mail"][resource.CPU] != 2 || after.WorkerCapacity[resource.Memory] != 1000 ||
		after.WorkerCustomKeys[0] != "gpu" {
		t.Fatal("caller changed engine configuration")
	}
}
