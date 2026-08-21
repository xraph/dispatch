package job_test

import (
	"context"
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

func TestWithResourcesMergesVariadicSets(t *testing.T) {
	opts := job.DefaultOptions()

	for _, o := range []job.Option{
		job.WithResources(resource.CPUs(4), resource.MemoryGB(16)),
	} {
		o(&opts)
	}

	if opts.Resources[resource.CPU] != 4000 {
		t.Errorf("cpu = %d, want 4000", opts.Resources[resource.CPU])
	}
	if opts.Resources[resource.Memory] != 16<<30 {
		t.Errorf("memory = %d, want 16 GiB", opts.Resources[resource.Memory])
	}
}

func TestWithResourcesAccumulatesAcrossCalls(t *testing.T) {
	opts := job.DefaultOptions()

	job.WithResources(resource.CPUs(2))(&opts)
	job.WithResources(resource.MemoryGB(8))(&opts)

	if opts.Resources[resource.CPU] != 2000 || opts.Resources[resource.Memory] != 8<<30 {
		t.Errorf("got %v, want both keys retained", opts.Resources)
	}
}

func TestWithResourcesLaterCallWinsPerKey(t *testing.T) {
	opts := job.DefaultOptions()

	job.WithResources(resource.MemoryGB(8))(&opts)
	job.WithResources(resource.MemoryGB(32))(&opts)

	if opts.Resources[resource.Memory] != 32<<30 {
		t.Errorf("memory = %d, want the later 32 GiB",
			opts.Resources[resource.Memory])
	}
}

func TestWithResourceFuncIsStored(t *testing.T) {
	opts := job.DefaultOptions()

	job.WithResourceFunc(func(_ context.Context, r resource.Request) (resource.Set, error) {
		return resource.MemoryBytes(r.InputBytes * 3), nil
	})(&opts)

	if opts.ResourceFunc == nil {
		t.Fatal("ResourceFunc was not stored")
	}

	got, err := opts.ResourceFunc(context.Background(), resource.Request{InputBytes: 100})
	if err != nil {
		t.Fatalf("ResourceFunc() error = %v", err)
	}
	if got[resource.Memory] != 300 {
		t.Errorf("got %v, want 300 bytes", got)
	}
}

func TestDefaultOptionsDeclareNoResources(t *testing.T) {
	opts := job.DefaultOptions()

	if !opts.Resources.IsZero() {
		t.Errorf("Resources = %v; the default must declare nothing so "+
			"existing jobs are unaffected", opts.Resources)
	}
}
