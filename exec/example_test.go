package exec_test

import (
	"context"
	"fmt"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/job"
)

type modelInput struct {
	Detail int `json:"detail"`
}

// ExampleRegistry_Select shows how a definition's declared isolation
// chooses the executor that runs it.
func ExampleRegistry_Select() {
	registry := job.NewRegistry()

	// A handler that parses untrusted geometry declares that it needs a
	// separate address space at minimum.
	job.NewDefinition("tessellate.model",
		func(context.Context, modelInput) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(registry)

	executors := exec.NewRegistry(inproc.New(registry))

	_, err := executors.Select(registry.Policy("tessellate.model"))
	fmt.Println(err != nil)

	// A handler that declares nothing runs in-process, as it always has.
	e, err := executors.Select(registry.Policy("send.email"))
	fmt.Println(e.Name(), err)

	// Output:
	// true
	// inprocess <nil>
}
