package workflow_test

import (
	"reflect"
	"testing"

	"github.com/xraph/dispatch/workflow"
)

func TestRegistryVersions(t *testing.T) {
	r := workflow.NewRegistry()
	for _, version := range []int{3, 0, 2, 3} {
		def := workflow.NewWorkflow("versions", func(*workflow.Workflow, struct{}) error { return nil })
		def.Version = version
		workflow.RegisterDefinition(r, def)
	}
	got := r.Versions("versions")
	if !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatalf("versions = %v", got)
	}
	got[0] = 999
	if !reflect.DeepEqual(r.Versions("versions"), []int{1, 2, 3}) {
		t.Fatal("caller changed registry")
	}
	if got := r.Versions("missing"); len(got) != 0 {
		t.Fatalf("unknown workflow = %v", got)
	}
}
