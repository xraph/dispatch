package exec_test

import (
	"go/build"
	"strings"
	"testing"
)

// TestExecIsALeafPackage guards the import constraint the whole design
// rests on. job imports exec for Options.Execution, so exec importing job
// would be a cycle; importing worker or engine would drag the store, and
// with it the credentials, into a package the sandbox links.
func TestExecIsALeafPackage(t *testing.T) {
	const self = "github.com/xraph/dispatch/exec"

	allowed := map[string]bool{
		"github.com/xraph/dispatch":          true,
		"github.com/xraph/dispatch/id":       true,
		"github.com/xraph/dispatch/scope":    true,
		"github.com/xraph/dispatch/artifact": true,
		// Request.ResourceLimits carries job.Job.ResourceLimits across the
		// execution boundary so exec/subprocess can enforce it per job.
		// resource is a leaf like id and artifact: it imports neither job
		// nor exec, so this adds no cycle.
		"github.com/xraph/dispatch/resource": true,
	}

	pkg, err := build.Import(self, "", 0)
	if err != nil {
		t.Fatalf("import %s: %v", self, err)
	}

	for _, imp := range pkg.Imports {
		if !strings.HasPrefix(imp, "github.com/xraph/dispatch") {
			continue // standard library and third-party are fine
		}
		if !allowed[imp] {
			t.Errorf("exec imports %q, which breaks the leaf constraint", imp)
		}
	}
}
