package engine

import (
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/resource"
)

// InputSizesForTest exposes inputSizes to the external test package.
func InputSizesForTest(b map[string]artifact.Ref) ([]resource.InputSize, int64, string) {
	return inputSizes(b)
}
