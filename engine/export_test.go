package engine

import (
	"context"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/resource"
)

// InputSizesForTest exposes inputSizes to the external test package.
func InputSizesForTest(b map[string]artifact.Ref) ([]resource.InputSize, int64, string) {
	return inputSizes(b)
}

// CheckReaperMarginForTest exposes checkReaperMargin to the external test
// package.
func CheckReaperMarginForTest(cfg dispatch.Config, leaseAware bool) error {
	return checkReaperMargin(cfg, leaseAware)
}

// StartHeartbeatForTest runs the row heartbeat's start step on its own, so
// a test can order it after Stop without running the rest of Start.
func StartHeartbeatForTest(ctx context.Context, eng *Engine) {
	eng.startHeartbeat(ctx)
}
