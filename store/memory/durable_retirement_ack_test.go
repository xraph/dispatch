package memory

import (
	"testing"

	"github.com/xraph/dispatch/durable/durabletest"
)

func TestRetirementCancellationAcknowledgment(t *testing.T) {
	durabletest.RunRetirementCancellationAcknowledgment(t, New())
}
