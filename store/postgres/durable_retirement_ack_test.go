package postgres_test

import (
	"testing"

	"github.com/xraph/dispatch/durable/durabletest"
)

func TestRetirementCancellationAcknowledgment(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunRetirementCancellationAcknowledgment(t, s)
}
