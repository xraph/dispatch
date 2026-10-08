package memory_test

import (
	"testing"

	"github.com/xraph/dispatch/durable/durabletest"
	"github.com/xraph/dispatch/store/memory"
)

func TestDurable(t *testing.T) {
	durabletest.Run(t, memory.New())
}
