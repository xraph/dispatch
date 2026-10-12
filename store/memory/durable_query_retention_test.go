package memory

import (
	"testing"

	"github.com/xraph/dispatch/durable/durabletest"
)

func TestQueryRuntimeRetention(t *testing.T) { durabletest.RunQueryRuntimeRetention(t, New()) }

func TestQueryEmptyRetiredLastBinding(t *testing.T) {
	durabletest.RunQueryEmptyRetiredLastBinding(t, New())
}
