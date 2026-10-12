package memory_test

import (
	"testing"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/store/storetest"
)

func TestClusterSuite(t *testing.T) {
	storetest.RunClusterSuite(t, func(_ *testing.T) cluster.Store {
		return memory.New()
	})
}
