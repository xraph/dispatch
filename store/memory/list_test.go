package memory_test

import (
	"testing"

	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/store/storetest"
)

func TestListSuite(t *testing.T) {
	storetest.RunListSuite(t, func(_ *testing.T) storetest.ListStore {
		return memory.New()
	})
}
