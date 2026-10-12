package store_test

import (
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/store/mongo"
	"github.com/xraph/dispatch/store/postgres"
	"github.com/xraph/dispatch/store/redis"
	"github.com/xraph/dispatch/store/sqlite"
)

// Every backend must satisfy the whole aggregate, including the paged
// reads the dashboard depends on. Before this file, nothing checked that
// a backend implemented store.Store as one unit; each asserted the
// subsystem interfaces separately, so a capability added to store.Store
// could be missing from one backend until something called it.
var (
	_ store.Store = (*memory.Store)(nil)
	_ store.Store = (*postgres.Store)(nil)
	_ store.Store = (*sqlite.Store)(nil)
	_ store.Store = (*mongo.Store)(nil)
	_ store.Store = (*redis.Store)(nil)
)

// requirePagedReads fails to compile until store.Store carries the paged
// reads: a method value on an interface only exists if the method does.
func requirePagedReads(s store.Store) {
	_ = s.ListJobs
	_ = s.ListRunsPage
	_ = s.CountRuns
	_ = s.ListDLQPage
	_ = s.CountDLQEntries
	_ = s.ListArtifactsPage
	_ = s.GetWorker
}

var _ = requirePagedReads
