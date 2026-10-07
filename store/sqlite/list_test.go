package sqlite_test

import (
	"context"
	"testing"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

// TestListConformance runs the paged list contract against SQLite. Every
// subtest gets its own migrated database from openSqliteStore
// (store/sqlite/reap_test.go:19).
func TestListConformance(t *testing.T) {
	storetest.RunListSuite(t, func(t *testing.T) storetest.ListStore {
		t.Helper()

		return openSqliteStore(t)
	})
}

// TestListJobsNamePrefixCountsCharacters pins the SQLite-only half of the
// name prefix: substr counts characters on TEXT, so the length it is given
// must be the prefix's rune count. "é-" is two characters and three bytes;
// with len() the predicate would compare "é-1" to "é-" and match nothing.
func TestListJobsNamePrefixCountsCharacters(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()

	hit := storetest.PendingJob("é-1", "multibyte", 0)
	miss := storetest.PendingJob("éa-1", "multibyte", 0)
	for _, j := range []*job.Job{hit, miss} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: "multibyte", NamePrefix: "é-"})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].ID != hit.ID {
		got := make([]string, len(page.Jobs))
		for i, j := range page.Jobs {
			got[i] = j.Name
		}
		t.Fatalf(`prefix "é-" matched %q, want only "é-1"`, got)
	}
}
