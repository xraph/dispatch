//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

// TestListConformance runs the paged list suite against Postgres.
//
// One container serves every case, the way TestDequeueConformance shares
// one: the suite isolates each case by a queue, name prefix or scope
// nobody else uses and asserts only on the rows it created.
func TestListConformance(t *testing.T) {
	shared := setupTestStore(t)

	storetest.RunListSuite(t, func(t *testing.T) storetest.ListStore {
		t.Helper()

		return shared
	})
}

// TestListJobsOrdersByIDUnderDefaultCollation pins the assumption every
// paged list rests on: that ORDER BY id DESC on a TEXT column, under the
// database's default collation, is the same order as Go's byte comparison
// of the IDs. Dispatch IDs only use [0-9a-z_], and a linguistic collation
// is free to weigh '_' or digits differently from their bytes, so this is
// a property of the server, not of our code.
//
// Fifty IDs minted in a tight loop share milliseconds, so their order
// within a millisecond comes only from the monotonic counter in the low
// bits. They are inserted in a scrambled order so that neither insertion
// order nor heap order can produce the expected result by accident.
func TestListJobsOrdersByIDUnderDefaultCollation(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	var collation string
	if err := pgdriver.Unwrap(s.DB()).NewRaw(
		`SELECT datcollate FROM pg_database WHERE datname = current_database()`,
	).Scan(ctx, &collation); err != nil {
		t.Fatalf("read database collation: %v", err)
	}
	t.Logf("database collation: %s", collation)

	const n = 50
	queue := "collation-" + id.NewJobID().String()

	jobs := make([]*job.Job, n)
	for i := range jobs {
		jobs[i] = storetest.PendingJob(fmt.Sprintf("collation-%02d", i), queue, 0)
	}

	minted := make([]string, n)
	for i, j := range jobs {
		minted[i] = j.ID.String()
	}
	if !slices.IsSorted(minted) {
		t.Fatalf("IDs minted in a loop are not ascending in Go byte order: %v", minted)
	}

	// 7 is coprime with 50, so k*7 mod 50 visits every index once, in an
	// order that is neither minting order nor its reverse.
	for k := range n {
		j := jobs[(k*7)%n]
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: queue, Limit: 100})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}

	got := make([]string, len(page.Jobs))
	for i, j := range page.Jobs {
		got[i] = j.ID.String()
	}

	want := slices.Clone(minted)
	slices.Reverse(want)

	if !slices.Equal(got, want) {
		t.Fatalf("ORDER BY id DESC under %q disagrees with Go byte order:\n got  %s\n want %s",
			collation, strings.Join(got, " "), strings.Join(want, " "))
	}
	if page.NextCursor != "" || !page.Complete {
		t.Fatalf("page = next %q, complete %v; want one complete page", page.NextCursor, page.Complete)
	}
}
