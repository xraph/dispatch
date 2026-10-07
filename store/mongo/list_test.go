package mongo_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/storetest"
)

// TestListSuite runs the paged list suite against MongoDB.
//
// One container and one store serve every subtest, as in
// TestDequeueConformance: each case isolates itself with a queue, name
// prefix or scope nobody else uses. Migrate runs first so the pages are
// read with the indexes production has.
func TestListSuite(t *testing.T) {
	uri := startMongo(t)
	shared := openStore(t, uri)

	if err := shared.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	storetest.RunListSuite(t, func(t *testing.T) storetest.ListStore {
		t.Helper()

		return shared
	})
}

// TestListJobsNamePrefixIsNotARegex covers what the shared suite cannot:
// "%" and "_" are ordinary characters to a regex, so the suite's prefix
// case passes even if the prefix reaches Mongo's $regex unescaped. A "."
// would then match any character, and an unbalanced "(" would fail the
// whole query instead of matching nothing.
func TestListJobsNamePrefixIsNotARegex(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	ctx := context.Background()

	const queue = "regex-literal"

	dot := storetest.PendingJob("a.b-1", queue, 0)
	anyChar := storetest.PendingJob("axb-1", queue, 0)
	paren := storetest.PendingJob("a(b-1", queue, 0)

	for _, j := range []*job.Job{dot, anyChar, paren} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue %s: %v", j.Name, err)
		}
	}

	for _, tc := range []struct {
		prefix string
		want   *job.Job
	}{
		{"a.", dot},
		{"a(", paren},
	} {
		page, err := s.ListJobs(ctx, job.ListJobsOpts{Queue: queue, NamePrefix: tc.prefix})
		if err != nil {
			t.Fatalf("ListJobs(prefix %q): %v", tc.prefix, err)
		}

		got := make([]string, len(page.Jobs))
		for i, j := range page.Jobs {
			got[i] = j.Name
		}

		if !slices.Equal(got, []string{tc.want.Name}) {
			t.Errorf("ListJobs(prefix %q) = %v, want [%s]", tc.prefix, got, tc.want.Name)
		}
	}
}

// TestDLQReplayedFilterMatchesBothUnreplayedShapes pins the replayed_at
// filter against both ways a document can say "never replayed". PushDLQ
// goes through grove's insert, which writes an explicit null; the raw
// driver's encoder honours "omitempty" and leaves the key out. The suite
// only ever produces the first, so the second is made here with $unset.
func TestDLQReplayedFilterMatchesBothUnreplayedShapes(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	col := rawDatabase(t, uri).Collection("dispatch_dlq")
	ctx := context.Background()

	const queue = "replayed-shapes"

	now := time.Now().UTC().Truncate(time.Millisecond)
	entry := func(name string) *dlq.Entry {
		return &dlq.Entry{
			ID:        id.NewDLQID(),
			JobID:     id.NewJobID(),
			JobName:   name,
			Queue:     queue,
			Payload:   []byte(`{}`),
			Error:     "boom",
			FailedAt:  now,
			CreatedAt: now,
		}
	}

	nullKey, absentKey, replayed := entry("null"), entry("absent"), entry("replayed")
	for _, e := range []*dlq.Entry{nullKey, absentKey, replayed} {
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatalf("push %s: %v", e.JobName, err)
		}
	}

	if err := s.ReplayDLQ(ctx, replayed.ID); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if _, err := col.UpdateOne(ctx,
		bson.M{"_id": absentKey.ID.String()},
		bson.M{"$unset": bson.M{"replayed_at": ""}},
	); err != nil {
		t.Fatalf("unset replayed_at: %v", err)
	}

	// The two shapes really are different, or the rest proves nothing.
	var nullDoc, absentDoc bson.M
	if err := col.FindOne(ctx, bson.M{"_id": nullKey.ID.String()}).Decode(&nullDoc); err != nil {
		t.Fatalf("read null doc: %v", err)
	}
	if v, ok := nullDoc["replayed_at"]; !ok || v != nil {
		t.Fatalf("pushed replayed_at = %#v (present=%t), want present and null", v, ok)
	}
	if err := col.FindOne(ctx, bson.M{"_id": absentKey.ID.String()}).Decode(&absentDoc); err != nil {
		t.Fatalf("read absent doc: %v", err)
	}
	if v, ok := absentDoc["replayed_at"]; ok {
		t.Fatalf("unset replayed_at = %#v, want key absent", v)
	}

	yes, no := true, false
	for _, tc := range []struct {
		what     string
		replayed *bool
		want     []string
	}{
		{"unreplayed", &no, []string{absentKey.ID.String(), nullKey.ID.String()}},
		{"replayed", &yes, []string{replayed.ID.String()}},
	} {
		page, err := s.ListDLQPage(ctx, dlq.PageOpts{Queue: queue, Replayed: tc.replayed})
		if err != nil {
			t.Fatalf("ListDLQPage %s: %v", tc.what, err)
		}

		got := make([]string, len(page.Entries))
		for i, e := range page.Entries {
			got[i] = e.ID.String()
		}

		if !slices.Equal(got, tc.want) {
			t.Errorf("ListDLQPage %s:\n got  %v\n want %v", tc.what, got, tc.want)
		}

		n, err := s.CountDLQEntries(ctx, dlq.CountOpts{Queue: queue, Replayed: tc.replayed})
		if err != nil {
			t.Fatalf("CountDLQEntries %s: %v", tc.what, err)
		}

		if n != int64(len(tc.want)) {
			t.Errorf("CountDLQEntries %s = %d, want %d", tc.what, n, len(tc.want))
		}
	}
}
