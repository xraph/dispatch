package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/kv/driver"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/paging"
	"github.com/xraph/dispatch/workflow"
)

var (
	_ job.Lister          = (*Store)(nil)
	_ workflow.PageLister = (*Store)(nil)
	_ dlq.PageLister      = (*Store)(nil)
	_ artifact.PageLister = (*Store)(nil)
)

const (
	// listScanBudget caps how many index members one paged list call
	// examines. Redis cannot filter inside the index, so a filter that
	// matches rarely would otherwise read every row the tenant has before
	// returning an empty page. At the budget the call returns what it
	// found with Complete false and a cursor to continue from, which the
	// dashboard shows as "more may match". 5000 is about 25 windows,
	// enough that an ordinary filter fills its page long before it.
	listScanBudget = 5000

	// listWindow is how many members one range read takes from an index,
	// and so how many entities one pipelined read fetches.
	listWindow = 200
)

// indexScan describes one entity kind to its page method: which index to
// walk, which ID set backfills it, where each member's entity lives, how
// to decode it and which rows to keep.
type indexScan[T any] struct {
	entity string
	ids    string
	keyOf  func(member string) string
	decode func(raw []byte) (T, error)
	match  func(T) bool
}

// scanResult is one page of an indexScan, in the shape every Page type
// shares.
type scanResult[T any] struct {
	rows     []T
	next     string
	complete bool
}

// page walks the created-order index newest first, starting just below
// cursor, and collects up to limit rows that match.
//
// It reads the index listWindow members at a time, fetches each window's
// entities in one pipelined read, and stops at the first of these:
//
//   - limit+1 matches. The page is full and the extra match proves a next
//     page exists, so NextCursor is the last row returned.
//   - the end of the index. The page is complete and has no next page.
//   - s.scanBudget members examined. The page is incomplete and
//     NextCursor is the last member examined, so the next call carries on
//     exactly where this one stopped. If the budget runs out on the last
//     member of the index, that next call returns an empty complete page.
//
// Members whose entity is gone are skipped and still count against the
// budget. They are not removed here: a create writes its member before
// its entity, so a member with no entity may be a row about to exist.
func (sc indexScan[T]) page(ctx context.Context, s *Store, cursor id.ID, limit int) (scanResult[T], error) {
	if err := s.ensureBackfilled(ctx, sc.entity, sc.ids); err != nil {
		return scanResult[T]{}, err
	}

	limit = paging.Limit(limit)
	budget := max(s.scanBudget, 1)
	index := s.keys.byCreated(sc.entity)

	// Scores are milliseconds and members are IDs, so "below the cursor"
	// is a score at or below the cursor's, minus the members at that same
	// score which sort at or above it.
	spec := driver.RangeSpec{Reverse: true}
	after := ""
	if !cursor.IsNil() {
		spec.Max, spec.HasMax, after = createdScore(cursor), true, cursor.String()
	}

	out := scanResult[T]{rows: []T{}}
	matched := make([]string, 0, limit)
	examined := 0
	lastSeen := ""

	for examined < budget {
		spec.Count = int64(min(listWindow, budget-examined))

		window, err := s.kv.ZRangeWithScores(ctx, index, spec)
		if err != nil {
			return scanResult[T]{}, fmt.Errorf("dispatch/redis: range %s index: %w", sc.entity, err)
		}
		exhausted := int64(len(window)) < spec.Count

		// A range bounded by the last score read returns the members
		// already passed at that score first. Drop them.
		fresh := window
		for len(fresh) > 0 && spec.HasMax && fresh[0].Score == spec.Max && fresh[0].Member >= after {
			fresh = fresh[1:]
		}

		if len(fresh) == 0 {
			if exhausted {
				out.complete = true

				return out, nil
			}

			// A whole window already passed means more than a window of
			// IDs share this millisecond. Step over them.
			spec.Offset += int64(len(window))

			continue
		}
		spec.Offset = 0

		keys := make([]string, len(fresh))
		for i, m := range fresh {
			keys[i] = sc.keyOf(m.Member)
		}

		raws, err := s.kv.MGetRaw(ctx, keys)
		if err != nil {
			return scanResult[T]{}, fmt.Errorf("dispatch/redis: read %s window: %w", sc.entity, err)
		}

		for i, m := range fresh {
			examined++
			lastSeen = m.Member

			if raws[i] == nil {
				continue
			}

			row, dErr := sc.decode(raws[i])
			if dErr != nil {
				return scanResult[T]{}, fmt.Errorf("dispatch/redis: decode %s %s: %w", sc.entity, m.Member, dErr)
			}
			if !sc.match(row) {
				continue
			}

			if len(out.rows) == limit {
				out.next, out.complete = matched[limit-1], true

				return out, nil
			}
			out.rows = append(out.rows, row)
			matched = append(matched, m.Member)
		}

		if exhausted {
			out.complete = true

			return out, nil
		}

		last := fresh[len(fresh)-1]
		spec.Max, spec.HasMax, after = last.Score, true, last.Member
	}

	out.next = lastSeen

	return out, nil
}

// countMatching decodes every entity in the ID set at idsKey and counts
// the ones match keeps. Counts are not paged, so this is O(n) in the set
// on Redis, the same as CountJobs.
func countMatching[T any](
	ctx context.Context,
	s *Store,
	idsKey string,
	keyOf func(member string) string,
	decode func(raw []byte) (T, error),
	match func(T) bool,
) (int64, error) {
	members, err := s.kv.SMembers(ctx, idsKey)
	if err != nil {
		return 0, fmt.Errorf("dispatch/redis: count members: %w", err)
	}

	var n int64
	for start := 0; start < len(members); start += listWindow {
		chunk := members[start:min(start+listWindow, len(members))]

		keys := make([]string, len(chunk))
		for i, m := range chunk {
			keys[i] = keyOf(m)
		}

		raws, err := s.kv.MGetRaw(ctx, keys)
		if err != nil {
			return 0, fmt.Errorf("dispatch/redis: count read: %w", err)
		}

		for i, raw := range raws {
			if raw == nil {
				continue
			}

			row, dErr := decode(raw)
			if dErr != nil {
				return 0, fmt.Errorf("dispatch/redis: count decode %s: %w", chunk[i], dErr)
			}
			if match(row) {
				n++
			}
		}
	}

	return n, nil
}

func decodeJob(raw []byte) (*job.Job, error) {
	var e jobEntity
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}

	return fromJobEntity(&e)
}

func decodeRun(raw []byte) (*workflow.Run, error) {
	var e runEntity
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}

	return fromRunEntity(&e)
}

func decodeDLQ(raw []byte) (*dlq.Entry, error) {
	var e dlqEntity
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}

	return fromDLQEntity(&e)
}

func decodeArtifact(raw []byte) (*artifact.Artifact, error) {
	var e artifactEntity
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}

	return fromArtifactEntity(&e)
}

// ListJobs returns jobs newest first by ID, filtered and paged. See
// indexScan.page for when a page comes back incomplete.
func (s *Store) ListJobs(ctx context.Context, opts job.ListJobsOpts) (job.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixJob)
	if err != nil {
		return job.Page{}, err
	}

	res, err := indexScan[*job.Job]{
		entity: entityJob,
		ids:    s.keys.jobIDs(),
		keyOf:  s.keys.job,
		decode: decodeJob,
		match:  opts.Match,
	}.page(ctx, s, cursor, opts.Limit)
	if err != nil {
		return job.Page{}, err
	}

	return job.Page{Jobs: res.rows, NextCursor: res.next, Complete: res.complete}, nil
}

// ListRunsPage returns workflow runs newest first by ID, filtered and
// paged.
func (s *Store) ListRunsPage(ctx context.Context, opts workflow.ListRunsPageOpts) (workflow.RunPage, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixRun)
	if err != nil {
		return workflow.RunPage{}, err
	}

	res, err := indexScan[*workflow.Run]{
		entity: entityRun,
		ids:    s.keys.runIDs(),
		keyOf:  s.keys.run,
		decode: decodeRun,
		match:  opts.Match,
	}.page(ctx, s, cursor, opts.Limit)
	if err != nil {
		return workflow.RunPage{}, err
	}

	return workflow.RunPage{Runs: res.rows, NextCursor: res.next, Complete: res.complete}, nil
}

// CountRuns counts workflow runs by state and exact name. It reads every
// run: O(n) on Redis, like CountJobs.
func (s *Store) CountRuns(ctx context.Context, opts workflow.CountRunsOpts) (int64, error) {
	return countMatching(ctx, s, s.keys.runIDs(), s.keys.run, decodeRun, opts.Match)
}

// ListDLQPage returns dead letter entries newest first by ID, filtered
// and paged.
func (s *Store) ListDLQPage(ctx context.Context, opts dlq.PageOpts) (dlq.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixDLQ)
	if err != nil {
		return dlq.Page{}, err
	}

	res, err := indexScan[*dlq.Entry]{
		entity: entityDLQ,
		ids:    s.keys.dlqIDs(),
		keyOf:  s.keys.dlq,
		decode: decodeDLQ,
		match:  opts.Match,
	}.page(ctx, s, cursor, opts.Limit)
	if err != nil {
		return dlq.Page{}, err
	}

	return dlq.Page{Entries: res.rows, NextCursor: res.next, Complete: res.complete}, nil
}

// CountDLQEntries counts dead letter entries under the given filters. It
// reads every entry: O(n) on Redis, like CountJobs.
func (s *Store) CountDLQEntries(ctx context.Context, opts dlq.CountOpts) (int64, error) {
	return countMatching(ctx, s, s.keys.dlqIDs(), s.keys.dlq, decodeDLQ, opts.Match)
}

// ListArtifactsPage returns artifacts newest first by ID, filtered and
// paged.
func (s *Store) ListArtifactsPage(ctx context.Context, opts artifact.PageOpts) (artifact.Page, error) {
	cursor, err := paging.Cursor(opts.Cursor, id.PrefixArtifact)
	if err != nil {
		return artifact.Page{}, err
	}

	res, err := indexScan[*artifact.Artifact]{
		entity: entityArtifact,
		ids:    s.keys.artifactIDs(),
		keyOf:  s.keys.artifact,
		decode: decodeArtifact,
		match:  opts.Match,
	}.page(ctx, s, cursor, opts.Limit)
	if err != nil {
		return artifact.Page{}, err
	}

	return artifact.Page{Artifacts: res.rows, NextCursor: res.next, Complete: res.complete}, nil
}
