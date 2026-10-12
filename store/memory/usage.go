package memory

import (
	"context"
	"sort"
	"time"

	"github.com/xraph/dispatch/job"
)

// RecordJobUsage persists one attempt's measurements.
func (s *Store) RecordJobUsage(_ context.Context, u *job.Usage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec := *u
	s.jobUsage = append(s.jobUsage, &rec)

	return nil
}

// ListJobUsage returns recorded attempts, most recent first.
func (s *Store) ListJobUsage(_ context.Context, opts job.UsageListOpts) ([]*job.Usage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*job.Usage, 0, len(s.jobUsage))

	for _, u := range s.jobUsage {
		if opts.Name != "" && u.Name != opts.Name {
			continue
		}

		if !opts.Since.IsZero() && u.RecordedAt.Before(opts.Since) {
			continue
		}

		rec := *u
		out = append(out, &rec)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].RecordedAt.Equal(out[j].RecordedAt) {
			return out[i].ID.String() > out[j].ID.String()
		}

		return out[i].RecordedAt.After(out[j].RecordedAt)
	})

	if opts.Offset > 0 {
		if opts.Offset >= len(out) {
			return nil, nil
		}

		out = out[opts.Offset:]
	}

	if opts.Limit > 0 && opts.Limit < len(out) {
		out = out[:opts.Limit]
	}

	return out, nil
}

// PurgeJobUsage deletes records older than before, up to limit rows.
func (s *Store) PurgeJobUsage(_ context.Context, before time.Time, limit int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	kept := s.jobUsage[:0]

	var removed int64

	for _, u := range s.jobUsage {
		if u.RecordedAt.Before(before) && (limit <= 0 || removed < int64(limit)) {
			removed++

			continue
		}

		kept = append(kept, u)
	}

	s.jobUsage = kept

	return removed, nil
}
