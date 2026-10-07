package redis

import (
	"context"
	"fmt"

	"github.com/xraph/grove/kv/driver"

	"github.com/xraph/dispatch/id"
)

// The entity kinds that have a created-order index. Each is the stem of
// its byCreated key.
const (
	entityJob      = "job"
	entityRun      = "run"
	entityDLQ      = "dlq"
	entityArtifact = "artifact"
)

// createdScore is an ID's score in a created-order index: the Unix
// millisecond minted into its UUIDv7. A millisecond count is far below
// 2^53, so the float64 holds it exactly. Redis orders members that share
// a score by their bytes, and IDs minted in the same millisecond sort by
// their monotonic counter, so a reverse range over the index is exactly
// ID-descending, the order every paged list returns.
func createdScore(i id.ID) float64 {
	return float64(i.Time().UnixMilli())
}

// indexCreated adds member to entity's created-order index.
//
// The create paths that write one key at a time call this BEFORE they
// write the entity. A crash between the two then leaves an index member
// with no entity, which the list reads skip. The other order would leave
// a row the lists cannot see until a later list call notices its ID set
// has outgrown the index and backfills. The create paths that already
// write their indexes in one MULTI (job enqueue) add the member inside it
// instead.
func (s *Store) indexCreated(ctx context.Context, entity string, member id.ID) error {
	_, err := s.kv.ZAdd(ctx, s.keys.byCreated(entity), driver.ScoredMember{
		Member: member.String(),
		Score:  createdScore(member),
	})

	return err
}

// backfillChunk bounds how many members one ZADD carries while an index
// is built from its ID set, so a large set does not become one huge
// command.
const backfillChunk = 500

// ensureBackfilled puts every member of the ID set at idsKey into
// entity's created-order index when the set has outgrown it. Every list
// call runs it before reading the index; the check is two O(1) counts.
//
// On this release every create writes its index member before, or in the
// same MULTI as, its ID-set member, so the set holds more members than
// the index only when something wrote the set alone. That is every row
// from a release before the index existed, and every row a process still
// on that release writes during a rolling upgrade. Either way this ZADDs
// the whole set with its scores. ZADD of a member already present with
// the same score changes nothing, so a backfill racing another backfill
// or a create is harmless.
//
// The index can also hold more members than the set, and that is no
// reason to backfill. A create writes its member before its entity, and a
// delete racing a backfill can leave a member whose entity is gone; the
// reads skip both. The one gap is a rolling upgrade in which a previous
// release process deletes rows (SREM without ZREM) and also adds them:
// each stale member it leaves can offset one row it adds, and that row
// stays out of the lists until the counts next differ. Once every process
// runs this release, nothing writes the set without the index.
func (s *Store) ensureBackfilled(ctx context.Context, entity, idsKey string) error {
	index := s.keys.byCreated(entity)

	inSet, err := s.kv.SCard(ctx, idsKey)
	if err != nil {
		return fmt.Errorf("dispatch/redis: count %s ids: %w", entity, err)
	}

	inIndex, err := s.kv.ZCard(ctx, index)
	if err != nil {
		return fmt.Errorf("dispatch/redis: count %s index: %w", entity, err)
	}

	if inSet <= inIndex {
		return nil
	}

	members, err := s.kv.SMembers(ctx, idsKey)
	if err != nil {
		return fmt.Errorf("dispatch/redis: read %s ids for backfill: %w", entity, err)
	}

	batch := make([]driver.ScoredMember, 0, min(len(members), backfillChunk))
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, zErr := s.kv.ZAdd(ctx, index, batch...); zErr != nil {
			return fmt.Errorf("dispatch/redis: backfill %s index: %w", entity, zErr)
		}
		batch = batch[:0]

		return nil
	}

	for _, m := range members {
		parsed, pErr := id.Parse(m)
		if pErr != nil {
			// Only IDs are ever written to the ID sets. Something else has
			// no creation time to sort by, so it stays out of the index,
			// and a set holding one makes every list call backfill again.
			continue
		}
		batch = append(batch, driver.ScoredMember{Member: m, Score: createdScore(parsed)})
		if len(batch) == backfillChunk {
			if fErr := flush(); fErr != nil {
				return fErr
			}
		}
	}

	return flush()
}
