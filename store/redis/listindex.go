package redis

import (
	"context"

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
