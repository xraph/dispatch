package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// The conditional writes on JSON entities (the replay claim and its
// release, the cron lock and the cron field writes, the run reopen) all go
// through updateEntity, an optimistic compare-and-set on the whole stored
// blob.
//
// Go does everything except the final check: it reads the raw blob,
// decodes it, decides (the claim is already taken, the run is already
// running), mutates the decoded copy and marshals it with encoding/json.
// casSetScript then writes the new blob only if the key still holds
// exactly the bytes Go read. If anything else wrote the key in between,
// the script refuses and Go starts again from a fresh read, so the
// decision is always made on the value that gets replaced.
//
// Two other shapes were considered and rejected.
//
// Decoding, patching and cjson.encode-ing the entity inside Lua is out for
// the reason lease.go records: cjson renders an integer past 2^53 in
// scientific notation, which encoding/json then cannot read back into an
// int64. dlqEntity carries Timeout, LeaseTTL, InputBytes and resource
// sets, all int64, so a claim would corrupt a long-timeout dead letter.
// TestClaimReplayKeepsLargeDurations pins that.
//
// Checking only the guard field in Lua (replayed_at is null, state is not
// running) before SETting Go's blob is the lease.go shape, and it leaves a
// window: a write to any other field between Go's read and the script is
// silently undone. That window is the bug for cron. AcquireCronLock,
// UpdateCronLastRun, ReleaseCronLock and SetCronEnabled all rewrite the
// same blob, so a scheduler write that read the entry before an operator
// disabled it would put enabled back. Comparing the whole blob closes it:
// no write here can land on a value it did not read.
//
// The cost of comparing the whole blob is a retry when an unrelated field
// changed, and that is cheap here, unlike for lease renewal: none of these
// writes has a deadline, and every retry means some other write landed,
// so the system as a whole always makes progress.

// casSetScript replaces a key's value only if it still holds the value
// the caller read.
//
// KEYS[1] the entity key. ARGV[1] the raw blob the caller read, ARGV[2]
// the blob to write in its place.
// Returns casWritten, casMissing when the key no longer exists, or
// casChanged when it holds something else.
var casSetScript = goredis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then
  return 0
end
if cur ~= ARGV[1] then
  return 2
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
`)

const (
	casMissing = 0
	casWritten = 1
	casChanged = 2
)

// casAttempts bounds how many times updateEntity re-reads a key that keeps
// changing under it. Each lost attempt means another write landed, so
// running out takes that many writes to one entity inside one call, which
// no caller in this store comes near.
const casAttempts = 64

// errSkipWrite is what a mutate function returns when the entity already
// says what the caller wanted, or the write does not apply to it. The
// update then returns nil without writing.
var errSkipWrite = errors.New("dispatch/redis: nothing to write")

// updateEntity applies mutate to the entity stored at key as one atomic
// compare-and-set, retrying from a fresh read when another write lands
// between the read and the set. notFound is returned when the key does
// not exist, whether before the first read or by the time the set runs.
// Any error from mutate other than errSkipWrite is returned as is, so a
// refusal can be a sentinel the caller passes straight up.
func updateEntity[T any](ctx context.Context, s *Store, key string, notFound error, mutate func(e *T) error) error {
	for range casAttempts {
		raw, err := s.rdb.Get(ctx, key).Bytes()
		if errors.Is(err, goredis.Nil) {
			return notFound
		}
		if err != nil {
			return fmt.Errorf("dispatch/redis: read %s: %w", key, err)
		}

		var e T
		if decodeErr := json.Unmarshal(raw, &e); decodeErr != nil {
			return fmt.Errorf("dispatch/redis: decode %s: %w", key, decodeErr)
		}

		if mutateErr := mutate(&e); mutateErr != nil {
			if errors.Is(mutateErr, errSkipWrite) {
				return nil
			}
			return mutateErr
		}

		next, marshalErr := json.Marshal(&e)
		if marshalErr != nil {
			return fmt.Errorf("dispatch/redis: marshal %s: %w", key, marshalErr)
		}

		res, runErr := casSetScript.Run(ctx, s.rdb, []string{key}, raw, next).Int64()
		if runErr != nil {
			return fmt.Errorf("dispatch/redis: write %s: %w", key, runErr)
		}
		switch res {
		case casWritten:
			return nil
		case casMissing:
			return notFound
		}

		// casChanged: another write landed after the read. Decide again on
		// what is there now, unless the caller has given up.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}

	return fmt.Errorf("dispatch/redis: %s changed under %d consecutive attempts to update it", key, casAttempts)
}
