package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrBudgetExceeded means the cache could not free enough space for a
// stage request.
//
// It is returned both when a single artifact is larger than the whole
// budget — which can never succeed and so fails immediately — and when
// every cached entry is currently leased and the caller's deadline
// elapsed while waiting for one to be released.
var ErrBudgetExceeded = errors.New("dispatch/artifact/cache: budget exceeded")

// evictor frees space on the budget's behalf. It returns the number of
// bytes reclaimed, or zero when nothing is evictable.
type evictor func() int64

// budget accounts for the bytes the cache holds on disk.
//
// Acquire blocks until the requested space is available, evicting
// unleased entries as needed. This is what makes a job needing more
// staging space than is free wait rather than exhaust the volume.
type budget struct {
	mu    sync.Mutex
	cond  *sync.Cond
	limit int64
	used  int64
	evict evictor
}

func newBudget(limit int64) *budget {
	b := &budget{limit: limit}
	b.cond = sync.NewCond(&b.mu)

	return b
}

// setEvictor installs the eviction callback. It is separate from
// construction because the evictor needs the cache, which needs the
// budget.
func (b *budget) setEvictor(e evictor) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.evict = e
}

// Limit returns the configured budget in bytes.
func (b *budget) Limit() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.limit
}

// Used returns the bytes currently accounted for.
func (b *budget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.used
}

// Acquire reserves n bytes, evicting and then waiting as needed.
//
// A request larger than the entire budget fails immediately rather than
// waiting: no amount of eviction can satisfy it, so blocking would only
// delay an inevitable error until the caller's deadline.
func (b *budget) Acquire(ctx context.Context, n int64) error {
	if n <= 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if n > b.limit {
		return fmt.Errorf("%w: %d bytes exceeds the %d byte cache budget", ErrBudgetExceeded, n, b.limit)
	}

	// Wake the waiter when the caller's context ends, so a blocked stage
	// cannot outlive its job.
	stop := b.watchContext(ctx)
	defer stop()

	for b.used+n > b.limit {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: waiting for %d bytes: %w", ErrBudgetExceeded, n, err)
		}

		if b.evict != nil {
			if freed := b.evict(); freed > 0 {
				// The evictor only removes the file and forgets the
				// entry; the budget owns its own accounting, so it
				// subtracts here rather than letting the callback reach
				// into these fields while this mutex is held.
				b.used -= freed
				if b.used < 0 {
					b.used = 0
				}

				continue
			}
		}

		// Nothing evictable. Every entry is leased, so only a release can
		// help — wait for one, or for the context to end.
		b.cond.Wait()
	}

	b.used += n

	return nil
}

// watchContext broadcasts on the condition when ctx ends, so Acquire's
// wait is interruptible. The returned stop function tears the watcher
// down.
func (b *budget) watchContext(ctx context.Context) func() {
	if ctx.Done() == nil {
		return func() {}
	}

	done := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		case <-done:
		}
	}()

	return func() { close(done) }
}

// Release returns n bytes to the budget and wakes any waiter.
func (b *budget) Release(n int64) {
	if n <= 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.used -= n
	if b.used < 0 {
		b.used = 0
	}

	b.cond.Broadcast()
}

// Adjust corrects the accounting when an object turned out to be a
// different size than reserved, which happens whenever a ref carried no
// size and the cache reserved optimistically.
func (b *budget) Adjust(reserved, actual int64) {
	delta := actual - reserved
	if delta == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.used += delta
	if b.used < 0 {
		b.used = 0
	}

	b.cond.Broadcast()
}

// Reset clears the accounting, used after the cache is purged.
func (b *budget) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.used = 0
	b.cond.Broadcast()
}

// Wake broadcasts to any waiter, used after a release frees an entry.
func (b *budget) Wake() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.cond.Broadcast()
}
