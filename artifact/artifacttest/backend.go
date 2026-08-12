package artifacttest

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xraph/dispatch/artifact"
)

// Backend is an in-memory artifact.Backend for tests. It counts calls so
// tests can assert on caching and single-flight behaviour.
type Backend struct {
	// DelayOpen makes Open sleep before returning, which is what lets a
	// single-flight test observe concurrent stagers colliding.
	DelayOpen time.Duration

	mu      sync.Mutex
	objects map[string][]byte

	opens   atomic.Int64
	creates atomic.Int64
	deletes atomic.Int64
	stats   atomic.Int64
}

// Compile-time check that the double satisfies the contract.
var _ artifact.Backend = (*Backend)(nil)

// NewBackend returns an empty in-memory backend.
func NewBackend() *Backend {
	return &Backend{objects: make(map[string][]byte)}
}

// Name identifies this backend.
func (b *Backend) Name() string { return "memory" }

// Opens returns how many times Open was called.
func (b *Backend) Opens() int64 { return b.opens.Load() }

// Creates returns how many times Create was called.
func (b *Backend) Creates() int64 { return b.creates.Load() }

// Deletes returns how many times Delete was called.
func (b *Backend) Deletes() int64 { return b.deletes.Load() }

// Stats returns how many times Stat was called.
func (b *Backend) Stats() int64 { return b.stats.Load() }

// Put seeds an object directly, bypassing the Writer path.
func (b *Backend) Put(bucket, key string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.objects[objectKey(bucket, key)] = append([]byte(nil), data...)
}

// Has reports whether an object exists.
func (b *Backend) Has(bucket, key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, ok := b.objects[objectKey(bucket, key)]

	return ok
}

func objectKey(bucket, key string) string { return bucket + "/" + key }

// Open returns a reader over the object's bytes.
func (b *Backend) Open(ctx context.Context, ref artifact.Ref) (io.ReadCloser, error) {
	b.opens.Add(1)

	if b.DelayOpen > 0 {
		select {
		case <-time.After(b.DelayOpen):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	b.mu.Lock()
	data, ok := b.objects[objectKey(ref.Bucket, ref.Key)]
	b.mu.Unlock()

	if !ok {
		return nil, artifact.ErrNotFound
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

// Stat reports the object's size.
func (b *Backend) Stat(_ context.Context, ref artifact.Ref) (artifact.ObjectInfo, error) {
	b.stats.Add(1)

	b.mu.Lock()
	data, ok := b.objects[objectKey(ref.Bucket, ref.Key)]
	b.mu.Unlock()

	if !ok {
		return artifact.ObjectInfo{}, artifact.ErrNotFound
	}

	return artifact.ObjectInfo{Size: int64(len(data))}, nil
}

// Delete removes an object. Deleting a missing object is not an error.
func (b *Backend) Delete(_ context.Context, ref artifact.Ref) error {
	b.deletes.Add(1)

	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.objects, objectKey(ref.Bucket, ref.Key))

	return nil
}

// Create begins writing a new object.
func (b *Backend) Create(_ context.Context, bucket, key string) (artifact.Writer, error) {
	b.creates.Add(1)

	return &memWriter{backend: b, bucket: bucket, key: key}, nil
}

// memWriter buffers writes and only publishes on Commit.
type memWriter struct {
	backend *Backend
	bucket  string
	key     string
	buf     bytes.Buffer
	done    bool
}

func (w *memWriter) Write(p []byte) (int, error) {
	if w.done {
		return 0, io.ErrClosedPipe
	}

	return w.buf.Write(p)
}

func (w *memWriter) Commit(_ context.Context) (artifact.ObjectInfo, error) {
	if w.done {
		return artifact.ObjectInfo{}, io.ErrClosedPipe
	}

	w.done = true

	size := int64(w.buf.Len())
	w.backend.Put(w.bucket, w.key, w.buf.Bytes())
	w.buf.Reset()

	return artifact.ObjectInfo{Size: size}, nil
}

// Abort discards the partial object. It is a no-op after Commit, so
// `defer w.Abort()` is safe alongside a successful commit.
func (w *memWriter) Abort() error {
	if w.done {
		return nil
	}

	w.done = true
	w.buf.Reset()

	return nil
}
