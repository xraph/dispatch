package artifact

import (
	"context"
	"io"
	"time"
)

// Backend is the pluggable object-storage contract behind an artifact.
// Dispatch ships an adapter for Trove; any store can implement this.
type Backend interface {
	// Name returns the backend's identifier, recorded in Artifact.Backend.
	Name() string

	// Open returns a reader over the object's bytes. It returns
	// ErrNotFound if the object does not exist.
	Open(ctx context.Context, ref Ref) (io.ReadCloser, error)

	// Create begins writing a new object. The bytes are not visible until
	// Commit. Callers must call Commit or Abort.
	Create(ctx context.Context, bucket, key string) (Writer, error)

	// Stat reports the object's size and content type without reading it.
	// It returns ErrNotFound if the object does not exist.
	Stat(ctx context.Context, ref Ref) (ObjectInfo, error)

	// Delete removes the object. Deleting a missing object is not an error.
	Delete(ctx context.Context, ref Ref) error
}

// Writer accumulates bytes for a new object.
//
// Commit reports the logical size of the bytes written, which may differ
// from what the backend stored — compression and encryption middleware
// change the stored form, and the artifact row records what the handler
// produced.
//
// Abort after a successful Commit is a no-op, so `defer w.Abort()` is the
// correct idiom.
type Writer interface {
	io.Writer

	// Commit finalises the object and returns its logical info.
	Commit(ctx context.Context) (ObjectInfo, error)

	// Abort discards the partial object. It is a no-op after Commit.
	Abort() error
}

// RangeReader is an optional Backend capability for partial reads.
type RangeReader interface {
	// OpenRange returns a reader over n bytes starting at off. A negative
	// n reads to the end.
	OpenRange(ctx context.Context, ref Ref, off, n int64) (io.ReadCloser, error)
}

// Presigner is an optional Backend capability for direct client access.
//
// It is what lets a remote worker fetch a large object straight from
// object storage instead of streaming it through the coordinator, which
// would otherwise make the coordinator a bandwidth bottleneck.
type Presigner interface {
	// PresignGet returns a time-limited URL granting read access.
	PresignGet(ctx context.Context, ref Ref, ttl time.Duration) (string, error)
}
