package trove

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	trovelib "github.com/xraph/trove"
	trovedriver "github.com/xraph/trove/driver"

	"github.com/xraph/dispatch/artifact"
)

// DefaultName is the backend identifier used when none is configured.
const DefaultName = "trove"

// Backend adapts a *trove.Trove as an artifact.Backend.
type Backend struct {
	trove *trovelib.Trove
	name  string
}

// Compile-time checks. RangeReader and Presigner are advertised
// unconditionally; whether the underlying driver actually supports them
// is resolved per call, because a Trove instance can be reconfigured and
// its multi-backend routing can send different keys to different drivers.
var (
	_ artifact.Backend     = (*Backend)(nil)
	_ artifact.RangeReader = (*Backend)(nil)
	_ artifact.Presigner   = (*Backend)(nil)
)

// Option configures the Backend.
type Option func(*Backend)

// WithName overrides the backend identifier recorded on each artifact.
// Use the Trove store's name when running multi-store so an artifact's
// backend field points at the store that actually holds it.
func WithName(name string) Option {
	return func(b *Backend) { b.name = name }
}

// New wraps a Trove instance as an artifact.Backend.
func New(t *trovelib.Trove, opts ...Option) *Backend {
	b := &Backend{trove: t, name: DefaultName}

	for _, opt := range opts {
		opt(b)
	}

	return b
}

// Name identifies this backend.
func (b *Backend) Name() string { return b.name }

// translate maps Trove's permanent failures onto the artifact plane's.
//
// This distinction is load-bearing. Callers use
// errors.Is(err, artifact.ErrPermanent) to tell a failure that can never
// succeed, which must fail the job immediately, from a transient backend
// failure, which should be retried with backoff. Getting it wrong means a
// deleted input burns every retry before reaching the DLQ.
//
// Only conditions Trove classifies are translated. Everything else,
// including a quota or rate limit, stays as it is and is retried: a quota
// may be granted later, and an error Trove has not classified is assumed
// transient, because dead-lettering a transient failure throws away work
// that would have succeeded.
//
// The underlying error is wrapped rather than replaced, so the driver's
// own message survives into the DLQ entry and errors.Is still reaches the
// Trove sentinel underneath for diagnostics.
func translate(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, trovelib.ErrNotFound):
		// ErrObjectNotFound and ErrBucketNotFound unwrap to ErrNotFound,
		// so this one case covers all three.
		return fmt.Errorf("%w: %w", artifact.ErrNotFound, err)
	case errors.Is(err, trovelib.ErrPermissionDenied):
		return fmt.Errorf("%w: %w", artifact.ErrPermissionDenied, err)
	default:
		return err
	}
}

// Open returns a reader over the object's bytes.
func (b *Backend) Open(ctx context.Context, ref artifact.Ref) (io.ReadCloser, error) {
	r, err := b.trove.Get(ctx, ref.Bucket, ref.Key)
	if err != nil {
		return nil, fmt.Errorf("trove: open %s/%s: %w", ref.Bucket, ref.Key, translate(err))
	}

	return r, nil
}

// ErrRangeUnsupported means the underlying Trove driver cannot serve
// byte ranges, so the caller must read the whole object instead.
var ErrRangeUnsupported = errors.New("trove: driver does not support range reads")

// OpenRange returns a reader over n bytes starting at off. A negative n
// reads to the end.
//
// Range support is a Trove driver capability, not a Get option: a driver
// that lacks it silently returns the entire object. Rather than hand back
// more bytes than asked for, this reports ErrRangeUnsupported so callers
// can fall back deliberately.
func (b *Backend) OpenRange(ctx context.Context, ref artifact.Ref, off, n int64) (io.ReadCloser, error) {
	rd, ok := b.trove.Driver().(trovedriver.RangeDriver)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrRangeUnsupported, b.trove.Driver())
	}

	r, err := rd.GetRange(ctx, ref.Bucket, ref.Key, off, n)
	if err != nil {
		return nil, fmt.Errorf("trove: open range [%d,%d) of %s/%s: %w",
			off, off+n, ref.Bucket, ref.Key, translate(err))
	}

	return r, nil
}

// Stat reports the object's size and content type.
func (b *Backend) Stat(ctx context.Context, ref artifact.Ref) (artifact.ObjectInfo, error) {
	info, err := b.trove.Head(ctx, ref.Bucket, ref.Key)
	if err != nil {
		return artifact.ObjectInfo{}, fmt.Errorf("trove: stat %s/%s: %w",
			ref.Bucket, ref.Key, translate(err))
	}

	return artifact.ObjectInfo{
		Size:        info.Size,
		ContentType: info.ContentType,
		ETag:        info.ETag,
	}, nil
}

// Delete removes the object. Deleting a missing object is not an error,
// which makes the purge pass idempotent under retry.
func (b *Backend) Delete(ctx context.Context, ref artifact.Ref) error {
	err := b.trove.Delete(ctx, ref.Bucket, ref.Key)
	if err == nil {
		return nil
	}

	terr := translate(err)
	if errors.Is(terr, artifact.ErrNotFound) {
		return nil
	}

	// Return the translated error, not the raw one: a delete refused for
	// permissions is permanent, and the sweeper would otherwise retry it
	// on every pass forever.
	return fmt.Errorf("trove: delete %s/%s: %w", ref.Bucket, ref.Key, terr)
}

// PresignGet returns a time-limited read URL when the underlying driver
// supports pre-signing, and ErrNotFound-free failure otherwise.
func (b *Backend) PresignGet(ctx context.Context, ref artifact.Ref, ttl time.Duration) (string, error) {
	p, ok := b.trove.Driver().(trovedriver.PresignDriver)
	if !ok {
		return "", fmt.Errorf("trove: driver %T does not support pre-signed URLs", b.trove.Driver())
	}

	url, err := p.PresignGet(ctx, ref.Bucket, ref.Key, ttl)
	if err != nil {
		return "", fmt.Errorf("trove: presign %s/%s: %w", ref.Bucket, ref.Key, translate(err))
	}

	return url, nil
}

// Create begins writing a new object.
//
// Trove's Put consumes a reader, so the writer drives it from a pipe on a
// background goroutine and joins that goroutine in Commit or Abort.
func (b *Backend) Create(ctx context.Context, bucket, key string) (artifact.Writer, error) {
	pr, pw := io.Pipe()

	w := &pipeWriter{
		pw:     pw,
		done:   make(chan struct{}),
		bucket: bucket,
		key:    key,
	}

	go func() {
		defer close(w.done)

		info, err := b.trove.Put(ctx, bucket, key, pr)

		w.info = info
		w.putErr = err

		// Unblock any writer still feeding the pipe after Put returned —
		// on error Put stops reading, and without this the next Write
		// would block forever.
		_ = pr.CloseWithError(err)
	}()

	return w, nil
}

// pipeWriter feeds Trove's Put from an io.Pipe.
//
// It counts the bytes the caller wrote so Commit reports the *logical*
// size. Trove's write middleware (compress, encrypt) changes the stored
// form, and the artifact row must record what the handler produced, not
// what landed on disk.
type pipeWriter struct {
	pw     *io.PipeWriter
	done   chan struct{}
	bucket string
	key    string

	written int64
	info    *trovedriver.ObjectInfo
	putErr  error

	once     sync.Once
	finished bool
}

// Write appends bytes to the pending object.
func (w *pipeWriter) Write(p []byte) (int, error) {
	if w.finished {
		return 0, io.ErrClosedPipe
	}

	n, err := w.pw.Write(p)
	w.written += int64(n)

	return n, err
}

// Commit closes the pipe, waits for Put, and reports the logical info.
func (w *pipeWriter) Commit(_ context.Context) (artifact.ObjectInfo, error) {
	if w.finished {
		return artifact.ObjectInfo{}, io.ErrClosedPipe
	}

	w.finished = true

	w.once.Do(func() { _ = w.pw.Close() })
	<-w.done

	if w.putErr != nil {
		return artifact.ObjectInfo{}, fmt.Errorf("trove: put %s/%s: %w",
			w.bucket, w.key, translate(w.putErr))
	}

	out := artifact.ObjectInfo{Size: w.written}
	if w.info != nil {
		out.ContentType = w.info.ContentType
		out.ETag = w.info.ETag
	}

	return out, nil
}

// Abort fails the pipe so Put stores nothing, then waits for it to
// unwind. It is a no-op after Commit.
func (w *pipeWriter) Abort() error {
	if w.finished {
		return nil
	}

	w.finished = true

	w.once.Do(func() { _ = w.pw.CloseWithError(errAborted) })
	<-w.done

	// Best effort: a driver that already materialised the object before
	// the pipe failed would leave bytes behind. Those become an orphan
	// with no artifact row, which the orphan sweep collects.
	return nil
}

// errAborted fails the pipe so Trove's Put returns rather than storing a
// truncated object.
var errAborted = errors.New("trove: write aborted")
