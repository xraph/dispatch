package shim

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/zeebo/blake3"

	"github.com/xraph/dispatch/artifact"
)

// dirMode is the permission bits for directories LocalFS creates. It
// grants the owner and group read/execute, and nothing to everyone else,
// so a sandboxed handler does not leave world-readable output behind.
const dirMode = 0o750

// fileMode is the permission bits for files LocalFS writes. It grants the
// owner and group read/write, and nothing to everyone else.
const fileMode = 0o640

// hashPrefix labels a content digest with its algorithm, matching the
// format artifact/cache uses for Ref.ContentHash so a hash computed here
// is directly comparable to one computed there.
const hashPrefix = "blake3:"

// tempPattern names the scratch file Create writes into before it is
// renamed into place. The leading dot hides it from casual directory
// listings of the output tree.
const tempPattern = ".tmp-*"

// LocalFS is a directory-backed artifact.Backend. It stores every object
// as a plain file under root, so the process holding it needs no object
// store credential — only a directory it can read and write.
//
// LocalFS is built for the exec/shim boundary: a sandboxed child process
// writes its outputs here, and the parent collects them from the
// filesystem once the child exits, without the child ever holding a
// network client for the real object store.
type LocalFS struct {
	root string
}

// Compile-time check that LocalFS satisfies the contract it exists for.
var _ artifact.Backend = (*LocalFS)(nil)

// NewLocalFS returns a Backend that stores objects as files under root.
// The caller is responsible for root existing and being writable; LocalFS
// creates subdirectories under it as needed but never creates root itself.
func NewLocalFS(root string) *LocalFS {
	return &LocalFS{root: root}
}

// Name identifies this backend.
func (fs *LocalFS) Name() string { return "localfs" }

// resolve maps a bucket-relative key onto a path under root, rejecting
// any key that would place the result outside root.
//
// A key is attacker-influenced in the general case: this backend runs
// inside the process that is parsing a possibly-malicious file, and a key
// that escaped root would let that process write or read anywhere the
// sandbox UID can reach. resolve is therefore the single choke point
// every method routes through, and it fails closed rather than silently
// clamping a traversal into some other path inside root: a caller that
// asked for "../escape" gets an error, not a different object.
func (fs *LocalFS) resolve(key string) (string, error) {
	if filepath.IsAbs(key) {
		return "", fmt.Errorf("shim: key %q must not be absolute", key)
	}

	cleaned := filepath.Clean(key)

	full := filepath.Join(fs.root, cleaned)

	rel, err := filepath.Rel(fs.root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("shim: key %q escapes root", key)
	}

	return full, nil
}

// Open returns a reader over the object's bytes.
func (fs *LocalFS) Open(_ context.Context, ref artifact.Ref) (io.ReadCloser, error) {
	path, err := fs.resolve(ref.Key)
	if err != nil {
		return nil, fmt.Errorf("shim: open %s/%s: %w", ref.Bucket, ref.Key, err)
	}

	// #nosec G304 -- path is confined to fs.root by resolve's containment check.
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("shim: open %s/%s: %w", ref.Bucket, ref.Key, artifact.ErrNotFound)
		}

		return nil, fmt.Errorf("shim: open %s/%s: %w", ref.Bucket, ref.Key, err)
	}

	return f, nil
}

// Create begins writing a new object. The bytes are not visible until
// Commit.
func (fs *LocalFS) Create(_ context.Context, bucket, key string) (artifact.Writer, error) {
	path, err := fs.resolve(key)
	if err != nil {
		return nil, fmt.Errorf("shim: create %s/%s: %w", bucket, key, err)
	}

	dir := filepath.Dir(path)
	if mkerr := os.MkdirAll(dir, dirMode); mkerr != nil {
		return nil, fmt.Errorf("shim: create %s/%s: %w", bucket, key, mkerr)
	}

	// Create the temp file in the same directory as the final path so the
	// rename in Commit is an atomic, same-filesystem operation.
	tmp, err := os.CreateTemp(dir, tempPattern)
	if err != nil {
		return nil, fmt.Errorf("shim: create %s/%s: %w", bucket, key, err)
	}

	if cherr := tmp.Chmod(fileMode); cherr != nil {
		_ = tmp.Close()
		// tmp.Name() is a sibling of path inside the directory resolve
		// already confined to fs.root; nothing here reads attacker input.
		_ = os.Remove(tmp.Name()) //nolint:gosec // G703: temp file path is derived from resolve's containment check, not from a raw key.

		return nil, fmt.Errorf("shim: create %s/%s: %w", bucket, key, cherr)
	}

	return &localWriter{
		file:   tmp,
		hash:   blake3.New(),
		bucket: bucket,
		key:    key,
		final:  path,
	}, nil
}

// Stat reports the object's size without reading it.
func (fs *LocalFS) Stat(_ context.Context, ref artifact.Ref) (artifact.ObjectInfo, error) {
	path, err := fs.resolve(ref.Key)
	if err != nil {
		return artifact.ObjectInfo{}, fmt.Errorf("shim: stat %s/%s: %w", ref.Bucket, ref.Key, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return artifact.ObjectInfo{}, fmt.Errorf("shim: stat %s/%s: %w", ref.Bucket, ref.Key, artifact.ErrNotFound)
		}

		return artifact.ObjectInfo{}, fmt.Errorf("shim: stat %s/%s: %w", ref.Bucket, ref.Key, err)
	}

	return artifact.ObjectInfo{Size: info.Size()}, nil
}

// Delete removes the object. Deleting a missing object is not an error.
func (fs *LocalFS) Delete(_ context.Context, ref artifact.Ref) error {
	path, err := fs.resolve(ref.Key)
	if err != nil {
		return fmt.Errorf("shim: delete %s/%s: %w", ref.Bucket, ref.Key, err)
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("shim: delete %s/%s: %w", ref.Bucket, ref.Key, err)
	}

	return nil
}

// errWriterFinished means Commit or Abort was called on a localWriter
// that had already been finalised by one or the other.
var errWriterFinished = errors.New("shim: writer already finished")

// localWriter accumulates bytes in a temp file and renames it into place
// on Commit. It hashes the bytes as they stream through so Commit never
// has to re-read the file to report a content hash.
type localWriter struct {
	file   *os.File
	hash   *blake3.Hasher
	bucket string
	key    string
	final  string

	written int64
	done    bool
}

// Write appends bytes to the pending object.
func (w *localWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.written += int64(n)

	if n > 0 {
		// hash.Hash.Write never returns an error, per the io.Writer
		// contract documented on the standard library's hash.Hash.
		if _, herr := w.hash.Write(p[:n]); herr != nil {
			return n, herr
		}
	}

	return n, err
}

// Commit finalises the object: it flushes and closes the temp file,
// renames it into place, and reports the logical size and content hash.
func (w *localWriter) Commit(_ context.Context) (artifact.ObjectInfo, error) {
	if w.done {
		return artifact.ObjectInfo{}, fmt.Errorf("shim: commit %s/%s: %w", w.bucket, w.key, errWriterFinished)
	}

	w.done = true
	tmpName := w.file.Name()

	// tmpName is the temp file this writer created inside the directory
	// resolve already confined to fs.root; it is not attacker input.
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		_ = os.Remove(tmpName) //nolint:gosec // G703: tmpName is our own temp file under the resolved, contained directory.

		return artifact.ObjectInfo{}, fmt.Errorf("shim: commit %s/%s: %w", w.bucket, w.key, err)
	}

	if err := w.file.Close(); err != nil {
		_ = os.Remove(tmpName) //nolint:gosec // G703: tmpName is our own temp file under the resolved, contained directory.

		return artifact.ObjectInfo{}, fmt.Errorf("shim: commit %s/%s: %w", w.bucket, w.key, err)
	}

	// tmpName and w.final both passed through resolve's containment check
	// (w.final at Create time; tmpName is a sibling CreateTemp made inside
	// that same, already-contained directory).
	if err := os.Rename(tmpName, w.final); err != nil { //nolint:gosec // G703: both paths are confined to fs.root by resolve.
		_ = os.Remove(tmpName) //nolint:gosec // G703: tmpName is our own temp file under the resolved, contained directory.

		return artifact.ObjectInfo{}, fmt.Errorf("shim: commit %s/%s: %w", w.bucket, w.key, err)
	}

	sum := hex.EncodeToString(w.hash.Sum(nil))

	return artifact.ObjectInfo{
		Size: w.written,
		ETag: hashPrefix + sum,
	}, nil
}

// Abort discards the partial object. It is a no-op after Commit.
func (w *localWriter) Abort() error {
	if w.done {
		return nil
	}

	w.done = true
	name := w.file.Name()

	_ = w.file.Close()

	// name is this writer's own temp file, created inside the directory
	// resolve already confined to fs.root at Create time.
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) { //nolint:gosec // G703: name is our own temp file under the resolved, contained directory.
		return fmt.Errorf("shim: abort %s/%s: %w", w.bucket, w.key, err)
	}

	return nil
}
