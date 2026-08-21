package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/xraph/dispatch/id"
)

// DefaultEphemeralPrefix is where Dispatch-owned objects live when no
// prefix is configured.
const DefaultEphemeralPrefix = "ephemeral"

// Service is the operational face of the artifact plane. It pairs a Store
// with a Backend and owns the rules that keep the two consistent:
// registration is idempotent, ephemeral keys embed the attempt, and — on
// the CommitWriter path (Create/CreateFenced/Commit) — an artifact row is
// never written without its link. Register is the exception: it writes a
// durable row for a pre-existing backend object with a nil link, which
// Store.CreateArtifact's own contract supports, since there is no
// job/attempt to link it to yet.
type Service struct {
	store   Store
	backend Backend

	ephemeralPrefix string
	defaultBucket   string
	retention       time.Duration
}

// ServiceOption configures a Service.
type ServiceOption func(*Service)

// WithEphemeralPrefix sets the key prefix for Dispatch-owned objects.
func WithEphemeralPrefix(prefix string) ServiceOption {
	return func(s *Service) { s.ephemeralPrefix = strings.Trim(prefix, "/") }
}

// WithDefaultBucket sets the bucket ephemeral objects are written to.
func WithDefaultBucket(bucket string) ServiceOption {
	return func(s *Service) { s.defaultBucket = bucket }
}

// WithRetention sets the default retention applied to ephemeral artifacts
// that do not carry their own expiry.
func WithRetention(d time.Duration) ServiceOption {
	return func(s *Service) { s.retention = d }
}

// NewService creates a Service. A nil backend leaves the artifact plane
// disabled: every method returns ErrNoBackend and Dispatch behaves exactly
// as it did before artifacts existed.
func NewService(store Store, backend Backend, opts ...ServiceOption) *Service {
	s := &Service{
		store:           store,
		backend:         backend,
		ephemeralPrefix: DefaultEphemeralPrefix,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Store returns the underlying persistence layer.
func (s *Service) Store() Store { return s.store }

// Backend returns the underlying object storage.
func (s *Service) Backend() Backend { return s.backend }

// Enabled reports whether a backend is configured.
func (s *Service) Enabled() bool { return s != nil && s.backend != nil }

// DefaultBucket returns the bucket ephemeral objects are written to.
func (s *Service) DefaultBucket() string { return s.defaultBucket }

// ── Register ──────────────────────────────────────────────────────

// RegisterOptions configures registration of a durable artifact.
type RegisterOptions struct {
	ScopeAppID  string
	ScopeOrgID  string
	ContentType string
}

// RegisterOption configures Register.
type RegisterOption func(*RegisterOptions)

// WithScope tags the artifact with a tenant application and organization.
func WithScope(appID, orgID string) RegisterOption {
	return func(o *RegisterOptions) {
		o.ScopeAppID = appID
		o.ScopeOrgID = orgID
	}
}

// WithContentType records the artifact's media type.
func WithContentType(ct string) RegisterOption {
	return func(o *RegisterOptions) { o.ContentType = ct }
}

// Register records an object the application already uploaded, returning
// a durable Ref that Dispatch will read but never delete.
//
// Register does not hash the object. Hashing a multi-gigabyte file would
// turn enqueue into a full read pass; the hash is filled in later by the
// staging cache, which is already streaming every byte to disk. Until
// then the artifact is identified by its storage coordinates.
//
// Registering the same coordinates twice returns the existing ref rather
// than an error, so callers can register unconditionally.
func (s *Service) Register(ctx context.Context, bucket, key string, opts ...RegisterOption) (Ref, error) {
	if !s.Enabled() {
		return Ref{}, ErrNoBackend
	}

	var cfg RegisterOptions
	for _, opt := range opts {
		opt(&cfg)
	}

	probe := Ref{Backend: s.backend.Name(), Bucket: bucket, Key: key}

	info, err := s.backend.Stat(ctx, probe)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Ref{}, fmt.Errorf("register %s/%s: %w", bucket, key, ErrNotFound)
		}

		return Ref{}, fmt.Errorf("dispatch/artifact: stat %s/%s: %w", bucket, key, err)
	}

	contentType := cfg.ContentType
	if contentType == "" {
		contentType = info.ContentType
	}

	a := &Artifact{
		ID:          id.NewArtifactID(),
		Backend:     s.backend.Name(),
		Bucket:      bucket,
		Key:         key,
		Size:        info.Size,
		ContentType: contentType,
		Lifecycle:   Durable,
		ScopeAppID:  cfg.ScopeAppID,
		ScopeOrgID:  cfg.ScopeOrgID,
		CreatedAt:   time.Now().UTC(),
	}

	err = s.store.CreateArtifact(ctx, a, nil)

	switch {
	case err == nil:
		return a.Ref(), nil

	case errors.Is(err, ErrExists):
		existing, ferr := s.store.FindArtifactByKey(ctx, a.Backend, bucket, key)
		if ferr != nil {
			return Ref{}, fmt.Errorf("dispatch/artifact: resolve existing artifact: %w", ferr)
		}

		return existing.Ref(), nil

	default:
		return Ref{}, fmt.Errorf("dispatch/artifact: register: %w", err)
	}
}

// Get resolves a ref to its stored artifact.
func (s *Service) Get(ctx context.Context, artifactID id.ArtifactID) (*Artifact, error) {
	if !s.Enabled() {
		return nil, ErrNoBackend
	}

	return s.store.GetArtifact(ctx, artifactID)
}

// Open streams an artifact's bytes.
func (s *Service) Open(ctx context.Context, ref Ref) (io.ReadCloser, error) {
	if !s.Enabled() {
		return nil, ErrNoBackend
	}

	return s.backend.Open(ctx, ref)
}

// ── Create ────────────────────────────────────────────────────────

// CreateOptions configures creation of an ephemeral artifact.
type CreateOptions struct {
	ContentType string
	ScopeAppID  string
	ScopeOrgID  string
	Retention   time.Duration
	IfAbsent    bool
}

// CreateOption configures Create.
type CreateOption func(*CreateOptions)

// ContentType records the media type of the object being written.
func ContentType(ct string) CreateOption {
	return func(o *CreateOptions) { o.ContentType = ct }
}

// Scope tags the created artifact with a tenant application and org.
func Scope(appID, orgID string) CreateOption {
	return func(o *CreateOptions) {
		o.ScopeAppID = appID
		o.ScopeOrgID = orgID
	}
}

// Retain overrides the default retention for this artifact.
func Retain(d time.Duration) CreateOption {
	return func(o *CreateOptions) { o.Retention = d }
}

// IfAbsent makes Create return ErrExists when a previous attempt of the
// same owner already committed this name.
//
// This is what lets a retried handler skip work it already did: a job
// splitting a 400-page PDF can resume at page 317 instead of re-rendering
// the 316 pages a prior attempt committed.
func IfAbsent() CreateOption {
	return func(o *CreateOptions) { o.IfAbsent = true }
}

// EphemeralKey returns the storage key for an owner's output.
//
// The attempt is part of the key because Commit is attempt-scoped while
// storage coordinates are unique: without it, a retried job writing the
// same name would collide with its own previous attempt.
func (s *Service) EphemeralKey(owner OwnerRef, attempt int, name string) string {
	return path.Join(
		s.ephemeralPrefix,
		string(owner.Kind),
		owner.ID,
		strconv.Itoa(attempt),
		name,
	)
}

// FindExisting returns the artifact a previous attempt committed under
// this owner and name, across all attempts.
func (s *Service) FindExisting(ctx context.Context, owner OwnerRef, name string) (Ref, error) {
	if !s.Enabled() {
		return Ref{}, ErrNoBackend
	}

	link, err := s.store.FindLinkByName(ctx, owner, name)
	if err != nil {
		return Ref{}, err
	}

	a, err := s.store.GetArtifact(ctx, link.ArtifactID)
	if err != nil {
		return Ref{}, err
	}

	return a.Ref(), nil
}

// FindCommitted returns the artifact already sitting at the exact
// storage coordinates Create or CreateFenced would use for
// (owner, attempt, name, fenceToken), if one exists. It returns
// ErrNotFound when none does.
//
// This is narrower than FindExisting on purpose: FindExisting answers
// "has ANY attempt committed this name," which is what a handler's own
// Existing/IfAbsent check wants. FindCommitted answers "did THIS EXACT
// caller already commit THIS EXACT thing" — which is what lets a caller
// recognise its own earlier, successful partial work as a no-op to skip
// rather than a collision to fail on, without also treating a
// DIFFERENT holder's commit of the same name under a different
// fenceToken as anything but what it is: a separate object at a
// separate key. See worker.Runner.commitOutputFile, the motivating
// caller: a retry of a launch-failure-classified commit failure reuses
// the identical (owner, attempt) — RetryCount does not advance for a
// launch failure — so without this, re-committing a name that already
// landed in an earlier, partially-failed pass would collide with
// itself on every subsequent attempt.
func (s *Service) FindCommitted(ctx context.Context, owner OwnerRef, attempt int, name, fenceToken string) (Ref, error) {
	if !s.Enabled() {
		return Ref{}, ErrNoBackend
	}

	key := s.EphemeralKey(owner, attempt, name)
	if fenceToken != "" {
		key = path.Join(key, fenceToken)
	}

	a, err := s.store.FindArtifactByKey(ctx, s.backend.Name(), s.defaultBucket, key)
	if err != nil {
		return Ref{}, err
	}

	return a.Ref(), nil
}

// Create begins writing an ephemeral artifact owned by owner.
//
// The returned writer publishes nothing until Commit; Abort discards it.
// With IfAbsent, a name a prior attempt already committed returns
// ErrExists so the caller can skip the work.
func (s *Service) Create(
	ctx context.Context,
	owner OwnerRef,
	attempt int,
	name string,
	opts ...CreateOption,
) (*CommitWriter, error) {
	return s.create(ctx, owner, attempt, name, "", opts...)
}

// CreateFenced is Create, except the storage key additionally
// incorporates fenceToken, so two callers racing to commit the same
// (owner, attempt, name) under different fenceTokens can never collide
// on the same backend object.
//
// This exists for a caller whose own claim to "the current holder of
// (owner, attempt)" is itself fenced — a worker committing an
// out-of-process rung's outputs under a lease epoch is the motivating
// case (see worker.Runner.commitOutputFile). Two workers can each
// believe they hold the same job at the same RetryCount at once: a
// lease reclaim races a worker that has not yet noticed its lease
// expired and is still finishing a long attempt. Create's key is a
// pure function of (owner, attempt, name), so both would resolve to
// the identical backend object — whichever Commit lands second would
// silently overwrite the first's bytes, behind a store row that still
// claims the first writer's size, with no error surfaced to anyone. A
// distinct fenceToken per holder (their lease epoch) gives each
// holder its own object instead: the loser's write lands next to the
// winner's rather than on top of it, and the store's own uniqueness
// check on (backend, bucket, key) can then only ever reject an actual
// repeat of the SAME holder recommitting the SAME name, never one
// holder clobbering another's bytes purely by losing a race.
//
// fenceToken is never recorded on Link or Artifact — Attempt keeps
// meaning exactly what it means everywhere else in this package — it
// only ever changes the storage key most callers never see. A caller
// with no fence to offer should use Create; an empty fenceToken here
// is refused rather than silently behaving like Create, so a caller
// that meant to fence but passed a zero value fails loudly instead of
// losing the protection without noticing.
func (s *Service) CreateFenced(
	ctx context.Context,
	owner OwnerRef,
	attempt int,
	name string,
	fenceToken string,
	opts ...CreateOption,
) (*CommitWriter, error) {
	if fenceToken == "" {
		return nil, fmt.Errorf("dispatch/artifact: create fenced %q: empty fence token", name)
	}

	return s.create(ctx, owner, attempt, name, fenceToken, opts...)
}

// create is the shared implementation behind Create and CreateFenced.
// An empty fenceToken makes it behave exactly as Create always has;
// Create is a thin wrapper passing exactly that.
func (s *Service) create(
	ctx context.Context,
	owner OwnerRef,
	attempt int,
	name string,
	fenceToken string,
	opts ...CreateOption,
) (*CommitWriter, error) {
	if !s.Enabled() {
		return nil, ErrNoBackend
	}

	if !owner.Valid() {
		return nil, fmt.Errorf("dispatch/artifact: create %q: invalid owner", name)
	}

	if err := validateName(name); err != nil {
		return nil, err
	}

	var cfg CreateOptions
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.IfAbsent {
		if _, err := s.FindExisting(ctx, owner, name); err == nil {
			return nil, ErrExists
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}

	bucket := s.defaultBucket
	key := s.EphemeralKey(owner, attempt, name)
	if fenceToken != "" {
		key = path.Join(key, fenceToken)
	}

	w, err := s.backend.Create(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("dispatch/artifact: create %s/%s: %w", bucket, key, err)
	}

	retention := cfg.Retention
	if retention == 0 {
		retention = s.retention
	}

	return &CommitWriter{
		svc:       s,
		inner:     w,
		owner:     owner,
		attempt:   attempt,
		name:      name,
		bucket:    bucket,
		key:       key,
		cfg:       cfg,
		retention: retention,
	}, nil
}

// validateName rejects names that would escape the ephemeral prefix or
// the staging directory. The name becomes both a path component in the
// storage key and a filename on disk.
func validateName(name string) error {
	switch {
	case name == "":
		return errors.New("dispatch/artifact: name must not be empty")
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf("dispatch/artifact: name %q must not contain a path separator", name)
	case strings.Contains(name, ".."):
		return fmt.Errorf("dispatch/artifact: name %q must not contain %q", name, "..")
	default:
		return nil
	}
}

// CommitWriter writes an ephemeral artifact's bytes and, on Commit,
// records the artifact and its link atomically.
type CommitWriter struct {
	svc       *Service
	inner     Writer
	owner     OwnerRef
	attempt   int
	name      string
	bucket    string
	key       string
	cfg       CreateOptions
	retention time.Duration

	committed bool
	aborted   bool
}

// Write appends bytes to the pending object.
func (w *CommitWriter) Write(p []byte) (int, error) { return w.inner.Write(p) }

// Commit finalises the object, inserts the artifact, and links it to the
// owner as an output. The store writes both in one operation, so a
// zero-link artifact cannot result from a normal race.
func (w *CommitWriter) Commit(ctx context.Context) (Ref, error) {
	if w.committed {
		return Ref{}, errors.New("dispatch/artifact: writer already committed")
	}

	if w.aborted {
		return Ref{}, errors.New("dispatch/artifact: writer already aborted")
	}

	info, err := w.inner.Commit(ctx)
	if err != nil {
		return Ref{}, fmt.Errorf("dispatch/artifact: commit %s/%s: %w", w.bucket, w.key, err)
	}

	w.committed = true

	now := time.Now().UTC()

	contentType := w.cfg.ContentType
	if contentType == "" {
		contentType = info.ContentType
	}

	a := &Artifact{
		ID:          id.NewArtifactID(),
		Backend:     w.svc.backend.Name(),
		Bucket:      w.bucket,
		Key:         w.key,
		Size:        info.Size,
		ContentType: contentType,
		Lifecycle:   Ephemeral,
		ScopeAppID:  w.cfg.ScopeAppID,
		ScopeOrgID:  w.cfg.ScopeOrgID,
		CreatedAt:   now,
	}

	if w.retention > 0 {
		expires := now.Add(w.retention)
		a.ExpiresAt = &expires
	}

	link := &Link{
		ArtifactID: a.ID,
		OwnerKind:  w.owner.Kind,
		OwnerID:    w.owner.ID,
		Role:       RoleOutput,
		Name:       w.name,
		Attempt:    w.attempt,
		CreatedAt:  now,
	}

	if err := w.svc.store.CreateArtifact(ctx, a, link); err != nil {
		return Ref{}, fmt.Errorf("dispatch/artifact: record %s/%s: %w", w.bucket, w.key, err)
	}

	return a.Ref(), nil
}

// Abort discards the pending object. It is a no-op after Commit, so
// `defer w.Abort()` is the correct idiom alongside a successful commit.
func (w *CommitWriter) Abort() error {
	if w.committed || w.aborted {
		return nil
	}

	w.aborted = true

	return w.inner.Abort()
}

// ── Link ──────────────────────────────────────────────────────────

// Link records that an owner references an existing artifact. This is how
// a declared input is attributed to the job that consumed it.
func (s *Service) Link(
	ctx context.Context,
	ref Ref,
	owner OwnerRef,
	role Role,
	name string,
	attempt int,
) error {
	if !s.Enabled() {
		return ErrNoBackend
	}

	return s.store.LinkArtifact(ctx, &Link{
		ArtifactID: ref.ID,
		OwnerKind:  owner.Kind,
		OwnerID:    owner.ID,
		Role:       role,
		Name:       name,
		Attempt:    attempt,
		CreatedAt:  time.Now().UTC(),
	})
}
