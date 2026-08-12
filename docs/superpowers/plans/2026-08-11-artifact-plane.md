# Artifact Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make data a first-class concept in Dispatch — a tracked artifact entity backed by pluggable object storage, with declared job inputs, imperative outputs, a content-addressed staging cache, and safe lifecycle sweeping.

**Architecture:** A leaf `artifact` package defines the entity, the `Backend` storage interface, and the `Store` persistence interface, which joins the existing composite `store.Store`. `artifact/cache` is a worker-local content-addressed disk cache with leases, a byte budget, and single-flight downloads. `artifact/staging` is a `middleware.Middleware` that stages declared inputs before the handler runs and finalizes outputs after — so `worker/executor.go` is untouched. `artifact/trove` adapts `*trove.Trove` as the reference `Backend`.

**Tech Stack:** Go 1.25.7, bun (Postgres/SQLite), mongo-driver v2, go-redis v9, grove/migrate, `golang.org/x/sync/singleflight`, `zeebo/blake3`, testcontainers-go, Trove.

**Spec:** `docs/superpowers/specs/2026-08-11-artifact-plane-design.md`

## Global Constraints

- Go 1.25.7. Module `github.com/xraph/dispatch`.
- Lint: `.golangci.yml` (golangci-lint v2). Run `make lint` before every commit. Exported identifiers require doc comments starting with the identifier name.
- `artifact` is a **leaf package**. It may import only `github.com/xraph/dispatch` (root), `github.com/xraph/dispatch/id`, and stdlib. It MUST NOT import `job`, `workflow`, `middleware`, or `store`.
- The staging middleware lives in `artifact/staging` because it imports `job` and `middleware`.
- Store implementations verify interface satisfaction with compile-time assertions (`var _ artifact.Store = (*Store)(nil)`), never by importing `store` (import cycle).
- All five backends must implement `artifact.Store`: memory, postgres, sqlite, mongo, redis.
- IDs use `id.New(id.PrefixArtifact)`. Prefix string is `art`.
- Migrations register into the existing `migrate.NewGroup("dispatch")` with a `Version` string strictly greater than every existing version in that backend's `migrations.go`.
- Every feature is opt-in. With no `Backend` configured, Dispatch behaves exactly as it does today.
- `lifecycle = 'ephemeral'` appears as a **literal** in every sweep statement. Never bound from a variable.
- Commit messages: no `Co-Authored-By` trailers, ever.
- Tests are table-driven where there is more than one case.

---

## File Structure

**Phase 1 — entity and stores**
- Create `artifact/doc.go` — package documentation.
- Create `artifact/artifact.go` — `Artifact`, `Ref`, `Lifecycle`, `Role`, `Link`, `ObjectInfo`.
- Create `artifact/errors.go` — sentinel errors.
- Create `artifact/store.go` — `Store` interface, `ListOpts`, `SweepOpts`.
- Modify `id/id.go` — add `PrefixArtifact`, `ArtifactID`, `NewArtifactID`, `ParseArtifactID`.
- Modify `store/store.go` — embed `artifact.Store` in the composite.
- Create `artifact/artifacttest/suite.go` — shared conformance suite.
- Create `store/memory/artifact.go` + modify `store/memory/store.go`.
- Create `store/postgres/artifact.go`, `store/postgres/artifact_models.go`, modify `store/postgres/migrations.go`.
- Same shape for `store/sqlite/`, `store/mongo/`, `store/redis/`.

**Phase 2 — backend and Trove adapter**
- Create `artifact/backend.go` — `Backend`, `Writer`, `RangeReader`, `Presigner`.
- Create `artifact/service.go` — `Service`: `Register`, `Get`, `Open`, `Create`, `Link`.
- Create `artifact/trove/backend.go`, `artifact/trove/doc.go`.
- Create `artifact/artifacttest/backend.go` — in-memory `Backend` with call counters.

**Phase 3 — cache**
- Create `artifact/cache/doc.go`, `cache.go`, `budget.go`, `index.go`, `entry.go`.

**Phase 4 — staging middleware and handler API**
- Create `artifact/input.go` — `InputSpec`, `Input`, `Required`, `MaxSize`, `StageAsPath`, `StageLazy`.
- Create `artifact/accessor.go` — `Accessor` interface, `From`, context key.
- Modify `job/options.go` — add `Inputs []artifact.InputSpec` to `Options`.
- Create `artifact/staging/doc.go`, `middleware.go`, `accessor.go`, `bind.go`.
- Modify `engine/engine.go` — validate declarations at `Register`, accept `artifact.Bind` at `Enqueue`.

**Phase 5 — extension wiring**
- Modify `extension/config.go`, `extension/options.go`, `extension/extension.go`.
- Create `extension/artifact.go` — backend resolution.

**Phase 6 — sweeper**
- Create `artifact/sweeper/doc.go`, `sweeper.go`.
- Modify `ext/` — add `EmitArtifactSwept`.

---

## Phase 1 — Entity and Stores

### Task 1: TypeID prefix for artifacts

**Files:**
- Modify: `id/id.go`
- Test: `id/id_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `id.PrefixArtifact Prefix = "art"`, `id.ArtifactID = ID`, `id.NewArtifactID() ID`, `id.ParseArtifactID(string) (ID, error)`.

- [ ] **Step 1: Write the failing test**

Append to `id/id_test.go`:

```go
func TestArtifactID(t *testing.T) {
	got := NewArtifactID()
	if got.Prefix() != PrefixArtifact {
		t.Fatalf("prefix = %q, want %q", got.Prefix(), PrefixArtifact)
	}
	if got.IsNil() {
		t.Fatal("NewArtifactID returned nil ID")
	}

	parsed, err := ParseArtifactID(got.String())
	if err != nil {
		t.Fatalf("ParseArtifactID(%q) error = %v", got.String(), err)
	}
	if parsed.String() != got.String() {
		t.Fatalf("round trip = %q, want %q", parsed.String(), got.String())
	}

	if _, err := ParseArtifactID("job_01h2xcejqtf2nbrexx3vqjhp41"); err == nil {
		t.Fatal("ParseArtifactID accepted a job ID, want error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./id/ -run TestArtifactID -v`
Expected: FAIL — `undefined: NewArtifactID`.

- [ ] **Step 3: Write minimal implementation**

In `id/id.go`, add to the prefix const block (after `PrefixWorker`):

```go
	// PrefixArtifact identifies artifact entities.
	PrefixArtifact Prefix = "art"
```

Add to the type alias block (after `WorkerID`):

```go
// ArtifactID is a type-safe identifier for artifacts (prefix: "art").
type ArtifactID = ID
```

Add to the convenience constructor block:

```go
// NewArtifactID generates a new unique artifact ID.
func NewArtifactID() ID { return New(PrefixArtifact) }
```

Add to the convenience parser block:

```go
// ParseArtifactID parses a string and validates the "art" prefix.
func ParseArtifactID(s string) (ID, error) { return ParseWithPrefix(s, PrefixArtifact) }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./id/ -run TestArtifactID -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add id/id.go id/id_test.go
git commit -m "feat(id): add artifact TypeID prefix"
```

---

### Task 2: Artifact entity types

**Files:**
- Create: `artifact/doc.go`, `artifact/artifact.go`, `artifact/errors.go`
- Test: `artifact/artifact_test.go`

**Interfaces:**
- Consumes: `id.ArtifactID`, `id.NewArtifactID`.
- Produces:
  - `type Lifecycle string`, consts `Durable Lifecycle = "durable"`, `Ephemeral Lifecycle = "ephemeral"`.
  - `type Role string`, consts `RoleInput Role = "input"`, `RoleOutput Role = "output"`, `RoleIntermediate Role = "intermediate"`.
  - `type OwnerKind string`, consts `OwnerJob OwnerKind = "job"`, `OwnerRun OwnerKind = "run"`, `OwnerStep OwnerKind = "step"`.
  - `type Ref struct { ID id.ArtifactID; Backend, Bucket, Key string; Size int64; ContentHash string }`
  - `type Artifact struct{...}` with method `func (a *Artifact) Ref() Ref`.
  - `type Link struct{...}`
  - `type ObjectInfo struct { Size int64; ContentType string; ETag string }`
  - Errors: `ErrNotFound`, `ErrExists`, `ErrSizeExceeded`, `ErrImmutable`, `ErrNoBackend`.

- [ ] **Step 1: Write the failing test**

Create `artifact/artifact_test.go`:

```go
package artifact

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
)

func TestArtifactRef(t *testing.T) {
	aid := id.NewArtifactID()
	a := &Artifact{
		ID:          aid,
		Backend:     "primary",
		Bucket:      "models",
		Key:         "tower.ifc",
		Size:        2 << 30,
		ContentHash: "blake3:9f2a",
		Lifecycle:   Durable,
		CreatedAt:   time.Now().UTC(),
	}

	ref := a.Ref()
	if ref.ID != aid {
		t.Fatalf("ref.ID = %v, want %v", ref.ID, aid)
	}
	if ref.Size != 2<<30 {
		t.Fatalf("ref.Size = %d, want %d", ref.Size, int64(2<<30))
	}
	if ref.Key != "tower.ifc" {
		t.Fatalf("ref.Key = %q, want %q", ref.Key, "tower.ifc")
	}
}

func TestLifecycleValid(t *testing.T) {
	tests := []struct {
		name string
		lc   Lifecycle
		want bool
	}{
		{"durable", Durable, true},
		{"ephemeral", Ephemeral, true},
		{"empty", Lifecycle(""), false},
		{"garbage", Lifecycle("permanent"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lc.Valid(); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArtifactIsDeleted(t *testing.T) {
	a := &Artifact{}
	if a.IsDeleted() {
		t.Fatal("fresh artifact reported deleted")
	}
	now := time.Now().UTC()
	a.DeletedAt = &now
	if !a.IsDeleted() {
		t.Fatal("soft-deleted artifact not reported deleted")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./artifact/ -v`
Expected: FAIL — package does not compile, `undefined: Artifact`.

- [ ] **Step 3: Write minimal implementation**

Create `artifact/doc.go`:

```go
// Package artifact defines Dispatch's data plane: tracked references to
// objects in external storage, the pluggable Backend interface those
// objects live behind, and the Store contract that persists their
// metadata and ownership links.
//
// This package is a leaf. It imports only the root dispatch package,
// the id package, and stdlib. The staging middleware, which needs job
// and middleware, lives in the artifact/staging sub-package so that
// job may import artifact without a cycle.
package artifact
```

Create `artifact/artifact.go`:

```go
package artifact

import (
	"time"

	"github.com/xraph/dispatch/id"
)

// Lifecycle determines whether Dispatch may delete an artifact's bytes.
type Lifecycle string

const (
	// Durable artifacts are written by the application and merely tracked
	// by Dispatch. They are read-only here and are never swept.
	Durable Lifecycle = "durable"

	// Ephemeral artifacts are created by Dispatch on a handler's behalf.
	// They are refcounted through links and swept once every owner is
	// terminal and the retention window has passed.
	Ephemeral Lifecycle = "ephemeral"
)

// Valid reports whether the lifecycle is a recognised value.
func (l Lifecycle) Valid() bool {
	return l == Durable || l == Ephemeral
}

// Role describes how an owner relates to an artifact.
type Role string

const (
	// RoleInput marks an artifact consumed by the owner.
	RoleInput Role = "input"
	// RoleOutput marks an artifact produced by the owner.
	RoleOutput Role = "output"
	// RoleIntermediate marks an artifact passed between workflow steps.
	RoleIntermediate Role = "intermediate"
)

// Valid reports whether the role is a recognised value.
func (r Role) Valid() bool {
	return r == RoleInput || r == RoleOutput || r == RoleIntermediate
}

// OwnerKind identifies which entity owns a link.
type OwnerKind string

const (
	// OwnerJob links an artifact to a job.
	OwnerJob OwnerKind = "job"
	// OwnerRun links an artifact to a workflow run.
	OwnerRun OwnerKind = "run"
	// OwnerStep links an artifact to a single workflow step.
	OwnerStep OwnerKind = "step"
)

// Valid reports whether the owner kind is a recognised value.
func (k OwnerKind) Valid() bool {
	return k == OwnerJob || k == OwnerRun || k == OwnerStep
}

// Ref is a lightweight handle to a tracked artifact. It is what callers
// pass to Bind, what handlers receive from Commit, and what workflow
// steps store in checkpoints — small enough to serialise freely.
type Ref struct {
	ID          id.ArtifactID `json:"id"`
	Backend     string        `json:"backend"`
	Bucket      string        `json:"bucket"`
	Key         string        `json:"key"`
	Size        int64         `json:"size"`
	ContentHash string        `json:"content_hash,omitempty"`
}

// IsZero reports whether the ref is unset.
func (r Ref) IsZero() bool { return r.ID.IsNil() }

// Artifact is a tracked object in external storage.
type Artifact struct {
	ID          id.ArtifactID `json:"id"`
	Backend     string        `json:"backend"`
	Bucket      string        `json:"bucket"`
	Key         string        `json:"key"`
	Size        int64         `json:"size"`
	ContentHash string        `json:"content_hash,omitempty"`
	ContentType string        `json:"content_type,omitempty"`
	Lifecycle   Lifecycle     `json:"lifecycle"`
	ScopeAppID  string        `json:"scope_app_id,omitempty"`
	ScopeOrgID  string        `json:"scope_org_id,omitempty"`
	ExpiresAt   *time.Time    `json:"expires_at,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	DeletedAt   *time.Time    `json:"deleted_at,omitempty"`
}

// Ref returns a lightweight handle to this artifact.
func (a *Artifact) Ref() Ref {
	return Ref{
		ID:          a.ID,
		Backend:     a.Backend,
		Bucket:      a.Bucket,
		Key:         a.Key,
		Size:        a.Size,
		ContentHash: a.ContentHash,
	}
}

// IsDeleted reports whether the artifact has been soft-deleted by the
// sweeper. A soft-deleted artifact is no longer served but its bytes
// survive until the purge pass.
func (a *Artifact) IsDeleted() bool { return a.DeletedAt != nil }

// Link records that an owner references an artifact in a given role.
// Attempt scopes the link to one execution attempt so a retried job's
// outputs do not collide with its previous attempt's.
type Link struct {
	ArtifactID id.ArtifactID `json:"artifact_id"`
	OwnerKind  OwnerKind     `json:"owner_kind"`
	OwnerID    string        `json:"owner_id"`
	Role       Role          `json:"role"`
	Name       string        `json:"name"`
	Attempt    int           `json:"attempt"`
	CreatedAt  time.Time     `json:"created_at"`
}

// ObjectInfo is what a Backend reports about a stored object.
type ObjectInfo struct {
	Size        int64
	ContentType string
	ETag        string
}
```

Create `artifact/errors.go`:

```go
package artifact

import "errors"

var (
	// ErrNotFound means the artifact or its underlying object does not
	// exist. Staging treats this as permanent: retrying a fetch of
	// something that no longer exists cannot succeed.
	ErrNotFound = errors.New("dispatch/artifact: not found")

	// ErrExists means an artifact already exists for this owner, name,
	// and a prior attempt. Create with IfAbsent returns it alongside the
	// existing ref so a retried handler can skip recomputation.
	ErrExists = errors.New("dispatch/artifact: already exists")

	// ErrSizeExceeded means a bound artifact is larger than the input
	// declaration's MaxSize.
	ErrSizeExceeded = errors.New("dispatch/artifact: size exceeds declared maximum")

	// ErrImmutable means an attempt was made to delete or overwrite a
	// durable artifact through a path reserved for ephemeral ones.
	ErrImmutable = errors.New("dispatch/artifact: durable artifacts are immutable")

	// ErrNoBackend means no storage backend is configured. Every
	// artifact operation is a no-op in this state and Dispatch behaves
	// exactly as it did before the artifact plane existed.
	ErrNoBackend = errors.New("dispatch/artifact: no backend configured")
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./artifact/ -v`
Expected: PASS — three tests.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add artifact/
git commit -m "feat(artifact): add entity types and sentinel errors"
```

---

### Task 3: Store interface

**Files:**
- Create: `artifact/store.go`
- Modify: `store/store.go`

**Interfaces:**
- Consumes: `Artifact`, `Link`, `Ref`, `Lifecycle`, `OwnerKind`, `Role` (Task 2).
- Produces: `artifact.Store` interface, `artifact.ListOpts`, `artifact.SweepOpts`, `artifact.OwnerRef`.

- [ ] **Step 1: Write the interface**

Create `artifact/store.go`:

```go
package artifact

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
)

// OwnerRef identifies a link owner.
type OwnerRef struct {
	Kind OwnerKind
	ID   string
}

// ListOpts controls pagination and filtering for artifact list queries.
type ListOpts struct {
	// Limit is the maximum number of artifacts to return. Zero means no limit.
	Limit int
	// Offset is the number of artifacts to skip.
	Offset int
	// Lifecycle filters by lifecycle. Empty means all.
	Lifecycle Lifecycle
	// ScopeAppID filters by tenant application. Empty means all.
	ScopeAppID string
	// ScopeOrgID filters by tenant organization. Empty means all.
	ScopeOrgID string
	// IncludeDeleted includes soft-deleted artifacts. Default false.
	IncludeDeleted bool
}

// SweepOpts controls a lifecycle sweep.
type SweepOpts struct {
	// Retention is the grace period after the last owner reaches a
	// terminal state before an artifact becomes eligible.
	Retention time.Duration
	// Limit caps how many artifacts a single sweep call may mark.
	Limit int
	// DryRun computes eligibility and returns the artifacts that would
	// be marked without modifying anything.
	DryRun bool
}

// Store defines the persistence contract for artifacts and their links.
//
// Implementations must guarantee that CreateArtifact inserts the
// artifact and its link in a single atomic operation, so a zero-link
// artifact can only result from a partial failure and never from a
// normal race.
type Store interface {
	// CreateArtifact inserts an artifact and, when link is non-nil, its
	// first link atomically. Returns ErrExists if an artifact already
	// exists at the same backend, bucket, and key.
	CreateArtifact(ctx context.Context, a *Artifact, link *Link) error

	// GetArtifact retrieves an artifact by ID. Returns ErrNotFound if it
	// does not exist or has been soft-deleted.
	GetArtifact(ctx context.Context, artifactID id.ArtifactID) (*Artifact, error)

	// FindArtifactByKey retrieves an artifact by its storage coordinates.
	// Returns ErrNotFound if none exists.
	FindArtifactByKey(ctx context.Context, backend, bucket, key string) (*Artifact, error)

	// UpdateArtifact persists changes to size, content hash, content
	// type, and expiry. It must not permit changing lifecycle.
	UpdateArtifact(ctx context.Context, a *Artifact) error

	// ListArtifacts returns artifacts matching the given options.
	ListArtifacts(ctx context.Context, opts ListOpts) ([]*Artifact, error)

	// LinkArtifact records that an owner references an artifact.
	// Linking the same artifact, owner, name, and attempt twice is a
	// no-op rather than an error.
	LinkArtifact(ctx context.Context, link *Link) error

	// ListLinks returns every link belonging to the given owner.
	ListLinks(ctx context.Context, owner OwnerRef) ([]*Link, error)

	// FindLinkByName returns the link for an owner and name with the
	// highest attempt number, ignoring attempt. This is what IfAbsent
	// uses to detect that a prior attempt already produced an output.
	// Returns ErrNotFound if no attempt has produced it.
	FindLinkByName(ctx context.Context, owner OwnerRef, name string) (*Link, error)

	// ListArtifactsByOwner returns the artifacts linked to an owner,
	// optionally filtered by role. An empty role returns all.
	ListArtifactsByOwner(ctx context.Context, owner OwnerRef, role Role) ([]*Artifact, error)

	// SweepEphemeral marks eligible ephemeral artifacts as deleted and
	// returns them. Implementations MUST constrain the statement to
	// lifecycle = 'ephemeral' as a literal. Durable artifacts must be
	// unreachable from this method.
	SweepEphemeral(ctx context.Context, opts SweepOpts) ([]*Artifact, error)

	// SweepOrphans marks ephemeral artifacts that have no links at all
	// and were created before the cutoff. Same literal constraint.
	SweepOrphans(ctx context.Context, cutoff time.Time, limit int) ([]*Artifact, error)

	// ListPurgeable returns soft-deleted artifacts whose deleted_at is
	// older than grace, so their bytes may be removed from the backend.
	ListPurgeable(ctx context.Context, grace time.Duration, limit int) ([]*Artifact, error)

	// PurgeArtifact hard-deletes an artifact row and its links after the
	// bytes have been removed from the backend.
	PurgeArtifact(ctx context.Context, artifactID id.ArtifactID) error
}
```

- [ ] **Step 2: Add to the composite store**

In `store/store.go`, add the import and embed:

```go
	"github.com/xraph/dispatch/artifact"
```

```go
type Store interface {
	job.Store
	workflow.Store
	cron.Store
	dlq.Store
	event.Store
	cluster.Store
	artifact.Store
	// ... existing Migrate/Ping/Close
}
```

- [ ] **Step 3: Verify it fails to build**

Run: `go build ./...`
Expected: FAIL — every store backend no longer satisfies `store.Store`. This is the expected state; Tasks 4–8 fix it one backend at a time.

- [ ] **Step 4: Commit the interface**

```bash
git add artifact/store.go store/store.go
git commit -m "feat(artifact): define Store interface and add to composite"
```

Note: the tree does not build until Task 8 completes. That is intentional — the conformance suite in Task 4 is what proves each backend correct, and splitting the interface from its implementations keeps each backend's diff reviewable.

---

### Task 4: Conformance suite and memory store

**Files:**
- Create: `artifact/artifacttest/doc.go`, `artifact/artifacttest/suite.go`
- Create: `store/memory/artifact.go`
- Modify: `store/memory/store.go`
- Test: `store/memory/artifact_test.go`

**Interfaces:**
- Consumes: `artifact.Store` (Task 3), all entity types (Task 2).
- Produces: `artifacttest.RunStoreSuite(t *testing.T, newStore func() artifact.Store)` — the single suite every backend runs.

- [ ] **Step 1: Write the conformance suite**

Create `artifact/artifacttest/doc.go`:

```go
// Package artifacttest provides a shared conformance suite and test
// doubles for artifact storage. Every artifact.Store implementation
// runs RunStoreSuite so all five backends are held to one contract.
package artifacttest
```

Create `artifact/artifacttest/suite.go`:

```go
package artifacttest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// RunStoreSuite exercises the artifact.Store contract. newStore must
// return a fresh, empty store on every call.
func RunStoreSuite(t *testing.T, newStore func() artifact.Store) {
	t.Helper()

	t.Run("CreateAndGet", func(t *testing.T) { testCreateAndGet(t, newStore()) })
	t.Run("CreateDuplicateKey", func(t *testing.T) { testCreateDuplicateKey(t, newStore()) })
	t.Run("GetMissing", func(t *testing.T) { testGetMissing(t, newStore()) })
	t.Run("FindByKey", func(t *testing.T) { testFindByKey(t, newStore()) })
	t.Run("UpdateHash", func(t *testing.T) { testUpdateHash(t, newStore()) })
	t.Run("LinkAndList", func(t *testing.T) { testLinkAndList(t, newStore()) })
	t.Run("LinkIdempotent", func(t *testing.T) { testLinkIdempotent(t, newStore()) })
	t.Run("FindLinkByNameAcrossAttempts", func(t *testing.T) { testFindLinkAcrossAttempts(t, newStore()) })
	t.Run("SweepNeverTouchesDurable", func(t *testing.T) { testSweepNeverTouchesDurable(t, newStore()) })
	t.Run("SweepOrphans", func(t *testing.T) { testSweepOrphans(t, newStore()) })
	t.Run("PurgeFlow", func(t *testing.T) { testPurgeFlow(t, newStore()) })
}

func newArtifact(key string, lc artifact.Lifecycle) *artifact.Artifact {
	return &artifact.Artifact{
		ID:        id.NewArtifactID(),
		Backend:   "primary",
		Bucket:    "models",
		Key:       key,
		Size:      1024,
		Lifecycle: lc,
		CreatedAt: time.Now().UTC(),
	}
}

func testCreateAndGet(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("tower.ifc", artifact.Durable)

	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	got, err := s.GetArtifact(ctx, a.ID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}
	if got.Key != a.Key || got.Size != a.Size || got.Lifecycle != a.Lifecycle {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, a)
	}
}

func testCreateDuplicateKey(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("dup.ifc", artifact.Durable)
	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("first CreateArtifact: %v", err)
	}

	b := newArtifact("dup.ifc", artifact.Durable)
	err := s.CreateArtifact(ctx, b, nil)
	if !errors.Is(err, artifact.ErrExists) {
		t.Fatalf("duplicate key error = %v, want ErrExists", err)
	}
}

func testGetMissing(t *testing.T, s artifact.Store) {
	_, err := s.GetArtifact(context.Background(), id.NewArtifactID())
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("GetArtifact(missing) = %v, want ErrNotFound", err)
	}
}

func testFindByKey(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("find.ifc", artifact.Durable)
	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	got, err := s.FindArtifactByKey(ctx, "primary", "models", "find.ifc")
	if err != nil {
		t.Fatalf("FindArtifactByKey: %v", err)
	}
	if got.ID != a.ID {
		t.Fatalf("FindArtifactByKey ID = %v, want %v", got.ID, a.ID)
	}

	_, err = s.FindArtifactByKey(ctx, "primary", "models", "nope.ifc")
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("FindArtifactByKey(missing) = %v, want ErrNotFound", err)
	}
}

func testUpdateHash(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("hash.ifc", artifact.Durable)
	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	a.ContentHash = "blake3:9f2a"
	a.Size = 4096
	if err := s.UpdateArtifact(ctx, a); err != nil {
		t.Fatalf("UpdateArtifact: %v", err)
	}

	got, err := s.GetArtifact(ctx, a.ID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}
	if got.ContentHash != "blake3:9f2a" || got.Size != 4096 {
		t.Fatalf("update not persisted: hash=%q size=%d", got.ContentHash, got.Size)
	}
}

func testLinkAndList(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("linked.ifc", artifact.Ephemeral)
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}
	link := &artifact.Link{
		ArtifactID: a.ID,
		OwnerKind:  owner.Kind,
		OwnerID:    owner.ID,
		Role:       artifact.RoleOutput,
		Name:       "mesh.glb",
		Attempt:    0,
		CreatedAt:  time.Now().UTC(),
	}
	if err := s.CreateArtifact(ctx, a, link); err != nil {
		t.Fatalf("CreateArtifact with link: %v", err)
	}

	links, err := s.ListLinks(ctx, owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}
	if len(links) != 1 || links[0].Name != "mesh.glb" {
		t.Fatalf("ListLinks = %+v, want one link named mesh.glb", links)
	}

	arts, err := s.ListArtifactsByOwner(ctx, owner, artifact.RoleOutput)
	if err != nil {
		t.Fatalf("ListArtifactsByOwner: %v", err)
	}
	if len(arts) != 1 || arts[0].ID != a.ID {
		t.Fatalf("ListArtifactsByOwner = %+v, want artifact %v", arts, a.ID)
	}
}

func testLinkIdempotent(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("idem.ifc", artifact.Ephemeral)
	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}
	link := &artifact.Link{
		ArtifactID: a.ID, OwnerKind: owner.Kind, OwnerID: owner.ID,
		Role: artifact.RoleOutput, Name: "out.bin", Attempt: 0,
		CreatedAt: time.Now().UTC(),
	}

	for i := 0; i < 2; i++ {
		if err := s.LinkArtifact(ctx, link); err != nil {
			t.Fatalf("LinkArtifact call %d: %v", i, err)
		}
	}

	links, err := s.ListLinks(ctx, owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}
	if len(links) != 1 {
		t.Fatalf("ListLinks returned %d links, want 1 (link must be idempotent)", len(links))
	}
}

func testFindLinkAcrossAttempts(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}

	for attempt := 0; attempt < 3; attempt++ {
		a := newArtifact("page-317-"+string(rune('a'+attempt))+".png", artifact.Ephemeral)
		link := &artifact.Link{
			ArtifactID: a.ID, OwnerKind: owner.Kind, OwnerID: owner.ID,
			Role: artifact.RoleOutput, Name: "page-317.png", Attempt: attempt,
			CreatedAt: time.Now().UTC(),
		}
		if err := s.CreateArtifact(ctx, a, link); err != nil {
			t.Fatalf("CreateArtifact attempt %d: %v", attempt, err)
		}
	}

	got, err := s.FindLinkByName(ctx, owner, "page-317.png")
	if err != nil {
		t.Fatalf("FindLinkByName: %v", err)
	}
	if got.Attempt != 2 {
		t.Fatalf("FindLinkByName attempt = %d, want 2 (highest)", got.Attempt)
	}

	_, err = s.FindLinkByName(ctx, owner, "never-made.png")
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("FindLinkByName(missing) = %v, want ErrNotFound", err)
	}
}

// testSweepNeverTouchesDurable is the safety invariant of the whole
// design. A durable artifact must be unreachable from any sweep path,
// regardless of age, links, or owner state.
func testSweepNeverTouchesDurable(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	long := time.Now().UTC().Add(-365 * 24 * time.Hour)

	durable := newArtifact("customer-upload.ifc", artifact.Durable)
	durable.CreatedAt = long
	if err := s.CreateArtifact(ctx, durable, nil); err != nil {
		t.Fatalf("CreateArtifact durable: %v", err)
	}

	swept, err := s.SweepEphemeral(ctx, artifact.SweepOpts{Retention: 0, Limit: 100})
	if err != nil {
		t.Fatalf("SweepEphemeral: %v", err)
	}
	for _, a := range swept {
		if a.ID == durable.ID {
			t.Fatal("SweepEphemeral marked a DURABLE artifact — safety invariant violated")
		}
	}

	orphaned, err := s.SweepOrphans(ctx, time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	for _, a := range orphaned {
		if a.ID == durable.ID {
			t.Fatal("SweepOrphans marked a DURABLE artifact — safety invariant violated")
		}
	}

	got, err := s.GetArtifact(ctx, durable.ID)
	if err != nil {
		t.Fatalf("durable artifact no longer retrievable after sweeps: %v", err)
	}
	if got.IsDeleted() {
		t.Fatal("durable artifact was soft-deleted — safety invariant violated")
	}
}

func testSweepOrphans(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	old := newArtifact("orphan.bin", artifact.Ephemeral)
	old.CreatedAt = time.Now().UTC().Add(-48 * time.Hour)
	if err := s.CreateArtifact(ctx, old, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	fresh := newArtifact("fresh.bin", artifact.Ephemeral)
	if err := s.CreateArtifact(ctx, fresh, nil); err != nil {
		t.Fatalf("CreateArtifact fresh: %v", err)
	}

	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	swept, err := s.SweepOrphans(ctx, cutoff, 100)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if len(swept) != 1 || swept[0].ID != old.ID {
		t.Fatalf("SweepOrphans = %+v, want only the 48h-old orphan", swept)
	}
}

func testPurgeFlow(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("purge.bin", artifact.Ephemeral)
	a.CreatedAt = time.Now().UTC().Add(-72 * time.Hour)
	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	if _, err := s.SweepOrphans(ctx, time.Now().UTC().Add(-24*time.Hour), 100); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	purgeable, err := s.ListPurgeable(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ListPurgeable: %v", err)
	}
	if len(purgeable) != 1 || purgeable[0].ID != a.ID {
		t.Fatalf("ListPurgeable = %+v, want the swept artifact", purgeable)
	}

	if err := s.PurgeArtifact(ctx, a.ID); err != nil {
		t.Fatalf("PurgeArtifact: %v", err)
	}
	if _, err := s.GetArtifact(ctx, a.ID); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("GetArtifact after purge = %v, want ErrNotFound", err)
	}
}
```

- [ ] **Step 2: Write the memory store test**

Create `store/memory/artifact_test.go`:

```go
package memory

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

func TestArtifactStoreConformance(t *testing.T) {
	artifacttest.RunStoreSuite(t, func() artifact.Store { return New() })
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `go test ./store/memory/ -run TestArtifactStoreConformance -v`
Expected: FAIL — `*Store does not implement artifact.Store`.

- [ ] **Step 4: Implement the memory store**

In `store/memory/store.go`, add `artifact` to the imports, add the compile-time assertion `_ artifact.Store = (*Store)(nil)`, add these fields to the `Store` struct:

```go
	artifacts     map[string]*artifact.Artifact
	artifactLinks []*artifact.Link
```

and initialise `artifacts` in `New()`.

Create `store/memory/artifact.go` implementing all fourteen methods against those maps under `s.mu`. Key requirements the suite enforces:

- `CreateArtifact` returns `artifact.ErrExists` when any existing non-deleted artifact shares `(Backend, Bucket, Key)`; when `link != nil` it appends the link in the same critical section.
- `GetArtifact` and `FindArtifactByKey` return `artifact.ErrNotFound` for missing **and** soft-deleted artifacts.
- `LinkArtifact` scans `artifactLinks` for a match on `(ArtifactID, OwnerKind, OwnerID, Name, Attempt)` and returns nil without appending when found.
- `FindLinkByName` filters by owner and name, then returns the highest `Attempt`.
- `SweepEphemeral` and `SweepOrphans` both start with `if a.Lifecycle != artifact.Ephemeral { continue }` as the first statement of the loop body — the in-memory equivalent of the SQL literal.
- `SweepOrphans` skips any artifact that has at least one link.
- Sweeps set `DeletedAt` to now and return copies.
- `ListPurgeable` returns soft-deleted artifacts where `now - *DeletedAt >= grace`.
- `PurgeArtifact` deletes from `artifacts` and filters `artifactLinks`.

Return deep copies from every read so callers cannot mutate stored state — match the copying discipline already used by the job methods in `store/memory/store.go`.

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./store/memory/ -v`
Expected: PASS — all eleven suite subtests.

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add artifact/artifacttest/ store/memory/
git commit -m "feat(artifact): add store conformance suite and memory implementation"
```

---

### Task 5: Postgres store

**Files:**
- Create: `store/postgres/artifact.go`, `store/postgres/artifact_models.go`
- Modify: `store/postgres/migrations.go`, `store/postgres/store.go`
- Test: `store/postgres/artifact_test.go`

**Interfaces:**
- Consumes: `artifact.Store` (Task 3), `artifacttest.RunStoreSuite` (Task 4).
- Produces: nothing new — satisfies the existing interface.

- [ ] **Step 1: Write the test**

Create `store/postgres/artifact_test.go` following the existing testcontainers pattern in `store/postgres/store_test.go`:

```go
package postgres

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

func TestArtifactStoreConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers suite in short mode")
	}
	artifacttest.RunStoreSuite(t, func() artifact.Store {
		return newTestStore(t) // existing helper: fresh migrated DB per call
	})
}
```

Check `store/postgres/store_test.go` for the exact name of the existing per-test store helper and use it rather than introducing a second one.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./store/postgres/ -run TestArtifactStoreConformance -v`
Expected: FAIL — `*Store does not implement artifact.Store`.

- [ ] **Step 3: Add the migration**

In `store/postgres/migrations.go`, register a new migration inside `init()`. Use a `Version` strictly greater than every existing one in the file:

```go
		// 007: Create artifacts and artifact links tables.
		&migrate.Migration{
			Name:    "create_artifacts_tables",
			Version: "20260811120000",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				if _, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_artifacts (
						id              TEXT PRIMARY KEY,
						backend         TEXT NOT NULL,
						bucket          TEXT NOT NULL,
						key             TEXT NOT NULL,
						size            BIGINT NOT NULL DEFAULT 0,
						content_hash    TEXT,
						content_type    TEXT,
						lifecycle       TEXT NOT NULL,
						scope_app_id    TEXT,
						scope_org_id    TEXT,
						expires_at      TIMESTAMPTZ,
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						deleted_at      TIMESTAMPTZ,
						CONSTRAINT uq_dispatch_artifacts_key UNIQUE (backend, bucket, key)
					)`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_artifacts_sweep
						ON dispatch_artifacts (lifecycle, created_at)
						WHERE deleted_at IS NULL`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_artifacts_purge
						ON dispatch_artifacts (deleted_at)
						WHERE deleted_at IS NOT NULL`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_artifacts_hash
						ON dispatch_artifacts (content_hash)
						WHERE content_hash IS NOT NULL`); err != nil {
					return err
				}

				if _, err := exec.Exec(ctx, `
					CREATE TABLE IF NOT EXISTS dispatch_artifact_links (
						artifact_id     TEXT NOT NULL REFERENCES dispatch_artifacts(id) ON DELETE CASCADE,
						owner_kind      TEXT NOT NULL,
						owner_id        TEXT NOT NULL,
						role            TEXT NOT NULL,
						name            TEXT NOT NULL,
						attempt         INTEGER NOT NULL DEFAULT 0,
						created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
						PRIMARY KEY (artifact_id, owner_kind, owner_id, name, attempt)
					)`); err != nil {
					return err
				}

				_, err := exec.Exec(ctx, `
					CREATE INDEX IF NOT EXISTS idx_dispatch_artifact_links_owner
						ON dispatch_artifact_links (owner_kind, owner_id)`)
				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				if _, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_artifact_links`); err != nil {
					return err
				}
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS dispatch_artifacts`)
				return err
			},
		},
```

Match the `Down` style used by the existing migrations in the file — if they omit `Down`, omit it here too.

- [ ] **Step 4: Add the bun models**

Create `store/postgres/artifact_models.go` with `artifactModel` and `artifactLinkModel` structs plus `toArtifactModel`, `fromArtifactModel`, `toLinkModel`, `fromLinkModel`. Follow the conventions in `store/postgres/models.go` exactly — same `bun:"table:...,alias:..."` tag style, same nullable handling for `*time.Time`, same `id.ID` scanning.

- [ ] **Step 5: Implement the store methods**

Create `store/postgres/artifact.go`. Key requirements:

- `CreateArtifact` runs inside `s.pgdb.RunInTx` when `link != nil`, inserting artifact then link. Map unique-violation to `artifact.ErrExists` using the existing `isDuplicateKey(err)` helper.
- `GetArtifact` / `FindArtifactByKey` add `AND deleted_at IS NULL`; map `sql.ErrNoRows` to `artifact.ErrNotFound`.
- `LinkArtifact` uses `ON CONFLICT DO NOTHING` for idempotency.
- `FindLinkByName` orders by `attempt DESC LIMIT 1`.
- `SweepEphemeral` — two statements per owner kind (job and run), each with `lifecycle = 'ephemeral'` written as a **literal**:

```go
const sweepEphemeralJobsSQL = `
	UPDATE dispatch_artifacts SET deleted_at = NOW()
	WHERE lifecycle = 'ephemeral'
	  AND deleted_at IS NULL
	  AND id IN (
	    SELECT l.artifact_id
	    FROM dispatch_artifact_links l
	    JOIN dispatch_jobs j ON j.id = l.owner_id AND l.owner_kind = 'job'
	    GROUP BY l.artifact_id
	    HAVING bool_and(j.state IN ('completed', 'failed', 'cancelled'))
	       AND MAX(COALESCE(j.completed_at, j.updated_at)) + $1::interval < NOW()
	  )
	  AND (expires_at IS NULL OR expires_at < NOW())
	RETURNING *`
```

Write the workflow-run variant against `dispatch_workflow_runs` with its terminal states. An artifact linked to owners of both kinds must satisfy both, so run the statements as an intersection rather than a union — compute eligibility per kind, then mark only IDs eligible under every kind that links to them.

- `SweepOrphans`:

```go
const sweepOrphansSQL = `
	UPDATE dispatch_artifacts a SET deleted_at = NOW()
	WHERE a.lifecycle = 'ephemeral'
	  AND a.deleted_at IS NULL
	  AND a.created_at < $1
	  AND NOT EXISTS (SELECT 1 FROM dispatch_artifact_links l WHERE l.artifact_id = a.id)
	LIMIT $2
	RETURNING *`
```

Postgres does not accept `LIMIT` directly on `UPDATE`; use a `WHERE id IN (SELECT ... LIMIT $2)` subquery.

- `DryRun` runs the same predicate as a `SELECT` and skips the `UPDATE`.

- [ ] **Step 6: Add the assertion and run**

In `store/postgres/store.go`, add `_ artifact.Store = (*Store)(nil)` to the assertion block.

Run: `go test ./store/postgres/ -v`
Expected: PASS

- [ ] **Step 7: Lint and commit**

```bash
make lint
git add store/postgres/
git commit -m "feat(artifact): add postgres store implementation"
```

---

### Task 6: SQLite store

**Files:**
- Create: `store/sqlite/artifact.go`, `store/sqlite/artifact_models.go`
- Modify: `store/sqlite/migrations.go`, `store/sqlite/store.go`
- Test: `store/sqlite/artifact_test.go`

- [ ] **Step 1: Write the test**

```go
package sqlite

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

func TestArtifactStoreConformance(t *testing.T) {
	artifacttest.RunStoreSuite(t, func() artifact.Store { return newTestStore(t) })
}
```

Use the existing per-test store helper from `store/sqlite/store_test.go`.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./store/sqlite/ -run TestArtifactStoreConformance -v`
Expected: FAIL — interface not satisfied.

- [ ] **Step 3: Implement**

Port Task 5 with these dialect changes:
- `TIMESTAMPTZ` → `TIMESTAMP`, `BIGINT` → `INTEGER`, `NOW()` → `CURRENT_TIMESTAMP`.
- No partial indexes with `WHERE` on older SQLite; check what the existing migrations in this file do and match. If they avoid partial indexes, use plain indexes.
- `bool_and(...)` → `MIN(CASE WHEN ... THEN 1 ELSE 0 END) = 1`.
- No `RETURNING *` on older drivers — check the existing SQLite store; if it avoids `RETURNING`, select eligible IDs first, then `UPDATE ... WHERE id IN (...)`, then re-select.
- Interval arithmetic: compute the cutoff timestamp in Go and bind it, rather than using SQL interval syntax.

The `lifecycle = 'ephemeral'` literal requirement is unchanged.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./store/sqlite/ -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add store/sqlite/
git commit -m "feat(artifact): add sqlite store implementation"
```

---

### Task 7: Mongo store

**Files:**
- Create: `store/mongo/artifact.go`
- Modify: `store/mongo/store.go`, and the index-creation function in that package
- Test: `store/mongo/artifact_test.go`

- [ ] **Step 1: Write the test**

```go
package mongo

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

func TestArtifactStoreConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers suite in short mode")
	}
	artifacttest.RunStoreSuite(t, func() artifact.Store { return newTestStore(t) })
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./store/mongo/ -run TestArtifactStoreConformance -v`
Expected: FAIL — interface not satisfied.

- [ ] **Step 3: Implement**

Two collections: `dispatch_artifacts` and `dispatch_artifact_links`.

- Unique index on `{backend: 1, bucket: 1, key: 1}`; map duplicate-key errors to `artifact.ErrExists` using the package's existing duplicate detection helper.
- Unique index on `{artifact_id: 1, owner_kind: 1, owner_id: 1, name: 1, attempt: 1}`; `LinkArtifact` uses an upsert so duplicates are no-ops.
- Index on `{owner_kind: 1, owner_id: 1}` for `ListLinks`.
- `CreateArtifact` with a link uses a session transaction when the deployment is a replica set. Testcontainers Mongo may be standalone — check what the existing store does for multi-document writes and follow it. If transactions are unavailable, insert the artifact first, then the link, and document that the orphan pass covers the gap.
- Sweeps: aggregate over links joined to jobs/runs with `$lookup`. Every pipeline's **first** `$match` stage is `{"lifecycle": "ephemeral", "deleted_at": nil}` written as a literal in the code, not built from a parameter.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./store/mongo/ -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add store/mongo/
git commit -m "feat(artifact): add mongo store implementation"
```

---

### Task 8: Redis store

**Files:**
- Create: `store/redis/artifact.go`
- Modify: `store/redis/store.go`
- Test: `store/redis/artifact_test.go`

- [ ] **Step 1: Write the test**

```go
package redis

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

func TestArtifactStoreConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers suite in short mode")
	}
	artifacttest.RunStoreSuite(t, func() artifact.Store { return newTestStore(t) })
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./store/redis/ -run TestArtifactStoreConformance -v`
Expected: FAIL — interface not satisfied.

- [ ] **Step 3: Implement**

Key layout, following the conventions already used by this package's job and cluster code:

```
dispatch:artifact:<id>                  HASH   the artifact
dispatch:artifact:key:<b>:<bk>:<key>    STRING artifact id (uniqueness guard)
dispatch:artifact:lifecycle:ephemeral   ZSET   score = created_at unix, member = id
dispatch:artifact:deleted               ZSET   score = deleted_at unix, member = id
dispatch:link:<ownerKind>:<ownerID>     HASH   field "<name>:<attempt>" → JSON link
dispatch:artifact:links:<artifactID>    SET    "<ownerKind>:<ownerID>:<name>:<attempt>"
```

- `CreateArtifact` uses `SETNX` on the key-guard, returning `artifact.ErrExists` when it is already held; then a `TxPipeline` writes the hash, the lifecycle ZSET entry (ephemeral only), and any link.
- `SweepOrphans` reads `ZRANGEBYSCORE` on the ephemeral ZSET up to the cutoff, then filters to members whose `dispatch:artifact:links:<id>` set is empty. The ZSET holds only ephemeral artifacts by construction, which is this backend's form of the literal constraint — assert it explicitly with a `Lifecycle != Ephemeral → continue` guard after loading each artifact.
- `SweepEphemeral` needs owner terminal state, which Redis cannot join. Load each candidate's links and `GET` each owner's job/run hash via the existing helpers in this package. Cap the work with `SweepOpts.Limit`.
- `ListArtifacts` with filters scans the lifecycle ZSET rather than `KEYS`.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./store/redis/ -v && go build ./...`
Expected: PASS, and the whole tree builds again for the first time since Task 3.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add store/redis/
git commit -m "feat(artifact): add redis store implementation"
```

---

## Phase 2 — Backend and Trove Adapter

### Task 9: Backend interface and test double

**Files:**
- Create: `artifact/backend.go`
- Create: `artifact/artifacttest/backend.go`
- Test: `artifact/artifacttest/backend_test.go`

**Interfaces:**
- Consumes: `Ref`, `ObjectInfo`, `ErrNotFound` (Task 2).
- Produces:
  - `type Backend interface { Name() string; Open(ctx, Ref) (io.ReadCloser, error); Create(ctx, bucket, key string) (Writer, error); Stat(ctx, Ref) (ObjectInfo, error); Delete(ctx, Ref) error }`
  - `type Writer interface { io.Writer; Commit(ctx context.Context) (ObjectInfo, error); Abort() error }`
  - `type RangeReader interface { OpenRange(ctx, Ref, off, n int64) (io.ReadCloser, error) }`
  - `type Presigner interface { PresignGet(ctx, Ref, ttl time.Duration) (string, error) }`
  - `artifacttest.NewBackend() *Backend` with `Opens()`, `Creates()`, `Deletes()` counters and a `Put(bucket, key string, data []byte)` seeding helper.

- [ ] **Step 1: Write the failing test**

Create `artifact/artifacttest/backend_test.go`:

```go
package artifacttest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/xraph/dispatch/artifact"
)

func TestBackendRoundTrip(t *testing.T) {
	ctx := context.Background()
	b := NewBackend()
	b.Put("models", "tower.ifc", []byte("hello"))

	ref := artifact.Ref{Backend: b.Name(), Bucket: "models", Key: "tower.ifc"}
	rc, err := b.Open(ctx, ref)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("read %q, want %q", got, "hello")
	}
	if b.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1", b.Opens())
	}
}

func TestBackendOpenMissing(t *testing.T) {
	_, err := NewBackend().Open(context.Background(),
		artifact.Ref{Bucket: "models", Key: "nope"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Open(missing) = %v, want ErrNotFound", err)
	}
}

func TestBackendWriterCommitAndAbort(t *testing.T) {
	ctx := context.Background()
	b := NewBackend()

	w, err := b.Create(ctx, "models", "mesh.glb")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("meshdata")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	info, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if info.Size != 8 {
		t.Fatalf("info.Size = %d, want 8", info.Size)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("Abort after Commit must be a no-op, got %v", err)
	}

	w2, _ := b.Create(ctx, "models", "aborted.glb")
	w2.Write([]byte("partial"))
	if err := w2.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	_, err = b.Open(ctx, artifact.Ref{Bucket: "models", Key: "aborted.glb"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("aborted object is readable; Open = %v, want ErrNotFound", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./artifact/artifacttest/ -v`
Expected: FAIL — `undefined: NewBackend`.

- [ ] **Step 3: Write `artifact/backend.go`**

```go
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

	// Create begins writing a new object. The bytes are not visible
	// until Commit. Callers must call Commit or Abort.
	Create(ctx context.Context, bucket, key string) (Writer, error)

	// Stat reports the object's size and content type without reading it.
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
// Abort after a successful Commit is a no-op, so `defer w.Abort()` is
// the correct idiom.
type Writer interface {
	io.Writer

	// Commit finalises the object and returns its logical info.
	Commit(ctx context.Context) (ObjectInfo, error)

	// Abort discards the partial object. It is a no-op after Commit.
	Abort() error
}

// RangeReader is an optional Backend capability for partial reads.
type RangeReader interface {
	// OpenRange returns a reader over n bytes starting at off. A
	// negative n reads to the end.
	OpenRange(ctx context.Context, ref Ref, off, n int64) (io.ReadCloser, error)
}

// Presigner is an optional Backend capability for direct client access.
// It is what lets a DWP remote worker fetch a large object straight from
// object storage instead of streaming it through the coordinator.
type Presigner interface {
	// PresignGet returns a time-limited URL granting read access.
	PresignGet(ctx context.Context, ref Ref, ttl time.Duration) (string, error)
}
```

- [ ] **Step 4: Write `artifact/artifacttest/backend.go`**

An in-memory `Backend` guarded by a mutex, storing `map[string][]byte` keyed by `bucket + "/" + key`, with `atomic.Int64` counters for `Opens`, `Creates`, and `Deletes`. Its `Writer` buffers into a `bytes.Buffer` and only inserts into the map on `Commit`; `Abort` sets a `done` flag and drops the buffer; `Commit` sets the same flag so a later `Abort` is a no-op. Add a `DelayOpen time.Duration` field that `Open` sleeps for — Task 12 needs it to prove single-flight.

- [ ] **Step 5: Run to verify it passes**

Run: `go test ./artifact/artifacttest/ -v`
Expected: PASS — three tests.

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add artifact/backend.go artifact/artifacttest/
git commit -m "feat(artifact): add Backend interface and in-memory test double"
```

---

### Task 10: Service — register, open, create, link

**Files:**
- Create: `artifact/service.go`
- Test: `artifact/service_test.go`

**Interfaces:**
- Consumes: `Store` (Task 3), `Backend` (Task 9).
- Produces:
  - `func NewService(s Store, b Backend, opts ...ServiceOption) *Service`
  - `func (s *Service) Register(ctx, bucket, key string, opts ...RegisterOption) (Ref, error)`
  - `func (s *Service) Open(ctx, ref Ref) (io.ReadCloser, error)`
  - `func (s *Service) Create(ctx, owner OwnerRef, attempt int, name string, opts ...CreateOption) (*CommitWriter, error)`
  - `func (s *Service) Link(ctx, ref Ref, owner OwnerRef, role Role, name string, attempt int) error`
  - `type CommitWriter` with `Write`, `Commit(ctx) (Ref, error)`, `Abort()`.
  - Options: `WithScope(appID, orgID string)`, `ContentType(string)`, `Retain(time.Duration)`, `IfAbsent()`.
  - `func (s *Service) EphemeralKey(owner OwnerRef, attempt int, name string) string`

- [ ] **Step 1: Write the failing test**

Create `artifact/service_test.go` with these cases:

```go
package artifact_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/memory"
)

func newService(t *testing.T) (*artifact.Service, *artifacttest.Backend) {
	t.Helper()
	b := artifacttest.NewBackend()
	svc := artifact.NewService(memory.New(), b,
		artifact.WithEphemeralPrefix("ephemeral"),
		artifact.WithDefaultBucket("dispatch"))
	return svc, b
}

func TestRegisterDurable(t *testing.T) {
	ctx := context.Background()
	svc, b := newService(t)
	b.Put("models", "tower.ifc", []byte("0123456789"))

	ref, err := svc.Register(ctx, "models", "tower.ifc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if ref.Size != 10 {
		t.Fatalf("ref.Size = %d, want 10 (Register must Stat)", ref.Size)
	}
	if ref.ID.Prefix() != id.PrefixArtifact {
		t.Fatalf("ref.ID prefix = %q, want %q", ref.ID.Prefix(), id.PrefixArtifact)
	}
	if ref.ContentHash != "" {
		t.Fatal("Register must NOT hash — hashing is deferred to first staging")
	}
}

func TestRegisterMissingObject(t *testing.T) {
	svc, _ := newService(t)
	_, err := svc.Register(context.Background(), "models", "nope.ifc")
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Register(missing) = %v, want ErrNotFound", err)
	}
}

func TestRegisterIsIdempotent(t *testing.T) {
	ctx := context.Background()
	svc, b := newService(t)
	b.Put("models", "same.ifc", []byte("abc"))

	first, err := svc.Register(ctx, "models", "same.ifc")
	if err != nil {
		t.Fatalf("first Register: %v", err)
	}
	second, err := svc.Register(ctx, "models", "same.ifc")
	if err != nil {
		t.Fatalf("second Register: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("Register not idempotent: %v then %v", first.ID, second.ID)
	}
}

func TestCreateCommitLinksOutput(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}

	w, err := svc.Create(ctx, owner, 0, "mesh.glb", artifact.ContentType("model/gltf-binary"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.Copy(w, strings.NewReader("meshbytes")); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	ref, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if ref.Size != 9 {
		t.Fatalf("ref.Size = %d, want 9", ref.Size)
	}
	if !strings.Contains(ref.Key, "/0/mesh.glb") {
		t.Fatalf("ephemeral key %q must embed the attempt", ref.Key)
	}
}

func TestCreateKeysDifferPerAttempt(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}

	keys := make(map[string]bool)
	for attempt := 0; attempt < 3; attempt++ {
		w, err := svc.Create(ctx, owner, attempt, "mesh.glb")
		if err != nil {
			t.Fatalf("Create attempt %d: %v", attempt, err)
		}
		w.Write([]byte("x"))
		ref, err := w.Commit(ctx)
		if err != nil {
			t.Fatalf("Commit attempt %d: %v", attempt, err)
		}
		if keys[ref.Key] {
			t.Fatalf("attempt %d reused key %q — unique constraint would fire", attempt, ref.Key)
		}
		keys[ref.Key] = true
	}
}

func TestCreateIfAbsentFindsPriorAttempt(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}

	w, _ := svc.Create(ctx, owner, 0, "page-317.png")
	w.Write([]byte("pixels"))
	first, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	_, err = svc.Create(ctx, owner, 1, "page-317.png", artifact.IfAbsent())
	if !errors.Is(err, artifact.ErrExists) {
		t.Fatalf("IfAbsent on attempt 1 = %v, want ErrExists", err)
	}

	existing, err := svc.FindExisting(ctx, owner, "page-317.png")
	if err != nil {
		t.Fatalf("FindExisting: %v", err)
	}
	if existing.ID != first.ID {
		t.Fatalf("FindExisting = %v, want the attempt-0 artifact %v", existing.ID, first.ID)
	}
}

func TestAbortLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	svc, b := newService(t)
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}

	w, _ := svc.Create(ctx, owner, 0, "partial.bin")
	w.Write([]byte("half"))
	w.Abort()

	links, err := svc.Store().ListLinks(ctx, owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}
	if len(links) != 0 {
		t.Fatalf("aborted write left %d links, want 0", len(links))
	}
	if b.Creates() != 1 {
		t.Fatalf("Creates() = %d, want 1", b.Creates())
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./artifact/ -run 'TestRegister|TestCreate|TestAbort' -v`
Expected: FAIL — `undefined: NewService`.

- [ ] **Step 3: Implement `artifact/service.go`**

Requirements the tests pin down:

- `NewService(store, backend, opts...)`. Options: `WithEphemeralPrefix(string)` (default `"ephemeral"`), `WithDefaultBucket(string)`, `WithRetention(time.Duration)`. A nil backend makes every method return `ErrNoBackend`.
- `Register` calls `Stat`, maps a missing object to `ErrNotFound`, then `CreateArtifact` with `Lifecycle: Durable` and **no** content hash. On `ErrExists` it calls `FindArtifactByKey` and returns that existing ref, which is what makes registration idempotent.
- `EphemeralKey(owner, attempt, name)` returns `<prefix>/<kind>/<ownerID>/<attempt>/<name>`.
- `Create` with `IfAbsent()` first calls `FindLinkByName`; on a hit it returns `nil, ErrExists`. Otherwise it calls `backend.Create` and returns a `CommitWriter`.
- `CommitWriter.Commit` calls the backend writer's `Commit`, builds the `Artifact` with `Lifecycle: Ephemeral` and the reported size, then calls `store.CreateArtifact(ctx, a, link)` with `Role: RoleOutput` — one atomic call, per the Store contract.
- `CommitWriter.Abort` calls the backend writer's `Abort` and writes nothing to the store. Idempotent, no-op after `Commit`.
- `FindExisting(ctx, owner, name) (Ref, error)` resolves via `FindLinkByName` then `GetArtifact`.
- `Store()` returns the underlying store (the test uses it; keep it exported and documented).
- `Retain(d)` sets `ExpiresAt = now + d` on create.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./artifact/ -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add artifact/service.go artifact/service_test.go
git commit -m "feat(artifact): add Service for register, create, commit, and link"
```

---

### Task 11: Trove backend adapter

**Files:**
- Create: `artifact/trove/doc.go`, `artifact/trove/backend.go`
- Test: `artifact/trove/backend_test.go`
- Modify: `go.mod` (Trove is already required; confirm no new module is needed)

**Interfaces:**
- Consumes: `artifact.Backend`, `artifact.Writer` (Task 9).
- Produces: `func New(t *trove.Trove, opts ...Option) *Backend` implementing `artifact.Backend`, `artifact.RangeReader`, and `artifact.Presigner` where Trove's driver supports them.

- [ ] **Step 1: Write the failing test**

Create `artifact/trove/backend_test.go` using Trove's `memdriver` so the test needs no external service:

```go
package trove_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/xraph/dispatch/artifact"
	troveadapter "github.com/xraph/dispatch/artifact/trove"
	"github.com/xraph/trove"
	"github.com/xraph/trove/drivers/memdriver"
)

func newBackend(t *testing.T) artifact.Backend {
	t.Helper()
	ctx := context.Background()
	drv := memdriver.New()
	if err := drv.Open(ctx, "mem://"); err != nil {
		t.Fatalf("driver open: %v", err)
	}
	tr, err := trove.Open(drv, trove.WithDefaultBucket("dispatch"))
	if err != nil {
		t.Fatalf("trove open: %v", err)
	}
	t.Cleanup(func() { tr.Close(ctx) })
	return troveadapter.New(tr)
}

func TestTroveRoundTrip(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	w, err := b.Create(ctx, "dispatch", "mesh.glb")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("meshbytes")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	info, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if info.Size != 9 {
		t.Fatalf("info.Size = %d, want 9", info.Size)
	}

	ref := artifact.Ref{Backend: b.Name(), Bucket: "dispatch", Key: "mesh.glb"}
	rc, err := b.Open(ctx, ref)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, []byte("meshbytes")) {
		t.Fatalf("read %q, want %q", got, "meshbytes")
	}
}

func TestTroveOpenMissingMapsToErrNotFound(t *testing.T) {
	_, err := newBackend(t).Open(context.Background(),
		artifact.Ref{Bucket: "dispatch", Key: "absent"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Open(missing) = %v, want ErrNotFound", err)
	}
}

func TestTroveDeleteMissingIsNotAnError(t *testing.T) {
	err := newBackend(t).Delete(context.Background(),
		artifact.Ref{Bucket: "dispatch", Key: "absent"})
	if err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./artifact/trove/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement the adapter**

`artifact/trove/backend.go`:

- `Backend` wraps `*trove.Trove` plus a `name string` (default `"trove"`, settable with `WithName`).
- `Open` calls `t.Get(ctx, ref.Bucket, ref.Key)`; map Trove's not-found error to `artifact.ErrNotFound` with `errors.Is` against whatever sentinel Trove exports — read `trove/errors.go` and use its actual sentinel, do not guess.
- `Create` returns a writer built on an `io.Pipe` feeding `t.Put`, running the `Put` in a goroutine and joining it in `Commit`. `Abort` closes the pipe with an error so `Put` fails and stores nothing, then waits for the goroutine. Track bytes written in an `int64` so `Commit` reports the **logical** size even when compression middleware changes the stored form.
- `Stat` calls Trove's stat/head operation; map missing to `ErrNotFound`.
- `Delete` calls Trove's delete and swallows not-found.
- Implement `OpenRange` only if Trove's driver exposes a range capability — check `trove/driver` for the capability interface and type-assert at construction, storing whether it is available. Same for `PresignGet`.

Read Trove's actual API surface in `/Users/rexraphael/Work/xraph/forgery/trove/trove.go` before writing this; the method names above are from the README and must be verified.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./artifact/trove/ -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
go mod tidy
git add artifact/trove/ go.mod go.sum
git commit -m "feat(artifact): add Trove backend adapter"
```

---

## Phase 3 — Staging Cache

### Task 12: Cache with single-flight, leases, and budget

**Files:**
- Create: `artifact/cache/doc.go`, `artifact/cache/cache.go`, `artifact/cache/budget.go`, `artifact/cache/index.go`
- Test: `artifact/cache/cache_test.go`, `artifact/cache/budget_test.go`

**Interfaces:**
- Consumes: `artifact.Ref`, `artifact.Backend` (Tasks 2, 9).
- Produces:
  - `func New(dir string, b artifact.Backend, opts ...Option) (*Cache, error)`
  - `func (c *Cache) Stage(ctx context.Context, ref artifact.Ref) (path string, hash string, release func(), err error)`
  - `func (c *Cache) Close() error`
  - Options: `WithBudget(bytes int64)`, `WithLogger(log.Logger)`.
  - `var ErrBudgetExceeded = errors.New("dispatch/artifact/cache: budget exceeded")`

- [ ] **Step 1: Write the failing tests**

Create `artifact/cache/cache_test.go`:

```go
package cache_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
)

func newCache(t *testing.T, budget int64) (*cache.Cache, *artifacttest.Backend) {
	t.Helper()
	b := artifacttest.NewBackend()
	c, err := cache.New(t.TempDir(), b, cache.WithBudget(budget))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, b
}

func TestStageDownloadsAndCaches(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 1<<20)
	b.Put("models", "tower.ifc", []byte("hello world"))
	ref := artifact.Ref{Bucket: "models", Key: "tower.ifc", Size: 11}

	path, hash, release, err := c.Stage(ctx, ref)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	if string(data) != "hello world" {
		t.Fatalf("staged content = %q, want %q", data, "hello world")
	}
	if hash == "" {
		t.Fatal("Stage must compute the hash during download")
	}
	release()

	// Second stage of the same ref must not re-download.
	_, hash2, release2, err := c.Stage(ctx, ref)
	if err != nil {
		t.Fatalf("second Stage: %v", err)
	}
	release2()
	if b.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1 (second Stage must hit the cache)", b.Opens())
	}
	if hash2 != hash {
		t.Fatalf("hash changed between stages: %q then %q", hash, hash2)
	}
}

func TestStageSingleFlight(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 1<<20)
	b.Put("models", "big.ifc", []byte("payload"))
	b.DelayOpen = 50 * time.Millisecond
	ref := artifact.Ref{Bucket: "models", Key: "big.ifc", Size: 7}

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, release, err := c.Stage(ctx, ref)
			errs[i] = err
			if err == nil {
				release()
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if b.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1 — %d concurrent stages must share one download", b.Opens(), n)
	}
}

func TestStageMissingObject(t *testing.T) {
	c, _ := newCache(t, 1<<20)
	_, _, _, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "models", Key: "absent"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Stage(missing) = %v, want ErrNotFound", err)
	}
}

func TestLeaseBlocksEviction(t *testing.T) {
	ctx := context.Background()
	c, b := newCache(t, 20) // room for two 10-byte objects
	b.Put("m", "a", []byte("0123456789"))
	b.Put("m", "b", []byte("0123456789"))
	b.Put("m", "c", []byte("0123456789"))

	_, _, releaseA, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "a", Size: 10})
	if err != nil {
		t.Fatalf("Stage a: %v", err)
	}
	// Hold the lease on a.
	_, _, releaseB, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "b", Size: 10})
	if err != nil {
		t.Fatalf("Stage b: %v", err)
	}
	releaseB() // b is now evictable, a is not

	_, _, releaseC, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "c", Size: 10})
	if err != nil {
		t.Fatalf("Stage c should evict b, got: %v", err)
	}
	releaseC()
	releaseA()
}

func TestBudgetExceededRespectsDeadline(t *testing.T) {
	c, b := newCache(t, 10)
	b.Put("m", "a", []byte("0123456789"))
	b.Put("m", "b", []byte("0123456789"))

	ctx := context.Background()
	_, _, releaseA, err := c.Stage(ctx, artifact.Ref{Bucket: "m", Key: "a", Size: 10})
	if err != nil {
		t.Fatalf("Stage a: %v", err)
	}
	defer releaseA()

	// a is leased and fills the budget; b cannot fit.
	deadlined, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, _, _, err = c.Stage(deadlined, artifact.Ref{Bucket: "m", Key: "b", Size: 10})
	if !errors.Is(err, cache.ErrBudgetExceeded) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stage under exhausted budget = %v, want ErrBudgetExceeded or DeadlineExceeded", err)
	}
}

func TestOversizeRefRejectedImmediately(t *testing.T) {
	c, b := newCache(t, 10)
	b.Put("m", "huge", make([]byte, 100))

	_, _, _, err := c.Stage(context.Background(),
		artifact.Ref{Bucket: "m", Key: "huge", Size: 100})
	if !errors.Is(err, cache.ErrBudgetExceeded) {
		t.Fatalf("Stage of a ref larger than the whole budget = %v, want ErrBudgetExceeded immediately", err)
	}
}

func TestRecoveryWipesTmpAndRebuildsIndex(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := artifacttest.NewBackend()
	b.Put("m", "a", []byte("0123456789"))
	ref := artifact.Ref{Bucket: "m", Key: "a", Size: 10}

	c1, err := cache.New(dir, b, cache.WithBudget(1<<20))
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	_, _, release, err := c1.Stage(ctx, ref)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	release()
	c1.Close()

	// Simulate a crash: leave junk in tmp/ and drop the index.
	os.WriteFile(dir+"/tmp/leftover", []byte("junk"), 0o600)
	os.Remove(dir + "/index.db")

	c2, err := cache.New(dir, b, cache.WithBudget(1<<20))
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	defer c2.Close()

	if _, err := os.Stat(dir + "/tmp/leftover"); !os.IsNotExist(err) {
		t.Fatal("startup must wipe tmp/")
	}

	_, _, release2, err := c2.Stage(ctx, ref)
	if err != nil {
		t.Fatalf("Stage after recovery: %v", err)
	}
	release2()
	if b.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1 — index must be rebuilt from disk, not re-downloaded", b.Opens())
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./artifact/cache/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement the cache**

`artifact/cache/cache.go` requirements:

- Layout `<dir>/tmp/<uuid>`, `<dir>/blake3/<first2>/<hash>`, `<dir>/index.db`.
- `New` creates directories, wipes `tmp/`, then rebuilds the index by walking `blake3/` and stat-ing each file. The index is an optimisation; the walk is the source of truth.
- `Stage`:
  1. If `ref.ContentHash != ""` and that hash is present, take a lease and return immediately.
  2. Otherwise resolve the cache entry keyed by `backend/bucket/key` from the index; on a hit, lease and return.
  3. On a miss, call `singleflight.Group.Do` keyed on `backend/bucket/key`.
  4. Inside the flight: `budget.Acquire(ctx, ref.Size)`; `backend.Open`; copy through `blake3.New()` into `tmp/<uuid>`; `rename` to the hash path; record in the index; release the acquired bytes back into the accounted-used total (the entry now owns them).
  5. Return the path, the hash string formatted `blake3:<hex>`, and a `release` closure that decrements the lease count exactly once (guard with `sync.Once`).
- `ref.Size == 0` means unknown: acquire optimistically against the full remaining budget and correct the accounting after the copy reports the real size.
- Every returned error from a missing object must wrap `artifact.ErrNotFound` so callers can `errors.Is` it.

`artifact/cache/budget.go` requirements:

- `budget` holds `limit`, `used`, a `sync.Mutex`, and a `sync.Cond`.
- `Acquire(ctx, n)`: if `n > limit`, return `ErrBudgetExceeded` immediately without waiting — this is `TestOversizeRefRejectedImmediately`. Otherwise loop: while `used + n > limit`, try `evictLRU()`; if nothing is evictable, wait on the cond with a context-cancellation goroutine that broadcasts so the wait cannot outlive the deadline. On context done, return `ctx.Err()` wrapped with `ErrBudgetExceeded`.
- `evictLRU()` picks the least-recently-used entry with zero leases, removes the file, and subtracts its size. Returns false when nothing is evictable.
- `Release(n)` subtracts and broadcasts.

`artifact/cache/index.go`: a small SQLite-free implementation is preferable — use a plain JSON file rewritten atomically on close plus in-memory state, since the walk already rebuilds on start. Name the file `index.db` regardless so the recovery test's `os.Remove` matches. If you prefer real SQLite, the module already depends on a driver through grove; either is acceptable so long as the recovery test passes.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./artifact/cache/ -race -v`
Expected: PASS — all seven tests, including under `-race`.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add artifact/cache/
git commit -m "feat(artifact): add content-addressed staging cache with budget and leases"
```

---

## Phase 4 — Staging Middleware and Handler API

### Task 13: Input declarations

**Files:**
- Create: `artifact/input.go`
- Modify: `job/options.go`, `job/definition.go`
- Test: `artifact/input_test.go`

**Interfaces:**
- Produces:
  - `type StageMode int` with `StageModePath`, `StageModeLazy`.
  - `type InputSpec struct { Name string; Required bool; MaxSize int64; Mode StageMode }`
  - `func Input(name string, opts ...InputOption) InputSpec`
  - `InputOption`s: `Required`, `MaxSize(int64)`, `StageAsPath`, `StageLazy`.
  - `func (s InputSpec) Validate() error`
- Modify `job.Options` to add `Inputs []artifact.InputSpec`, and add `job.Option` constructor `job.WithArtifactInputs(specs ...artifact.InputSpec) Option`.

- [ ] **Step 1: Write the failing test**

```go
package artifact_test

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
)

func TestInputDefaults(t *testing.T) {
	in := artifact.Input("model")
	if in.Name != "model" {
		t.Fatalf("Name = %q, want %q", in.Name, "model")
	}
	if in.Required {
		t.Fatal("inputs must be optional by default")
	}
	if in.Mode != artifact.StageModePath {
		t.Fatal("default mode must be StageModePath")
	}
}

func TestInputOptions(t *testing.T) {
	in := artifact.Input("model",
		artifact.Required,
		artifact.MaxSize(8<<30),
		artifact.StageLazy)
	if !in.Required {
		t.Fatal("Required not applied")
	}
	if in.MaxSize != 8<<30 {
		t.Fatalf("MaxSize = %d, want %d", in.MaxSize, int64(8)<<30)
	}
	if in.Mode != artifact.StageModeLazy {
		t.Fatal("StageLazy not applied")
	}
}

func TestInputValidate(t *testing.T) {
	tests := []struct {
		name    string
		spec    artifact.InputSpec
		wantErr bool
	}{
		{"valid", artifact.Input("model"), false},
		{"empty name", artifact.Input(""), true},
		{"negative max size", artifact.Input("m", artifact.MaxSize(-1)), true},
		{"path traversal in name", artifact.Input("../etc/passwd"), true},
		{"slash in name", artifact.Input("a/b"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./artifact/ -run TestInput -v`
Expected: FAIL — `undefined: Input`.

- [ ] **Step 3: Implement**

Write `artifact/input.go`. `Validate` rejects an empty name, a negative `MaxSize`, and any name containing `/`, `\`, or `..` — the name becomes a path component in the ephemeral key and a filename in the staging directory, so traversal must be impossible.

In `job/options.go`, add `Inputs []artifact.InputSpec` to `Options` and:

```go
// WithArtifactInputs declares the artifact inputs a job consumes. The
// engine validates every binding against these declarations at enqueue
// and stages them before the handler runs.
func WithArtifactInputs(specs ...artifact.InputSpec) Option {
	return func(o *Options) {
		o.Inputs = append(o.Inputs, specs...)
	}
}
```

Confirm `job` importing `artifact` does not create a cycle: `artifact` imports only `id` and the root package.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./artifact/ ./job/ -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add artifact/input.go artifact/input_test.go job/options.go
git commit -m "feat(artifact): add input declarations and job option"
```

---

### Task 14: Accessor and staging middleware

**Files:**
- Create: `artifact/accessor.go`
- Create: `artifact/staging/doc.go`, `artifact/staging/middleware.go`, `artifact/staging/accessor.go`, `artifact/staging/bind.go`
- Test: `artifact/staging/middleware_test.go`

**Interfaces:**
- Consumes: `Service` (Task 10), `Cache` (Task 12), `InputSpec` (Task 13), `middleware.Middleware`, `job.Job`.
- Produces:
  - In `artifact`: `type Accessor interface { Path(name string) string; Open(ctx, name string) (io.ReadCloser, error); Ref(name string) (Ref, bool); Create(ctx, name string, opts ...CreateOption) (*CommitWriter, error) }`, `func From(ctx) Accessor`, `func WithAccessor(ctx, Accessor) context.Context`.
  - In `artifact/staging`: `func Middleware(svc *artifact.Service, c *cache.Cache, specs func(jobName string) []artifact.InputSpec) middleware.Middleware`.
  - Binding carried on the job: `staging.Bindings` encoded into a job metadata field.

- [ ] **Step 1: Decide where bindings live, then write the failing test**

Bindings must reach the worker, so they are persisted with the job. `job.Job` has no metadata column, so add one:

- Modify `job/job.go`: add `ArtifactBindings []byte \`json:"artifact_bindings,omitempty"\`` .
- Add a migration per backend adding `artifact_bindings BYTEA` / `BLOB` / a Mongo field / a Redis hash field.

This is a schema change to an existing table, so it is its own migration with a version above Task 5's.

Create `artifact/staging/middleware_test.go`:

```go
package staging_test

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/artifact/staging"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

func TestMiddlewareStagesDeclaredInput(t *testing.T) {
	ctx := context.Background()
	b := artifacttest.NewBackend()
	b.Put("models", "tower.ifc", []byte("ifcdata"))
	st := memory.New()
	svc := artifact.NewService(st, b, artifact.WithEphemeralPrefix("ephemeral"),
		artifact.WithDefaultBucket("dispatch"))
	c, err := cache.New(t.TempDir(), b, cache.WithBudget(1<<20))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	defer c.Close()

	ref, err := svc.Register(ctx, "models", "tower.ifc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	specs := func(string) []artifact.InputSpec {
		return []artifact.InputSpec{artifact.Input("model", artifact.Required)}
	}
	mw := staging.Middleware(svc, c, specs)

	j := &job.Job{ID: id.NewJobID(), Name: "tessellate"}
	if err := staging.SetBindings(j, map[string]artifact.Ref{"model": ref}); err != nil {
		t.Fatalf("SetBindings: %v", err)
	}

	var gotPath string
	err = mw(ctx, j, func(ctx context.Context) error {
		gotPath = artifact.From(ctx).Path("model")
		return nil
	})
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}
	data, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatalf("staged file unreadable: %v", err)
	}
	if string(data) != "ifcdata" {
		t.Fatalf("staged content = %q, want %q", data, "ifcdata")
	}
}

func TestMiddlewareMissingRequiredInput(t *testing.T) {
	ctx := context.Background()
	b := artifacttest.NewBackend()
	svc := artifact.NewService(memory.New(), b)
	c, _ := cache.New(t.TempDir(), b, cache.WithBudget(1<<20))
	defer c.Close()

	specs := func(string) []artifact.InputSpec {
		return []artifact.InputSpec{artifact.Input("model", artifact.Required)}
	}
	mw := staging.Middleware(svc, c, specs)

	j := &job.Job{ID: id.NewJobID(), Name: "tessellate"}
	called := false
	err := mw(ctx, j, func(context.Context) error { called = true; return nil })
	if err == nil {
		t.Fatal("missing required input must fail the job")
	}
	if called {
		t.Fatal("handler must not run when a required input is unbound")
	}
}

func TestMiddlewareDeletedInputFailsFast(t *testing.T) {
	ctx := context.Background()
	b := artifacttest.NewBackend()
	svc := artifact.NewService(memory.New(), b)
	c, _ := cache.New(t.TempDir(), b, cache.WithBudget(1<<20))
	defer c.Close()

	specs := func(string) []artifact.InputSpec {
		return []artifact.InputSpec{artifact.Input("model", artifact.Required)}
	}
	mw := staging.Middleware(svc, c, specs)

	j := &job.Job{ID: id.NewJobID(), Name: "tessellate"}
	staging.SetBindings(j, map[string]artifact.Ref{
		"model": {ID: id.NewArtifactID(), Bucket: "models", Key: "gone.ifc"},
	})

	err := mw(ctx, j, func(context.Context) error { return nil })
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("staging a deleted input = %v, want ErrNotFound (permanent, fail fast)", err)
	}
}

func TestMiddlewareReleasesLeasesOnHandlerError(t *testing.T) {
	ctx := context.Background()
	b := artifacttest.NewBackend()
	b.Put("m", "a", []byte("0123456789"))
	st := memory.New()
	svc := artifact.NewService(st, b)
	c, _ := cache.New(t.TempDir(), b, cache.WithBudget(10))
	defer c.Close()

	ref, _ := svc.Register(ctx, "m", "a")
	specs := func(string) []artifact.InputSpec {
		return []artifact.InputSpec{artifact.Input("in")}
	}
	mw := staging.Middleware(svc, c, specs)

	handlerErr := errors.New("boom")
	for i := 0; i < 3; i++ {
		j := &job.Job{ID: id.NewJobID(), Name: "j"}
		staging.SetBindings(j, map[string]artifact.Ref{"in": ref})
		err := mw(ctx, j, func(context.Context) error { return handlerErr })
		if !errors.Is(err, handlerErr) {
			t.Fatalf("run %d: middleware returned %v, want the handler error", i, err)
		}
	}
	// If leases leaked, the third run would have blocked on the 10-byte budget.
}

func TestAccessorCreateLinksToJobAndAttempt(t *testing.T) {
	ctx := context.Background()
	b := artifacttest.NewBackend()
	st := memory.New()
	svc := artifact.NewService(st, b, artifact.WithEphemeralPrefix("ephemeral"),
		artifact.WithDefaultBucket("dispatch"))
	c, _ := cache.New(t.TempDir(), b, cache.WithBudget(1<<20))
	defer c.Close()

	mw := staging.Middleware(svc, c, func(string) []artifact.InputSpec { return nil })
	j := &job.Job{ID: id.NewJobID(), Name: "split", RetryCount: 2}

	err := mw(ctx, j, func(ctx context.Context) error {
		w, err := artifact.From(ctx).Create(ctx, "page-1.png")
		if err != nil {
			return err
		}
		if _, err := io.WriteString(w, "pixels"); err != nil {
			return err
		}
		_, err = w.Commit(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: j.ID.String()}
	links, err := st.ListLinks(ctx, owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}
	if len(links) != 1 {
		t.Fatalf("got %d links, want 1", len(links))
	}
	if links[0].Attempt != 2 {
		t.Fatalf("link attempt = %d, want 2 (from job.RetryCount)", links[0].Attempt)
	}
	if links[0].Role != artifact.RoleOutput {
		t.Fatalf("link role = %q, want output", links[0].Role)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./artifact/staging/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

`artifact/accessor.go` — the `Accessor` interface, a context key, `From` (returns a no-op accessor when unset, so a handler calling `artifact.From(ctx).Path("x")` on a job with no artifacts gets `""` rather than a nil panic), and `WithAccessor`.

`artifact/staging/bind.go` — `SetBindings(*job.Job, map[string]artifact.Ref) error` and `GetBindings(*job.Job) (map[string]artifact.Ref, error)`, JSON-encoding into `job.ArtifactBindings`.

`artifact/staging/middleware.go` — the middleware:

1. Read specs for `j.Name` and bindings from `j`.
2. Reject a binding with no matching spec; reject a missing `Required` spec. Both are permanent failures.
3. For each spec, check `MaxSize` against `ref.Size` and fail with `ErrSizeExceeded` if exceeded.
4. For `StageModePath`, call `cache.Stage`. Collect every `release` into a slice and `defer` releasing all of them — this is what `TestMiddlewareReleasesLeasesOnHandlerError` proves. Release must happen whether the handler returns, errors, or panics.
5. If `cache.Stage` returns something wrapping `artifact.ErrNotFound`, return it unwrapped enough that `errors.Is` still matches — the executor's retry policy depends on it.
6. When staging yields a hash and the stored artifact has none, call `svc.Store().UpdateArtifact` to persist it. Failure here is logged, never fatal.
7. Build the accessor with `owner = {OwnerJob, j.ID.String()}` and `attempt = j.RetryCount`, put it in the context, call `next`.

`artifact/staging/accessor.go` — the concrete accessor holding staged paths, refs, the service, owner, and attempt. `Create` delegates to `svc.Create(ctx, owner, attempt, name, opts...)`. `Open` on a lazily-staged input delegates to `svc.Open`.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./artifact/staging/ -race -v`
Expected: PASS — five tests.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add artifact/accessor.go artifact/staging/ job/job.go store/
git commit -m "feat(artifact): add accessor and staging middleware"
```

---

### Task 15: Engine wiring — register validation and enqueue binding

**Files:**
- Modify: `engine/engine.go`
- Test: `engine/artifact_test.go`

**Interfaces:**
- Consumes: everything from Tasks 10–14.
- Produces: `engine.WithArtifacts(svc *artifact.Service, c *cache.Cache) Option`, and `artifact.Bind(name string, ref artifact.Ref) EnqueueOption`.

- [ ] **Step 1: Write the failing test**

```go
package engine_test

// TestRegisterRejectsUnstageableDefinition asserts a definition whose
// declared MaxSize total exceeds the cache budget fails at Register.
// TestEnqueueRejectsOversizeBinding asserts a bound ref larger than the
// declaration's MaxSize is rejected at Enqueue, not at run time.
// TestEnqueueRejectsUnknownBindingName asserts binding a name with no
// matching declaration is an error.
// TestEndToEndStageAndCommit runs a real job through the pool with a
// memory store and the test backend, asserting the input was staged and
// the output artifact was linked.
```

Write these four out fully, following the existing style in `engine/engine_test.go` for constructing an engine with a memory store.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./engine/ -run 'TestRegister|TestEnqueue|TestEndToEnd' -v`
Expected: FAIL.

- [ ] **Step 3: Implement**

- `engine.WithArtifacts(svc, cache)` stores both and appends `staging.Middleware(...)` to the middleware chain, passing a spec lookup closure backed by the job registry.
- `Register` validates every definition's `Inputs` with `InputSpec.Validate()`, rejects duplicate names, and rejects a definition whose summed `MaxSize` exceeds the cache budget. Expose the budget from `cache.Cache` as `Budget() int64` for this check.
- `Enqueue` accepts `artifact.Bind` options, validates each against the definition's declarations, and calls `staging.SetBindings`.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./engine/ -race -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add engine/
git commit -m "feat(artifact): wire artifacts into engine register and enqueue"
```

---

## Phase 5 — Extension Wiring

### Task 16: Forge extension configuration and DI resolution

**Files:**
- Create: `extension/artifact.go`
- Modify: `extension/config.go`, `extension/options.go`, `extension/extension.go`
- Test: `extension/artifact_test.go`

**Interfaces:**
- Produces: `extension.WithArtifactBackend(artifact.Backend) ExtOption`, `ArtifactConfig` struct, `(*Extension).resolveArtifactBackend(forge.App) (artifact.Backend, error)`.

- [ ] **Step 1: Write the failing test**

Test that resolution honours the three-tier precedence — programmatic beats named config beats auto-discovery — and that a missing Trove leaves artifacts disabled without erroring. Follow the existing extension test style in `extension/extension_test.go`.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./extension/ -run TestArtifact -v`
Expected: FAIL.

- [ ] **Step 3: Implement**

Add to `extension/config.go`:

```go
// ArtifactConfig configures the artifact plane.
type ArtifactConfig struct {
	Enabled         bool          `yaml:"enabled" json:"enabled"`
	TroveStore      string        `yaml:"trove_store" json:"trove_store"`
	Bucket          string        `yaml:"bucket" json:"bucket"`
	EphemeralPrefix string        `yaml:"ephemeral_prefix" json:"ephemeral_prefix"`
	Retention       time.Duration `yaml:"retention" json:"retention"`
	PurgeGrace      time.Duration `yaml:"purge_grace" json:"purge_grace"`
	Cache           CacheConfig   `yaml:"cache" json:"cache"`
}

// CacheConfig configures the worker-local staging cache.
type CacheConfig struct {
	Dir    string `yaml:"dir" json:"dir"`
	Budget int64  `yaml:"budget" json:"budget"`
}
```

Add `Artifacts ArtifactConfig` to `Config`, defaults in `DefaultConfig` (`EphemeralPrefix: "ephemeral"`, `Retention: 168h`, `PurgeGrace: 24h`, `Cache.Dir: "/var/lib/dispatch/cache"`), and merge handling in `mergeWithDefaults` and `mergeConfigurations` matching the existing style.

Write `extension/artifact.go` with `resolveArtifactBackend` exactly as specified in the design doc §5, plus construction of the `Service` and `Cache` and their registration into DI via `vessel.Provide`. Call it from `init()` in `extension.go` after the store is resolved and before `engine.Build`, appending `engine.WithArtifacts(...)` to `engOpts` when a backend was found.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./extension/ -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add extension/
git commit -m "feat(artifact): resolve Trove backend from Forge DI and wire the extension"
```

---

## Phase 6 — Sweeper

### Task 17: Sweeper with two-phase deletion

**Files:**
- Create: `artifact/sweeper/doc.go`, `artifact/sweeper/sweeper.go`
- Modify: `ext/` — add `ArtifactSweptHook` and `EmitArtifactSwept`
- Test: `artifact/sweeper/sweeper_test.go`

**Interfaces:**
- Produces: `func New(store artifact.Store, b artifact.Backend, opts ...Option) *Sweeper`, `(*Sweeper).SweepOnce(ctx) (Result, error)`, `(*Sweeper).PurgeOnce(ctx) (Result, error)`, `(*Sweeper).Start(ctx) error`, `(*Sweeper).Stop(ctx) error`.

- [ ] **Step 1: Write the failing tests**

The essential cases:

```go
// TestSweeperNeverDeletesDurable — property test. Generate a random
// sequence of register/create/commit/fail/retry operations, run
// SweepOnce and PurgeOnce repeatedly, assert every durable artifact is
// still retrievable and its bytes still readable from the backend.
//
// TestSweeperTwoPhase — an eligible ephemeral artifact is soft-deleted
// by SweepOnce, its bytes still readable; PurgeOnce with a grace longer
// than its age leaves it; PurgeOnce with zero grace removes the bytes
// and the row.
//
// TestSweeperSkipsLiveOwner — an ephemeral artifact linked to a running
// job is never swept.
//
// TestSweeperDryRun — DryRun reports candidates and changes nothing.
//
// TestSweeperDisabled — with the kill switch set, SweepOnce is a no-op.
```

Write the property test with a fixed seed so failures reproduce; `math/rand.New(rand.NewSource(1))`.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./artifact/sweeper/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

- `SweepOnce` calls `store.SweepEphemeral` then `store.SweepOrphans`, emitting `EmitArtifactSwept` per artifact and incrementing metrics.
- `PurgeOnce` calls `store.ListPurgeable`, then for each: `backend.Delete` (missing is not an error), then `store.PurgeArtifact`. Backend failure logs and skips that artifact so the next pass retries it.
- `Start` runs a ticker loop guarded by a leadership check supplied as `WithLeaderCheck(func() bool)`, so only the elected leader sweeps.
- `WithEnabled(bool)` is the kill switch; `WithDryRun(bool)`, `WithRetention`, `WithPurgeGrace`, `WithBatchSize`, `WithInterval`.
- Metrics `dispatch_artifacts_swept_total` and `dispatch_artifacts_bytes_reclaimed` via the existing metric factory pattern in `observability/`.

Add the hook to `ext/` following the shape of the existing lifecycle hooks.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./artifact/sweeper/ -race -v`
Expected: PASS

- [ ] **Step 5: Wire into the extension and commit**

Start the sweeper from `(*Extension).Start` when artifacts are enabled, with `WithLeaderCheck` bound to the cluster leadership state, and stop it in `(*Extension).Stop`.

```bash
make lint
go test ./... -short
git add artifact/sweeper/ ext/ extension/
git commit -m "feat(artifact): add leader-only two-phase lifecycle sweeper"
```

---

### Task 18: Documentation

**Files:**
- Create: `docs/content/docs/artifacts.mdx`
- Modify: `README.md` — add `artifact` to the package index table
- Modify: `doc.go` — mention the artifact plane

- [ ] **Step 1: Write the docs page**

Cover: what an artifact is, durable versus ephemeral, declaring inputs, creating outputs, `IfAbsent` resumption, the staging cache and its budget, Trove wiring in Forge, retention and sweeping, and the full YAML config block. Follow the structure and tone of the existing pages under `docs/content/docs/`.

- [ ] **Step 2: Update the package index**

Add to the README table:

```
| `artifact` | Tracked object-storage artifacts — declared inputs, imperative outputs, staging cache, lifecycle sweeping |
```

- [ ] **Step 3: Verify and commit**

```bash
make lint
go test ./... -short
git add docs/ README.md doc.go
git commit -m "docs: document the artifact plane"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Tasks |
|---|---|
| §3 Package layout | 2, 9, 12, 14 |
| §4 Data model | 1, 3, 4, 5, 6, 7, 8 |
| §5 Trove extension integration | 11, 16 |
| §6 Handler API | 10, 13, 14 |
| §7 Staging cache | 12 |
| §8 Lifecycle sweeping | 5 (SQL), 17 (driver) |
| §9 Error handling | 10, 12, 14, 15 |
| §10 Testing | 4, 9, 12, 14, 17 |
| §11 Backward compatibility | 16 |
| §12 Phasing | Phase headings |

**Gap found and closed:** the spec's handler API implies bindings travel with the job, but `job.Job` had no field for them. Task 14 Step 1 adds `ArtifactBindings []byte` plus per-backend migrations. Without this the middleware has no way to learn what was bound.

**Type consistency:** `Ref`, `OwnerRef`, `Role`, `Lifecycle`, `InputSpec`, `Accessor`, `Service`, `Cache`, and `Backend` are used with identical signatures across Tasks 2–17. `Create` takes `(ctx, owner, attempt, name, opts...)` on `Service` and `(ctx, name, opts...)` on `Accessor` — the accessor closes over owner and attempt, which is stated in Task 14.
