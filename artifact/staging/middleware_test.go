package staging_test

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/artifact/staging"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

type harness struct {
	svc     *artifact.Service
	cache   *cache.Cache
	backend *artifacttest.Backend
	store   artifact.Store
}

func newHarness(t *testing.T, budget int64) *harness {
	t.Helper()

	b := artifacttest.NewBackend()
	st := memory.New()
	svc := artifact.NewService(st, b,
		artifact.WithEphemeralPrefix("ephemeral"),
		artifact.WithDefaultBucket("dispatch"))

	c, err := cache.New(t.TempDir(), b, cache.WithBudget(budget))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	t.Cleanup(func() {
		if cerr := c.Close(); cerr != nil {
			t.Errorf("cache close: %v", cerr)
		}
	})

	return &harness{svc: svc, cache: c, backend: b, store: st}
}

func specsFor(specs ...artifact.InputSpec) staging.SpecLookup {
	return func(string) []artifact.InputSpec { return specs }
}

func newJob() *job.Job {
	return &job.Job{ID: id.NewJobID(), Name: "tessellate"}
}

func TestMiddlewareStagesDeclaredInput(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)
	h.backend.Put("models", "tower.ifc", []byte("ifcdata"))

	ref, err := h.svc.Register(ctx, "models", "tower.ifc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	mw := staging.Middleware(h.svc, h.cache,
		specsFor(artifact.Input("model", artifact.Required)))

	j := newJob()
	if serr := staging.SetBindings(j, staging.Bindings{"model": ref}); serr != nil {
		t.Fatalf("SetBindings: %v", serr)
	}

	var gotPath string

	err = mw(ctx, j, func(ctx context.Context) error {
		gotPath = artifact.From(ctx).Path("model")

		return nil
	})
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}

	if gotPath == "" {
		t.Fatal("handler saw no staged path for a declared input")
	}

	data, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatalf("staged file unreadable: %v", err)
	}

	if string(data) != "ifcdata" {
		t.Fatalf("staged content = %q, want %q", data, "ifcdata")
	}
}

func TestMiddlewareRecordsHashLearnedDuringStaging(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)
	h.backend.Put("models", "hashme.ifc", []byte("bytes"))

	ref, err := h.svc.Register(ctx, "models", "hashme.ifc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if ref.ContentHash != "" {
		t.Fatal("Register should not have hashed")
	}

	mw := staging.Middleware(h.svc, h.cache, specsFor(artifact.Input("model")))

	j := newJob()
	if serr := staging.SetBindings(j, staging.Bindings{"model": ref}); serr != nil {
		t.Fatalf("SetBindings: %v", serr)
	}

	if merr := mw(ctx, j, func(context.Context) error { return nil }); merr != nil {
		t.Fatalf("middleware: %v", merr)
	}

	a, err := h.store.GetArtifact(ctx, ref.ID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}

	if a.ContentHash == "" {
		t.Fatal("staging must record the hash it computed during the download")
	}
}

func TestMiddlewareMissingRequiredInput(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)

	mw := staging.Middleware(h.svc, h.cache,
		specsFor(artifact.Input("model", artifact.Required)))

	called := false

	err := mw(ctx, newJob(), func(context.Context) error {
		called = true

		return nil
	})
	if !errors.Is(err, artifact.ErrUnbound) {
		t.Fatalf("missing required input = %v, want ErrUnbound", err)
	}

	if called {
		t.Fatal("handler must not run when a required input is unbound")
	}
}

func TestMiddlewareRejectsUndeclaredBinding(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)
	h.backend.Put("models", "x.ifc", []byte("data"))

	ref, err := h.svc.Register(ctx, "models", "x.ifc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	mw := staging.Middleware(h.svc, h.cache, specsFor(artifact.Input("model")))

	j := newJob()
	if serr := staging.SetBindings(j, staging.Bindings{"surprise": ref}); serr != nil {
		t.Fatalf("SetBindings: %v", serr)
	}

	if err := mw(ctx, j, func(context.Context) error { return nil }); !errors.Is(err, artifact.ErrUndeclared) {
		t.Fatalf("undeclared binding = %v, want ErrUndeclared", err)
	}
}

func TestMiddlewareRejectsOversizeBinding(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)
	h.backend.Put("models", "big.ifc", make([]byte, 100))

	ref, err := h.svc.Register(ctx, "models", "big.ifc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	mw := staging.Middleware(h.svc, h.cache,
		specsFor(artifact.Input("model", artifact.MaxSize(10))))

	j := newJob()
	if serr := staging.SetBindings(j, staging.Bindings{"model": ref}); serr != nil {
		t.Fatalf("SetBindings: %v", serr)
	}

	if err := mw(ctx, j, func(context.Context) error { return nil }); !errors.Is(err, artifact.ErrSizeExceeded) {
		t.Fatalf("oversize binding = %v, want ErrSizeExceeded", err)
	}
}

// TestMiddlewareDeletedInputFailsFast pins the retry-classification
// behaviour: a job whose input no longer exists must fail permanently
// rather than burn its retry budget on a fetch that can never succeed.
func TestMiddlewareDeletedInputFailsFast(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)

	mw := staging.Middleware(h.svc, h.cache,
		specsFor(artifact.Input("model", artifact.Required)))

	j := newJob()
	if serr := staging.SetBindings(j, staging.Bindings{
		"model": {ID: id.NewArtifactID(), Bucket: "models", Key: "gone.ifc"},
	}); serr != nil {
		t.Fatalf("SetBindings: %v", serr)
	}

	err := mw(ctx, j, func(context.Context) error { return nil })
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("staging a deleted input = %v, want ErrNotFound", err)
	}
}

// TestMiddlewareDeniedInput is the deleted-input test's sibling. The input
// exists but cannot be read, which is just as permanent.
//
// Before the backend classified permission failures this error reached the
// executor carrying no artifact sentinel at all. It now arrives classified.
// Acting on it is still the executor's to do: handleFailure counts retries
// without consulting the error, so neither this nor a deleted input fails
// fast yet.
func TestMiddlewareDeniedInput(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)
	h.backend.Put("models", "secret.ifc", []byte("classified"))
	h.backend.DenyOpen = true

	mw := staging.Middleware(h.svc, h.cache,
		specsFor(artifact.Input("model", artifact.Required)))

	j := newJob()
	if serr := staging.SetBindings(j, staging.Bindings{
		"model": {ID: id.NewArtifactID(), Bucket: "models", Key: "secret.ifc"},
	}); serr != nil {
		t.Fatalf("SetBindings: %v", serr)
	}

	err := mw(ctx, j, func(context.Context) error { return nil })
	if !errors.Is(err, dispatch.ErrPermanent) {
		t.Fatalf("staging a forbidden input = %v, want ErrPermanent", err)
	}

	if !errors.Is(err, artifact.ErrPermissionDenied) {
		t.Fatalf("staging a forbidden input = %v, want ErrPermissionDenied", err)
	}
}

// TestMiddlewareReleasesLeasesOnHandlerError would deadlock on the third
// run if a failed job leaked its cache lease.
func TestMiddlewareReleasesLeasesOnHandlerError(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 10)
	h.backend.Put("m", "a", []byte("0123456789"))

	ref, err := h.svc.Register(ctx, "m", "a")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	mw := staging.Middleware(h.svc, h.cache, specsFor(artifact.Input("in")))

	handlerErr := errors.New("boom")

	for i := range 3 {
		j := newJob()
		if serr := staging.SetBindings(j, staging.Bindings{"in": ref}); serr != nil {
			t.Fatalf("SetBindings: %v", serr)
		}

		if rerr := mw(ctx, j, func(context.Context) error { return handlerErr }); !errors.Is(rerr, handlerErr) {
			t.Fatalf("run %d: middleware returned %v, want the handler error", i, rerr)
		}
	}
}

func TestMiddlewareReleasesLeasesOnPanic(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 10)
	h.backend.Put("m", "a", []byte("0123456789"))

	ref, err := h.svc.Register(ctx, "m", "a")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	mw := staging.Middleware(h.svc, h.cache, specsFor(artifact.Input("in")))

	run := func() {
		defer func() { _ = recover() }()

		j := newJob()
		_ = staging.SetBindings(j, staging.Bindings{"in": ref})

		_ = mw(ctx, j, func(context.Context) error { panic("handler exploded") })
	}

	for range 3 {
		run()
	}

	// A leaked lease would have exhausted the 10-byte budget by now.
	j := newJob()
	if serr := staging.SetBindings(j, staging.Bindings{"in": ref}); serr != nil {
		t.Fatalf("SetBindings: %v", serr)
	}

	if rerr := mw(ctx, j, func(context.Context) error { return nil }); rerr != nil {
		t.Fatalf("staging after panics: %v — leases leaked", rerr)
	}
}

func TestAccessorCreateLinksToJobAndAttempt(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)

	mw := staging.Middleware(h.svc, h.cache, specsFor())

	j := newJob()
	j.RetryCount = 2

	err := mw(ctx, j, func(ctx context.Context) error {
		w, cerr := artifact.From(ctx).Create(ctx, "page-1.png")
		if cerr != nil {
			return cerr
		}

		if _, werr := io.WriteString(w, "pixels"); werr != nil {
			return werr
		}

		_, cerr = w.Commit(ctx)

		return cerr
	})
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: j.ID.String()}

	links, err := h.store.ListLinks(ctx, owner)
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

func TestAccessorOpenReadsStagedFile(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)
	h.backend.Put("models", "read.ifc", []byte("streamed"))

	ref, err := h.svc.Register(ctx, "models", "read.ifc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	mw := staging.Middleware(h.svc, h.cache, specsFor(artifact.Input("model")))

	j := newJob()
	if serr := staging.SetBindings(j, staging.Bindings{"model": ref}); serr != nil {
		t.Fatalf("SetBindings: %v", serr)
	}

	err = mw(ctx, j, func(ctx context.Context) error {
		rc, oerr := artifact.From(ctx).Open(ctx, "model")
		if oerr != nil {
			return oerr
		}

		defer func() { _ = rc.Close() }()

		data, rerr := io.ReadAll(rc)
		if rerr != nil {
			return rerr
		}

		if string(data) != "streamed" {
			t.Fatalf("Open returned %q, want %q", data, "streamed")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}

	// A path-staged input is read from disk, so Open must not re-download.
	if h.backend.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1 — Open on a staged input must read local disk", h.backend.Opens())
	}
}

func TestLazyInputIsNotDownloadedUpFront(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1<<20)
	h.backend.Put("models", "lazy.csv", []byte("a,b,c"))

	ref, err := h.svc.Register(ctx, "models", "lazy.csv")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	mw := staging.Middleware(h.svc, h.cache,
		specsFor(artifact.Input("data", artifact.StageLazy)))

	j := newJob()
	if serr := staging.SetBindings(j, staging.Bindings{"data": ref}); serr != nil {
		t.Fatalf("SetBindings: %v", serr)
	}

	err = mw(ctx, j, func(ctx context.Context) error {
		if path := artifact.From(ctx).Path("data"); path != "" {
			t.Fatalf("lazy input has a path %q, want none", path)
		}

		if h.backend.Opens() != 0 {
			t.Fatalf("Opens() = %d before Open — lazy inputs must not pre-download", h.backend.Opens())
		}

		rc, oerr := artifact.From(ctx).Open(ctx, "data")
		if oerr != nil {
			return oerr
		}

		return rc.Close()
	})
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}

	if h.backend.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1", h.backend.Opens())
	}
}

func TestMiddlewareNoOpWhenServiceDisabled(t *testing.T) {
	ctx := context.Background()
	svc := artifact.NewService(memory.New(), nil)

	mw := staging.Middleware(svc, nil, specsFor(artifact.Input("model", artifact.Required)))

	called := false

	// With no backend the plane is off, so even a required declaration
	// must not block execution: Dispatch behaves as it did before.
	if err := mw(ctx, newJob(), func(context.Context) error { called = true; return nil }); err != nil {
		t.Fatalf("disabled service must be a pass-through, got %v", err)
	}

	if !called {
		t.Fatal("handler did not run with the artifact plane disabled")
	}
}

func TestBindingsRoundTrip(t *testing.T) {
	j := newJob()
	want := staging.Bindings{
		"model": {ID: id.NewArtifactID(), Bucket: "models", Key: "a.ifc", Size: 42},
	}

	if err := staging.SetBindings(j, want); err != nil {
		t.Fatalf("SetBindings: %v", err)
	}

	got, err := staging.GetBindings(j)
	if err != nil {
		t.Fatalf("GetBindings: %v", err)
	}

	if got["model"].Key != "a.ifc" || got["model"].Size != 42 {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}

	if total := staging.TotalBoundSize(got); total != 42 {
		t.Fatalf("TotalBoundSize = %d, want 42", total)
	}
}

func TestGetBindingsEmptyJob(t *testing.T) {
	got, err := staging.GetBindings(newJob())
	if err != nil {
		t.Fatalf("GetBindings on a job with no bindings: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("got %d bindings, want 0", len(got))
	}
}
