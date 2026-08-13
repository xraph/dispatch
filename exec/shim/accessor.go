package shim

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/store/memory"
)

// accessor is the artifact.Accessor handed to a handler running inside the
// shim. It closes over the service, the owner, and the attempt — mirroring
// artifact/staging's own accessor — but resolves inputs against the
// Request's InputSlots instead of a staged-inputs map, since the shim
// receives its request as a wire value rather than building one from a
// staging plan.
type accessor struct {
	svc     *artifact.Service
	req     *exec.Request
	owner   artifact.OwnerRef
	attempt int
}

var _ artifact.Accessor = (*accessor)(nil)

// newAccessorService builds the artifact service a sandboxed handler runs
// against: a real artifact.Service over a local directory and an in-memory
// store.
//
// The handler therefore exercises the genuine Create/Commit/IfAbsent code
// path and cannot tell which side of the boundary it is on, while holding
// no backend credential and reaching no database. The in-memory rows are
// not a record of truth; they exist so Commit can return a Ref and
// Existing can answer within the attempt. The worker outside verifies what
// actually landed in the directory.
func newAccessorService(req *exec.Request) *artifact.Service {
	return artifact.NewService(
		memory.New(),
		NewLocalFS(req.OutputDir),
		artifact.WithDefaultBucket("shim"),
	)
}

// newAccessor builds the Accessor for req, scoped to owner and attempt.
func newAccessor(svc *artifact.Service, req *exec.Request, owner artifact.OwnerRef, attempt int) *accessor {
	return &accessor{svc: svc, req: req, owner: owner, attempt: attempt}
}

// Path returns the local file path of a declared input, or an empty
// string when the request carries no such input.
func (a *accessor) Path(name string) string {
	for _, in := range a.req.Inputs {
		if in.Name == name {
			return filepath.Join(a.req.InputDir, in.Path)
		}
	}

	return ""
}

// Open opens a declared input from local disk.
func (a *accessor) Open(_ context.Context, name string) (io.ReadCloser, error) {
	path := a.Path(name)
	if path == "" {
		return nil, fmt.Errorf("dispatch/exec/shim: open %q: %w", name, artifact.ErrUnbound)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("dispatch/exec/shim: open %q: %w", name, err)
	}

	return f, nil
}

// Ref always reports no bound ref.
//
// exec.InputSlot carries only a Name and a Path — inputs are not staged
// through the artifact plane for out-of-process rungs yet, so there is no
// Ref to hand back. This is a known Phase 2 limitation, not a bug: a
// handler that calls Ref for a declared input gets false here even though
// Path resolves.
func (a *accessor) Ref(string) (artifact.Ref, bool) {
	return artifact.Ref{}, false
}

// Create begins writing an output owned by the running job.
func (a *accessor) Create(
	ctx context.Context,
	name string,
	opts ...artifact.CreateOption,
) (*artifact.CommitWriter, error) {
	return a.svc.Create(ctx, a.owner, a.attempt, name, opts...)
}

// Existing reports an artifact a previous attempt committed under this
// name, letting a retried handler skip work already done.
func (a *accessor) Existing(ctx context.Context, name string) (artifact.Ref, bool) {
	ref, err := a.svc.FindExisting(ctx, a.owner, name)
	if err != nil {
		return artifact.Ref{}, false
	}

	return ref, true
}

// seedPriorOutputs inserts req.PriorOutputs into svc's in-memory store as
// links for owner, so Existing and IfAbsent answer correctly for work an
// earlier attempt finished.
//
// Each prior output is seeded at Attempt 0. FindLinkByName returns the
// link with the highest attempt for a given name, and there is exactly one
// seeded link per name, so 0 only has to be lower than the attempt
// currently running — never equal to it — so a fresh Create in this
// attempt does not collide with the seed.
func seedPriorOutputs(ctx context.Context, svc *artifact.Service, owner artifact.OwnerRef, prior []exec.PriorOutput) error {
	store := svc.Store()

	for _, po := range prior {
		a := &artifact.Artifact{
			ID:          po.Ref.ID,
			Backend:     po.Ref.Backend,
			Bucket:      po.Ref.Bucket,
			Key:         po.Ref.Key,
			Size:        po.Ref.Size,
			ContentHash: po.Ref.ContentHash,
			Lifecycle:   artifact.Ephemeral,
			CreatedAt:   time.Now().UTC(),
		}
		link := &artifact.Link{
			ArtifactID: po.Ref.ID,
			OwnerKind:  owner.Kind,
			OwnerID:    owner.ID,
			Role:       artifact.RoleOutput,
			Name:       po.Name,
			Attempt:    0,
			CreatedAt:  time.Now().UTC(),
		}

		if err := store.CreateArtifact(ctx, a, link); err != nil {
			return fmt.Errorf("dispatch/exec/shim: seed prior output %q: %w", po.Name, err)
		}
	}

	return nil
}
