package contract

import (
	"context"
	"errors"
	"strings"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

type artifactReadFailure struct {
	artifact.Store
	artifact.RecordReader
	artifact.PageLister
	fail     bool
	deadline bool
}

func (s *artifactReadFailure) readError(ctx context.Context) error {
	_, s.deadline = ctx.Deadline()
	if s.fail {
		return errors.New("private artifact store credentials")
	}
	return nil
}
func (s *artifactReadFailure) ListArtifactsPage(ctx context.Context, _ artifact.PageOpts) (artifact.Page, error) {
	if err := s.readError(ctx); err != nil {
		return artifact.Page{}, err
	}
	return artifact.Page{Artifacts: []*artifact.Artifact{}, NextCursor: "scan-next", Complete: false}, nil
}
func (s *artifactReadFailure) GetArtifactRecord(ctx context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	if err := s.readError(ctx); err != nil {
		return nil, err
	}
	return s.RecordReader.GetArtifactRecord(ctx, artifactID)
}
func (s *artifactReadFailure) GetArtifact(ctx context.Context, artifactID id.ArtifactID) (*artifact.Artifact, error) {
	if err := s.readError(ctx); err != nil {
		return nil, err
	}
	return s.Store.GetArtifact(ctx, artifactID)
}
func (s *artifactReadFailure) ListLinks(ctx context.Context, owner artifact.OwnerRef) ([]*artifact.Link, error) {
	if err := s.readError(ctx); err != nil {
		return nil, err
	}
	return s.Store.ListLinks(ctx, owner)
}
func TestArtifactReadsPropagateFailureAndIncompletePages(t *testing.T) {
	base := memory.New()
	custom := &artifactReadFailure{Store: base, RecordReader: base, PageLister: base, fail: true}
	d := contractDeps(t, memory.New(), engine.WithArtifacts(artifact.NewService(custom, artifacttest.NewBackend()), nil))
	owner := seedJob(t, d, "owner", job.StateCompleted, "", "", "default")
	input := IDInput{ID: id.NewArtifactID().String()}
	ctx := context.Background()
	p := fc.Principal{}
	reads := map[string]func() error{
		"list":    func() error { _, err := artifactsListHandler(d)(ctx, ArtifactsListInput{}, p); return err },
		"get":     func() error { _, err := artifactsGetHandler(d)(ctx, input, p); return err },
		"links":   func() error { _, err := artifactsForJobHandler(d)(ctx, IDInput{ID: owner.ID.String()}, p); return err },
		"presign": func() error { _, err := artifactsPresignHandler(d)(ctx, input, p); return err },
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			err := read()
			if !errors.Is(err, fc.ErrInternal) || strings.Contains(err.Error(), "credentials") || !custom.deadline {
				t.Fatalf("error=%v deadline=%v", err, custom.deadline)
			}
		})
	}
	custom.fail = false
	page, err := artifactsListHandler(d)(ctx, ArtifactsListInput{}, p)
	if err != nil || page.Complete || page.NextCursor == nil || *page.NextCursor != "scan-next" || page.Items == nil {
		t.Fatalf("incomplete=%+v, %v", page, err)
	}
}
