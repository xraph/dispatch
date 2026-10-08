package contract

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
)

type contractSigner struct {
	*artifacttest.Backend
	supported bool
	calls     int
	last      artifact.Ref
	ttl       time.Duration
	failure   error
	rawURL    string
	deadline  bool
}

func (s *contractSigner) SupportsPresign() bool { return s.supported }
func (s *contractSigner) PresignGet(ctx context.Context, ref artifact.Ref, ttl time.Duration) (string, error) {
	s.calls++
	s.last = ref
	s.ttl = ttl
	_, s.deadline = ctx.Deadline()
	if s.failure != nil {
		return "", s.failure
	}
	if s.rawURL != "" {
		return s.rawURL, nil
	}
	return fmt.Sprintf("https://objects.example/download/%s?signature=%d", ref.ID, s.calls), nil
}
func newContractSigner() *contractSigner {
	return &contractSigner{Backend: artifacttest.NewBackend(), supported: true}
}
func seedArtifact(t *testing.T, s artifact.Store, key string, lc artifact.Lifecycle, app, org string) *artifact.Artifact {
	t.Helper()
	expires := time.Now().UTC().Add(time.Hour)
	a := &artifact.Artifact{ID: id.NewArtifactID(), Backend: "memory", Bucket: "models", Key: key, Size: 12345, ContentHash: "sha256:abc", ContentType: "model/gltf-binary",
		Lifecycle: lc, ScopeAppID: app, ScopeOrgID: org, CreatedAt: time.Now().Add(-time.Hour), ExpiresAt: &expires}
	if err := s.CreateArtifact(context.Background(), a, nil); err != nil {
		t.Fatal(err)
	}
	return a
}
func runArtifactDomain(t *testing.T, s store.Store) {
	t.Helper()
	ctx := context.Background()
	signer := newContractSigner()
	d := contractDeps(t, s, engine.WithArtifacts(artifact.NewService(s, signer), nil))
	rows := []*artifact.Artifact{
		seedArtifact(t, s, "a", artifact.Durable, "app-a", "org-a"),
		seedArtifact(t, s, "b", artifact.Durable, "app-b", "org-b"),
		seedArtifact(t, s, "deleted", artifact.Ephemeral, "app-a", "org-a"),
		seedArtifact(t, s, "unscoped", artifact.Durable, "", ""),
	}
	if swept, err := s.SweepOrphans(ctx, time.Now(), 10); err != nil || len(swept) != 1 {
		t.Fatalf("sweep=%v, %v", swept, err)
	}
	p := fc.Principal{Claims: map[string]any{"scope_app_id": "app-a", "scope_org_id": "org-a"}}
	cursor := ""
	got := []string{}
	for pages := 0; ; pages++ {
		if pages > len(rows) {
			t.Fatal("cursor loop")
		}
		page, err := artifactsListHandler(d)(ctx, ArtifactsListInput{Limit: 2, Cursor: cursor, IncludeDeleted: true}, p)
		if err != nil || !page.Enabled || !page.PresignSupported || page.Items == nil || page.AsOf == "" {
			t.Fatalf("page=%+v, %v", page, err)
		}
		for _, a := range page.Items {
			got = append(got, a.ID)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	want := make([]string, 0, len(rows))
	for _, a := range rows {
		want = append(want, a.ID.String())
	}
	slices.Sort(want)
	slices.Reverse(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("operator-wide order=%v want=%v", got, want)
	}
	page, err := artifactsListHandler(d)(ctx, ArtifactsListInput{Lifecycle: artifact.Durable, ScopeAppID: "app-b", ScopeOrgID: "org-b"}, p)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != rows[1].ID.String() {
		t.Fatalf("filtered=%+v, %v", page, err)
	}
	live, err := artifactsListHandler(d)(ctx, ArtifactsListInput{}, p)
	if err != nil || len(live.Items) != 3 {
		t.Fatalf("live=%+v, %v", live, err)
	}
	detail, err := artifactsGetHandler(d)(ctx, IDInput{ID: rows[0].ID.String()}, p)
	if err != nil || detail.Artifact == nil || detail.Artifact.ContentHash == nil || *detail.Artifact.ContentHash != "sha256:abc" || !detail.Artifact.DownloadAvailable {
		t.Fatalf("detail=%+v, %v", detail, err)
	}
	deleted, err := artifactsGetHandler(d)(ctx, IDInput{ID: rows[2].ID.String()}, p)
	if err != nil || deleted.Artifact == nil || deleted.Artifact.DeletedAt == nil || deleted.Artifact.DownloadAvailable {
		t.Fatalf("deleted=%+v, %v", deleted, err)
	}
	if _, readErr := artifactsPresignHandler(d)(ctx, IDInput{ID: rows[2].ID.String()}, p); !errors.Is(readErr, fc.ErrNotFound) {
		t.Fatalf("deleted download=%v", readErr)
	}
	j := seedJob(t, d, "artifact-owner", job.StateCompleted, "app-b", "org-b", "default")
	for i, role := range []artifact.Role{artifact.RoleInput, artifact.RoleOutput} {
		if linkErr := s.LinkArtifact(ctx, &artifact.Link{ArtifactID: rows[i].ID, OwnerKind: artifact.OwnerJob, OwnerID: j.ID.String(), Role: role, Name: string(role), Attempt: i, CreatedAt: time.Now()}); linkErr != nil {
			t.Fatal(linkErr)
		}
	}
	links, err := artifactsForJobHandler(d)(ctx, IDInput{ID: j.ID.String()}, p)
	if err != nil || !links.Enabled || len(links.Links) != 2 || links.JobID != j.ID.String() || links.AsOf == "" {
		t.Fatalf("links=%+v, %v", links, err)
	}
	for _, input := range []ArtifactsListInput{{Lifecycle: "other"}, {Limit: -1}, {Cursor: "broken"}} {
		if _, readErr := artifactsListHandler(d)(ctx, input, p); !errors.Is(readErr, fc.ErrBadRequest) {
			t.Fatalf("invalid list=%v", readErr)
		}
	}
	if _, readErr := artifactsGetHandler(d)(ctx, IDInput{ID: id.NewJobID().String()}, p); !errors.Is(readErr, fc.ErrBadRequest) {
		t.Fatalf("bad ID=%v", readErr)
	}
	if _, readErr := artifactsGetHandler(d)(ctx, IDInput{ID: id.NewArtifactID().String()}, p); !errors.Is(readErr, fc.ErrNotFound) {
		t.Fatalf("missing=%v", readErr)
	}
	if _, readErr := artifactsForJobHandler(d)(ctx, IDInput{ID: id.NewJobID().String()}, p); !errors.Is(readErr, fc.ErrNotFound) {
		t.Fatalf("missing owner=%v", readErr)
	}
}
func TestArtifactDomainMemoryAndSQLite(t *testing.T) {
	t.Run("memory", func(t *testing.T) { runArtifactDomain(t, memory.New()) })
	t.Run("sqlite", func(t *testing.T) { runArtifactDomain(t, sqliteContractStore(t)) })
}
func TestArtifactContractUsesConfiguredServiceStore(t *testing.T) {
	engineStore, artifactStore := memory.New(), memory.New()
	signer := newContractSigner()
	d := contractDeps(t, engineStore, engine.WithArtifacts(artifact.NewService(artifactStore, signer), nil))
	a := seedArtifact(t, artifactStore, "separate", artifact.Durable, "", "")
	seedArtifact(t, engineStore, "wrong-store", artifact.Durable, "", "")
	page, err := artifactsListHandler(d)(context.Background(), ArtifactsListInput{}, fc.Principal{})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != a.ID.String() {
		t.Fatalf("configured store=%+v, %v", page, err)
	}
	detail, err := artifactsGetHandler(d)(context.Background(), IDInput{ID: a.ID.String()}, fc.Principal{})
	if err != nil || detail.Artifact == nil || detail.Artifact.ScopeAppID != nil {
		t.Fatalf("configured detail=%+v, %v", detail, err)
	}
	j := seedJob(t, d, "owner", job.StateCompleted, "", "", "default")
	if linkErr := artifactStore.LinkArtifact(context.Background(), &artifact.Link{ArtifactID: a.ID, OwnerKind: artifact.OwnerJob, OwnerID: j.ID.String(), Role: artifact.RoleInput, Name: "input", CreatedAt: time.Now()}); linkErr != nil {
		t.Fatal(linkErr)
	}
	links, err := artifactsForJobHandler(d)(context.Background(), IDInput{ID: j.ID.String()}, fc.Principal{})
	if err != nil || len(links.Links) != 1 || links.Links[0].ArtifactID != a.ID.String() {
		t.Fatalf("configured links=%+v, %v", links, err)
	}
	download, err := artifactsPresignHandler(d)(context.Background(), IDInput{ID: a.ID.String()}, fc.Principal{})
	if err != nil || download.URL == nil || signer.last.ID != a.ID {
		t.Fatalf("configured download=%+v, %v", download, err)
	}
}
func TestArtifactPresignCapabilityFailuresAndTTL(t *testing.T) {
	s := memory.New()
	signer := newContractSigner()
	d := contractDeps(t, s, engine.WithArtifacts(artifact.NewService(s, signer), nil))
	a := seedArtifact(t, s, "signed", artifact.Durable, "", "")
	input := IDInput{ID: a.ID.String()}
	ctx := context.Background()
	p := fc.Principal{}
	first, err := artifactsPresignHandler(d)(ctx, input, p)
	if err != nil || !first.Enabled || !first.Supported || first.URL == nil || first.ExpiresAt == nil {
		t.Fatalf("first=%+v, %v", first, err)
	}
	second, err := artifactsPresignHandler(d)(ctx, input, p)
	if err != nil || second.URL == nil || *second.URL == *first.URL || signer.calls != 2 || signer.ttl != 5*time.Minute || !signer.deadline {
		t.Fatalf("fresh=%+v calls=%d ttl=%v, %v", second, signer.calls, signer.ttl, err)
	}
	expires, err := time.Parse(time.RFC3339Nano, *second.ExpiresAt)
	if err != nil || time.Until(expires) > 5*time.Minute || time.Until(expires) < 4*time.Minute {
		t.Fatalf("expires=%v, %v", expires, err)
	}
	signer.supported = false
	unsupported, err := artifactsPresignHandler(d)(ctx, input, p)
	if err != nil || unsupported.Supported || unsupported.URL != nil || unsupported.ExpiresAt != nil || signer.calls != 2 {
		t.Fatalf("unsupported=%+v, %v", unsupported, err)
	}
	signer.supported = true
	foreign := seedArtifact(t, s, "foreign", artifact.Durable, "", "")
	// Backend is immutable through UpdateArtifact, so create a separate foreign record.
	foreign.ID = id.NewArtifactID()
	foreign.Key = "foreign-backend"
	foreign.Backend = "other"
	if createErr := s.CreateArtifact(ctx, foreign, nil); createErr != nil {
		t.Fatal(createErr)
	}
	mismatch, err := artifactsPresignHandler(d)(ctx, IDInput{ID: foreign.ID.String()}, p)
	if err != nil || mismatch.Supported || mismatch.URL != nil || signer.calls != 2 {
		t.Fatalf("foreign backend=%+v, %v", mismatch, err)
	}
	signer.failure = artifact.ErrPermissionDenied
	if _, readErr := artifactsPresignHandler(d)(ctx, input, p); !errors.Is(readErr, fc.ErrPermissionDenied) {
		t.Fatalf("denied=%v", readErr)
	}
	signer.failure = errors.New("credential in backend error")
	if _, readErr := artifactsPresignHandler(d)(ctx, input, p); !errors.Is(readErr, fc.ErrInternal) || strings.Contains(readErr.Error(), "credential") {
		t.Fatalf("unsafe error=%v", readErr)
	}
	signer.failure = nil
	for _, unsafe := range []string{"javascript:alert(1)", "/relative", "https://user:password@objects.example/key"} {
		signer.rawURL = unsafe
		if _, readErr := artifactsPresignHandler(d)(ctx, input, p); !errors.Is(readErr, fc.ErrInternal) {
			t.Fatalf("unsafe URL=%v", readErr)
		}
	}
}

type artifactBaseOnly struct{ artifact.Store }

func TestArtifactDisabledAndMissingInspectionCapabilities(t *testing.T) {
	ctx := context.Background()
	p := fc.Principal{}
	d := contractDeps(t, memory.New())
	input := IDInput{ID: id.NewArtifactID().String()}
	page, err := artifactsListHandler(d)(ctx, ArtifactsListInput{}, p)
	if err != nil || page.Enabled || page.Items == nil || page.PresignSupported {
		t.Fatalf("disabled list=%+v, %v", page, err)
	}
	detail, err := artifactsGetHandler(d)(ctx, input, p)
	if err != nil || detail.Enabled || detail.Artifact != nil {
		t.Fatalf("disabled detail=%+v, %v", detail, err)
	}
	download, err := artifactsPresignHandler(d)(ctx, input, p)
	if err != nil || download.Enabled || download.URL != nil || download.Supported {
		t.Fatalf("disabled download=%+v, %v", download, err)
	}
	custom := artifactBaseOnly{Store: memory.New()}
	other := contractDeps(t, memory.New(), engine.WithArtifacts(artifact.NewService(custom, artifacttest.NewBackend()), nil))
	if _, readErr := artifactsListHandler(other)(ctx, ArtifactsListInput{}, p); !errors.Is(readErr, fc.ErrUnavailable) {
		t.Fatalf("missing paging=%v", readErr)
	}
	if _, readErr := artifactsGetHandler(other)(ctx, input, p); !errors.Is(readErr, fc.ErrUnavailable) {
		t.Fatalf("missing inspection=%v", readErr)
	}
}
