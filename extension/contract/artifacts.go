package contract

import (
	"context"
	"fmt"
	"net/url"
	"time"

	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

const artifactDownloadTTL = 5 * time.Minute

type ArtifactsListInput struct {
	Lifecycle      artifact.Lifecycle `json:"lifecycle"`
	ScopeAppID     string             `json:"scopeAppId"`
	ScopeOrgID     string             `json:"scopeOrgId"`
	IncludeDeleted bool               `json:"includeDeleted"`
	Cursor         string             `json:"cursor"`
	Limit          int                `json:"limit"`
}
type ArtifactRow struct {
	ID                string             `json:"id"`
	Backend           string             `json:"backend"`
	Bucket            string             `json:"bucket"`
	Key               string             `json:"key"`
	Size              int64              `json:"size"`
	ContentHash       *string            `json:"contentHash"`
	ContentType       *string            `json:"contentType"`
	Lifecycle         artifact.Lifecycle `json:"lifecycle"`
	ScopeAppID        *string            `json:"scopeAppId"`
	ScopeOrgID        *string            `json:"scopeOrgId"`
	ExpiresAt         *string            `json:"expiresAt"`
	CreatedAt         *string            `json:"createdAt"`
	DeletedAt         *string            `json:"deletedAt"`
	DownloadAvailable bool               `json:"downloadAvailable"`
}
type ArtifactsPage struct {
	Page[ArtifactRow]
	Enabled          bool `json:"enabled"`
	PresignSupported bool `json:"presignSupported"`
}
type ArtifactDetail struct {
	Enabled  bool         `json:"enabled"`
	Artifact *ArtifactRow `json:"artifact"`
	AsOf     string       `json:"asOf"`
}
type ArtifactJobLinks struct {
	JobArtifactLinks
	JobID string `json:"jobId"`
	AsOf  string `json:"asOf"`
}
type ArtifactDownload struct {
	Enabled   bool    `json:"enabled"`
	Supported bool    `json:"supported"`
	URL       *string `json:"url"`
	ExpiresAt *string `json:"expiresAt"`
	AsOf      string  `json:"asOf"`
}

func projectArtifact(a *artifact.Artifact, service *artifact.Service) ArtifactRow {
	return ArtifactRow{ID: a.ID.String(), Backend: a.Backend, Bucket: a.Bucket, Key: a.Key, Size: a.Size, ContentHash: nullable(a.ContentHash), ContentType: nullable(a.ContentType),
		Lifecycle: a.Lifecycle, ScopeAppID: nullable(a.ScopeAppID), ScopeOrgID: nullable(a.ScopeOrgID), ExpiresAt: timestampPtr(a.ExpiresAt), CreatedAt: timestamp(a.CreatedAt), DeletedAt: timestampPtr(a.DeletedAt),
		DownloadAvailable: service.Enabled() && !a.IsDeleted() && a.Backend == service.Backend().Name() && artifact.SupportsPresign(service.Backend())}
}
func parseArtifactID(raw string) (id.ArtifactID, error) {
	parsed, err := id.ParseArtifactID(raw)
	if err != nil || parsed.IsNil() {
		return id.ArtifactID{}, badRequest("id must be an artifact ID")
	}
	return parsed, nil
}
func artifactInspectionUnavailable(message string) error {
	return &fc.Error{Code: fc.CodeUnavailable, Message: message}
}
func artifactsListHandler(deps Deps) func(context.Context, ArtifactsListInput, fc.Principal) (ArtifactsPage, error) {
	return handle(deps, "artifacts.list", false, func(ctx context.Context, input ArtifactsListInput, _ fc.Principal) (ArtifactsPage, error) {
		limit, err := pageLimit(input.Limit)
		if err != nil {
			return ArtifactsPage{}, err
		}
		if input.Lifecycle != "" && !input.Lifecycle.Valid() {
			return ArtifactsPage{}, badRequest("unknown artifact lifecycle")
		}
		service := deps.Engine.Artifacts()
		out := ArtifactsPage{Page: newPage([]ArtifactRow{}, "", true, time.Now()), Enabled: service.Enabled()}
		if !out.Enabled {
			return out, nil
		}
		reader, ok := service.Store().(artifact.PageLister)
		if !ok {
			return ArtifactsPage{}, artifactInspectionUnavailable("the configured artifact store does not support cursor inspection")
		}
		page, err := reader.ListArtifactsPage(ctx, artifact.PageOpts{Lifecycle: input.Lifecycle, ScopeAppID: input.ScopeAppID, ScopeOrgID: input.ScopeOrgID, IncludeDeleted: input.IncludeDeleted, Cursor: input.Cursor, Limit: limit})
		if err != nil {
			return ArtifactsPage{}, err
		}
		rows := make([]ArtifactRow, 0, len(page.Artifacts))
		for _, a := range page.Artifacts {
			rows = append(rows, projectArtifact(a, service))
		}
		out.Page = newPage(rows, page.NextCursor, page.Complete, time.Now())
		out.PresignSupported = artifact.SupportsPresign(service.Backend())
		return out, nil
	})
}
func artifactsGetHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (ArtifactDetail, error) {
	return handle(deps, "artifacts.get", false, func(ctx context.Context, input IDInput, _ fc.Principal) (ArtifactDetail, error) {
		artifactID, err := parseArtifactID(input.ID)
		if err != nil {
			return ArtifactDetail{}, err
		}
		service := deps.Engine.Artifacts()
		out := ArtifactDetail{Enabled: service.Enabled(), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}
		if !out.Enabled {
			return out, nil
		}
		reader, ok := service.Store().(artifact.RecordReader)
		if !ok {
			return ArtifactDetail{}, artifactInspectionUnavailable("the configured artifact store does not support deleted-record inspection")
		}
		a, err := reader.GetArtifactRecord(ctx, artifactID)
		if err != nil {
			return ArtifactDetail{}, err
		}
		row := projectArtifact(a, service)
		out.Artifact = &row
		out.AsOf = time.Now().UTC().Format(time.RFC3339Nano)
		return out, nil
	})
}
func artifactsForJobHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (ArtifactJobLinks, error) {
	return handle(deps, "artifacts.forJob", false, func(ctx context.Context, input IDInput, _ fc.Principal) (ArtifactJobLinks, error) {
		jobID, err := parseJobID(input.ID)
		if err != nil {
			return ArtifactJobLinks{}, err
		}
		if _, readErr := deps.Store.GetJob(ctx, jobID); readErr != nil {
			return ArtifactJobLinks{}, readErr
		}
		links, err := jobLinks(ctx, deps, jobID)
		if err != nil {
			return ArtifactJobLinks{}, err
		}
		return ArtifactJobLinks{JobArtifactLinks: links, JobID: jobID.String(), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})
}
func artifactsPresignHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (ArtifactDownload, error) {
	return handle(deps, "artifacts.presign", false, func(ctx context.Context, input IDInput, _ fc.Principal) (ArtifactDownload, error) {
		if writer := dashauth.ResponseWriterFromContext(ctx); writer != nil {
			writer.Header().Set("Cache-Control", "no-store")
		}
		artifactID, err := parseArtifactID(input.ID)
		if err != nil {
			return ArtifactDownload{}, err
		}
		service := deps.Engine.Artifacts()
		out := ArtifactDownload{Enabled: service.Enabled(), AsOf: time.Now().UTC().Format(time.RFC3339Nano)}
		if !out.Enabled {
			return out, nil
		}
		a, err := service.Store().GetArtifact(ctx, artifactID)
		if err != nil {
			return ArtifactDownload{}, err
		}
		presigner, canSign := service.Backend().(artifact.Presigner)
		out.Supported = canSign && a.Backend == service.Backend().Name() && artifact.SupportsPresign(service.Backend())
		if !out.Supported {
			return out, nil
		}
		started := time.Now()
		signed, err := presigner.PresignGet(ctx, a.Ref(), artifactDownloadTTL)
		if err != nil {
			return ArtifactDownload{}, err
		}
		parsed, err := url.Parse(signed)
		if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return ArtifactDownload{}, fmt.Errorf("artifact backend returned an invalid download URL")
		}
		expires := started.Add(artifactDownloadTTL)
		out.URL = &signed
		out.ExpiresAt = timestamp(expires)
		out.AsOf = time.Now().UTC().Format(time.RFC3339Nano)
		return out, nil
	})
}
