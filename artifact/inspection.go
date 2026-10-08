package artifact

import (
	"context"

	"github.com/xraph/dispatch/id"
)

// RecordReader inspects metadata, including soft-deleted records. It never serves bytes.
type RecordReader interface {
	GetArtifactRecord(ctx context.Context, artifactID id.ArtifactID) (*Artifact, error)
}

// PresignSupport reports whether the driver for an artifact can sign URLs.
type PresignSupport interface{ SupportsPresign(ref Ref) bool }

// SupportsPresign reports the backend's current signing capability.
func SupportsPresign(backend Backend, ref Ref) bool {
	if _, ok := backend.(Presigner); !ok {
		return false
	}
	if support, ok := backend.(PresignSupport); ok {
		return support.SupportsPresign(ref)
	}
	return true
}
