package artifact_test

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

func TestArtifactRef(t *testing.T) {
	aid := id.NewArtifactID()
	a := &artifact.Artifact{
		ID:          aid,
		Backend:     "primary",
		Bucket:      "models",
		Key:         "tower.ifc",
		Size:        2 << 30,
		ContentHash: "blake3:9f2a",
		Lifecycle:   artifact.Durable,
		CreatedAt:   time.Now().UTC(),
	}

	ref := a.Ref()
	if ref.ID != aid {
		t.Fatalf("ref.ID = %v, want %v", ref.ID, aid)
	}

	if ref.Size != 2<<30 {
		t.Fatalf("ref.Size = %d, want %d", ref.Size, int64(2)<<30)
	}

	if ref.Key != "tower.ifc" {
		t.Fatalf("ref.Key = %q, want %q", ref.Key, "tower.ifc")
	}

	if ref.IsZero() {
		t.Fatal("ref with an ID reported as zero")
	}
}

func TestLifecycleValid(t *testing.T) {
	tests := []struct {
		name string
		lc   artifact.Lifecycle
		want bool
	}{
		{"durable", artifact.Durable, true},
		{"ephemeral", artifact.Ephemeral, true},
		{"empty", artifact.Lifecycle(""), false},
		{"garbage", artifact.Lifecycle("permanent"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lc.Valid(); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRoleValid(t *testing.T) {
	tests := []struct {
		name string
		role artifact.Role
		want bool
	}{
		{"input", artifact.RoleInput, true},
		{"output", artifact.RoleOutput, true},
		{"intermediate", artifact.RoleIntermediate, true},
		{"empty", artifact.Role(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.role.Valid(); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOwnerKindValid(t *testing.T) {
	tests := []struct {
		name string
		kind artifact.OwnerKind
		want bool
	}{
		{"job", artifact.OwnerJob, true},
		{"run", artifact.OwnerRun, true},
		{"step", artifact.OwnerStep, true},
		{"empty", artifact.OwnerKind(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.kind.Valid(); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArtifactIsDeleted(t *testing.T) {
	a := &artifact.Artifact{}
	if a.IsDeleted() {
		t.Fatal("fresh artifact reported deleted")
	}

	now := time.Now().UTC()
	a.DeletedAt = &now

	if !a.IsDeleted() {
		t.Fatal("soft-deleted artifact not reported deleted")
	}
}

func TestArtifactCloneIsDeep(t *testing.T) {
	now := time.Now().UTC()
	a := &artifact.Artifact{
		ID:        id.NewArtifactID(),
		Lifecycle: artifact.Ephemeral,
		ExpiresAt: &now,
		DeletedAt: &now,
	}

	clone := a.Clone()
	later := now.Add(time.Hour)
	*clone.ExpiresAt = later
	*clone.DeletedAt = later

	if a.ExpiresAt.Equal(later) {
		t.Fatal("Clone shares the ExpiresAt pointer")
	}

	if a.DeletedAt.Equal(later) {
		t.Fatal("Clone shares the DeletedAt pointer")
	}
}
