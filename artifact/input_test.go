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
		t.Fatalf("default mode = %v, want StageModePath", in.Mode)
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
		t.Fatalf("Mode = %v, want StageModeLazy", in.Mode)
	}
}

func TestInputValidate(t *testing.T) {
	tests := []struct {
		name    string
		spec    artifact.InputSpec
		wantErr bool
	}{
		{"valid", artifact.Input("model"), false},
		{"valid with dots", artifact.Input("model.v2"), false},
		{"empty name", artifact.Input(""), true},
		{"negative max size", artifact.Input("m", artifact.MaxSize(-1)), true},
		{"parent traversal", artifact.Input("../etc/passwd"), true},
		{"forward slash", artifact.Input("a/b"), true},
		{"backslash", artifact.Input(`a\b`), true},
		{"bare traversal", artifact.Input(".."), true},
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

func TestValidateInputsRejectsDuplicates(t *testing.T) {
	err := artifact.ValidateInputs([]artifact.InputSpec{
		artifact.Input("model"),
		artifact.Input("model"),
	})
	if err == nil {
		t.Fatal("duplicate input names must be rejected")
	}
}

func TestValidateInputsAcceptsDistinct(t *testing.T) {
	err := artifact.ValidateInputs([]artifact.InputSpec{
		artifact.Input("model"),
		artifact.Input("textures"),
	})
	if err != nil {
		t.Fatalf("ValidateInputs: %v", err)
	}
}

func TestTotalMaxSize(t *testing.T) {
	tests := []struct {
		name  string
		specs []artifact.InputSpec
		want  int64
	}{
		{
			name:  "none",
			specs: nil,
			want:  0,
		},
		{
			name: "all bounded",
			specs: []artifact.InputSpec{
				artifact.Input("a", artifact.MaxSize(100)),
				artifact.Input("b", artifact.MaxSize(200)),
			},
			want: 300,
		},
		{
			// One unbounded declaration makes the total unknown, not small.
			name: "one unbounded",
			specs: []artifact.InputSpec{
				artifact.Input("a", artifact.MaxSize(100)),
				artifact.Input("b"),
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := artifact.TotalMaxSize(tt.specs); got != tt.want {
				t.Fatalf("TotalMaxSize() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFindInput(t *testing.T) {
	specs := []artifact.InputSpec{
		artifact.Input("model"),
		artifact.Input("textures"),
	}

	got, ok := artifact.FindInput(specs, "textures")
	if !ok || got.Name != "textures" {
		t.Fatalf("FindInput = %+v, %v; want the textures spec", got, ok)
	}

	if _, ok := artifact.FindInput(specs, "absent"); ok {
		t.Fatal("FindInput found a declaration that does not exist")
	}
}

func TestStageModeString(t *testing.T) {
	if got := artifact.StageModePath.String(); got != "path" {
		t.Fatalf("StageModePath.String() = %q, want %q", got, "path")
	}

	if got := artifact.StageModeLazy.String(); got != "lazy" {
		t.Fatalf("StageModeLazy.String() = %q, want %q", got, "lazy")
	}
}
