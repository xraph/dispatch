package artifact

import (
	"errors"
	"fmt"
	"strings"
)

// StageMode determines how a declared input is presented to the handler.
type StageMode int

const (
	// StageModePath materialises the artifact to local disk before the
	// handler runs, and Accessor.Path returns the file path.
	//
	// This is the default because the libraries that read Dispatch's heavy
	// inputs — CAD kernels, mesh importers, PDF engines — seek and
	// memory-map. They need a file, not a stream.
	StageModePath StageMode = iota

	// StageModeLazy downloads nothing up front. The handler calls
	// Accessor.Open to stream the bytes if and when it needs them.
	//
	// Right for data read once, front to back. Wrong for a multi-gigabyte
	// model a native library will seek around in.
	StageModeLazy
)

// String renders the mode for logs and errors.
func (m StageMode) String() string {
	switch m {
	case StageModePath:
		return "path"
	case StageModeLazy:
		return "lazy"
	default:
		return fmt.Sprintf("StageMode(%d)", int(m))
	}
}

// InputSpec declares one artifact a job consumes.
//
// Declaring inputs, rather than burying refs in an opaque payload, is
// what lets the engine know the total input size before it schedules the
// job, validate bindings at enqueue instead of at run time, and stage the
// bytes before the handler is ever called.
type InputSpec struct {
	// Name identifies the input in the handler and in bindings.
	Name string
	// Required fails the job when no binding is supplied.
	Required bool
	// MaxSize rejects an oversized binding at enqueue. Zero means no limit.
	MaxSize int64
	// Mode selects staging behaviour.
	Mode StageMode
}

// InputOption configures an InputSpec.
type InputOption func(*InputSpec)

// Required marks an input as mandatory.
func Required(s *InputSpec) { s.Required = true }

// MaxSize caps the size of a bound artifact. The limit is enforced at
// enqueue, so an oversized input never becomes a failed job.
func MaxSize(bytes int64) InputOption {
	return func(s *InputSpec) { s.MaxSize = bytes }
}

// StageAsPath pre-downloads the input and exposes it as a file path.
func StageAsPath(s *InputSpec) { s.Mode = StageModePath }

// StageLazy skips the download; the handler streams via Open.
func StageLazy(s *InputSpec) { s.Mode = StageModeLazy }

// Input declares an artifact input on a job definition.
func Input(name string, opts ...InputOption) InputSpec {
	spec := InputSpec{Name: name, Mode: StageModePath}

	for _, opt := range opts {
		opt(&spec)
	}

	return spec
}

// Validate reports whether the declaration is usable.
//
// The name becomes both a path component in the storage key and a
// filename in the staging directory, so anything that could escape either
// is rejected here rather than at run time.
func (s InputSpec) Validate() error {
	switch {
	case s.Name == "":
		return errors.New("dispatch/artifact: input name must not be empty")
	case strings.ContainsAny(s.Name, `/\`):
		return fmt.Errorf("dispatch/artifact: input name %q must not contain a path separator", s.Name)
	case strings.Contains(s.Name, ".."):
		return fmt.Errorf("dispatch/artifact: input name %q must not contain %q", s.Name, "..")
	case s.MaxSize < 0:
		return fmt.Errorf("dispatch/artifact: input %q has a negative MaxSize", s.Name)
	default:
		return nil
	}
}

// ValidateInputs checks a definition's declarations as a set, rejecting
// invalid names and duplicates.
func ValidateInputs(specs []InputSpec) error {
	seen := make(map[string]bool, len(specs))

	for _, spec := range specs {
		if err := spec.Validate(); err != nil {
			return err
		}

		if seen[spec.Name] {
			return fmt.Errorf("dispatch/artifact: duplicate input declaration %q", spec.Name)
		}

		seen[spec.Name] = true
	}

	return nil
}

// TotalMaxSize sums the declared limits. It is zero when any declaration
// is unbounded, since the total is then unknown rather than small.
//
// The engine uses this to reject, at registration, a definition whose
// inputs could never fit the staging budget — so an unstageable job fails
// on a developer's machine instead of at 3am.
func TotalMaxSize(specs []InputSpec) int64 {
	var total int64

	for _, spec := range specs {
		if spec.MaxSize <= 0 {
			return 0
		}

		total += spec.MaxSize
	}

	return total
}

// FindInput returns the declaration with the given name.
func FindInput(specs []InputSpec, name string) (InputSpec, bool) {
	for _, spec := range specs {
		if spec.Name == name {
			return spec, true
		}
	}

	return InputSpec{}, false
}
