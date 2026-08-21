package staging

import (
	"encoding/json"
	"fmt"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/job"
)

// Bindings maps declared input names to the artifacts bound to them.
type Bindings map[string]artifact.Ref

// SetBindings encodes bindings onto a job.
//
// They travel in their own field rather than inside Payload because the
// payload is opaque to the engine: refs buried there would be invisible
// to the scheduler, which needs the total input size before it picks a
// worker, and to this middleware, which needs them before the handler
// runs.
func SetBindings(j *job.Job, b Bindings) error {
	if len(b) == 0 {
		j.ArtifactBindings = nil

		return nil
	}

	raw, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("dispatch/artifact/staging: encode bindings: %w", err)
	}

	j.ArtifactBindings = raw

	return nil
}

// GetBindings decodes a job's bindings. A job with none yields an empty
// map rather than an error.
func GetBindings(j *job.Job) (Bindings, error) {
	if len(j.ArtifactBindings) == 0 {
		return Bindings{}, nil
	}

	var b Bindings
	if err := json.Unmarshal(j.ArtifactBindings, &b); err != nil {
		return nil, fmt.Errorf("dispatch/artifact/staging: decode bindings: %w", err)
	}

	return b, nil
}

// TotalBoundSize sums the sizes of the bound artifacts, which is the
// figure the scheduler and the staging budget both care about.
func TotalBoundSize(b Bindings) int64 {
	var total int64

	for _, ref := range b {
		total += ref.Size
	}

	return total
}

// Validate checks bindings against a definition's declarations.
//
// Both directions are errors: a binding with no declaration is a
// programming mistake, and a missing required declaration means the job
// cannot run. Catching them at enqueue keeps them out of the retry path.
func Validate(specs []artifact.InputSpec, b Bindings) error {
	for name, ref := range b {
		spec, ok := artifact.FindInput(specs, name)
		if !ok {
			return fmt.Errorf("%w: %q", artifact.ErrUndeclared, name)
		}

		if spec.MaxSize > 0 && ref.Size > spec.MaxSize {
			return fmt.Errorf("%w: input %q is %d bytes, limit is %d",
				artifact.ErrSizeExceeded, name, ref.Size, spec.MaxSize)
		}
	}

	for _, spec := range specs {
		if !spec.Required {
			continue
		}

		if _, ok := b[spec.Name]; !ok {
			return fmt.Errorf("%w: %q", artifact.ErrUnbound, spec.Name)
		}
	}

	return nil
}
