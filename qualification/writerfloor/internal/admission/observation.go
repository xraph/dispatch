package admission

import (
	"context"

	"github.com/xraph/ctrlplane/managed"
)

// IncarnationReader reads the trusted launcher's independently retained restore
// identity. It cannot derive it from an authority row, process name or request.
type IncarnationReader interface {
	ReadIncarnation(ctx context.Context, authority string) (string, error)
}

// ProtectedObserver combines the published protected marker reader with the
// trusted launcher source. It refuses an incarnation change during the read.
type ProtectedObserver struct {
	authority string
	reader    managed.ManagedWriterFloorReader
	launcher  IncarnationReader
}

func NewProtectedObserver(authority string, reader managed.ManagedWriterFloorReader, launcher IncarnationReader) (*ProtectedObserver, error) {
	if !token(authority) || nilValue(reader) || nilValue(launcher) {
		return nil, ErrDenied
	}
	return &ProtectedObserver{authority, reader, launcher}, nil
}
func (o *ProtectedObserver) Observe(ctx context.Context) (Observation, error) {
	before, e := o.launcher.ReadIncarnation(ctx, o.authority)
	if e != nil {
		return Observation{}, e
	}
	if !token(before) {
		return Observation{}, ErrDenied
	}
	floor, e := o.reader.ReadManagedWriterFloor(ctx)
	if e != nil {
		return Observation{}, e
	}
	if _, e = managed.EncodeWriterFloor(floor); e != nil || floor.AuthorityID != o.authority {
		return Observation{}, ErrDenied
	}
	after, e := o.launcher.ReadIncarnation(ctx, o.authority)
	if e != nil {
		return Observation{}, e
	}
	if before != after {
		return Observation{}, ErrDenied
	}
	if callErr := ctx.Err(); callErr != nil {
		return Observation{}, callErr
	}
	return Observation{floor, after}, nil
}
