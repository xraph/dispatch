package operator

import (
	"context"
	"errors"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

type Options struct {
	Store          durable.Store
	Reads          durable.ReadStore
	Catalog        durable.NamespaceStore
	InstallationID string
	Authorizer     Authorizer
	Audit          security.Boundary
	CursorKeys     CursorKeys
	// RuntimeAvailable must check the exact persisted namespace and build.
	RuntimeAvailable func(namespace, build string) bool
}
type Service struct {
	store            durable.Store
	reads            durable.ReadStore
	catalog          durable.NamespaceStore
	installation     string
	authorizer       Authorizer
	audit            security.Boundary
	cursors          cursorCodec
	runtimeAvailable func(string, string) bool
}

func New(o Options) (*Service, error) {
	if o.Store == nil || o.Reads == nil || o.Catalog == nil || o.Authorizer == nil || !durable.DeliveryIdentifier(o.InstallationID) || o.Audit.Resource.InstallationID != o.InstallationID {
		return nil, durable.ErrInvalid
	}
	c, err := newCodec(o.CursorKeys)
	if err != nil {
		return nil, err
	}
	return &Service{store: o.Store, reads: o.Reads, catalog: o.Catalog, installation: o.InstallationID, authorizer: o.Authorizer, audit: o.Audit, cursors: c, runtimeAvailable: o.RuntimeAvailable}, nil
}
func bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 5*time.Second)
}
func pageLimit(n int) (int, error) {
	if n == 0 {
		return 25, nil
	}
	if n < 1 || n > durable.MaxReadPage {
		return 0, durable.ErrInvalid
	}
	return n, nil
}
func safeError(err error) error {
	if err == nil {
		return nil
	}
	for _, known := range []error{durable.ErrInvalid, durable.ErrNotFound, security.ErrForbidden, security.ErrUnauthenticated} {
		if errors.Is(err, known) {
			return known
		}
	}
	return security.ErrUnavailable
}

type Page[T any] struct {
	Items       []T       `json:"items"`
	Cursor      string    `json:"cursor,omitempty"`
	Complete    bool      `json:"complete"`
	AsOf        time.Time `json:"as_of"`
	Observation string    `json:"observation"`
	Total       *string   `json:"total"`
	Revision    string    `json:"revision,omitempty"`
	HighWater   string    `json:"high_water,omitempty"`
}

func observed[T any](items []T) Page[T] {
	return Page[T]{Items: items, Complete: true, AsOf: time.Now().UTC(), Observation: "current_page"}
}
