package operator

import (
	"context"
	"errors"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
)

type Options struct {
	// WorkerControl resolves an immutable trusted process handle.
	WorkerControl func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error)
	// BuildIdentity resolves actual deployed artifact evidence at the trusted host.
	BuildIdentity  func(context.Context, durable.BuildTarget) (durable.BuildQueryIdentity, error)
	Store          durable.Store
	Reads          durable.ReadStore
	Catalog        durable.NamespaceStore
	InstallationID string
	Authorizer     Authorizer
	Audit          security.Boundary
	CursorKeys     CursorKeys
	// RuntimeAvailable reports read-only host availability when Runtime is absent.
	// Runtime takes precedence and must resolve the exact namespace and build.
	RuntimeAvailable func(namespace, build string) bool
	// Runtime resolves only the requested immutable namespace/build.
	Runtime func(namespace, build string) (*drt.Worker, error)
}
type Service struct {
	workerControl    func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error)
	buildIdentity    func(context.Context, durable.BuildTarget) (durable.BuildQueryIdentity, error)
	store            durable.Store
	reads            durable.ReadStore
	catalog          durable.NamespaceStore
	installation     string
	authorizer       Authorizer
	audit            security.Boundary
	cursors          cursorCodec
	runtimeAvailable func(string, string) bool
	runtime          func(string, string) (*drt.Worker, error)
}

func New(o Options) (*Service, error) {
	if o.Store == nil || o.Reads == nil || o.Catalog == nil || o.Authorizer == nil || !durable.DeliveryIdentifier(o.InstallationID) || o.Audit.Resource.InstallationID != o.InstallationID {
		return nil, durable.ErrInvalid
	}
	c, err := newCodec(o.CursorKeys)
	if err != nil {
		return nil, err
	}
	return &Service{workerControl: o.WorkerControl, buildIdentity: o.BuildIdentity, store: o.Store, reads: o.Reads, catalog: o.Catalog, installation: o.InstallationID, authorizer: o.Authorizer, audit: o.Audit, cursors: c, runtimeAvailable: o.RuntimeAvailable, runtime: o.Runtime}, nil
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

func (s *Service) hasRuntime(namespace, build string) bool {
	if s.runtime != nil {
		_, err := s.worker(namespace, build)
		return err == nil
	}
	return s.runtimeAvailable != nil && s.runtimeAvailable(namespace, build)
}
