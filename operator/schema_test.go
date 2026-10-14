package operator

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/xraph/warden"
	"github.com/xraph/warden/resourcetype"
	wm "github.com/xraph/warden/store/memory"

	"github.com/xraph/dispatch/durable"
)

func schemaNamespace() durable.NamespaceRecord {
	return durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: "install", Namespace: "namespace", AppID: "app", TenantID: "tenant", SchemaVersion: 1}}
}

func TestNamespaceSchemaExactReuse(t *testing.T) {
	store := wm.New()
	namespace := schemaNamespace()
	if err := RegisterNamespaceSchema(t.Context(), store, namespace); err != nil {
		t.Fatal(err)
	}
	before, err := store.GetResourceTypeByName(t.Context(), namespace.TenantID, namespace.Namespace, "dispatch_namespace")
	if err != nil {
		t.Fatal(err)
	}
	if err = RegisterNamespaceSchema(t.Context(), store, namespace); err != nil {
		t.Fatal(err)
	}
	after, err := store.GetResourceTypeByName(t.Context(), namespace.TenantID, namespace.Namespace, "dispatch_namespace")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("reuse changed the saved schema")
	}
	for _, changed := range []durable.NamespaceRecord{
		{NamespaceConfig: durable.NamespaceConfig{InstallationID: "install", Namespace: "other", AppID: "app", TenantID: "tenant", SchemaVersion: 1}},
		{NamespaceConfig: durable.NamespaceConfig{InstallationID: "install", Namespace: "namespace", AppID: "app", TenantID: "other", SchemaVersion: 1}},
	} {
		if err = RegisterNamespaceSchema(t.Context(), store, changed); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNamespaceSchemaRefusesChangedDefinition(t *testing.T) {
	for name, mutate := range map[string]func(*resourcetype.ResourceType){
		"app":      func(r *resourcetype.ResourceType) { r.AppID = "foreign" },
		"relation": func(r *resourcetype.ResourceType) { r.Relations[0].Name = "owner" },
		"subject": func(r *resourcetype.ResourceType) {
			r.Relations[0].AllowedSubjects = append(r.Relations[0].AllowedSubjects, "group")
		},
		"permission expression": func(r *resourcetype.ResourceType) { r.Permissions[0].Expression = "owner" },
		"permission name":       func(r *resourcetype.ResourceType) { r.Permissions[0].Name = "foreign" },
		"missing permission":    func(r *resourcetype.ResourceType) { r.Permissions = r.Permissions[1:] },
		"extra permission": func(r *resourcetype.ResourceType) {
			r.Permissions = append(r.Permissions, resourcetype.PermissionDef{Name: "foreign", Expression: "operator"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := wm.New()
			namespace := schemaNamespace()
			if err := RegisterNamespaceSchema(t.Context(), store, namespace); err != nil {
				t.Fatal(err)
			}
			saved, err := store.GetResourceTypeByName(t.Context(), namespace.TenantID, namespace.Namespace, "dispatch_namespace")
			if err != nil {
				t.Fatal(err)
			}
			mutate(saved)
			if err = store.UpdateResourceType(t.Context(), saved); err != nil {
				t.Fatal(err)
			}
			before, err := store.GetResourceTypeByName(t.Context(), namespace.TenantID, namespace.Namespace, "dispatch_namespace")
			if err != nil {
				t.Fatal(err)
			}
			if err = RegisterNamespaceSchema(t.Context(), store, namespace); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("changed schema accepted: %v", err)
			}
			after, err := store.GetResourceTypeByName(t.Context(), namespace.TenantID, namespace.Namespace, "dispatch_namespace")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("conflicting schema overwritten")
			}
		})
	}
}

type concurrentSchemaStore struct {
	resourcetype.Store
	conflict bool
	creates  int
}

func (s *concurrentSchemaStore) CreateResourceType(ctx context.Context, r *resourcetype.ResourceType) error {
	s.creates++
	concurrent := *r
	if s.conflict {
		concurrent.AppID = "foreign"
	}
	if err := s.Store.CreateResourceType(ctx, &concurrent); err != nil {
		return err
	}
	return warden.ErrAlreadyExists
}

func TestNamespaceSchemaConcurrentCreate(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		store := &concurrentSchemaStore{Store: wm.New(), conflict: conflict}
		err := RegisterNamespaceSchema(t.Context(), store, schemaNamespace())
		if conflict && !errors.Is(err, durable.ErrInvalid) || !conflict && err != nil {
			t.Fatalf("conflict=%v: %v", conflict, err)
		}
		if store.creates != 1 {
			t.Fatalf("create calls: %d", store.creates)
		}
	}
}

type failedSchemaStore struct {
	resourcetype.Store
	readErr   error
	createErr error
	creates   int
}

func (s *failedSchemaStore) GetResourceTypeByName(context.Context, string, string, string) (*resourcetype.ResourceType, error) {
	return nil, s.readErr
}
func (s *failedSchemaStore) CreateResourceType(context.Context, *resourcetype.ResourceType) error {
	s.creates++
	return s.createErr
}

func TestNamespaceSchemaPreservesStoreErrorsAndCancellation(t *testing.T) {
	unavailable := errors.New("store unavailable")
	store := &failedSchemaStore{readErr: unavailable}
	if err := RegisterNamespaceSchema(t.Context(), store, schemaNamespace()); !errors.Is(err, unavailable) || store.creates != 0 {
		t.Fatalf("read failure: %v, creates=%d", err, store.creates)
	}
	store.readErr = warden.ErrResourceTypeNotFound
	store.createErr = unavailable
	if err := RegisterNamespaceSchema(t.Context(), store, schemaNamespace()); !errors.Is(err, unavailable) || store.creates != 1 {
		t.Fatalf("create failure: %v, creates=%d", err, store.creates)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := RegisterNamespaceSchema(ctx, store, schemaNamespace()); !errors.Is(err, context.Canceled) || store.creates != 1 {
		t.Fatalf("cancel: %v, creates=%d", err, store.creates)
	}
}
