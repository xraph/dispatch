// Package audittest composes real memory audit acceptance for positive fixtures.
package audittest

import (
	"context"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

func WithMemory(b security.Boundary) security.Boundary {
	store := memory.New()
	config := durable.NamespaceConfig{InstallationID: b.Resource.InstallationID, TenantID: b.Resource.PolicyTenant, AppID: "test", Namespace: "operator-audit", RequireAudit: true, SchemaVersion: durable.DeliverySchemaVersion}
	if _, err := store.RegisterNamespace(context.Background(), config); err != nil {
		panic(err)
	}
	b.Audit = &security.AuditService{}
	if err := b.Audit.Activate(context.Background(), store, store, b.Resource, config.Namespace, false); err != nil {
		panic(err)
	}
	return b
}
