package durable

import (
	"context"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const DeliverySchemaVersion = 1
const AuditWriterProtocol = 1
const MaxDeliveryBatch = 100

// NamespaceConfig is immutable ownership within one physical store. Execution
// keys omit installation, so two installations cannot own the same namespace.
type NamespaceConfig struct {
	InstallationID string
	Namespace      string
	AppID          string
	TenantID       string
	RequireAudit   bool
	RequireHooks   bool
	SchemaVersion  int
}

type NamespaceRecord struct {
	NamespaceConfig
	CoverageStartedAt time.Time
	WriterProtocol    int
}

type NamespaceList struct {
	InstallationID string
	After          string
	Limit          int
}

type NamespaceStore interface {
	RegisterNamespace(context.Context, NamespaceConfig) (NamespaceRecord, error)
	GetNamespace(context.Context, string, string) (NamespaceRecord, error)
	ListNamespaces(context.Context, NamespaceList) ([]NamespaceRecord, error)
}

func (c NamespaceConfig) Validate() error {
	for _, v := range []string{c.InstallationID, c.Namespace, c.AppID, c.TenantID} {
		if !DeliveryIdentifier(v) {
			return ErrInvalid
		}
	}
	if c.SchemaVersion != DeliverySchemaVersion {
		return ErrInvalid
	}
	return nil
}

// DeliveryIdentifier bounds catalog and metadata identifiers and rejects control
// characters. Hosts must supply identifiers, never claims, credentials or errors.
func DeliveryIdentifier(v string) bool {
	return v != "" && len(v) <= 256 && utf8.ValidString(v) && strings.TrimSpace(v) == v && strings.IndexFunc(v, unicode.IsControl) < 0
}

func (r NamespaceList) Validate() error {
	if !DeliveryIdentifier(r.InstallationID) || (r.After != "" && !DeliveryIdentifier(r.After)) || r.Limit < 1 || r.Limit > MaxDeliveryBatch {
		return ErrInvalid
	}
	return nil
}
