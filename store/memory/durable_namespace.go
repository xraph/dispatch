package memory

import (
	"context"
	"sort"
	"time"

	"github.com/xraph/dispatch/durable"
)

var _ durable.NamespaceStore = (*Store)(nil)
var _ durable.OutboxStore = (*Store)(nil)

func (m *Store) RegisterNamespace(ctx context.Context, c durable.NamespaceConfig) (durable.NamespaceRecord, error) {
	if err := c.Validate(); err != nil {
		return durable.NamespaceRecord{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return durable.NamespaceRecord{}, err
	}
	if old, ok := m.namespaces[c.Namespace]; ok {
		if old.NamespaceConfig != c {
			return durable.NamespaceRecord{}, durable.ErrRequestConflict
		}
		return old, nil
	}
	n := durable.NamespaceRecord{NamespaceConfig: c, CoverageStartedAt: time.Now().UTC().Truncate(time.Microsecond), WriterProtocol: durable.AuditWriterProtocol}
	m.namespaces[c.Namespace] = n
	return n, nil
}
func (m *Store) GetNamespace(ctx context.Context, installation, namespace string) (durable.NamespaceRecord, error) {
	if !durable.DeliveryIdentifier(installation) || !durable.DeliveryIdentifier(namespace) {
		return durable.NamespaceRecord{}, durable.ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.NamespaceRecord{}, err
	}
	n, ok := m.namespaces[namespace]
	if !ok || n.InstallationID != installation {
		return durable.NamespaceRecord{}, durable.ErrNotFound
	}
	return n, nil
}
func (m *Store) ListNamespaces(ctx context.Context, r durable.NamespaceList) ([]durable.NamespaceRecord, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := []durable.NamespaceRecord{}
	for _, n := range m.namespaces {
		if n.InstallationID == r.InstallationID && n.Namespace > r.After {
			result = append(result, n)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Namespace < result[j].Namespace })
	if len(result) > r.Limit {
		result = result[:r.Limit]
	}
	return result, nil
}
