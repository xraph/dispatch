package memory

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/xraph/dispatch/durable"
)

func (m *Store) AppendSecurityAudit(ctx context.Context, a durable.SecurityAudit) (durable.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return durable.Delivery{}, err
	}
	n, ok := m.namespaces[a.Namespace]
	if !ok {
		return durable.Delivery{}, durable.ErrNotFound
	}
	d, err := a.Delivery(n)
	if err != nil {
		return durable.Delivery{}, err
	}
	if old, exists := m.outbox[d.ID]; exists {
		if old.Delivery.Fingerprint != d.Fingerprint {
			return durable.Delivery{}, durable.ErrRequestConflict
		}
		return old.Delivery, nil
	}
	if m.outboxPrepare != nil {
		if err := m.outboxPrepare(d); err != nil {
			return durable.Delivery{}, err
		}
	}
	m.outbox[d.ID] = durable.DeliveryRecord{Delivery: d, AcceptedAt: time.Now().UTC()}
	return d, nil
}
func (m *Store) ClaimDeliveries(ctx context.Context, r durable.DeliveryClaim) ([]durable.DeliveryRecord, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	ids := []string{}
	for id, d := range m.outbox {
		if inDeliveryScope(d, r.DeliveryScope) && d.DeliveredAt.IsZero() && !d.NextAttemptAt.After(now) && !d.LeaseUntil.After(now) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > r.Limit {
		ids = ids[:r.Limit]
	}
	result := make([]durable.DeliveryRecord, 0, len(ids))
	for _, id := range ids {
		d := m.outbox[id]
		if d.Epoch == math.MaxInt64 || d.Attempts == math.MaxInt64 {
			return nil, durable.ErrInvalid
		}
		d.Owner = r.Owner
		d.Epoch++
		d.Attempts++
		d.LeaseUntil = now.Add(r.LeaseDuration)
		result = append(result, d)
	}
	for _, d := range result {
		m.outbox[d.Delivery.ID] = d
	}
	return result, nil
}
func inDeliveryScope(d durable.DeliveryRecord, s durable.DeliveryScope) bool {
	return d.Delivery.InstallationID == s.InstallationID && d.Delivery.Destination == s.Destination
}
func (m *Store) deliveryForToken(t durable.DeliveryToken) (durable.DeliveryRecord, error) {
	d, ok := m.outbox[t.ID]
	if !ok || !inDeliveryScope(d, t.DeliveryScope) || d.Owner != t.Owner || d.Epoch != t.Epoch || !d.LeaseUntil.After(time.Now().UTC()) || !d.DeliveredAt.IsZero() {
		return durable.DeliveryRecord{}, durable.ErrLeaseLost
	}
	return d, nil
}
func (m *Store) RenewDelivery(ctx context.Context, t durable.DeliveryToken, ttl time.Duration) (time.Time, error) {
	if err := durable.ValidateDeliveryRenewal(t, ttl); err != nil {
		return time.Time{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	d, err := m.deliveryForToken(t)
	if err != nil {
		return time.Time{}, err
	}
	d.LeaseUntil = time.Now().UTC().Add(ttl)
	m.outbox[t.ID] = d
	return d.LeaseUntil, nil
}
func (m *Store) AcknowledgeDelivery(ctx context.Context, t durable.DeliveryToken, r durable.SinkReceipt) error {
	if err := t.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	d, err := m.deliveryForToken(t)
	if err != nil {
		return err
	}
	if err := r.Verify(d.Delivery); err != nil {
		return err
	}
	d.Receipt = r
	d.DeliveredAt = time.Now().UTC()
	d.ErrorCategory = ""
	m.outbox[t.ID] = d
	return nil
}
func (m *Store) RetryDelivery(ctx context.Context, r durable.DeliveryRetry) error {
	if err := r.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	d, err := m.deliveryForToken(r.Token)
	if err != nil {
		return err
	}
	d.NextAttemptAt = time.Now().UTC().Add(r.Delay)
	d.ErrorCategory = r.Category
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	m.outbox[r.Token.ID] = d
	return nil
}
func (m *Store) DeliveryStatus(ctx context.Context, r durable.DeliveryStatusRequest) (durable.DeliveryStatus, error) {
	if err := r.Validate(); err != nil {
		return durable.DeliveryStatus{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.DeliveryStatus{}, err
	}
	result := durable.DeliveryStatus{Records: []durable.DeliveryRecord{}}
	for _, d := range m.outbox {
		if !inDeliveryScope(d, r.DeliveryScope) {
			continue
		}
		if d.DeliveredAt.IsZero() {
			result.Pending++
			if result.OldestAcceptedAt.IsZero() || d.AcceptedAt.Before(result.OldestAcceptedAt) {
				result.OldestAcceptedAt = d.AcceptedAt
			}
		}
		if d.Delivery.ID > r.After {
			result.Records = append(result.Records, d)
		}
	}
	sort.Slice(result.Records, func(i, j int) bool { return result.Records[i].Delivery.ID < result.Records[j].Delivery.ID })
	if len(result.Records) > r.Limit {
		result.Records = result.Records[:r.Limit]
	}
	return result, nil
}
