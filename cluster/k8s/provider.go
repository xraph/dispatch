package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
)

// Compile-time check that Provider implements cluster.Store.
var _ cluster.Store = (*Provider)(nil)

const (
	defaultLeaseName        = "dispatch-leader"
	defaultLabelSelector    = "app.kubernetes.io/component=dispatch-worker"
	defaultAnnotationPrefix = "dispatch.xraph.com/"
)

// Provider implements cluster.Store using Kubernetes primitives:
//   - Worker discovery via Pod annotations and label selectors
//   - Leader election via the coordination/v1 Lease API
type Provider struct {
	client           kubernetes.Interface
	namespace        string
	leaseName        string
	labelSelector    string
	annotationPrefix string
	logger           *slog.Logger
}

// New creates a Kubernetes cluster provider.
// The clientset and namespace are required. Use functional options to customise
// the lease name, label selector, annotation prefix, or logger.
func New(client kubernetes.Interface, namespace string, opts ...Option) *Provider {
	p := &Provider{
		client:           client,
		namespace:        namespace,
		leaseName:        defaultLeaseName,
		labelSelector:    defaultLabelSelector,
		annotationPrefix: defaultAnnotationPrefix,
		logger:           slog.Default(),
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// ──────────────────────────────────────────────────
// Worker registration (Pod annotations)
// ──────────────────────────────────────────────────

// RegisterWorker stores worker metadata as annotations on the worker's Pod.
// The Pod is located by matching the worker's Hostname to the Pod name.
func (p *Provider) RegisterWorker(ctx context.Context, w *cluster.Worker) error {
	pod, err := p.client.CoreV1().Pods(p.namespace).Get(ctx, w.Hostname, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("k8s: pod %q not found: %w", w.Hostname, dispatch.ErrWorkerNotFound)
		}
		return fmt.Errorf("k8s: register worker get pod: %w", err)
	}

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	p.setWorkerAnnotations(pod, w)

	_, err = p.client.CoreV1().Pods(p.namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("k8s: register worker update pod: %w", err)
	}
	return nil
}

// DeregisterWorker removes dispatch annotations from the worker's Pod.
func (p *Provider) DeregisterWorker(ctx context.Context, workerID id.WorkerID) error {
	pod, err := p.findPodByWorkerID(ctx, workerID.String())
	if err != nil {
		return err
	}
	if pod == nil {
		return dispatch.ErrWorkerNotFound
	}

	// Remove all dispatch annotations.
	p.removeWorkerAnnotations(pod)

	_, err = p.client.CoreV1().Pods(p.namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("k8s: deregister worker update pod: %w", err)
	}
	return nil
}

// HeartbeatWorker updates the last-seen annotation on the worker's Pod.
func (p *Provider) HeartbeatWorker(ctx context.Context, workerID id.WorkerID) error {
	pod, err := p.findPodByWorkerID(ctx, workerID.String())
	if err != nil {
		return err
	}
	if pod == nil {
		return dispatch.ErrWorkerNotFound
	}

	pod.Annotations[p.annotationPrefix+"last-seen"] = time.Now().UTC().Format(time.RFC3339Nano)

	_, err = p.client.CoreV1().Pods(p.namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("k8s: heartbeat worker update pod: %w", err)
	}
	return nil
}

// ListWorkers returns all registered dispatch workers by scanning Pod annotations.
func (p *Provider) ListWorkers(ctx context.Context) ([]*cluster.Worker, error) {
	pods, err := p.client.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: p.labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("k8s: list workers: %w", err)
	}

	workers := make([]*cluster.Worker, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		w, convErr := p.workerFromPod(pod)
		if convErr != nil {
			continue // pod has no/invalid dispatch annotations
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// ReapDeadWorkers returns workers whose last-seen annotation is older than
// the given threshold.
func (p *Provider) ReapDeadWorkers(ctx context.Context, threshold time.Duration) ([]*cluster.Worker, error) {
	all, err := p.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().UTC().Add(-threshold)
	var dead []*cluster.Worker
	for _, w := range all {
		if w.LastSeen.Before(cutoff) {
			dead = append(dead, w)
		}
	}
	return dead, nil
}

// ──────────────────────────────────────────────────
// Leadership (Lease API)
// ──────────────────────────────────────────────────

// AcquireLeadership attempts to become the cluster leader using
// the coordination/v1 Lease API.
func (p *Provider) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	now := metav1.NewMicroTime(time.Now().UTC())
	ttlSec := int32(ttl.Seconds())

	lease, err := p.client.CoordinationV1().Leases(p.namespace).Get(ctx, p.leaseName, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		// No lease exists — create one with us as holder.
		newLease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      p.leaseName,
				Namespace: p.namespace,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &wID,
				LeaseDurationSeconds: &ttlSec,
				AcquireTime:          &now,
				RenewTime:            &now,
			},
		}
		_, createErr := p.client.CoordinationV1().Leases(p.namespace).Create(ctx, newLease, metav1.CreateOptions{})
		if createErr != nil {
			if errors.IsAlreadyExists(createErr) {
				return false, nil // race: someone else created it first
			}
			return false, fmt.Errorf("k8s: create lease: %w", createErr)
		}
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("k8s: get lease: %w", err)
	}

	// Lease exists — check if it's expired or held by us.
	if p.isLeaseHeldByOther(lease, wID) {
		return false, nil
	}

	// Acquire or re-acquire.
	lease.Spec.HolderIdentity = &wID
	lease.Spec.LeaseDurationSeconds = &ttlSec
	lease.Spec.AcquireTime = &now
	lease.Spec.RenewTime = &now

	_, err = p.client.CoordinationV1().Leases(p.namespace).Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return false, fmt.Errorf("k8s: update lease (acquire): %w", err)
	}
	return true, nil
}

// RenewLeadership extends the leader's hold by updating the Lease.
func (p *Provider) RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	now := metav1.NewMicroTime(time.Now().UTC())
	ttlSec := int32(ttl.Seconds())

	lease, err := p.client.CoordinationV1().Leases(p.namespace).Get(ctx, p.leaseName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil // no lease exists
		}
		return false, fmt.Errorf("k8s: renew get lease: %w", err)
	}

	// We can only renew if we are the current holder.
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != wID {
		return false, nil
	}

	lease.Spec.LeaseDurationSeconds = &ttlSec
	lease.Spec.RenewTime = &now

	_, err = p.client.CoordinationV1().Leases(p.namespace).Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return false, fmt.Errorf("k8s: renew update lease: %w", err)
	}
	return true, nil
}

// GetLeader returns the current cluster leader from the Lease, or nil if
// there is no active leader.
func (p *Provider) GetLeader(ctx context.Context) (*cluster.Worker, error) {
	lease, err := p.client.CoordinationV1().Leases(p.namespace).Get(ctx, p.leaseName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("k8s: get leader lease: %w", err)
	}

	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return nil, nil
	}

	// Check expiry.
	if p.isLeaseExpired(lease) {
		return nil, nil
	}

	// Try to find the worker Pod.
	pod, err := p.findPodByWorkerID(ctx, *lease.Spec.HolderIdentity)
	if err != nil || pod == nil {
		// Return a minimal worker if Pod is gone.
		wID, parseErr := id.ParseWorkerID(*lease.Spec.HolderIdentity)
		if parseErr != nil {
			return nil, nil
		}
		return &cluster.Worker{
			ID:       wID,
			IsLeader: true,
		}, nil
	}

	w, err := p.workerFromPod(pod)
	if err != nil {
		return nil, nil
	}
	w.IsLeader = true
	return w, nil
}

// ──────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────

// setWorkerAnnotations writes all worker fields as Pod annotations.
func (p *Provider) setWorkerAnnotations(pod *corev1.Pod, w *cluster.Worker) {
	a := pod.Annotations
	prefix := p.annotationPrefix

	a[prefix+"worker-id"] = w.ID.String()
	a[prefix+"hostname"] = w.Hostname
	a[prefix+"concurrency"] = strconv.Itoa(w.Concurrency)
	a[prefix+"state"] = string(w.State)
	a[prefix+"last-seen"] = w.LastSeen.Format(time.RFC3339Nano)
	a[prefix+"created-at"] = w.CreatedAt.Format(time.RFC3339Nano)
	a[prefix+"is-leader"] = strconv.FormatBool(w.IsLeader)

	if len(w.Queues) > 0 {
		b, _ := json.Marshal(w.Queues) //nolint:errcheck // marshal of []string does not fail
		a[prefix+"queues"] = string(b)
	}
	if len(w.Metadata) > 0 {
		b, _ := json.Marshal(w.Metadata) //nolint:errcheck // marshal of map[string]string does not fail
		a[prefix+"metadata"] = string(b)
	}
	if w.LeaderUntil != nil {
		a[prefix+"leader-until"] = w.LeaderUntil.Format(time.RFC3339Nano)
	}
}

// removeWorkerAnnotations deletes all dispatch annotations from a Pod.
func (p *Provider) removeWorkerAnnotations(pod *corev1.Pod) {
	prefix := p.annotationPrefix
	keys := []string{
		"worker-id", "hostname", "concurrency", "state",
		"last-seen", "created-at", "is-leader", "queues",
		"metadata", "leader-until",
	}
	for _, k := range keys {
		delete(pod.Annotations, prefix+k)
	}
}

// workerFromPod converts Pod annotations to a cluster.Worker.
func (p *Provider) workerFromPod(pod *corev1.Pod) (*cluster.Worker, error) {
	prefix := p.annotationPrefix
	a := pod.Annotations

	rawID := a[prefix+"worker-id"]
	if rawID == "" {
		return nil, fmt.Errorf("k8s: pod %q missing worker-id annotation", pod.Name)
	}

	wID, err := id.ParseWorkerID(rawID)
	if err != nil {
		return nil, fmt.Errorf("k8s: parse worker id: %w", err)
	}

	concurrency, _ := strconv.Atoi(a[prefix+"concurrency"])              //nolint:errcheck // best-effort parse
	lastSeen, _ := time.Parse(time.RFC3339Nano, a[prefix+"last-seen"])   //nolint:errcheck // best-effort parse
	createdAt, _ := time.Parse(time.RFC3339Nano, a[prefix+"created-at"]) //nolint:errcheck // best-effort parse

	w := &cluster.Worker{
		ID:          wID,
		Hostname:    a[prefix+"hostname"],
		Concurrency: concurrency,
		State:       cluster.WorkerState(a[prefix+"state"]),
		IsLeader:    a[prefix+"is-leader"] == "true",
		LastSeen:    lastSeen,
		CreatedAt:   createdAt,
	}

	if q := a[prefix+"queues"]; q != "" {
		var queues []string
		if uErr := json.Unmarshal([]byte(q), &queues); uErr == nil {
			w.Queues = queues
		}
	}
	if m := a[prefix+"metadata"]; m != "" {
		meta := make(map[string]string)
		if uErr := json.Unmarshal([]byte(m), &meta); uErr == nil {
			w.Metadata = meta
		}
	}
	if v := a[prefix+"leader-until"]; v != "" {
		t, parseErr := time.Parse(time.RFC3339Nano, v)
		if parseErr == nil {
			w.LeaderUntil = &t
		}
	}

	return w, nil
}

// findPodByWorkerID scans pods with the label selector for one whose
// worker-id annotation matches.
func (p *Provider) findPodByWorkerID(ctx context.Context, workerID string) (*corev1.Pod, error) {
	pods, err := p.client.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: p.labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("k8s: find pod by worker id: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Annotations[p.annotationPrefix+"worker-id"] == workerID {
			return pod, nil
		}
	}
	return nil, nil
}

// isLeaseHeldByOther returns true if the lease is held by a different worker
// and has not expired.
func (p *Provider) isLeaseHeldByOther(lease *coordinationv1.Lease, myID string) bool {
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return false // no holder
	}
	if *lease.Spec.HolderIdentity == myID {
		return false // we hold it
	}
	return !p.isLeaseExpired(lease)
}

// isLeaseExpired returns true if the lease's renew time + duration is in the past.
func (p *Provider) isLeaseExpired(lease *coordinationv1.Lease) bool {
	if lease.Spec.RenewTime == nil || lease.Spec.LeaseDurationSeconds == nil {
		return true
	}
	renewTime := lease.Spec.RenewTime.Time
	dur := time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	return time.Now().UTC().After(renewTime.Add(dur))
}
