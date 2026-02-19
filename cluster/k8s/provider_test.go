package k8s

import (
	"context"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
)

const testNS = "default"

// newTestProvider creates a Provider backed by the fake K8s client, with the
// given pods pre-created. All pods get the dispatch-worker label.
func newTestProvider(t *testing.T, pods ...*corev1.Pod) (*Provider, *fake.Clientset) {
	t.Helper()
	objects := make([]corev1.Pod, len(pods))
	for i, pod := range pods {
		objects[i] = *pod
	}

	cs := fake.NewClientset()
	for i := range objects {
		_, err := cs.CoreV1().Pods(testNS).Create(context.Background(), &objects[i], metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create pod: %v", err)
		}
	}

	p := New(cs, testNS)
	return p, cs
}

// makeWorkerPod creates a labeled Pod suitable for dispatch.
func makeWorkerPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNS,
			Labels: map[string]string{
				"app.kubernetes.io/component": "dispatch-worker",
			},
			Annotations: make(map[string]string),
		},
	}
}

// makeWorker creates a cluster.Worker with the given hostname.
func makeWorker(t *testing.T, hostname string) *cluster.Worker {
	t.Helper()
	now := time.Now().UTC()
	return &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    hostname,
		Queues:      []string{"default", "email"},
		Concurrency: 5,
		State:       cluster.WorkerActive,
		LastSeen:    now,
		Metadata:    map[string]string{"zone": "us-east-1"},
		CreatedAt:   now,
	}
}

// ──────────────────────────────────────────────────
// Worker registration tests
// ──────────────────────────────────────────────────

func TestRegisterWorker(t *testing.T) {
	pod := makeWorkerPod("worker-pod-1")
	p, _ := newTestProvider(t, pod)
	ctx := context.Background()

	w := makeWorker(t, "worker-pod-1")
	if err := p.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	// Verify annotations were written.
	updated, err := p.client.CoreV1().Pods(testNS).Get(ctx, "worker-pod-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}

	prefix := defaultAnnotationPrefix
	if got := updated.Annotations[prefix+"worker-id"]; got != w.ID.String() {
		t.Errorf("worker-id annotation: got %q, want %q", got, w.ID.String())
	}
	if got := updated.Annotations[prefix+"hostname"]; got != "worker-pod-1" {
		t.Errorf("hostname annotation: got %q, want %q", got, "worker-pod-1")
	}
	if got := updated.Annotations[prefix+"state"]; got != "active" {
		t.Errorf("state annotation: got %q, want %q", got, "active")
	}
	if got := updated.Annotations[prefix+"concurrency"]; got != "5" {
		t.Errorf("concurrency annotation: got %q, want %q", got, "5")
	}
}

func TestRegisterWorker_PodNotFound(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	w := makeWorker(t, "nonexistent-pod")
	err := p.RegisterWorker(ctx, w)
	if err == nil {
		t.Fatal("expected error for nonexistent pod")
	}
}

func TestDeregisterWorker(t *testing.T) {
	pod := makeWorkerPod("worker-pod-1")
	p, _ := newTestProvider(t, pod)
	ctx := context.Background()

	w := makeWorker(t, "worker-pod-1")
	if err := p.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	if err := p.DeregisterWorker(ctx, w.ID); err != nil {
		t.Fatalf("DeregisterWorker: %v", err)
	}

	// Verify annotations were removed.
	updated, err := p.client.CoreV1().Pods(testNS).Get(ctx, "worker-pod-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	prefix := defaultAnnotationPrefix
	if _, ok := updated.Annotations[prefix+"worker-id"]; ok {
		t.Error("worker-id annotation should be removed")
	}
}

func TestDeregisterWorker_NotFound(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	err := p.DeregisterWorker(ctx, id.NewWorkerID())
	if err == nil {
		t.Fatal("expected ErrWorkerNotFound for unknown worker")
	}
}

// ──────────────────────────────────────────────────
// Heartbeat tests
// ──────────────────────────────────────────────────

func TestHeartbeatWorker(t *testing.T) {
	pod := makeWorkerPod("worker-pod-1")
	p, _ := newTestProvider(t, pod)
	ctx := context.Background()

	w := makeWorker(t, "worker-pod-1")
	w.LastSeen = time.Now().UTC().Add(-1 * time.Hour) // old timestamp
	if err := p.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	before := time.Now().UTC()
	if err := p.HeartbeatWorker(ctx, w.ID); err != nil {
		t.Fatalf("HeartbeatWorker: %v", err)
	}

	// Verify last-seen was updated.
	updated, err := p.client.CoreV1().Pods(testNS).Get(ctx, "worker-pod-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	prefix := defaultAnnotationPrefix
	lastSeen, parseErr := time.Parse(time.RFC3339Nano, updated.Annotations[prefix+"last-seen"])
	if parseErr != nil {
		t.Fatalf("parse last-seen: %v", parseErr)
	}
	if lastSeen.Before(before) {
		t.Error("last-seen should be updated to now or later")
	}
}

func TestHeartbeatWorker_NotFound(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	err := p.HeartbeatWorker(ctx, id.NewWorkerID())
	if err == nil {
		t.Fatal("expected ErrWorkerNotFound")
	}
}

// ──────────────────────────────────────────────────
// ListWorkers tests
// ──────────────────────────────────────────────────

func TestListWorkers(t *testing.T) {
	pod1 := makeWorkerPod("worker-pod-1")
	pod2 := makeWorkerPod("worker-pod-2")
	p, _ := newTestProvider(t, pod1, pod2)
	ctx := context.Background()

	w1 := makeWorker(t, "worker-pod-1")
	w2 := makeWorker(t, "worker-pod-2")
	if err := p.RegisterWorker(ctx, w1); err != nil {
		t.Fatalf("RegisterWorker 1: %v", err)
	}
	if err := p.RegisterWorker(ctx, w2); err != nil {
		t.Fatalf("RegisterWorker 2: %v", err)
	}

	workers, err := p.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if got := len(workers); got != 2 {
		t.Fatalf("expected 2 workers, got %d", got)
	}
}

func TestListWorkers_Empty(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	workers, err := p.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(workers) != 0 {
		t.Fatalf("expected 0 workers, got %d", len(workers))
	}
}

func TestListWorkers_SkipsNonDispatchPods(t *testing.T) {
	// Create a pod without dispatch annotations.
	pod := makeWorkerPod("plain-pod")
	p, _ := newTestProvider(t, pod)
	ctx := context.Background()

	workers, err := p.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(workers) != 0 {
		t.Fatalf("expected 0 workers (pod has no annotations), got %d", len(workers))
	}
}

// ──────────────────────────────────────────────────
// ReapDeadWorkers tests
// ──────────────────────────────────────────────────

func TestReapDeadWorkers(t *testing.T) {
	pod1 := makeWorkerPod("alive-pod")
	pod2 := makeWorkerPod("dead-pod")
	p, _ := newTestProvider(t, pod1, pod2)
	ctx := context.Background()

	now := time.Now().UTC()
	alive := makeWorker(t, "alive-pod")
	alive.LastSeen = now
	dead := makeWorker(t, "dead-pod")
	dead.LastSeen = now.Add(-2 * time.Hour) // 2 hours ago

	if err := p.RegisterWorker(ctx, alive); err != nil {
		t.Fatalf("RegisterWorker alive: %v", err)
	}
	if err := p.RegisterWorker(ctx, dead); err != nil {
		t.Fatalf("RegisterWorker dead: %v", err)
	}

	reaped, err := p.ReapDeadWorkers(ctx, 1*time.Hour)
	if err != nil {
		t.Fatalf("ReapDeadWorkers: %v", err)
	}
	if len(reaped) != 1 {
		t.Fatalf("expected 1 dead worker, got %d", len(reaped))
	}
	if reaped[0].Hostname != "dead-pod" {
		t.Errorf("expected dead worker hostname %q, got %q", "dead-pod", reaped[0].Hostname)
	}
}

// ──────────────────────────────────────────────────
// Leadership tests
// ──────────────────────────────────────────────────

func TestAcquireLeadership_New(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	wID := id.NewWorkerID()
	acquired, err := p.AcquireLeadership(ctx, wID, 30*time.Second)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire leadership")
	}

	// Verify lease was created.
	lease, err := p.client.CoordinationV1().Leases(testNS).Get(ctx, defaultLeaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get lease: %v", err)
	}
	if got := *lease.Spec.HolderIdentity; got != wID.String() {
		t.Errorf("holder identity: got %q, want %q", got, wID.String())
	}
}

func TestAcquireLeadership_ReAcquire(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	wID := id.NewWorkerID()

	acquired1, err := p.AcquireLeadership(ctx, wID, 30*time.Second)
	if err != nil {
		t.Fatalf("AcquireLeadership 1: %v", err)
	}
	if !acquired1 {
		t.Fatal("expected first acquire")
	}

	// Re-acquire with same worker.
	acquired2, err := p.AcquireLeadership(ctx, wID, 60*time.Second)
	if err != nil {
		t.Fatalf("AcquireLeadership 2: %v", err)
	}
	if !acquired2 {
		t.Fatal("expected re-acquire")
	}
}

func TestAcquireLeadership_Contested(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	w1 := id.NewWorkerID()
	w2 := id.NewWorkerID()

	acquired1, err := p.AcquireLeadership(ctx, w1, 30*time.Second)
	if err != nil {
		t.Fatalf("AcquireLeadership w1: %v", err)
	}
	if !acquired1 {
		t.Fatal("expected w1 to acquire")
	}

	// w2 should fail because w1 still holds it.
	acquired2, err := p.AcquireLeadership(ctx, w2, 30*time.Second)
	if err != nil {
		t.Fatalf("AcquireLeadership w2: %v", err)
	}
	if acquired2 {
		t.Fatal("expected w2 to NOT acquire (w1 holds lease)")
	}
}

func TestAcquireLeadership_ExpiredLease(t *testing.T) {
	p, cs := newTestProvider(t)
	ctx := context.Background()

	// Create an already-expired lease manually.
	w1 := id.NewWorkerID()
	w1Str := w1.String()
	ttlSec := int32(1) // 1 second
	pastTime := metav1.NewMicroTime(time.Now().UTC().Add(-5 * time.Second))

	expiredLease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultLeaseName,
			Namespace: testNS,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &w1Str,
			LeaseDurationSeconds: &ttlSec,
			AcquireTime:          &pastTime,
			RenewTime:            &pastTime,
		},
	}
	_, err := cs.CoordinationV1().Leases(testNS).Create(ctx, expiredLease, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create expired lease: %v", err)
	}

	// w2 should be able to acquire the expired lease.
	w2 := id.NewWorkerID()
	acquired, err := p.AcquireLeadership(ctx, w2, 30*time.Second)
	if err != nil {
		t.Fatalf("AcquireLeadership w2: %v", err)
	}
	if !acquired {
		t.Fatal("expected w2 to acquire expired lease")
	}
}

func TestRenewLeadership(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	wID := id.NewWorkerID()

	acquired, err := p.AcquireLeadership(ctx, wID, 30*time.Second)
	if err != nil || !acquired {
		t.Fatalf("AcquireLeadership: acquired=%v, err=%v", acquired, err)
	}

	renewed, err := p.RenewLeadership(ctx, wID, 60*time.Second)
	if err != nil {
		t.Fatalf("RenewLeadership: %v", err)
	}
	if !renewed {
		t.Fatal("expected renewal to succeed")
	}

	// Verify duration was updated.
	lease, _ := p.client.CoordinationV1().Leases(testNS).Get(ctx, defaultLeaseName, metav1.GetOptions{})
	if got := *lease.Spec.LeaseDurationSeconds; got != 60 {
		t.Errorf("expected duration 60, got %d", got)
	}
}

func TestRenewLeadership_NotLeader(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	w1 := id.NewWorkerID()
	w2 := id.NewWorkerID()

	acquired, _ := p.AcquireLeadership(ctx, w1, 30*time.Second)
	if !acquired {
		t.Fatal("expected w1 to acquire")
	}

	// w2 cannot renew — it's not the leader.
	renewed, err := p.RenewLeadership(ctx, w2, 30*time.Second)
	if err != nil {
		t.Fatalf("RenewLeadership: %v", err)
	}
	if renewed {
		t.Fatal("expected w2 renewal to fail")
	}
}

func TestRenewLeadership_NoLease(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	renewed, err := p.RenewLeadership(ctx, id.NewWorkerID(), 30*time.Second)
	if err != nil {
		t.Fatalf("RenewLeadership: %v", err)
	}
	if renewed {
		t.Fatal("expected false when no lease exists")
	}
}

func TestGetLeader(t *testing.T) {
	pod := makeWorkerPod("leader-pod")
	p, _ := newTestProvider(t, pod)
	ctx := context.Background()

	w := makeWorker(t, "leader-pod")
	if err := p.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	acquired, err := p.AcquireLeadership(ctx, w.ID, 30*time.Second)
	if err != nil || !acquired {
		t.Fatalf("AcquireLeadership: acquired=%v, err=%v", acquired, err)
	}

	leader, err := p.GetLeader(ctx)
	if err != nil {
		t.Fatalf("GetLeader: %v", err)
	}
	if leader == nil {
		t.Fatal("expected leader, got nil")
	}
	if leader.ID != w.ID {
		t.Errorf("leader ID: got %v, want %v", leader.ID, w.ID)
	}
	if !leader.IsLeader {
		t.Error("leader.IsLeader should be true")
	}
}

func TestGetLeader_NoLease(t *testing.T) {
	p, _ := newTestProvider(t)
	ctx := context.Background()

	leader, err := p.GetLeader(ctx)
	if err != nil {
		t.Fatalf("GetLeader: %v", err)
	}
	if leader != nil {
		t.Fatalf("expected nil leader, got %v", leader)
	}
}

func TestGetLeader_ExpiredLease(t *testing.T) {
	p, cs := newTestProvider(t)
	ctx := context.Background()

	wID := id.NewWorkerID()
	wIDStr := wID.String()
	ttlSec := int32(1)
	pastTime := metav1.NewMicroTime(time.Now().UTC().Add(-5 * time.Second))

	expiredLease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultLeaseName,
			Namespace: testNS,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &wIDStr,
			LeaseDurationSeconds: &ttlSec,
			AcquireTime:          &pastTime,
			RenewTime:            &pastTime,
		},
	}
	if _, err := cs.CoordinationV1().Leases(testNS).Create(ctx, expiredLease, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create expired lease: %v", err)
	}

	leader, err := p.GetLeader(ctx)
	if err != nil {
		t.Fatalf("GetLeader: %v", err)
	}
	if leader != nil {
		t.Fatal("expected nil leader for expired lease")
	}
}

// ──────────────────────────────────────────────────
// Options tests
// ──────────────────────────────────────────────────

func TestOptions(t *testing.T) {
	cs := fake.NewClientset()
	p := New(cs, testNS,
		WithLeaseName("my-leader"),
		WithLabelSelector("app=my-worker"),
		WithAnnotationPrefix("myapp.io/"),
	)

	if p.leaseName != "my-leader" {
		t.Errorf("leaseName: got %q, want %q", p.leaseName, "my-leader")
	}
	if p.labelSelector != "app=my-worker" {
		t.Errorf("labelSelector: got %q, want %q", p.labelSelector, "app=my-worker")
	}
	if p.annotationPrefix != "myapp.io/" {
		t.Errorf("annotationPrefix: got %q, want %q", p.annotationPrefix, "myapp.io/")
	}
}

// ──────────────────────────────────────────────────
// Round-trip annotation tests
// ──────────────────────────────────────────────────

func TestWorkerAnnotationRoundTrip(t *testing.T) {
	pod := makeWorkerPod("roundtrip-pod")
	p, _ := newTestProvider(t, pod)
	ctx := context.Background()

	original := makeWorker(t, "roundtrip-pod")
	if err := p.RegisterWorker(ctx, original); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	workers, err := p.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(workers))
	}

	w := workers[0]
	if w.ID != original.ID {
		t.Errorf("ID mismatch: got %v, want %v", w.ID, original.ID)
	}
	if w.Hostname != original.Hostname {
		t.Errorf("Hostname mismatch: got %q, want %q", w.Hostname, original.Hostname)
	}
	if w.Concurrency != original.Concurrency {
		t.Errorf("Concurrency mismatch: got %d, want %d", w.Concurrency, original.Concurrency)
	}
	if w.State != original.State {
		t.Errorf("State mismatch: got %q, want %q", w.State, original.State)
	}
	if len(w.Queues) != len(original.Queues) {
		t.Errorf("Queues length mismatch: got %d, want %d", len(w.Queues), len(original.Queues))
	}
	if len(w.Metadata) != len(original.Metadata) {
		t.Errorf("Metadata length mismatch: got %d, want %d", len(w.Metadata), len(original.Metadata))
	}
}
