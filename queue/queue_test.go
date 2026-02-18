package queue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Manager basics
// ---------------------------------------------------------------------------

func TestNewManager_Empty(t *testing.T) {
	m := NewManager()
	// No configs; Acquire/Release should always succeed.
	if !m.Acquire("any-queue", "") {
		t.Fatal("expected Acquire to succeed for unconfigured queue")
	}
	m.Release("any-queue", "")
}

func TestNewManager_WithConfig(t *testing.T) {
	m := NewManager(Config{
		Name:           "emails",
		MaxConcurrency: 2,
	})
	if m.ActiveCount("emails") != 0 {
		t.Fatal("expected 0 active jobs initially")
	}
}

// ---------------------------------------------------------------------------
// Concurrency limits
// ---------------------------------------------------------------------------

func TestManager_MaxConcurrency(t *testing.T) {
	m := NewManager(Config{
		Name:           "emails",
		MaxConcurrency: 2,
	})

	if !m.Acquire("emails", "") {
		t.Fatal("first Acquire should succeed")
	}
	if !m.Acquire("emails", "") {
		t.Fatal("second Acquire should succeed")
	}
	// Third should be blocked.
	if m.Acquire("emails", "") {
		t.Fatal("third Acquire should fail (max concurrency 2)")
	}

	// Release one slot.
	m.Release("emails", "")
	if !m.Acquire("emails", "") {
		t.Fatal("Acquire should succeed after Release")
	}
}

func TestManager_AcquireRelease_ActiveCount(t *testing.T) {
	m := NewManager(Config{
		Name:           "q",
		MaxConcurrency: 5,
	})

	for i := range 3 {
		if !m.Acquire("q", "") {
			t.Fatalf("Acquire %d should succeed", i)
		}
	}
	if m.ActiveCount("q") != 3 {
		t.Fatalf("expected 3 active, got %d", m.ActiveCount("q"))
	}

	m.Release("q", "")
	m.Release("q", "")
	if m.ActiveCount("q") != 1 {
		t.Fatalf("expected 1 active, got %d", m.ActiveCount("q"))
	}
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

func TestManager_RateLimit_Throttles(t *testing.T) {
	m := NewManager(Config{
		Name:      "limited",
		RateLimit: 1.0, // 1 per second
		RateBurst: 1,
	})

	// First should succeed (burst allows it).
	if !m.Acquire("limited", "") {
		t.Fatal("first Acquire should succeed (within burst)")
	}
	m.Release("limited", "")

	// Immediately after, token bucket is empty.
	if m.Acquire("limited", "") {
		t.Fatal("second Acquire should fail (rate limited)")
	}

	// Wait for token refill.
	time.Sleep(1100 * time.Millisecond)
	if !m.Acquire("limited", "") {
		t.Fatal("Acquire should succeed after token refill")
	}
	m.Release("limited", "")
}

func TestManager_RateLimit_BurstAllows(t *testing.T) {
	m := NewManager(Config{
		Name:      "bursty",
		RateLimit: 10.0,
		RateBurst: 3,
	})

	// Three immediate acquires should succeed (burst = 3).
	for i := range 3 {
		if !m.Acquire("bursty", "") {
			t.Fatalf("Acquire %d should succeed (within burst)", i)
		}
		m.Release("bursty", "")
	}
}

// ---------------------------------------------------------------------------
// Per-tenant isolation
// ---------------------------------------------------------------------------

func TestManager_TenantRateLimit(t *testing.T) {
	m := NewManager(Config{
		Name:           "shared",
		MaxConcurrency: 100, // high queue limit
	})

	m.SetTenantConfig(TenantConfig{
		QueueName:      "shared",
		TenantID:       "orgA",
		MaxConcurrency: 1,
	})

	// Tenant A: first job succeeds.
	if !m.Acquire("shared", "orgA") {
		t.Fatal("orgA first Acquire should succeed")
	}
	// Tenant A: second job blocked.
	if m.Acquire("shared", "orgA") {
		t.Fatal("orgA second Acquire should fail (tenant max 1)")
	}

	// Tenant B (no config): should still succeed.
	if !m.Acquire("shared", "orgB") {
		t.Fatal("orgB Acquire should succeed (no tenant limit)")
	}

	m.Release("shared", "orgA")
	m.Release("shared", "orgB")
}

func TestManager_TenantIsolation(t *testing.T) {
	m := NewManager(Config{
		Name:           "work",
		MaxConcurrency: 100,
	})

	m.SetTenantConfig(TenantConfig{
		QueueName:      "work",
		TenantID:       "orgA",
		MaxConcurrency: 2,
	})
	m.SetTenantConfig(TenantConfig{
		QueueName:      "work",
		TenantID:       "orgB",
		MaxConcurrency: 2,
	})

	// Fill orgA slots.
	m.Acquire("work", "orgA")
	m.Acquire("work", "orgA")

	// orgA is maxed.
	if m.Acquire("work", "orgA") {
		t.Fatal("orgA should be blocked at max concurrency")
	}

	// orgB is unaffected.
	if !m.Acquire("work", "orgB") {
		t.Fatal("orgB should not be affected by orgA's limits")
	}

	m.Release("work", "orgA")
	m.Release("work", "orgA")
	m.Release("work", "orgB")
}

func TestManager_TenantActiveCount(t *testing.T) {
	m := NewManager(Config{Name: "q", MaxConcurrency: 10})
	m.SetTenantConfig(TenantConfig{
		QueueName:      "q",
		TenantID:       "t1",
		MaxConcurrency: 5,
	})

	m.Acquire("q", "t1")
	m.Acquire("q", "t1")

	if got := m.TenantActiveCount("q", "t1"); got != 2 {
		t.Fatalf("expected tenant active 2, got %d", got)
	}

	m.Release("q", "t1")
	if got := m.TenantActiveCount("q", "t1"); got != 1 {
		t.Fatalf("expected tenant active 1, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Dynamic reconfiguration
// ---------------------------------------------------------------------------

func TestManager_SetQueueConfig(t *testing.T) {
	m := NewManager(Config{
		Name:           "dyn",
		MaxConcurrency: 1,
	})

	m.Acquire("dyn", "")
	if m.Acquire("dyn", "") {
		t.Fatal("should be blocked at concurrency 1")
	}

	// Raise the limit dynamically.
	m.SetQueueConfig(Config{
		Name:           "dyn",
		MaxConcurrency: 3,
	})

	// Now should succeed.
	if !m.Acquire("dyn", "") {
		t.Fatal("should succeed after raising concurrency")
	}
	m.Release("dyn", "")
	m.Release("dyn", "")
}

// ---------------------------------------------------------------------------
// Concurrency safety
// ---------------------------------------------------------------------------

func TestManager_ConcurrentAccess(t *testing.T) {
	m := NewManager(Config{
		Name:           "concurrent",
		MaxConcurrency: 50,
	})

	var acquired atomic.Int64
	var wg sync.WaitGroup

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m.Acquire("concurrent", "") {
				acquired.Add(1)
				// Simulate work.
				time.Sleep(time.Millisecond)
				m.Release("concurrent", "")
			}
		}()
	}

	wg.Wait()

	// At least some should have succeeded.
	if acquired.Load() == 0 {
		t.Fatal("expected some Acquires to succeed")
	}

	// Active should be back to 0.
	if m.ActiveCount("concurrent") != 0 {
		t.Fatalf("expected 0 active after all goroutines, got %d", m.ActiveCount("concurrent"))
	}
}

func TestManager_UnconfiguredQueue_AlwaysSucceeds(t *testing.T) {
	m := NewManager(Config{
		Name:           "configured",
		MaxConcurrency: 1,
	})

	// "other" queue has no config — no limits.
	for range 10 {
		if !m.Acquire("other", "") {
			t.Fatal("unconfigured queue should always allow Acquire")
		}
	}
	for range 10 {
		m.Release("other", "")
	}
}

func TestManager_ReleaseUnderflow(t *testing.T) {
	m := NewManager(Config{
		Name:           "q",
		MaxConcurrency: 5,
	})

	// Release without Acquire should not go negative.
	m.Release("q", "")
	if m.ActiveCount("q") != 0 {
		t.Fatal("active count should not go below 0")
	}
}
