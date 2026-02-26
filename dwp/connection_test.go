package dwp

import (
	"testing"
	"time"
)

func TestConnection(t *testing.T) {
	t.Parallel()

	identity := &Identity{
		Subject: "test-user",
		AppID:   "app-1",
		Scopes:  []string{ScopeJobWrite},
	}
	codec := &JSONCodec{}

	conn := NewConnection("conn-1", identity, codec)

	if conn.ID != "conn-1" {
		t.Errorf("ID = %q, want %q", conn.ID, "conn-1")
	}
	if conn.Identity.Subject != "test-user" {
		t.Errorf("Identity.Subject = %q, want %q", conn.Identity.Subject, "test-user")
	}
	if conn.Codec.Name() != "json" {
		t.Errorf("Codec.Name = %q, want %q", conn.Codec.Name(), "json")
	}
	if conn.ConnectedAt.IsZero() {
		t.Error("ConnectedAt should not be zero")
	}
}

func TestConnectionSubscriptions(t *testing.T) {
	t.Parallel()

	conn := NewConnection("conn-2", nil, &JSONCodec{})

	conn.AddSubscription("jobs")
	conn.AddSubscription("workflow:run-1")

	subs := conn.Subscriptions()
	if len(subs) != 2 {
		t.Fatalf("len(Subscriptions) = %d, want 2", len(subs))
	}

	conn.RemoveSubscription("jobs")
	subs = conn.Subscriptions()
	if len(subs) != 1 {
		t.Fatalf("len(Subscriptions) = %d, want 1", len(subs))
	}
}

func TestConnectionTouch(t *testing.T) {
	t.Parallel()

	conn := NewConnection("conn-3", nil, &JSONCodec{})
	before := conn.LastActivity.Load().(time.Time)

	time.Sleep(time.Millisecond)
	conn.Touch()

	after := conn.LastActivity.Load().(time.Time)
	if !after.After(before) {
		t.Error("Touch should update LastActivity")
	}
}

func TestConnectionManager(t *testing.T) {
	t.Parallel()

	cm := NewConnectionManager()

	c1 := NewConnection("c1", nil, &JSONCodec{})
	c2 := NewConnection("c2", nil, &JSONCodec{})

	cm.Add(c1)
	cm.Add(c2)

	if cm.Count() != 2 {
		t.Errorf("Count = %d, want 2", cm.Count())
	}

	got, ok := cm.Get("c1")
	if !ok || got.ID != "c1" {
		t.Error("Get(c1) should return the connection")
	}

	_, ok = cm.Get("nonexistent")
	if ok {
		t.Error("Get(nonexistent) should return false")
	}

	cm.Remove("c1")
	if cm.Count() != 1 {
		t.Errorf("Count after Remove = %d, want 1", cm.Count())
	}

	all := cm.All()
	if len(all) != 1 {
		t.Errorf("len(All) = %d, want 1", len(all))
	}
}
