package dwp

import (
	"encoding/json"
	"testing"
)

func TestFederationEnqueueRequest(t *testing.T) {
	t.Parallel()

	req := FederationEnqueueRequest{
		Name:     "process-order",
		Payload:  json.RawMessage(`{"order_id":"123"}`),
		Queue:    "critical",
		Priority: 10,
		SourceID: "peer-west",
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded FederationEnqueueRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Name != req.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, req.Name)
	}
	if decoded.Queue != req.Queue {
		t.Errorf("Queue = %q, want %q", decoded.Queue, req.Queue)
	}
	if decoded.Priority != req.Priority {
		t.Errorf("Priority = %d, want %d", decoded.Priority, req.Priority)
	}
	if decoded.SourceID != req.SourceID {
		t.Errorf("SourceID = %q, want %q", decoded.SourceID, req.SourceID)
	}
}

func TestFederationEventRequest(t *testing.T) {
	t.Parallel()

	req := FederationEventRequest{
		Name:     "order.shipped",
		Payload:  json.RawMessage(`{"tracking":"XYZ123"}`),
		SourceID: "peer-east",
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded FederationEventRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Name != req.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, req.Name)
	}
	if decoded.SourceID != req.SourceID {
		t.Errorf("SourceID = %q, want %q", decoded.SourceID, req.SourceID)
	}
}

func TestFederationHeartbeatRequest(t *testing.T) {
	t.Parallel()

	req := FederationHeartbeatRequest{PeerID: "peer-1"}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded FederationHeartbeatRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.PeerID != req.PeerID {
		t.Errorf("PeerID = %q, want %q", decoded.PeerID, req.PeerID)
	}
}

func TestFederationStats(t *testing.T) {
	t.Parallel()

	stats := FederationStats{
		TotalPeers:      3,
		ConnectedPeers:  2,
		JobsForwarded:   150,
		EventsForwarded: 42,
	}

	data, err := json.Marshal(stats)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded FederationStats
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.TotalPeers != 3 {
		t.Errorf("TotalPeers = %d, want 3", decoded.TotalPeers)
	}
	if decoded.ConnectedPeers != 2 {
		t.Errorf("ConnectedPeers = %d, want 2", decoded.ConnectedPeers)
	}
}

func TestPeerState(t *testing.T) {
	t.Parallel()

	peer := &Peer{
		ID:       "peer-1",
		URL:      "wss://peer-1.example.com/dwp",
		Token:    "dk_peer_1",
		State:    PeerStateDisconnected,
		Metadata: map[string]string{"region": "us-west"},
	}

	if peer.ID != "peer-1" {
		t.Errorf("ID = %q, want %q", peer.ID, "peer-1")
	}
	if peer.URL != "wss://peer-1.example.com/dwp" {
		t.Errorf("URL = %q, want %q", peer.URL, "wss://peer-1.example.com/dwp")
	}
	if peer.Token != "dk_peer_1" {
		t.Errorf("Token = %q, want %q", peer.Token, "dk_peer_1")
	}
	if peer.State != PeerStateDisconnected {
		t.Errorf("State = %q, want %q", peer.State, PeerStateDisconnected)
	}
	if peer.Metadata["region"] != "us-west" {
		t.Errorf("Metadata[region] = %q, want %q", peer.Metadata["region"], "us-west")
	}
}
