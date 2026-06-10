package health

import (
	"context"
	"testing"
	"time"
)

// TestHeartbeat_ClientDroppedAfterTimeout verifies a silent client is
// reported stale, while an active client is not.
func TestHeartbeat_ClientDroppedAfterTimeout(t *testing.T) {
	stale := make(chan string, 4)
	m := New(40*time.Millisecond, 10*time.Millisecond, func(id string) { stale <- id })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); m.Start(ctx) }()

	m.Touch("silent")
	m.Touch("active")

	// Keep "active" alive while "silent" goes quiet.
	keepAlive := time.NewTicker(15 * time.Millisecond)
	defer keepAlive.Stop()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case id := <-stale:
			if id != "silent" {
				t.Fatalf("active client %q must not be reported stale", id)
			}
			cancel()
			<-done
			if _, ok := m.LastSeen("silent"); ok {
				t.Error("stale client must be removed from tracking")
			}
			if _, ok := m.LastSeen("active"); !ok {
				t.Error("active client must still be tracked")
			}
			return
		case <-keepAlive.C:
			m.Touch("active")
		case <-deadline:
			t.Fatal("stale client was never reported")
		}
	}
}

func TestMonitor_RemoveStopsTracking(t *testing.T) {
	stale := make(chan string, 1)
	m := New(20*time.Millisecond, 5*time.Millisecond, func(id string) { stale <- id })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); m.Start(ctx) }()

	m.Touch("c1")
	m.Remove("c1")

	select {
	case id := <-stale:
		t.Fatalf("removed client %q must never be reported stale", id)
	case <-time.After(100 * time.Millisecond):
		// Quiet for well past the stale threshold: correct.
	}
	cancel()
	<-done
}

func TestMonitor_StaleFiresOncePerRegistration(t *testing.T) {
	stale := make(chan string, 8)
	m := New(20*time.Millisecond, 5*time.Millisecond, func(id string) { stale <- id })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); m.Start(ctx) }()

	m.Touch("c1")
	select {
	case <-stale:
	case <-time.After(2 * time.Second):
		t.Fatal("client never reported stale")
	}
	select {
	case <-stale:
		t.Fatal("stale must fire once per registration")
	case <-time.After(80 * time.Millisecond):
	}
	cancel()
	<-done
}

func TestMonitor_LastSeenAdvancesOnTouch(t *testing.T) {
	m := New(time.Hour, time.Hour, func(string) {})
	m.Touch("c1")
	first, ok := m.LastSeen("c1")
	if !ok {
		t.Fatal("c1 must be tracked")
	}
	m.Touch("c1")
	second, _ := m.LastSeen("c1")
	if second.Before(first) {
		t.Error("last seen must not move backwards")
	}
}
