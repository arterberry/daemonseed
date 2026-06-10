// Package health tracks client liveness. Any inbound message (including
// HEARTBEAT) refreshes a client's last-seen time; a background loop reports
// clients that have been silent longer than the stale threshold.
package health

import (
	"context"
	"sync"
	"time"
)

// Monitor tracks last-seen times and invokes a callback for stale clients.
// All methods are safe for concurrent use.
type Monitor struct {
	mu         sync.Mutex
	lastSeen   map[string]time.Time
	staleAfter time.Duration
	checkEvery time.Duration
	onStale    func(clientID string)
}

// New creates a Monitor. onStale is invoked (from the monitor goroutine,
// without holding internal locks) once per stale client; the client is
// removed from tracking before the callback so it fires exactly once per
// registration.
func New(staleAfter, checkEvery time.Duration, onStale func(clientID string)) *Monitor {
	if checkEvery <= 0 {
		checkEvery = time.Second
	}
	return &Monitor{
		lastSeen:   make(map[string]time.Time),
		staleAfter: staleAfter,
		checkEvery: checkEvery,
		onStale:    onStale,
	}
}

// Start runs the staleness check loop until ctx is canceled.
func (m *Monitor) Start(ctx context.Context) {
	ticker := time.NewTicker(m.checkEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, id := range m.collectStale(now) {
				m.onStale(id)
			}
		}
	}
}

func (m *Monitor) collectStale(now time.Time) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var stale []string
	for id, seen := range m.lastSeen {
		if now.Sub(seen) > m.staleAfter {
			stale = append(stale, id)
			delete(m.lastSeen, id)
		}
	}
	return stale
}

// Touch records activity for clientID, registering it if new.
func (m *Monitor) Touch(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSeen[clientID] = time.Now()
}

// Remove stops tracking clientID (normal disconnect).
func (m *Monitor) Remove(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.lastSeen, clientID)
}

// LastSeen returns the recorded last-seen time for clientID.
func (m *Monitor) LastSeen(clientID string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.lastSeen[clientID]
	return t, ok
}
