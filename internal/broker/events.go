package broker

import (
	"sync"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// eventHub fans broker events out to subscribers (the in-process TUI and
// observer connections). Publishing never blocks: a subscriber that falls
// behind loses events rather than stalling message routing (spec Appendix
// B.4 — TUI lag must not affect the broker).
type eventHub struct {
	mu   sync.Mutex
	subs map[int]chan protocol.EventPayload
	next int
}

func newEventHub() *eventHub {
	return &eventHub{subs: make(map[int]chan protocol.EventPayload)}
}

// Subscribe returns a buffered event channel and an unsubscribe function.
// The channel is closed by unsubscribe.
func (h *eventHub) Subscribe() (<-chan protocol.EventPayload, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.next
	h.next++
	ch := make(chan protocol.EventPayload, 256)
	h.subs[id] = ch
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if c, ok := h.subs[id]; ok {
			delete(h.subs, id)
			close(c)
		}
	}
}

// Publish delivers ev to all subscribers, dropping for any whose buffer is
// full.
func (h *eventHub) Publish(ev protocol.EventPayload) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// CloseAll unsubscribes everyone (daemon teardown).
func (h *eventHub) CloseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, ch := range h.subs {
		delete(h.subs, id)
		close(ch)
	}
}
