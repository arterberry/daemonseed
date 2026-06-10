package shutdown

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// fakeBus implements Notifier and lets tests script child behavior.
type fakeBus struct {
	mu       sync.Mutex
	children []Target
	sent     map[string]*protocol.Envelope
	forced   []string
	sendFail map[string]bool
	onNotice func(bus *fakeBus, c *Cascade, clientID string)
	cascade  *Cascade
}

func newFakeBus(children ...Target) *fakeBus {
	return &fakeBus{
		children: children,
		sent:     make(map[string]*protocol.Envelope),
		sendFail: make(map[string]bool),
	}
}

func (f *fakeBus) Children() []Target { return f.children }

func (f *fakeBus) Send(clientID string, env *protocol.Envelope) bool {
	f.mu.Lock()
	fail := f.sendFail[clientID]
	f.sent[clientID] = env
	cb := f.onNotice
	c := f.cascade
	f.mu.Unlock()
	if fail {
		return false
	}
	if cb != nil {
		go cb(f, c, clientID)
	}
	return true
}

func (f *fakeBus) ForceDisconnect(clientID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forced = append(f.forced, clientID)
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCascade_AllACK(t *testing.T) {
	bus := newFakeBus(Target{ID: "c1", Name: "api"}, Target{ID: "c2", Name: "ui"})
	c := New(bus, 5*time.Second, nil)
	bus.cascade = c
	bus.onNotice = func(_ *fakeBus, c *Cascade, id string) { c.Ack(id) }

	start := time.Now()
	res := c.Run(context.Background(), "test shutdown", "parent")
	if !equal(sorted(res.Acked), []string{"api", "ui"}) {
		t.Errorf("acked = %v", res.Acked)
	}
	if len(res.Forced) != 0 {
		t.Errorf("forced = %v, want none", res.Forced)
	}
	if time.Since(start) > 2*time.Second {
		t.Error("all-ACK cascade must complete well before the timeout")
	}
	// Every child got a SHUTDOWN_NOTICE with the right payload.
	for _, id := range []string{"c1", "c2"} {
		env := bus.sent[id]
		if env == nil || env.Type != protocol.TypeShutdownNotice {
			t.Fatalf("child %s did not receive SHUTDOWN_NOTICE", id)
		}
		var p protocol.ShutdownNoticePayload
		if err := env.DecodePayload(&p); err != nil {
			t.Fatalf("notice payload: %v", err)
		}
		if p.Reason != "test shutdown" || p.InitiatedBy != "parent" {
			t.Errorf("notice payload = %+v", p)
		}
	}
}

func TestCascade_SomeMissing_ForceDisconnect(t *testing.T) {
	bus := newFakeBus(Target{ID: "c1", Name: "api"}, Target{ID: "c2", Name: "ui"})
	c := New(bus, 100*time.Millisecond, nil)
	bus.cascade = c
	bus.onNotice = func(_ *fakeBus, c *Cascade, id string) {
		if id == "c1" {
			c.Ack(id) // c2 never answers
		}
	}

	res := c.Run(context.Background(), "shutdown", "parent")
	if !equal(res.Acked, []string{"api"}) {
		t.Errorf("acked = %v, want [api]", res.Acked)
	}
	if !equal(res.Forced, []string{"ui"}) {
		t.Errorf("forced = %v, want [ui]", res.Forced)
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if !equal(bus.forced, []string{"c2"}) {
		t.Errorf("force-disconnected = %v, want [c2]", bus.forced)
	}
}

func TestCascade_NoChildren_CompletesImmediately(t *testing.T) {
	bus := newFakeBus()
	c := New(bus, time.Hour, nil)
	done := make(chan Result, 1)
	go func() { done <- c.Run(context.Background(), "shutdown", "signal") }()
	select {
	case res := <-done:
		if len(res.Acked) != 0 || len(res.Forced) != 0 {
			t.Errorf("result = %+v, want empty", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cascade with no children must complete immediately")
	}
}

func TestCascade_ChildDisconnectedDuringCascade(t *testing.T) {
	bus := newFakeBus(Target{ID: "c1", Name: "api"}, Target{ID: "c2", Name: "ui"})
	c := New(bus, time.Hour, nil) // huge timeout: completion must not rely on it
	bus.cascade = c
	bus.onNotice = func(_ *fakeBus, c *Cascade, id string) {
		if id == "c1" {
			c.Ack(id)
		} else {
			c.ClientGone(id) // c2 drops between notice and ACK
		}
	}

	done := make(chan Result, 1)
	go func() { done <- c.Run(context.Background(), "shutdown", "parent") }()
	select {
	case res := <-done:
		if !equal(res.Acked, []string{"api"}) || !equal(res.Forced, []string{"ui"}) {
			t.Errorf("result = %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cascade must not hang when a child disconnects mid-cascade")
	}
}

func TestCascade_UndeliverableNoticeForcesImmediately(t *testing.T) {
	bus := newFakeBus(Target{ID: "c1", Name: "api"})
	bus.sendFail["c1"] = true
	c := New(bus, time.Hour, nil)
	bus.cascade = c

	done := make(chan Result, 1)
	go func() { done <- c.Run(context.Background(), "shutdown", "parent") }()
	select {
	case res := <-done:
		if !equal(res.Forced, []string{"api"}) {
			t.Errorf("forced = %v, want [api]", res.Forced)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("undeliverable notice must not stall the cascade")
	}
}

func TestCascade_ContextCancelForcesRemaining(t *testing.T) {
	bus := newFakeBus(Target{ID: "c1", Name: "api"})
	c := New(bus, time.Hour, nil)
	bus.cascade = c
	ctx, cancel := context.WithCancel(context.Background())
	bus.onNotice = func(_ *fakeBus, _ *Cascade, _ string) { cancel() }

	done := make(chan Result, 1)
	go func() { done <- c.Run(ctx, "shutdown", "signal") }()
	select {
	case res := <-done:
		if !equal(res.Forced, []string{"api"}) {
			t.Errorf("forced = %v, want [api]", res.Forced)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled cascade must return promptly")
	}
}
