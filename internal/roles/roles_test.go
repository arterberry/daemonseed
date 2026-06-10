package roles

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arterberry/daemonseed/internal/protocol"
)

func TestRole_Valid(t *testing.T) {
	for _, r := range []Role{RoleParent, RoleChild, RoleObserver} {
		if !r.Valid() {
			t.Errorf("%q must be valid", r)
		}
	}
	for _, r := range []Role{RoleUnset, Role("superadmin"), Role("Parent")} {
		if r.Valid() {
			t.Errorf("%q must be invalid", r)
		}
	}
}

func TestValidName(t *testing.T) {
	for _, name := range []string{"api-service", "ui_frontend", "Worker2", "a"} {
		if !ValidName(name) {
			t.Errorf("%q must be accepted", name)
		}
	}
	for _, name := range []string{"", "api service", "api.service", "日本語", "a/b", "x\n"} {
		if ValidName(name) {
			t.Errorf("%q must be rejected", name)
		}
	}
}

func TestRegistry_RegisterAndRetrieve(t *testing.T) {
	r := NewRegistry()
	if err := r.RegisterParent("p1"); err != nil {
		t.Fatalf("register parent: %v", err)
	}
	r.RegisterChild("c1", "api-service")
	r.RegisterChild("c2", "ui-frontend")

	id, ok := r.ParentID()
	if !ok || id != "p1" {
		t.Errorf("ParentID() = %q, %v", id, ok)
	}
	if got := len(r.ChildIDs()); got != 2 {
		t.Errorf("ChildIDs() len = %d, want 2", got)
	}
	if r.RoleOf("p1") != RoleParent || r.RoleOf("c1") != RoleChild || r.RoleOf("ghost") != RoleUnset {
		t.Error("RoleOf misreported roles")
	}
}

func TestRegistry_SecondParentRejected(t *testing.T) {
	r := NewRegistry()
	if err := r.RegisterParent("p1"); err != nil {
		t.Fatalf("first parent: %v", err)
	}
	err := r.RegisterParent("p2")
	if !errors.Is(err, protocol.ErrParentExists) {
		t.Errorf("second parent must fail with ErrParentExists, got %v", err)
	}
}

func TestRegistry_Deregister(t *testing.T) {
	r := NewRegistry()
	_ = r.RegisterParent("p1")
	r.RegisterChild("c1", "api-service")

	r.Deregister("p1")
	if _, ok := r.ParentID(); ok {
		t.Error("parent must be gone after deregister")
	}
	// Slot is free again.
	if err := r.RegisterParent("p2"); err != nil {
		t.Errorf("parent slot must be reusable: %v", err)
	}

	r.Deregister("c1")
	if len(r.ChildIDs()) != 0 {
		t.Error("child must be gone after deregister")
	}
	r.Deregister("never-registered") // must not panic
}

func TestRegistry_ChildByName(t *testing.T) {
	r := NewRegistry()
	r.RegisterChild("c1", "api-service")

	id, ok := r.ChildByName("api-service")
	if !ok || id != "c1" {
		t.Errorf("ChildByName = %q, %v", id, ok)
	}
	// Case-sensitive per spec Appendix B.7.
	if _, ok := r.ChildByName("API-Service"); ok {
		t.Error("name lookup must be case-sensitive")
	}
	if _, ok := r.ChildByName("nope"); ok {
		t.Error("unknown name must not resolve")
	}
}

// TestRegistry_ConcurrentParentRace hammers RegisterParent from many
// goroutines: exactly one must win.
func TestRegistry_ConcurrentParentRace(t *testing.T) {
	r := NewRegistry()
	const n = 64
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			if err := r.RegisterParent(string(rune('a' + id%26))); err == nil {
				wins.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 {
		t.Errorf("exactly one parent registration must win, got %d", wins.Load())
	}
}

// TestRegistry_ConcurrentChildren verifies concurrent child registration and
// lookup is race-free (run with -race).
func TestRegistry_ConcurrentChildren(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			r.RegisterChild(string(rune('A'+i)), "child")
		}(i)
		go func() {
			defer wg.Done()
			_ = r.ChildIDs()
			_, _ = r.ChildByName("child")
		}()
	}
	wg.Wait()
	if len(r.ChildIDs()) != 32 {
		t.Errorf("expected 32 children, got %d", len(r.ChildIDs()))
	}
}
