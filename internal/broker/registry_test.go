package broker

import (
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
)

func pipeClient(id, name string, role roles.Role) *Client {
	server, client := net.Pipe()
	_ = client // the far end is irrelevant for registry tests
	return newClient(id, server, protocol.HelloPayload{Name: name, Version: "1.0.0"}, role, 0)
}

func TestRegistry_RegisterAndRetrieve(t *testing.T) {
	r := NewRegistry(20)
	c := pipeClient("c1", "api-service", roles.RoleChild)
	if err := r.Add(c); err != nil {
		t.Fatalf("add: %v", err)
	}
	if got, ok := r.Get("c1"); !ok || got != c {
		t.Error("Get by id failed")
	}
	if got, ok := r.GetByName("api-service"); !ok || got != c {
		t.Error("Get by name failed")
	}
	if got, ok := r.Resolve("c1"); !ok || got != c {
		t.Error("Resolve by id failed")
	}
	if got, ok := r.Resolve("api-service"); !ok || got != c {
		t.Error("Resolve by name failed")
	}
	if r.Count() != 1 {
		t.Errorf("Count = %d", r.Count())
	}
}

func TestRegistry_Deregister(t *testing.T) {
	r := NewRegistry(20)
	c := pipeClient("c1", "api-service", roles.RoleChild)
	_ = r.Add(c)
	r.Remove("c1")
	if _, ok := r.Get("c1"); ok {
		t.Error("client must be gone by id")
	}
	if _, ok := r.GetByName("api-service"); ok {
		t.Error("client must be gone by name")
	}
	r.Remove("c1") // idempotent
}

func TestRegistry_ChildByName(t *testing.T) {
	r := NewRegistry(20)
	_ = r.Add(pipeClient("p1", "orchestrator", roles.RoleParent))
	_ = r.Add(pipeClient("c1", "api-service", roles.RoleChild))
	_ = r.Add(pipeClient("c2", "ui-frontend", roles.RoleChild))

	children := r.Children()
	if len(children) != 2 {
		t.Fatalf("Children() len = %d", len(children))
	}
	if len(r.All()) != 3 {
		t.Errorf("All() len = %d", len(r.All()))
	}
	// Case-sensitive lookups (spec Appendix B.7).
	if _, ok := r.GetByName("API-Service"); ok {
		t.Error("name lookup must be case-sensitive")
	}
}

func TestRegistry_DuplicateNameRejected(t *testing.T) {
	r := NewRegistry(20)
	_ = r.Add(pipeClient("c1", "api-service", roles.RoleChild))
	err := r.Add(pipeClient("c2", "api-service", roles.RoleChild))
	if !errors.Is(err, protocol.ErrNameTaken) {
		t.Errorf("want ErrNameTaken, got %v", err)
	}
}

func TestRegistry_MaxClients(t *testing.T) {
	r := NewRegistry(2)
	_ = r.Add(pipeClient("c1", "a", roles.RoleChild))
	_ = r.Add(pipeClient("c2", "b", roles.RoleChild))
	err := r.Add(pipeClient("c3", "c", roles.RoleChild))
	if !errors.Is(err, protocol.ErrMaxClientsReached) {
		t.Errorf("want ErrMaxClientsReached, got %v", err)
	}
	// Observers do not count toward the limit.
	if err := r.Add(pipeClient("o1", "watcher", roles.RoleObserver)); err != nil {
		t.Errorf("observer must be exempt from max_clients: %v", err)
	}
	if r.Count() != 2 {
		t.Errorf("Count must exclude observers, got %d", r.Count())
	}
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	r := NewRegistry(100)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			id := string(rune('A' + i))
			_ = r.Add(pipeClient(id, "n-"+id, roles.RoleChild))
		}(i)
		go func() {
			defer wg.Done()
			_ = r.All()
			_ = r.Count()
			_, _ = r.Resolve("n-A")
		}()
	}
	wg.Wait()
	if r.Count() != 32 {
		t.Errorf("Count = %d, want 32", r.Count())
	}
}
