package broker

import (
	"fmt"
	"sync"

	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
)

// Registry is the thread-safe set of connected, handshake-complete clients.
// Name uniqueness and the max-client limit are enforced atomically in Add.
type Registry struct {
	mu         sync.RWMutex
	byID       map[string]*Client
	byName     map[string]*Client
	maxClients int
}

// NewRegistry creates a Registry that admits at most maxClients
// parent/child clients (observers are exempt from the limit).
func NewRegistry(maxClients int) *Registry {
	return &Registry{
		byID:       make(map[string]*Client),
		byName:     make(map[string]*Client),
		maxClients: maxClients,
	}
}

// Add registers c. Duplicate names are rejected with ErrNameTaken — the
// spec (§17.4 TestBroker_DuplicateName) allows disambiguation or rejection;
// rejection was chosen so a name always identifies exactly the session the
// operator expects.
func (r *Registry) Add(c *Client) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.Role != roles.RoleObserver && r.countLocked() >= r.maxClients {
		return protocol.ErrMaxClientsReached
	}
	if _, exists := r.byName[c.Name]; exists {
		return fmt.Errorf("%w: %q", protocol.ErrNameTaken, c.Name)
	}
	r.byID[c.ID] = c
	r.byName[c.Name] = c
	return nil
}

// Remove deletes the client with id. Unknown ids are a no-op.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.byID[id]; ok {
		delete(r.byID, id)
		delete(r.byName, c.Name)
	}
}

// Get returns the client with the given client_id.
func (r *Registry) Get(id string) (*Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.byID[id]
	return c, ok
}

// GetByName returns the client registered under name (case-sensitive).
func (r *Registry) GetByName(name string) (*Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.byName[name]
	return c, ok
}

// Resolve looks up a target as a client_id first, then as a name.
func (r *Registry) Resolve(target string) (*Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if c, ok := r.byID[target]; ok {
		return c, true
	}
	c, ok := r.byName[target]
	return c, ok
}

// All returns every registered parent/child client (observers excluded).
func (r *Registry) All() []*Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Client, 0, len(r.byID))
	for _, c := range r.byID {
		if c.Role != roles.RoleObserver {
			out = append(out, c)
		}
	}
	return out
}

// Children returns all clients with the child role.
func (r *Registry) Children() []*Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*Client
	for _, c := range r.byID {
		if c.Role == roles.RoleChild {
			out = append(out, c)
		}
	}
	return out
}

// Observers returns all observer clients.
func (r *Registry) Observers() []*Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*Client
	for _, c := range r.byID {
		if c.Role == roles.RoleObserver {
			out = append(out, c)
		}
	}
	return out
}

// Count returns the number of parent/child clients (observers excluded).
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.countLocked()
}

func (r *Registry) countLocked() int {
	n := 0
	for _, c := range r.byID {
		if c.Role != roles.RoleObserver {
			n++
		}
	}
	return n
}
