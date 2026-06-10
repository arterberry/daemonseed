// Package roles defines the parent/child role model and the thread-safe
// registry that enforces the one-parent rule.
package roles

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// Role identifies what a connected client is allowed to do.
type Role string

const (
	RoleParent Role = "parent"
	RoleChild  Role = "child"

	// RoleObserver is a spec extension: a read-only role used by `daemonseed
	// tui` (attach mode) and `daemonseed status` to subscribe to the daemon's
	// event stream (§12.1). Observers receive EVENT messages, may only send
	// daemon queries (PING, LIST_REQUEST, WHOAMI_REQUEST), and do not count
	// toward limits.max_clients. It is not exposed through the MCP server.
	RoleObserver Role = "observer"
)

// RoleUnset is the state before handshake completes.
const RoleUnset Role = ""

// Valid reports whether r is a role a client may declare in HELLO.
func (r Role) Valid() bool {
	return r == RoleParent || r == RoleChild || r == RoleObserver
}

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// ValidName reports whether a client name is acceptable: non-empty and
// restricted to [a-zA-Z0-9_-]. Names are case-sensitive (spec Appendix B.7).
func ValidName(name string) bool {
	return nameRe.MatchString(name)
}

// RoleRegistry tracks which client holds the parent role and the set of
// child clients. All methods are safe for concurrent use. The one-parent
// rule is enforced under the mutex (spec Appendix B.8): two concurrent
// RegisterParent calls can never both succeed.
type RoleRegistry struct {
	mu       sync.RWMutex
	parent   *string           // client_id, nil if no parent
	children map[string]string // client_id → name
}

// NewRegistry returns an empty RoleRegistry.
func NewRegistry() *RoleRegistry {
	return &RoleRegistry{children: make(map[string]string)}
}

// RegisterParent claims the parent slot for clientID. It fails with
// protocol.ErrParentExists if another client already holds it.
func (r *RoleRegistry) RegisterParent(clientID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.parent != nil {
		return fmt.Errorf("%w (client %s)", protocol.ErrParentExists, *r.parent)
	}
	id := clientID
	r.parent = &id
	return nil
}

// RegisterChild records clientID as a child with the given name.
func (r *RoleRegistry) RegisterChild(clientID, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.children[clientID] = name
}

// Deregister removes clientID from whichever role it held. Unknown IDs are
// a no-op.
func (r *RoleRegistry) Deregister(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.parent != nil && *r.parent == clientID {
		r.parent = nil
	}
	delete(r.children, clientID)
}

// ParentID returns the current parent's client_id, if one is registered.
func (r *RoleRegistry) ParentID() (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.parent == nil {
		return "", false
	}
	return *r.parent, true
}

// ChildIDs returns the client_ids of all registered children.
func (r *RoleRegistry) ChildIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.children))
	for id := range r.children {
		ids = append(ids, id)
	}
	return ids
}

// ChildByName returns the client_id registered under name. Names are
// case-sensitive.
func (r *RoleRegistry) ChildByName(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, n := range r.children {
		if n == name {
			return id, true
		}
	}
	return "", false
}

// RoleOf returns the role currently held by clientID, or RoleUnset.
func (r *RoleRegistry) RoleOf(clientID string) Role {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.parent != nil && *r.parent == clientID {
		return RoleParent
	}
	if _, ok := r.children[clientID]; ok {
		return RoleChild
	}
	return RoleUnset
}
