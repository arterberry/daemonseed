package broker

import (
	"sync"
	"time"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// inboxLimit caps each child's named inbox; oldest entries are evicted.
const inboxLimit = 200

// namedInboxes buffers messages routed to each child, keyed by child name,
// so a hook process (`daemonseed inbox --drain`, §20.7) can surface them
// into the child's Claude session. This is in addition to live delivery on
// the child's connection: the hook aperture is opt-in, and a message may
// legitimately be seen both by the session's MCP (bus_check_messages) and
// by the hook. Entries survive child reconnects; drain clears them.
type namedInboxes struct {
	mu    sync.Mutex
	boxes map[string][]protocol.InboxEntry
}

func newNamedInboxes() *namedInboxes {
	return &namedInboxes{boxes: make(map[string][]protocol.InboxEntry)}
}

// record appends a delivered message to the named inbox.
func (n *namedInboxes) record(childName, fromName string, env *protocol.Envelope) {
	n.mu.Lock()
	defer n.mu.Unlock()
	box := n.boxes[childName]
	if len(box) >= inboxLimit {
		box = box[1:]
	}
	n.boxes[childName] = append(box, protocol.InboxEntry{
		From:       fromName,
		Type:       env.Type,
		Payload:    env.Payload,
		TaskID:     env.TaskID,
		ReceivedAt: time.Now().UTC(),
	})
}

// drain returns and clears the named inbox.
func (n *namedInboxes) drain(childName string) []protocol.InboxEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	box := n.boxes[childName]
	delete(n.boxes, childName)
	return box
}
