package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// pendingTask is one queued assignment, optionally expiring (scheduler
// queue-misfire policy, §20.8).
type pendingTask struct {
	task      protocol.TaskPayload
	expiresAt time.Time // zero = never expires
}

// taskStore holds tasks assigned by the parent until the owning child
// acknowledges them. Queues are keyed by child NAME (names are unique while
// connected and stable across reconnects), so a child that drops and
// reconnects — or a hook process draining on its behalf — still sees its
// pending work. In-memory only in v1 (spec §20.1 reserves durable storage).
type taskStore struct {
	mu         sync.Mutex
	pending    map[string][]pendingTask // child name → FIFO queue
	maxPerChld int
}

func newTaskStore(maxPerChild int) *taskStore {
	return &taskStore{
		pending:    make(map[string][]pendingTask),
		maxPerChld: maxPerChild,
	}
}

// assign queues task for the named child. expiresAt may be zero (no expiry).
// Fails when the child's queue is full.
func (s *taskStore) assign(childName string, task protocol.TaskPayload, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(childName)
	if len(s.pending[childName]) >= s.maxPerChld {
		return fmt.Errorf("child has %d pending tasks (limit %d)", len(s.pending[childName]), s.maxPerChld)
	}
	s.pending[childName] = append(s.pending[childName], pendingTask{task: task, expiresAt: expiresAt})
	return nil
}

// next returns the oldest unexpired pending task without removing it
// (removal happens on acknowledge, so an unacknowledged poll can be retried).
func (s *taskStore) next(childName string) (protocol.TaskPayload, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(childName)
	q := s.pending[childName]
	if len(q) == 0 {
		return protocol.TaskPayload{}, false
	}
	return q[0].task, true
}

// all returns every unexpired pending task for the named child (peek).
func (s *taskStore) all(childName string) []protocol.TaskPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(childName)
	q := s.pending[childName]
	out := make([]protocol.TaskPayload, 0, len(q))
	for _, p := range q {
		out = append(out, p.task)
	}
	return out
}

// acknowledge removes taskID from the child's queue.
func (s *taskStore) acknowledge(childName, taskID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.pending[childName]
	for i, p := range q {
		if p.task.TaskID == taskID {
			s.pending[childName] = append(q[:i:i], q[i+1:]...)
			return true
		}
	}
	return false
}

// pruneLocked drops expired tasks. Caller holds s.mu.
func (s *taskStore) pruneLocked(childName string) {
	q := s.pending[childName]
	if len(q) == 0 {
		return
	}
	now := time.Now()
	kept := q[:0]
	for _, p := range q {
		if p.expiresAt.IsZero() || now.Before(p.expiresAt) {
			kept = append(kept, p)
		}
	}
	if len(kept) == 0 {
		delete(s.pending, childName)
		return
	}
	s.pending[childName] = kept
}
