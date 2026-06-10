// Package trace records session traces — an OTel-flavored, local-only log
// of every MCP tool invocation and every parent↔child communication. Events
// are timestamped, correlated (trace_id follows the request/response chain),
// and carry truncated payload snippets rather than full message bodies.
//
// Two storage backends: JSONL (append-only, size-rotated) and SQLite
// (modernc.org/sqlite — pure Go, WAL mode, safe for the daemon and several
// MCP processes writing concurrently).
package trace

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Event kinds.
const (
	KindMessage   = "message"   // one parent↔child communication routed by the broker
	KindTool      = "tool"      // one MCP tool invocation (call + result, with duration)
	KindFire      = "fire"      // one scheduler occurrence
	KindLifecycle = "lifecycle" // connect/disconnect/failover/shutdown markers
)

// Event statuses.
const (
	StatusOK      = "ok"
	StatusError   = "error"
	StatusQueued  = "queued"
	StatusDropped = "dropped"
)

// Event is one trace record. TraceID groups a request/response exchange
// (the request envelope's ID); SpanID identifies this event (an envelope ID
// or a generated tool-invocation ID).
type Event struct {
	TS         time.Time `json:"ts"`
	Source     string    `json:"source"`             // "daemon" or "mcp:<name>"
	Kind       string    `json:"kind"`               // message|tool|fire|lifecycle
	Name       string    `json:"name"`               // message type or tool name
	TraceID    string    `json:"trace_id,omitempty"` // correlation chain
	SpanID     string    `json:"span_id,omitempty"`
	Session    string    `json:"session,omitempty"` // client name this event belongs to
	Role       string    `json:"role,omitempty"`
	From       string    `json:"from,omitempty"`
	To         string    `json:"to,omitempty"`
	DurationMs float64   `json:"duration_ms,omitempty"`
	Status     string    `json:"status,omitempty"`
	Detail     string    `json:"detail,omitempty"` // truncated payload/args snippet
}

// Store persists events. Implementations must be safe for concurrent use.
type Store interface {
	Write(Event) error
	// Recent returns up to n most-recent events, oldest first, optionally
	// filtered by session name and/or trace id ("" = no filter).
	Recent(n int, session, traceID string) ([]Event, error)
	Close() error
}

// Tracer wraps a Store with an async, never-blocking queue: the broker's
// routing path and MCP tool handlers must not stall on trace I/O. A full
// queue drops events and counts the drops (reported on Close).
type Tracer struct {
	store     Store
	queue     chan Event
	done      chan struct{}
	closeOnce sync.Once
	dropped   atomic.Uint64
	maxDetail int
	source    string
}

// New starts a Tracer writing to store. maxDetail bounds Detail snippets.
func New(store Store, source string, maxDetail int) *Tracer {
	if maxDetail <= 0 {
		maxDetail = 200
	}
	t := &Tracer{
		store:     store,
		queue:     make(chan Event, 1024),
		done:      make(chan struct{}),
		maxDetail: maxDetail,
		source:    source,
	}
	go t.run()
	return t
}

// Nop returns a disabled tracer: Emit is a no-op. Callers never need nil
// checks.
func Nop() *Tracer { return nil }

func (t *Tracer) run() {
	defer close(t.done)
	for ev := range t.queue {
		if err := t.store.Write(ev); err != nil {
			t.dropped.Add(1)
		}
	}
}

// Emit records an event without blocking. Nil-safe (disabled tracer).
func (t *Tracer) Emit(ev Event) {
	if t == nil {
		return
	}
	if ev.TS.IsZero() {
		ev.TS = time.Now().UTC()
	}
	if ev.Source == "" {
		ev.Source = t.source
	}
	ev.Detail = Truncate(ev.Detail, t.maxDetail)
	select {
	case t.queue <- ev:
	default:
		t.dropped.Add(1)
	}
}

// Dropped reports how many events were lost to backpressure or write errors.
func (t *Tracer) Dropped() uint64 {
	if t == nil {
		return 0
	}
	return t.dropped.Load()
}

// Close flushes the queue and closes the store. Nil-safe.
func (t *Tracer) Close() error {
	if t == nil {
		return nil
	}
	var err error
	t.closeOnce.Do(func() {
		close(t.queue)
		<-t.done
		err = t.store.Close()
	})
	return err
}

// Truncate clips s to at most max characters, marking the cut. Newlines are
// flattened so one event is always one line in JSONL output.
func Truncate(s string, max int) string {
	s = strings.ReplaceAll(s, "\n", "␤")
	if max > 0 && len(s) > max {
		return s[:max] + fmt.Sprintf("…(+%d chars)", len(s)-max)
	}
	return s
}
