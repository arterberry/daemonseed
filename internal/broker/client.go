package broker

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
)

// outboundBuffer is the per-client outbound queue depth. If a slow consumer
// fills it, further messages to that client are dropped with a log entry and
// the sender receives DELIVERY_FAILED (spec Appendix B.3) — one slow client
// must never stall the broker.
const outboundBuffer = 64

// Client is one connected, handshake-complete session.
type Client struct {
	ID          string
	Name        string
	Role        roles.Role
	Version     string // client-reported version from HELLO
	ConnectedAt time.Time

	conn        net.Conn
	out         chan *protocol.Envelope
	quit        chan struct{} // closed by Close; stops the write loop
	writeDone   chan struct{} // closed when the write loop exits
	closeOnce   sync.Once
	closed      atomic.Bool
	maxMsgBytes int

	mu          sync.RWMutex
	state       string // last reported status state (idle|working|...)
	currentTask string
}

func newClient(id string, conn net.Conn, hello protocol.HelloPayload, role roles.Role, maxMsgBytes int) *Client {
	return &Client{
		ID:          id,
		Name:        hello.Name,
		Role:        role,
		Version:     hello.Version,
		ConnectedAt: time.Now().UTC(),
		conn:        conn,
		out:         make(chan *protocol.Envelope, outboundBuffer),
		quit:        make(chan struct{}),
		writeDone:   make(chan struct{}),
		maxMsgBytes: maxMsgBytes,
		state:       "idle",
	}
}

// writeLoop is the only goroutine that writes to the connection after the
// handshake, so frames can never interleave.
func (c *Client) writeLoop() {
	defer close(c.writeDone)
	for {
		select {
		case <-c.quit:
			// Drain anything already queued so a final notice (e.g.
			// SHUTDOWN_NOTICE) still reaches the wire before close.
			for {
				select {
				case env := <-c.out:
					if protocol.WriteMessage(c.conn, env, c.maxMsgBytes) != nil {
						return
					}
				default:
					return
				}
			}
		case env := <-c.out:
			if err := protocol.WriteMessage(c.conn, env, c.maxMsgBytes); err != nil {
				return // connection is broken; read loop will observe it too
			}
		}
	}
}

// Send enqueues env for delivery without blocking. It returns false if the
// client is closed or its outbound buffer is full.
func (c *Client) Send(env *protocol.Envelope) bool {
	if c.closed.Load() {
		return false
	}
	select {
	case c.out <- env:
		return true
	default:
		return false
	}
}

// Close shuts the connection down. Safe to call multiple times and from any
// goroutine. The write loop is given a brief window to flush queued frames.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.quit)
		select {
		case <-c.writeDone:
		case <-time.After(time.Second):
		}
		_ = c.conn.Close()
	})
}

// SetStatus records the client's last reported state and task.
func (c *Client) SetStatus(state, currentTask string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = state
	c.currentTask = currentTask
}

// Status returns the last reported state and task.
func (c *Client) Status() (state, currentTask string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state, c.currentTask
}

// Info renders the client as a protocol.ClientInfo for LIST/WHOAMI responses.
func (c *Client) Info(lastSeen time.Time) protocol.ClientInfo {
	state, task := c.Status()
	return protocol.ClientInfo{
		ClientID:    c.ID,
		Name:        c.Name,
		Role:        string(c.Role),
		State:       state,
		CurrentTask: task,
		ConnectedAt: c.ConnectedAt,
		LastSeen:    lastSeen,
	}
}
