// Package mcp implements the daemonSeed MCP server: a stdio JSON-RPC server
// (launched by Claude Code) that bridges tool calls to the daemon's Unix
// socket. The two I/O streams are strictly separated (spec Appendix B.10):
// MCP traffic on stdin/stdout, bus traffic on the socket, logs on stderr.
package mcp

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arterberry/daemonseed/internal/config"
	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
)

// errDaemonGone is returned by requests after reconnection has failed.
var errDaemonGone = errors.New("daemonSeed daemon connection lost. Restart with: daemonseed start")

// inboxMessage is a message received outside any request/response exchange
// (broadcasts, direct messages, task pushes), buffered until the operator
// drains it with bus_check_messages.
type inboxMessage struct {
	From       string
	Type       protocol.MessageType
	Payload    string
	TaskID     string
	ReceivedAt time.Time
}

// busClient maintains the socket connection to the daemon: handshake,
// request/response correlation, heartbeats, auto-replies, and reconnection.
type busClient struct {
	cfg       *config.Config
	log       *slog.Logger
	role      roles.Role
	name      string
	version   string
	autoStart bool

	writeMu sync.Mutex // serializes frame writes
	connMu  sync.RWMutex
	conn    net.Conn

	idMu        sync.RWMutex
	clientID    string
	connectedAt time.Time

	pendingMu sync.Mutex
	pending   map[string]chan *protocol.Envelope

	inboxMu sync.Mutex
	inbox   []inboxMessage

	statusMu   sync.Mutex
	lastStatus protocol.StatusPayload

	shuttingDown atomic.Bool
	dead         atomic.Bool
	closed       chan struct{}
	readerDone   chan struct{}
	closeOnce    sync.Once
}

func newBusClient(cfg *config.Config, log *slog.Logger, role roles.Role, name, version string, autoStart bool) *busClient {
	return &busClient{
		cfg:       cfg,
		log:       log,
		role:      role,
		name:      name,
		version:   version,
		autoStart: autoStart,
		pending:   make(map[string]chan *protocol.Envelope),
		lastStatus: protocol.StatusPayload{
			State:      "idle",
			Message:    "no status reported yet",
			ReportedAt: time.Now().UTC(),
		},
		closed:     make(chan struct{}),
		readerDone: make(chan struct{}),
	}
}

// Connect dials the daemon (auto-starting it when configured), performs the
// HELLO handshake, and launches the reader and heartbeat loops.
func (b *busClient) Connect() error {
	conn, err := b.dial()
	if err != nil {
		return err
	}
	if err := b.handshake(conn); err != nil {
		conn.Close()
		return err
	}
	go b.readLoop()
	go b.heartbeatLoop()
	return nil
}

func (b *busClient) dial() (net.Conn, error) {
	socket := b.cfg.Daemon.SocketPath
	conn, err := net.DialTimeout("unix", socket, 2*time.Second)
	if err == nil {
		return conn, nil
	}
	if !b.autoStart {
		return nil, fmt.Errorf("%w. Start it with: daemonseed start", protocol.ErrDaemonNotRunning)
	}

	// auto_start: launch the daemon in the background and wait up to 3s for
	// the socket to appear (spec §7.2 option B).
	b.log.Info("daemon not running; auto-starting", "socket", socket)
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate daemonseed binary for auto-start: %w", err)
	}
	cmd := exec.Command(exe, "start", "--background", "--socket", socket)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("auto-start daemon: %w", err)
	}
	// Reap the launcher process when it exits; the daemon itself detaches.
	go func() { _ = cmd.Wait() }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("unix", socket, 250*time.Millisecond); err == nil {
			return conn, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("%w: auto-start did not produce a socket within 3s", protocol.ErrDaemonNotRunning)
}

// handshake sends HELLO and waits for HELLO_ACK on a fresh connection.
// Called before the reader loop owns the connection (or by the reader loop
// itself during reconnect), so it reads synchronously.
func (b *busClient) handshake(conn net.Conn) error {
	hello := protocol.NewEnvelope("pending", protocol.TargetDaemon, protocol.TypeHello,
		protocol.MustEncode(protocol.HelloPayload{Role: string(b.role), Name: b.name, Version: b.version}))
	if err := protocol.WriteMessage(conn, hello, b.cfg.Limits.MaxMessageBytes); err != nil {
		return fmt.Errorf("send HELLO: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	resp, err := protocol.ReadMessage(conn, b.cfg.Limits.MaxMessageBytes)
	if err != nil {
		return fmt.Errorf("read handshake response: %w", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	switch resp.Type {
	case protocol.TypeHelloAck:
		var ack protocol.HelloAckPayload
		if err := resp.DecodePayload(&ack); err != nil {
			return fmt.Errorf("decode HELLO_ACK: %w", err)
		}
		b.idMu.Lock()
		b.clientID = ack.ClientID
		b.connectedAt = time.Now().UTC()
		b.idMu.Unlock()
		b.connMu.Lock()
		b.conn = conn
		b.connMu.Unlock()
		b.log.Info("connected to daemon", "client_id", ack.ClientID,
			"role", string(b.role), "name", b.name, "daemon_version", ack.DaemonVersion)
		return nil
	case protocol.TypeHelloReject:
		var rej protocol.HelloRejectPayload
		_ = resp.DecodePayload(&rej)
		return fmt.Errorf("daemon rejected connection: %s", rej.Reason)
	default:
		return fmt.Errorf("unexpected handshake response %s", resp.Type)
	}
}

// ClientID returns the id assigned by the daemon (changes on reconnect).
func (b *busClient) ClientID() string {
	b.idMu.RLock()
	defer b.idMu.RUnlock()
	return b.clientID
}

func (b *busClient) currentConn() net.Conn {
	b.connMu.RLock()
	defer b.connMu.RUnlock()
	return b.conn
}

// Close tears the client down (process exit).
func (b *busClient) Close() {
	b.closeOnce.Do(func() {
		close(b.closed)
		if conn := b.currentConn(); conn != nil {
			conn.Close()
		}
	})
}

// send writes env to the daemon. Serialized so frames never interleave.
func (b *busClient) send(env *protocol.Envelope) error {
	if b.dead.Load() {
		return errDaemonGone
	}
	conn := b.currentConn()
	if conn == nil {
		return errDaemonGone
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if err := protocol.WriteMessage(conn, env, b.cfg.Limits.MaxMessageBytes); err != nil {
		return fmt.Errorf("send %s: %w", env.Type, err)
	}
	return nil
}

// request sends env and waits for the correlated response from the daemon.
func (b *busClient) request(env *protocol.Envelope, timeout time.Duration) (*protocol.Envelope, error) {
	ch := make(chan *protocol.Envelope, 1)
	b.pendingMu.Lock()
	b.pending[env.ID] = ch
	b.pendingMu.Unlock()
	defer func() {
		b.pendingMu.Lock()
		delete(b.pending, env.ID)
		b.pendingMu.Unlock()
	}()

	if err := b.send(env); err != nil {
		return nil, err
	}
	select {
	case resp := <-ch:
		if resp == nil { // injected by failAllPending on fatal disconnect
			return nil, errDaemonGone
		}
		return resp, nil
	case <-b.closed:
		return nil, errDaemonGone
	case <-time.After(timeout):
		return nil, fmt.Errorf("no response to %s within %s", env.Type, timeout)
	}
}

// newEnvelope builds an envelope stamped with the current client id.
func (b *busClient) newEnvelope(to string, typ protocol.MessageType, payload string) *protocol.Envelope {
	return protocol.NewEnvelope(b.ClientID(), to, typ, payload)
}

// readLoop dispatches inbound envelopes: correlated responses to their
// waiting requests, everything else to auto-reply handlers or the inbox.
func (b *busClient) readLoop() {
	defer close(b.readerDone)
	for {
		conn := b.currentConn()
		if conn == nil {
			return
		}
		env, err := protocol.ReadMessage(conn, b.cfg.Limits.MaxMessageBytes)
		if err != nil {
			select {
			case <-b.closed:
				return
			default:
			}
			if b.shuttingDown.Load() {
				b.log.Info("daemon closed connection during shutdown")
				b.dead.Store(true)
				return
			}
			b.log.Warn("daemon connection error", "error", err)
			if !b.reconnect() {
				b.dead.Store(true)
				b.failAllPending()
				return
			}
			continue
		}
		b.handleInbound(env)
	}
}

func (b *busClient) handleInbound(env *protocol.Envelope) {
	// Correlated response to an in-flight request?
	if env.CorrelationID != "" {
		b.pendingMu.Lock()
		ch, ok := b.pending[env.CorrelationID]
		b.pendingMu.Unlock()
		if ok {
			ch <- env
			return
		}
	}

	switch env.Type {
	case protocol.TypeStatusRequest:
		// Spec §7.3 bus_get_status: answer synchronously on the child's
		// behalf with the last status reported via bus_report_status.
		b.statusMu.Lock()
		status := b.lastStatus
		b.statusMu.Unlock()
		report := b.newEnvelope(protocol.TargetParent, protocol.TypeStatusReport, protocol.MustEncode(status))
		report.CorrelationID = env.CorrelationID
		if report.CorrelationID == "" {
			report.CorrelationID = env.ID
		}
		if err := b.send(report); err != nil {
			b.log.Warn("could not answer status request", "error", err)
		}
	case protocol.TypeShutdownNotice:
		// Acknowledge immediately so the cascade never waits on a live MCP.
		b.shuttingDown.Store(true)
		var notice protocol.ShutdownNoticePayload
		_ = env.DecodePayload(&notice)
		b.log.Info("shutdown notice received", "reason", notice.Reason, "initiated_by", notice.InitiatedBy)
		if err := b.send(b.newEnvelope(protocol.TargetDaemon, protocol.TypeShutdownAck, "")); err != nil {
			b.log.Warn("could not send shutdown ack", "error", err)
		}
		b.addToInbox(env)
	case protocol.TypeDirectMessage, protocol.TypeBroadcast, protocol.TypeAssignTask,
		protocol.TypeStatusReport, protocol.TypeAckTask, protocol.TypeCompleteTask:
		b.addToInbox(env)
	case protocol.TypeHeartbeatAck, protocol.TypePong:
		// Latency bookkeeping happens in the correlated path; uncorrelated
		// acks need no action.
	default:
		b.log.Debug("ignoring envelope", "type", string(env.Type), "from", env.From)
	}
}

// inboxLimit caps buffered messages; oldest entries are evicted first.
const inboxLimit = 200

func (b *busClient) addToInbox(env *protocol.Envelope) {
	b.inboxMu.Lock()
	defer b.inboxMu.Unlock()
	if len(b.inbox) >= inboxLimit {
		b.inbox = b.inbox[1:]
	}
	b.inbox = append(b.inbox, inboxMessage{
		From:       env.From,
		Type:       env.Type,
		Payload:    env.Payload,
		TaskID:     env.TaskID,
		ReceivedAt: time.Now().UTC(),
	})
}

// drainInbox returns and clears all buffered messages.
func (b *busClient) drainInbox() []inboxMessage {
	b.inboxMu.Lock()
	defer b.inboxMu.Unlock()
	msgs := b.inbox
	b.inbox = nil
	return msgs
}

// setLastStatus records the child's self-reported status for auto-replies.
func (b *busClient) setLastStatus(s protocol.StatusPayload) {
	b.statusMu.Lock()
	defer b.statusMu.Unlock()
	b.lastStatus = s
}

// failAllPending unblocks all in-flight requests after a fatal disconnect by
// delivering a nil envelope, which request() reports as errDaemonGone.
func (b *busClient) failAllPending() {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	for id, ch := range b.pending {
		select {
		case ch <- nil: // request() treats nil as "connection lost"
		default:
		}
		delete(b.pending, id)
	}
}

// reconnect retries the dial+handshake per config (spec §11.3). Returns
// true when a fresh session is established (with a new client_id).
func (b *busClient) reconnect() bool {
	backoff := time.Duration(b.cfg.Timeouts.ReconnectBackoffMs) * time.Millisecond
	for attempt := 1; attempt <= b.cfg.Timeouts.ReconnectAttempts; attempt++ {
		select {
		case <-b.closed:
			return false
		case <-time.After(backoff):
		}
		b.log.Info("reconnecting to daemon", "attempt", attempt)
		conn, err := net.DialTimeout("unix", b.cfg.Daemon.SocketPath, 2*time.Second)
		if err == nil {
			if err := b.handshake(conn); err == nil {
				b.log.Info("reconnected", "client_id", b.ClientID())
				return true
			}
			conn.Close()
		}
	}
	b.log.Error("reconnect failed; giving up", "attempts", b.cfg.Timeouts.ReconnectAttempts)
	return false
}

// heartbeatLoop keeps the daemon's last-seen tracking fresh.
func (b *busClient) heartbeatLoop() {
	interval := time.Duration(b.cfg.Timeouts.HeartbeatIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.closed:
			return
		case <-ticker.C:
			if b.dead.Load() || b.shuttingDown.Load() {
				return
			}
			if err := b.send(b.newEnvelope(protocol.TargetDaemon, protocol.TypeHeartbeat, "")); err != nil {
				b.log.Debug("heartbeat failed", "error", err)
			}
		}
	}
}
