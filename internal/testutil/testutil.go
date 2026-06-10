// Package testutil provides helpers for integration-testing the broker over
// a real Unix socket (spec §17.6).
package testutil

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arterberry/daemonseed/internal/broker"
	"github.com/arterberry/daemonseed/internal/config"
	"github.com/arterberry/daemonseed/internal/protocol"
)

// TestConfig returns a config tuned for fast tests: a short unique socket
// path (macOS caps sun_path at 104 bytes, so t.TempDir() is too long) and
// 1-second timeouts.
func TestConfig(t *testing.T) *config.Config {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dseed")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	cfg := config.Default()
	cfg.Daemon.SocketPath = filepath.Join(dir, "b.sock")
	cfg.Daemon.PIDFile = filepath.Join(dir, "b.pid")
	cfg.Timeouts.HandshakeSeconds = 1
	cfg.Timeouts.StatusRequestSeconds = 1
	cfg.Timeouts.ShutdownAckSeconds = 1
	cfg.Limits.MinScheduleIntervalSeconds = 0 // allow sub-second schedules in tests
	cfg.Audit.Enabled = false
	return cfg
}

// StartTestBroker starts a broker on a temp socket and returns the socket
// path and an idempotent cleanup function. The broker is also registered
// with t.Cleanup, so calling cleanup explicitly is optional.
func StartTestBroker(t *testing.T, cfg *config.Config) (socketPath string, cleanup func()) {
	t.Helper()
	b := StartTestBrokerInstance(t, cfg)
	return b.SocketPath(), func() {
		b.Shutdown("test cleanup", time.Second, "testutil", "")
	}
}

// StartTestBrokerInstance is like StartTestBroker but exposes the Broker for
// tests that drive shutdown or events directly.
func StartTestBrokerInstance(t *testing.T, cfg *config.Config) *broker.Broker {
	t.Helper()
	if cfg == nil {
		cfg = TestConfig(t)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	b := broker.New(cfg, log, nil, "test")
	if err := b.Start(); err != nil {
		t.Fatalf("start test broker: %v", err)
	}
	t.Cleanup(func() { b.Shutdown("test cleanup", time.Second, "testutil", "") })
	return b
}

// StartTestBrokerNoStart constructs a broker without binding it, for tests
// that exercise Start() failure modes (e.g. socket already in use).
func StartTestBrokerNoStart(t *testing.T, cfg *config.Config) *broker.Broker {
	t.Helper()
	if cfg == nil {
		cfg = TestConfig(t)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return broker.New(cfg, log, nil, "test")
}

// TestClient wraps a raw socket connection with assertion helpers. It speaks
// the wire protocol directly — no MCP layer — so tests exercise exactly what
// the broker sees.
type TestClient struct {
	t       *testing.T
	Conn    net.Conn
	ID      string // assigned by HELLO_ACK
	Name    string
	maxMsg  int
	inbox   chan *protocol.Envelope
	readErr chan error
}

// Dial opens a raw connection without performing a handshake.
func Dial(t *testing.T, socketPath string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("unix", socketPath, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", socketPath, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// TryHandshake dials and sends HELLO, returning either a connected
// TestClient (on HELLO_ACK) or the rejection envelope (on HELLO_REJECT).
func TryHandshake(t *testing.T, socketPath, role, name string) (*TestClient, *protocol.Envelope) {
	t.Helper()
	conn := Dial(t, socketPath)
	hello := protocol.NewEnvelope("pending", protocol.TargetDaemon, protocol.TypeHello,
		protocol.MustEncode(protocol.HelloPayload{Role: role, Name: name, Version: "1.0.0"}))
	if err := protocol.WriteMessage(conn, hello, 0); err != nil {
		t.Fatalf("send HELLO: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	resp, err := protocol.ReadMessage(conn, 0)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}
	switch resp.Type {
	case protocol.TypeHelloAck:
		var ack protocol.HelloAckPayload
		if err := resp.DecodePayload(&ack); err != nil {
			t.Fatalf("decode HELLO_ACK: %v", err)
		}
		c := &TestClient{
			t: t, Conn: conn, ID: ack.ClientID, Name: name,
			inbox:   make(chan *protocol.Envelope, 64),
			readErr: make(chan error, 1),
		}
		go c.readLoop()
		return c, nil
	case protocol.TypeHelloReject:
		return nil, resp
	default:
		t.Fatalf("unexpected handshake response type %s", resp.Type)
		return nil, nil
	}
}

// ConnectTestClient performs a handshake that must succeed.
func ConnectTestClient(t *testing.T, socketPath, role, name string) *TestClient {
	t.Helper()
	c, reject := TryHandshake(t, socketPath, role, name)
	if c == nil {
		var p protocol.HelloRejectPayload
		_ = reject.DecodePayload(&p)
		t.Fatalf("handshake rejected for %s/%s: %s", role, name, p.Reason)
	}
	return c
}

func (c *TestClient) readLoop() {
	for {
		env, err := protocol.ReadMessage(c.Conn, c.maxMessageBytes())
		if err != nil {
			select {
			case c.readErr <- err:
			default:
			}
			close(c.inbox)
			return
		}
		c.inbox <- env
	}
}

func (c *TestClient) maxMessageBytes() int {
	if c.maxMsg > 0 {
		return c.maxMsg
	}
	// Allow frames above the broker's limit so tests can verify the broker's
	// own MESSAGE_TOO_LARGE handling.
	return 16 * 1024 * 1024
}

// Send writes env to the broker, failing the test on error.
func (c *TestClient) Send(env *protocol.Envelope) {
	c.t.Helper()
	if err := protocol.WriteMessage(c.Conn, env, 16*1024*1024); err != nil {
		c.t.Fatalf("send %s: %v", env.Type, err)
	}
}

// NewEnvelope builds an envelope from this client (From is pre-filled).
func (c *TestClient) NewEnvelope(to string, typ protocol.MessageType, payload string) *protocol.Envelope {
	return protocol.NewEnvelope(c.ID, to, typ, payload)
}

// Receive returns the next envelope or nil if none arrives within timeout.
func (c *TestClient) Receive(timeout time.Duration) *protocol.Envelope {
	select {
	case env, ok := <-c.inbox:
		if !ok {
			return nil
		}
		return env
	case <-time.After(timeout):
		return nil
	}
}

// MustReceiveType reads envelopes until one of type typ arrives, failing the
// test on timeout. Envelopes of other types are skipped, so tests are not
// sensitive to interleaved receipts or heartbeat traffic.
func (c *TestClient) MustReceiveType(t *testing.T, typ protocol.MessageType, timeout time.Duration) *protocol.Envelope {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case env, ok := <-c.inbox:
			if !ok {
				t.Fatalf("connection closed while waiting for %s", typ)
			}
			if env.Type == typ {
				return env
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", typ)
		}
	}
}

// MustNotReceiveType asserts no envelope of type typ arrives within window.
func (c *TestClient) MustNotReceiveType(t *testing.T, typ protocol.MessageType, window time.Duration) {
	t.Helper()
	deadline := time.After(window)
	for {
		select {
		case env, ok := <-c.inbox:
			if !ok {
				return
			}
			if env.Type == typ {
				t.Fatalf("received unexpected %s: %s", typ, env.Payload)
			}
		case <-deadline:
			return
		}
	}
}

// WaitDisconnect blocks until the broker closes this connection.
func (c *TestClient) WaitDisconnect(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-c.inbox:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("connection was not closed in time")
		}
	}
}

// Close closes the underlying connection.
func (c *TestClient) Close() { c.Conn.Close() }

// DecodeReject extracts the reason from a HELLO_REJECT envelope.
func DecodeReject(t *testing.T, env *protocol.Envelope) string {
	t.Helper()
	if env == nil {
		t.Fatal("expected a HELLO_REJECT envelope, got nil")
	}
	var p protocol.HelloRejectPayload
	if err := env.DecodePayload(&p); err != nil {
		t.Fatalf("decode reject payload: %v", err)
	}
	return p.Reason
}

// IsConnRefused reports whether err indicates nothing is listening.
func IsConnRefused(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

// Fmt is a tiny convenience wrapper used by tests building payloads.
func Fmt(format string, args ...any) string { return fmt.Sprintf(format, args...) }
