package broker

import (
	"net"
	"testing"
	"time"

	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
)

func TestClient_WriteLoopDeliversFrames(t *testing.T) {
	server, far := net.Pipe()
	c := newClient("c1", server, protocol.HelloPayload{Name: "api"}, roles.RoleChild, 0)
	go c.writeLoop()
	defer c.Close()

	want := protocol.NewEnvelope("daemon", "c1", protocol.TypePong, "")
	if !c.Send(want) {
		t.Fatal("send must succeed on an open client")
	}
	if err := far.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := protocol.ReadMessage(far, 0)
	if err != nil {
		t.Fatalf("far end read: %v", err)
	}
	if got.ID != want.ID || got.Type != protocol.TypePong {
		t.Errorf("frame mismatch: %+v", got)
	}
}

func TestClient_SendNonBlockingWhenFull(t *testing.T) {
	// net.Pipe has no buffering and nothing reads the far end, so the write
	// loop blocks on the first frame and the outbound channel fills up.
	server, far := net.Pipe()
	defer far.Close()
	c := newClient("c1", server, protocol.HelloPayload{Name: "api"}, roles.RoleChild, 0)
	go c.writeLoop()
	defer c.Close()

	env := protocol.NewEnvelope("daemon", "c1", protocol.TypePong, "")
	dropped := false
	for i := 0; i < outboundBuffer+8; i++ {
		if !c.Send(env) {
			dropped = true
			break
		}
	}
	if !dropped {
		t.Error("Send must return false instead of blocking once the buffer is full")
	}
}

func TestClient_SendAfterCloseFails(t *testing.T) {
	server, far := net.Pipe()
	defer far.Close()
	c := newClient("c1", server, protocol.HelloPayload{Name: "api"}, roles.RoleChild, 0)
	go c.writeLoop()
	go func() { // drain so Close's flush isn't blocked by the pipe
		buf := make([]byte, 4096)
		for {
			if _, err := far.Read(buf); err != nil {
				return
			}
		}
	}()
	c.Close()
	c.Close() // idempotent
	if c.Send(protocol.NewEnvelope("daemon", "c1", protocol.TypePong, "")) {
		t.Error("Send after Close must fail")
	}
}

func TestClient_StatusRoundTrip(t *testing.T) {
	server, far := net.Pipe()
	defer far.Close()
	defer server.Close()
	c := newClient("c1", server, protocol.HelloPayload{Name: "api"}, roles.RoleChild, 0)
	if st, _ := c.Status(); st != "idle" {
		t.Errorf("initial state = %q, want idle", st)
	}
	c.SetStatus("working", "auth-001")
	st, task := c.Status()
	if st != "working" || task != "auth-001" {
		t.Errorf("Status() = %q, %q", st, task)
	}
	info := c.Info(time.Now())
	if info.State != "working" || info.CurrentTask != "auth-001" || info.Name != "api" {
		t.Errorf("Info() = %+v", info)
	}
}
