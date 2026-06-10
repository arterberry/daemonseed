package tui

import (
	"fmt"
	"net"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// Run starts the dashboard over an existing event stream (in-process mode:
// `daemonseed start --tui`). It blocks until the user quits or the stream
// closes and the user exits.
func Run(opts Options) error {
	p := tea.NewProgram(NewModel(opts), tea.WithAltScreen())
	_, err := p.Run()
	return err
}

// RunAttached connects to a running daemon as a read-only observer and
// streams its events into the dashboard (`daemonseed tui`).
func RunAttached(socketPath, version, tsFormat string, feedMax int) error {
	conn, err := net.DialTimeout("unix", socketPath, 2*time.Second)
	if err != nil {
		return fmt.Errorf("%w (socket %s). Start it with: daemonseed start",
			protocol.ErrDaemonNotRunning, socketPath)
	}

	// Observer handshake.
	hello := protocol.NewEnvelope("pending", protocol.TargetDaemon, protocol.TypeHello,
		protocol.MustEncode(protocol.HelloPayload{
			Role: "observer", Name: observerName(), Version: version,
		}))
	if err := protocol.WriteMessage(conn, hello, 0); err != nil {
		conn.Close()
		return fmt.Errorf("send HELLO: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		conn.Close()
		return err
	}
	resp, err := protocol.ReadMessage(conn, 0)
	if err != nil {
		conn.Close()
		return fmt.Errorf("read handshake response: %w", err)
	}
	if resp.Type == protocol.TypeHelloReject {
		var rej protocol.HelloRejectPayload
		_ = resp.DecodePayload(&rej)
		conn.Close()
		return fmt.Errorf("daemon rejected observer connection: %s", rej.Reason)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		conn.Close()
		return err
	}

	events := make(chan protocol.EventPayload, 256)
	go func() {
		defer close(events)
		for {
			env, err := protocol.ReadMessage(conn, 0)
			if err != nil {
				return // daemon stopped; the TUI shows DISCONNECTED
			}
			if env.Type != protocol.TypeEvent {
				continue
			}
			var ev protocol.EventPayload
			if err := env.DecodePayload(&ev); err != nil {
				continue
			}
			events <- ev
		}
	}()

	return Run(Options{
		Version:         version,
		SocketPath:      socketPath,
		TimestampFormat: tsFormat,
		FeedMaxLines:    feedMax,
		Events:          events,
		OnQuit:          func() { conn.Close() },
	})
}

// observerName builds a unique, charset-legal observer name so several TUIs
// can attach at once.
func observerName() string {
	return fmt.Sprintf("tui-observer-%d", time.Now().UnixNano()%1_000_000)
}
