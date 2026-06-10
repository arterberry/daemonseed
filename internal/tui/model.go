// Package tui implements the live dashboard (spec §12) as a Bubble Tea
// program. It consumes the broker's event stream — either in-process or via
// an observer socket connection — and never feeds back into routing, so a
// lagging TUI cannot affect the broker (spec Appendix B.4).
package tui

import (
	"time"

	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
)

// Pane identifies which panel has keyboard focus.
type Pane int

const (
	PaneClients Pane = iota
	PaneFeed
)

// ClientView is one row in the connected-clients panel.
type ClientView struct {
	ID          string
	Name        string
	Role        roles.Role
	State       string
	CurrentTask string
	ConnectedAt time.Time
	LastSeen    time.Time
}

// MessageView is one row in the message feed.
type MessageView struct {
	Timestamp time.Time
	From      string
	To        string
	Type      protocol.MessageType
	Summary   string
	Raw       string // full payload, shown in detail view
}

// FilterState narrows the feed by free-text match on client name or
// message type. Empty means no filtering.
type FilterState struct {
	Query  string
	Active bool // the filter input line is open for typing
}

// Matches reports whether m passes the filter.
func (f FilterState) Matches(m MessageView) bool {
	if f.Query == "" {
		return true
	}
	return containsFold(m.From, f.Query) ||
		containsFold(m.To, f.Query) ||
		containsFold(string(m.Type), f.Query)
}

// Model is the Bubble Tea model for the dashboard.
type Model struct {
	clients     []ClientView
	messages    []MessageView
	paused      bool
	filter      FilterState
	focusedPane Pane
	width       int
	height      int

	scroll       int // lines scrolled up from the bottom of the feed
	selected     int // selected message index (detail view), -1 none
	showDetail   bool
	showHelp     bool
	disconnected bool // event source went away (attach mode)

	version    string
	socketPath string
	tsFormat   string
	feedMax    int

	msgCount  uint64
	startedAt time.Time

	events <-chan protocol.EventPayload
	quit   func() // optional: invoked once when the TUI exits
	done   bool
}

// Options configures a dashboard.
type Options struct {
	Version         string
	SocketPath      string
	TimestampFormat string
	FeedMaxLines    int
	Events          <-chan protocol.EventPayload
	OnQuit          func()
}

// NewModel builds the initial model.
func NewModel(opts Options) Model {
	tsFormat := opts.TimestampFormat
	if tsFormat == "" {
		tsFormat = "15:04:05"
	}
	feedMax := opts.FeedMaxLines
	if feedMax <= 0 {
		feedMax = 500
	}
	return Model{
		version:     opts.Version,
		socketPath:  opts.SocketPath,
		tsFormat:    tsFormat,
		feedMax:     feedMax,
		events:      opts.Events,
		quit:        opts.OnQuit,
		focusedPane: PaneFeed,
		selected:    -1,
		startedAt:   time.Now(),
	}
}

// Clients returns the current client list (exported for tests).
func (m Model) Clients() []ClientView { return m.clients }

// Messages returns the current feed (exported for tests).
func (m Model) Messages() []MessageView { return m.messages }

// Paused reports whether the feed is paused.
func (m Model) Paused() bool { return m.paused }

// visibleMessages applies the filter.
func (m Model) visibleMessages() []MessageView {
	if m.filter.Query == "" {
		return m.messages
	}
	var out []MessageView
	for _, msg := range m.messages {
		if m.filter.Matches(msg) {
			out = append(out, msg)
		}
	}
	return out
}

// applyEvent folds one broker event into the model.
func (m Model) applyEvent(ev protocol.EventPayload) Model {
	if ev.MsgCount > 0 {
		m.msgCount = ev.MsgCount
	}
	if !ev.StartedAt.IsZero() {
		m.startedAt = ev.StartedAt
	}
	switch ev.Kind {
	case "snapshot", "client_connected", "client_disconnected":
		m.clients = m.clients[:0]
		for _, ci := range ev.Clients {
			m.clients = append(m.clients, ClientView{
				ID:          ci.ClientID,
				Name:        ci.Name,
				Role:        roles.Role(ci.Role),
				State:       ci.State,
				CurrentTask: ci.CurrentTask,
				ConnectedAt: ci.ConnectedAt,
				LastSeen:    ci.LastSeen,
			})
		}
	case "message":
		m.messages = append(m.messages, MessageView{
			Timestamp: ev.At,
			From:      ev.FromName,
			To:        ev.To,
			Type:      ev.Type,
			Summary:   ev.Summary,
			Raw:       ev.Raw,
		})
		if len(m.messages) > m.feedMax {
			m.messages = m.messages[len(m.messages)-m.feedMax:]
		}
	}
	return m
}

func containsFold(s, substr string) bool {
	// strings.Contains with ASCII case folding; payload summaries are short
	// so a simple scan is fine.
	n, m := len(s), len(substr)
	if m == 0 {
		return true
	}
	for i := 0; i+m <= n; i++ {
		j := 0
		for ; j < m; j++ {
			a, b := s[i+j], substr[j]
			if 'A' <= a && a <= 'Z' {
				a += 'a' - 'A'
			}
			if 'A' <= b && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				break
			}
		}
		if j == m {
			return true
		}
	}
	return false
}
