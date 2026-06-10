package tui

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/arterberry/daemonseed/internal/protocol"
)

func newTestModel() Model {
	m := NewModel(Options{Version: "1.0.0", SocketPath: "/tmp/test.sock"})
	m.width = 100
	m.height = 30
	return m
}

func key(s string) tea.KeyMsg {
	if len(s) == 1 {
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
	switch s {
	case "tab":
		return tea.KeyMsg{Type: tea.KeyTab}
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "esc":
		return tea.KeyMsg{Type: tea.KeyEsc}
	}
	return tea.KeyMsg{}
}

func update(t *testing.T, m Model, msg tea.Msg) Model {
	t.Helper()
	next, _ := m.Update(msg)
	model, ok := next.(Model)
	if !ok {
		t.Fatalf("Update returned %T", next)
	}
	return model
}

func messageEvent(from, to, typ, summary string) eventMsg {
	return eventMsg(protocol.EventPayload{
		Kind: "message", FromName: from, To: to,
		Type: protocol.MessageType(typ), Summary: summary, Raw: summary,
		At: time.Now(), MsgCount: 1,
	})
}

func TestModel_MessageEventAppendsToFeed(t *testing.T) {
	m := newTestModel()
	m = update(t, m, messageEvent("orchestrator", "children", "BROADCAST", "begin task"))
	if len(m.Messages()) != 1 {
		t.Fatalf("feed len = %d", len(m.Messages()))
	}
	if m.Messages()[0].Type != "BROADCAST" || m.Messages()[0].From != "orchestrator" {
		t.Errorf("message = %+v", m.Messages()[0])
	}
}

func TestModel_SnapshotPopulatesClients(t *testing.T) {
	m := newTestModel()
	m = update(t, m, eventMsg(protocol.EventPayload{
		Kind: "snapshot",
		Clients: []protocol.ClientInfo{
			{ClientID: "p1", Name: "orchestrator", Role: "parent"},
			{ClientID: "c1", Name: "api", Role: "child", State: "working"},
		},
	}))
	if len(m.Clients()) != 2 {
		t.Fatalf("clients = %d", len(m.Clients()))
	}
	if m.Clients()[1].State != "working" {
		t.Errorf("client state = %+v", m.Clients()[1])
	}
}

func TestModel_FeedCappedAtMaxLines(t *testing.T) {
	m := NewModel(Options{FeedMaxLines: 5})
	for i := 0; i < 12; i++ {
		m = m.applyEvent(protocol.EventPayload{Kind: "message", Type: "PING", At: time.Now()})
	}
	if len(m.Messages()) != 5 {
		t.Errorf("feed must be capped at 5, got %d", len(m.Messages()))
	}
}

func TestModel_PauseToggle(t *testing.T) {
	m := newTestModel()
	m = update(t, m, key("p"))
	if !m.Paused() {
		t.Error("p must pause")
	}
	m = update(t, m, key("p"))
	if m.Paused() {
		t.Error("p must resume")
	}
}

func TestModel_ClearFeed(t *testing.T) {
	m := newTestModel()
	m = update(t, m, messageEvent("a", "b", "PING", ""))
	m = update(t, m, key("c"))
	if len(m.Messages()) != 0 {
		t.Error("c must clear the feed")
	}
}

func TestModel_FilterNarrowsFeed(t *testing.T) {
	m := newTestModel()
	m = update(t, m, messageEvent("api", "parent", "STATUS_REPORT", "working"))
	m = update(t, m, messageEvent("ui", "parent", "COMPLETE_TASK", "done"))

	m = update(t, m, key("f")) // open filter input
	for _, r := range "status" {
		m = update(t, m, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	m = update(t, m, key("enter"))

	vis := m.visibleMessages()
	if len(vis) != 1 || vis[0].Type != "STATUS_REPORT" {
		t.Errorf("filtered feed = %+v", vis)
	}

	// Esc inside the filter input clears it.
	m = update(t, m, key("f"))
	m = update(t, m, key("esc"))
	if len(m.visibleMessages()) != 2 {
		t.Error("clearing the filter must restore the feed")
	}
}

func TestModel_QuitInvokesCallbackOnce(t *testing.T) {
	calls := 0
	m := NewModel(Options{OnQuit: func() { calls++ }})
	next, cmd := m.Update(key("q"))
	if cmd == nil {
		t.Fatal("q must produce a quit command")
	}
	if msg := cmd(); msg != tea.Quit() {
		t.Errorf("expected tea.Quit, got %v", msg)
	}
	_, _ = next.(Model).Update(key("q"))
	if calls != 1 {
		t.Errorf("OnQuit called %d times, want 1", calls)
	}
}

func TestModel_TabSwitchesFocus(t *testing.T) {
	m := newTestModel()
	start := m.focusedPane
	m = update(t, m, key("tab"))
	if m.focusedPane == start {
		t.Error("tab must switch panes")
	}
}

func TestModel_SourceClosedShowsDisconnected(t *testing.T) {
	m := newTestModel()
	m = update(t, m, sourceClosedMsg{})
	if !m.disconnected {
		t.Error("closed event source must mark the model disconnected")
	}
	if !strings.Contains(m.View(), "DISCONNECTED") {
		t.Error("view must surface the disconnected state")
	}
}

func TestModel_ViewRendersAllStates(t *testing.T) {
	m := newTestModel()
	m = update(t, m, eventMsg(protocol.EventPayload{
		Kind: "snapshot",
		Clients: []protocol.ClientInfo{
			{ClientID: "p1", Name: "orchestrator", Role: "parent"},
			{ClientID: "c1", Name: "api", Role: "child", State: "error", CurrentTask: "auth-001"},
		},
	}))
	m = update(t, m, messageEvent("orchestrator", "children", "BROADCAST", "begin"))

	out := m.View()
	for _, want := range []string{"daemonSeed", "CONNECTED CLIENTS", "MESSAGE FEED", "orchestrator", "api", "BROADCAST"} {
		if !strings.Contains(out, want) {
			t.Errorf("view missing %q", want)
		}
	}

	// Help overlay.
	helpView := update(t, m, key("?")).View()
	if !strings.Contains(helpView, "keys") {
		t.Error("help overlay must render")
	}

	// Detail view of the latest message.
	detail := update(t, m, key("d")).View()
	if !strings.Contains(detail, "MESSAGE DETAIL") {
		t.Error("detail view must render")
	}

	// Tiny window must not panic.
	small := m
	small.width, small.height = 10, 4
	_ = small.View()
}
