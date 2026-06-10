package tui

import (
	tea "github.com/charmbracelet/bubbletea"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// eventMsg wraps a broker event for Bubble Tea.
type eventMsg protocol.EventPayload

// sourceClosedMsg signals the event stream ended (daemon stopped).
type sourceClosedMsg struct{}

// waitForEvent blocks on the event channel as a Bubble Tea command.
func waitForEvent(events <-chan protocol.EventPayload) tea.Cmd {
	if events == nil {
		return nil
	}
	return func() tea.Msg {
		ev, ok := <-events
		if !ok {
			return sourceClosedMsg{}
		}
		return eventMsg(ev)
	}
}

// Init implements tea.Model.
func (m Model) Init() tea.Cmd {
	return waitForEvent(m.events)
}

// Update implements tea.Model.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case eventMsg:
		m = m.applyEvent(protocol.EventPayload(msg))
		if !m.paused && m.scroll != 0 {
			// Live mode keeps the feed pinned to the newest message unless
			// the user scrolled away while paused.
			m.scroll = 0
		}
		return m, waitForEvent(m.events)

	case sourceClosedMsg:
		m.disconnected = true
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// While the filter input is open, keys edit the query.
	if m.filter.Active {
		switch msg.Type {
		case tea.KeyEnter:
			m.filter.Active = false
		case tea.KeyEsc:
			m.filter = FilterState{}
		case tea.KeyBackspace:
			if len(m.filter.Query) > 0 {
				m.filter.Query = m.filter.Query[:len(m.filter.Query)-1]
			}
		case tea.KeyRunes:
			m.filter.Query += string(msg.Runes)
		case tea.KeyCtrlC:
			return m.quitNow()
		}
		return m, nil
	}

	switch msg.String() {
	case "q", "Q", "ctrl+c":
		return m.quitNow()
	case "c", "C":
		m.messages = nil
		m.scroll = 0
		m.selected = -1
		m.showDetail = false
	case "p", "P":
		m.paused = !m.paused
	case "f", "F":
		m.filter.Active = true
	case "?":
		m.showHelp = !m.showHelp
	case "tab":
		if m.focusedPane == PaneClients {
			m.focusedPane = PaneFeed
		} else {
			m.focusedPane = PaneClients
		}
	case "d", "D":
		if m.selected < 0 && len(m.visibleMessages()) > 0 {
			m.selected = len(m.visibleMessages()) - 1
		}
		m.showDetail = !m.showDetail && m.selected >= 0
	case "up":
		if m.showDetail || m.focusedPane == PaneFeed {
			if m.selected > 0 {
				m.selected--
			} else if m.selected < 0 && len(m.visibleMessages()) > 0 {
				m.selected = len(m.visibleMessages()) - 1
			}
			m.scroll++
		}
	case "down":
		if m.showDetail || m.focusedPane == PaneFeed {
			if vis := m.visibleMessages(); m.selected >= 0 && m.selected < len(vis)-1 {
				m.selected++
			}
			if m.scroll > 0 {
				m.scroll--
			}
		}
	}
	return m, nil
}

func (m Model) quitNow() (tea.Model, tea.Cmd) {
	if !m.done {
		m.done = true
		if m.quit != nil {
			m.quit()
		}
	}
	return m, tea.Quit
}
