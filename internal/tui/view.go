package tui

import (
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/arterberry/daemonseed/internal/roles"
	"github.com/arterberry/daemonseed/internal/tui/components"
)

// View implements tea.Model.
func (m Model) View() string {
	if m.done {
		return ""
	}
	width := m.width
	if width <= 0 {
		width = 80
	}
	height := m.height
	if height <= 0 {
		height = 24
	}

	header := components.RenderHeader(m.version, m.socketPath, !m.disconnected, width)

	// 1 header line + 2 status bar lines; panels get the rest.
	panelHeight := height - 3
	if panelHeight < 5 {
		panelHeight = 5
	}
	leftWidth := width / 3
	if leftWidth < 24 {
		leftWidth = 24
	}
	rightWidth := width - leftWidth

	var body string
	switch {
	case m.showHelp:
		body = m.renderHelp(width, panelHeight)
	case m.showDetail && m.selected >= 0:
		body = m.renderDetail(width, panelHeight)
	default:
		left := components.RenderClientList(m.clientRows(), leftWidth-2, panelHeight-2,
			m.focusedPane == PaneClients)
		right := components.RenderMessageFeed(m.feedRows(), rightWidth-2, panelHeight-2,
			m.focusedPane == PaneFeed, m.paused, m.scroll, m.tsFormat)
		body = lipgloss.JoinHorizontal(lipgloss.Top, left, right)
	}

	parents, children := 0, 0
	for _, c := range m.clients {
		if c.Role == roles.RoleParent {
			parents++
		} else if c.Role == roles.RoleChild {
			children++
		}
	}
	statusBar := components.RenderStatusBar(components.StatusBarState{
		Uptime:       time.Since(m.startedAt),
		MsgCount:     m.msgCount,
		Parents:      parents,
		Children:     children,
		AuditOn:      true,
		Paused:       m.paused,
		FilterQuery:  m.filter.Query,
		FilterActive: m.filter.Active,
		Disconnected: m.disconnected,
	}, width)

	return header + "\n" + body + "\n" + statusBar
}

func (m Model) clientRows() []components.ClientRow {
	rows := make([]components.ClientRow, 0, len(m.clients))
	for _, c := range m.clients {
		rows = append(rows, components.ClientRow{
			Name:        c.Name,
			Role:        string(c.Role),
			ID:          c.ID,
			State:       c.State,
			CurrentTask: c.CurrentTask,
			ConnectedAt: c.ConnectedAt,
		})
	}
	return rows
}

func (m Model) feedRows() []components.FeedRow {
	vis := m.visibleMessages()
	rows := make([]components.FeedRow, 0, len(vis))
	for i, msg := range vis {
		rows = append(rows, components.FeedRow{
			Timestamp: msg.Timestamp,
			From:      msg.From,
			To:        msg.To,
			Type:      string(msg.Type),
			Summary:   msg.Summary,
			Selected:  i == m.selected,
		})
	}
	return rows
}

func (m Model) renderDetail(width, height int) string {
	vis := m.visibleMessages()
	if m.selected < 0 || m.selected >= len(vis) {
		return components.RenderDetail(components.FeedRow{}, "(message no longer in feed)",
			width-2, height-2, m.tsFormat)
	}
	msg := vis[m.selected]
	row := components.FeedRow{
		Timestamp: msg.Timestamp, From: msg.From, To: msg.To, Type: string(msg.Type),
	}
	return components.RenderDetail(row, msg.Raw, width-2, height-2, m.tsFormat)
}

func (m Model) renderHelp(width, height int) string {
	help := strings.Join([]string{
		"daemonSeed dashboard — keys",
		"",
		"  Q / Ctrl+C   quit the TUI (the daemon keeps running)",
		"  C            clear the message feed display",
		"  P            pause / resume live feed scrolling",
		"  F            filter the feed (by client or message type)",
		"  D            toggle detail view for the selected message",
		"  ↑ / ↓        scroll the feed / move the selection",
		"  Tab          switch focus between panels",
		"  ?            close this help",
	}, "\n")
	return lipgloss.NewStyle().
		Border(lipgloss.NormalBorder()).
		BorderForeground(components.ColorBorder).
		Padding(1, 2).
		Width(width - 2).
		Height(height - 2).
		Render(help)
}
