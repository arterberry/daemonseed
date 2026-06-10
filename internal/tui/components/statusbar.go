package components

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// StatusBarState carries everything the bottom bar displays.
type StatusBarState struct {
	Uptime       time.Duration
	MsgCount     uint64
	Parents      int
	Children     int
	AuditOn      bool
	Paused       bool
	FilterQuery  string
	FilterActive bool
	Disconnected bool
}

// RenderStatusBar renders the keybinding help line and the stats line.
func RenderStatusBar(s StatusBarState, width int) string {
	keys := "[Q] Quit  [C] Clear feed  [P] Pause  [F] Filter  [D] Detail  [Tab] Focus  [?] Help"
	if s.FilterActive {
		keys = "filter> " + s.FilterQuery + "▌   (Enter apply · Esc clear)"
	} else if s.FilterQuery != "" {
		keys += "   " + StyleWarning.Render("filter: "+s.FilterQuery)
	}

	audit := "ON"
	if !s.AuditOn {
		audit = "OFF"
	}
	stats := fmt.Sprintf("uptime: %s   msgs: %d   clients: %d (%dP %dC)   log: %s",
		formatUptime(s.Uptime), s.MsgCount, s.Parents+s.Children, s.Parents, s.Children, audit)
	if s.Paused {
		stats += "   " + StyleWarning.Render("PAUSED")
	}
	if s.Disconnected {
		stats += "   " + StyleError.Render("DAEMON DISCONNECTED")
	}

	bar := StyleBar.Width(max(1, width))
	return bar.Render(padRight(keys, width)) + "\n" + bar.Render(padRight(stats, width))
}

// RenderHeader renders the top title bar.
func RenderHeader(version, socketPath string, live bool, width int) string {
	status := StyleChild.Render("▲ LIVE")
	if !live {
		status = StyleError.Render("▼ DOWN")
	}
	left := StyleTitle.Render("daemonSeed") + StyleInfo.Render("  v"+version)
	mid := StyleInfo.Render("socket: " + socketPath)
	line := left + "   " + mid + "   " + status
	return StyleBar.Width(max(1, width)).Render(line)
}

func formatUptime(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	sec := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, sec)
	}
	return fmt.Sprintf("%dm %ds", m, sec)
}

func padRight(s string, width int) string {
	if w := lipgloss.Width(s); w < width {
		return s + strings.Repeat(" ", width-w)
	}
	return s
}
