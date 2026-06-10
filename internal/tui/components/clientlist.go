package components

import (
	"fmt"
	"strings"
	"time"
)

// ClientRow is the render-ready projection of one connected client.
type ClientRow struct {
	Name        string
	Role        string
	ID          string
	State       string
	CurrentTask string
	ConnectedAt time.Time
}

// RenderClientList renders the CONNECTED CLIENTS panel at the given inner
// size.
func RenderClientList(rows []ClientRow, width, height int, focused bool) string {
	var b strings.Builder
	b.WriteString(StyleTitle.Render("CONNECTED CLIENTS"))
	b.WriteString("\n")
	b.WriteString(StyleInfo.Render(strings.Repeat("─", max(1, width-2))))
	b.WriteString("\n")

	if len(rows) == 0 {
		b.WriteString(StyleInfo.Render("(no clients connected)"))
	}
	for _, r := range rows {
		bullet, style := "○", StyleChild
		if r.Role == "parent" {
			bullet, style = "●", StyleParent
		}
		b.WriteString(style.Render(fmt.Sprintf("%s %s", bullet, r.Name)))
		b.WriteString("\n")
		b.WriteString(StyleInfo.Render("  role: " + r.Role))
		b.WriteString("\n")
		b.WriteString(StyleInfo.Render("  id: " + shortID(r.ID)))
		b.WriteString("\n")
		if r.Role == "child" && r.State != "" {
			stateStyle := StyleInfo
			switch r.State {
			case "error", "blocked":
				stateStyle = StyleError
			case "working":
				stateStyle = StyleWarning
			}
			b.WriteString(stateStyle.Render("  state: " + r.State))
			b.WriteString("\n")
		}
		if r.CurrentTask != "" {
			b.WriteString(StyleInfo.Render("  task: " + r.CurrentTask))
			b.WriteString("\n")
		}
		b.WriteString(StyleInfo.Render("  connected: " + humanAgo(r.ConnectedAt)))
		b.WriteString("\n\n")
	}

	panel := PanelBorder
	if focused {
		panel = PanelBorderFocused
	}
	return panel.Width(width).Height(height).Render(
		clipLines(strings.TrimRight(b.String(), "\n"), height))
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8] + "…"
	}
	return id
}

func humanAgo(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t).Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh%dm ago", int(d.Hours()), int(d.Minutes())%60)
	}
}

// clipLines truncates content to at most h lines so a long list never
// overflows its panel.
func clipLines(s string, h int) string {
	if h <= 0 {
		return s
	}
	lines := strings.Split(s, "\n")
	if len(lines) <= h {
		return s
	}
	return strings.Join(lines[:h], "\n")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
