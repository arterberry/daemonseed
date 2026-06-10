package components

import (
	"fmt"
	"strings"
	"time"
)

// FeedRow is the render-ready projection of one feed message.
type FeedRow struct {
	Timestamp time.Time
	From      string
	To        string
	Type      string
	Summary   string
	Selected  bool
}

// RenderMessageFeed renders the MESSAGE FEED panel. scroll is the number of
// rows the user has scrolled up from the newest message; paused appends a
// marker to the title.
func RenderMessageFeed(rows []FeedRow, width, height int, focused, paused bool, scroll int, tsFormat string) string {
	title := "MESSAGE FEED"
	if paused {
		title += "  " + StyleWarning.Render("[PAUSED]")
	}
	var b strings.Builder
	b.WriteString(StyleTitle.Render(title))
	b.WriteString("\n")
	b.WriteString(StyleInfo.Render(strings.Repeat("─", max(1, width-2))))
	b.WriteString("\n")

	// Each message takes two lines (header + summary). Show the newest
	// window that fits, offset by scroll.
	linesAvail := max(1, height-2)
	perMsg := 2
	visible := linesAvail / perMsg
	end := len(rows) - scroll
	if end > len(rows) {
		end = len(rows)
	}
	if end < 0 {
		end = 0
	}
	start := end - visible
	if start < 0 {
		start = 0
	}

	if len(rows) == 0 {
		b.WriteString(StyleInfo.Render("(no messages yet)"))
	}
	for _, r := range rows[start:end] {
		style := typeStyle(r.Type)
		header := fmt.Sprintf("%s [%s→%s] %s",
			r.Timestamp.Format(tsFormat), r.From, r.To, r.Type)
		if r.Selected {
			header = "▸ " + header
		}
		b.WriteString(style.Render(header))
		b.WriteString("\n")
		if r.Summary != "" {
			b.WriteString(StyleInfo.Render("           " + truncate(r.Summary, max(8, width-14))))
		}
		b.WriteString("\n")
	}

	panel := PanelBorder
	if focused {
		panel = PanelBorderFocused
	}
	return panel.Width(width).Height(height).Render(
		clipLines(strings.TrimRight(b.String(), "\n"), height))
}

// RenderDetail renders the full payload of the selected message.
func RenderDetail(r FeedRow, raw string, width, height int, tsFormat string) string {
	var b strings.Builder
	b.WriteString(StyleTitle.Render("MESSAGE DETAIL"))
	b.WriteString("\n")
	b.WriteString(StyleInfo.Render(strings.Repeat("─", max(1, width-2))))
	b.WriteString("\n")
	b.WriteString(typeStyle(r.Type).Render(fmt.Sprintf("%s  %s → %s  %s",
		r.Timestamp.Format(tsFormat), r.From, r.To, r.Type)))
	b.WriteString("\n\n")
	if raw == "" {
		raw = "(empty payload)"
	}
	b.WriteString(wrap(raw, max(8, width-4)))
	return PanelBorderFocused.Width(width).Height(height).Render(
		clipLines(b.String(), height))
}

func typeStyle(t string) interface{ Render(...string) string } {
	switch {
	case strings.HasPrefix(t, "INVALID") || strings.HasPrefix(t, "PERMISSION") ||
		t == "DELIVERY_FAILED" || t == "INTERNAL_ERROR" || t == "MESSAGE_TOO_LARGE":
		return StyleError
	case strings.HasPrefix(t, "SHUTDOWN") || t == "STATUS_TIMEOUT":
		return StyleWarning
	case t == "BROADCAST" || t == "ASSIGN_TASK":
		return StyleParent
	case t == "STATUS_REPORT" || t == "COMPLETE_TASK" || t == "ACK_TASK":
		return StyleChild
	default:
		return StyleInfo
	}
}

// truncate cuts s to at most n display bytes, appending an ellipsis.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return "…"
	}
	return s[:n-1] + "…"
}

// wrap hard-wraps s at width columns.
func wrap(s string, width int) string {
	var b strings.Builder
	col := 0
	for _, r := range s {
		if r == '\n' || col >= width {
			b.WriteByte('\n')
			col = 0
			if r == '\n' {
				continue
			}
		}
		b.WriteRune(r)
		col++
	}
	return b.String()
}
