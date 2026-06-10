// Package components renders the dashboard panels. Pure functions: state in,
// string out — all interactivity lives in the parent model.
package components

import "github.com/charmbracelet/lipgloss"

// Color scheme (spec §12.5).
var (
	ColorParent  = lipgloss.Color("#00D7FF")
	ColorChild   = lipgloss.Color("#87FF5F")
	ColorError   = lipgloss.Color("#FF5F5F")
	ColorWarning = lipgloss.Color("#FFD75F")
	ColorInfo    = lipgloss.Color("#878787")
	ColorBorder  = lipgloss.Color("#444444")
	ColorBarBg   = lipgloss.Color("#1C1C1C")
	ColorBarFg   = lipgloss.Color("#AAAAAA")
)

var (
	StyleParent  = lipgloss.NewStyle().Foreground(ColorParent)
	StyleChild   = lipgloss.NewStyle().Foreground(ColorChild)
	StyleError   = lipgloss.NewStyle().Foreground(ColorError)
	StyleWarning = lipgloss.NewStyle().Foreground(ColorWarning)
	StyleInfo    = lipgloss.NewStyle().Foreground(ColorInfo)
	StyleTitle   = lipgloss.NewStyle().Bold(true)
	StyleBar     = lipgloss.NewStyle().Background(ColorBarBg).Foreground(ColorBarFg)

	PanelBorder = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder()).
			BorderForeground(ColorBorder).
			Padding(0, 1)
	PanelBorderFocused = lipgloss.NewStyle().
				Border(lipgloss.NormalBorder()).
				BorderForeground(ColorParent).
				Padding(0, 1)
)
