package ui

import (
	"github.com/charmbracelet/lipgloss"
)

// Brand colors
var (
	ColorPrimary   = lipgloss.Color("#0EA5E9") // Sky blue
	ColorSecondary = lipgloss.Color("#8B5CF6") // Violet
	ColorSuccess   = lipgloss.Color("#10B981") // Emerald
	ColorWarning   = lipgloss.Color("#F59E0B") // Amber
	ColorError     = lipgloss.Color("#EF4444") // Red
	ColorMuted     = lipgloss.Color("#64748B") // Slate
	ColorSubtle    = lipgloss.Color("#94A3B8") // Slate light
)

// Text styles
var (
	Bold      = lipgloss.NewStyle().Bold(true)
	Italic    = lipgloss.NewStyle().Italic(true)
	Underline = lipgloss.NewStyle().Underline(true)
	Faint     = lipgloss.NewStyle().Faint(true)
)

// Semantic styles
var (
	Title = lipgloss.NewStyle().
		Bold(true).
		Foreground(ColorPrimary).
		MarginBottom(1)

	Subtitle = lipgloss.NewStyle().
			Foreground(ColorMuted)

	Success = lipgloss.NewStyle().
		Foreground(ColorSuccess)

	Warning = lipgloss.NewStyle().
		Foreground(ColorWarning)

	Error = lipgloss.NewStyle().
		Foreground(ColorError)

	Info = lipgloss.NewStyle().
		Foreground(ColorPrimary)

	Muted = lipgloss.NewStyle().
		Foreground(ColorMuted)

	Code = lipgloss.NewStyle().
		Background(lipgloss.Color("#1E293B")).
		Foreground(lipgloss.Color("#E2E8F0")).
		Padding(0, 1)

	URL = lipgloss.NewStyle().
		Foreground(ColorPrimary).
		Underline(true)
)

// Component styles
var (
	BoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(ColorMuted).
			Padding(1, 2)

	SelectedStyle = lipgloss.NewStyle().
			Foreground(ColorPrimary).
			Bold(true)

	HeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorPrimary).
			BorderStyle(lipgloss.NormalBorder()).
			BorderBottom(true).
			BorderForeground(ColorMuted)

	CellStyle = lipgloss.NewStyle().
			Padding(0, 1)

	EvenRowStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("#1E293B"))

	OddRowStyle = lipgloss.NewStyle()
)

// Status indicators
var (
	StatusActive = lipgloss.NewStyle().
			Foreground(ColorSuccess).
			SetString("●")

	StatusInactive = lipgloss.NewStyle().
			Foreground(ColorMuted).
			SetString("○")

	StatusError = lipgloss.NewStyle().
			Foreground(ColorError).
			SetString("●")
)

// Icons (using unicode)
const (
	IconSuccess  = "✓"
	IconError    = "✗"
	IconWarning  = "⚠"
	IconInfo     = "ℹ"
	IconBranch   = "⎇"
	IconDatabase = "⛁"
	IconArrow    = "→"
	IconDot      = "•"
	IconCheck    = "✔"
	IconCross    = "✘"
	IconStar     = "★"
	IconFolder   = "📁"
	IconFile     = "📄"
	IconClock    = "⏱"
	IconLock     = "🔒"
	IconUnlock   = "🔓"
)

// Emoji alternatives (more compatible)
const (
	EmojiSuccess = "✅"
	EmojiError   = "❌"
	EmojiWarning = "⚠️"
	EmojiInfo    = "ℹ️"
	EmojiBranch  = "🌿"
	EmojiDB      = "🗄️"
	EmojiRocket  = "🚀"
	EmojiStar    = "⭐"
	EmojiClock   = "⏰"
	EmojiLock    = "🔒"
	EmojiKey     = "🔑"
	EmojiTrash   = "🗑️"
	EmojiLink    = "🔗"
	EmojiGear    = "⚙️"
)
