package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Progress wraps a bubbletea progress bar
type Progress struct {
	total   int64
	current int64
	message string
	program *tea.Program
	done    chan struct{}
}

type progressModel struct {
	progress progress.Model
	message  string
	percent  float64
	width    int
}

type progressUpdateMsg struct {
	percent float64
	message string
}

type progressDoneMsg struct{}

func initialProgressModel(message string) progressModel {
	p := progress.New(
		progress.WithDefaultGradient(),
		progress.WithWidth(40),
	)
	return progressModel{
		progress: p,
		message:  message,
		width:    40,
	}
}

func (m *progressModel) Init() tea.Cmd {
	return nil
}

func (m *progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width - 20
		if m.width > 60 {
			m.width = 60
		}
		if m.width < 20 {
			m.width = 20
		}
		m.progress.Width = m.width
	case progressUpdateMsg:
		m.percent = msg.percent
		if msg.message != "" {
			m.message = msg.message
		}
		return m, nil
	case progressDoneMsg:
		return m, tea.Quit
	}
	return m, nil
}

func (m *progressModel) View() string {
	return fmt.Sprintf(
		"%s\n%s",
		m.message,
		m.progress.ViewAs(m.percent),
	)
}

// NewProgress creates a new progress bar
func NewProgress(total int64, message string) *Progress {
	return &Progress{
		total:   total,
		current: 0,
		message: message,
		done:    make(chan struct{}),
	}
}

// Start starts the progress display
func (p *Progress) Start() {
	model := initialProgressModel(p.message)
	p.program = tea.NewProgram(&model)

	go func() {
		_, _ = p.program.Run()
		close(p.done)
	}()
}

// Update updates the progress
func (p *Progress) Update(current int64, message string) {
	p.current = current
	percent := float64(current) / float64(p.total)
	if percent > 1 {
		percent = 1
	}
	if p.program != nil {
		p.program.Send(progressUpdateMsg{percent: percent, message: message})
	}
}

// Increment increments progress by delta
func (p *Progress) Increment(delta int64) {
	p.Update(p.current+delta, "")
}

// Done completes the progress
func (p *Progress) Done() {
	if p.program != nil {
		p.program.Send(progressDoneMsg{})
		<-p.done
	}
}

// SimpleProgress is a simpler progress bar without bubbletea
type SimpleProgress struct {
	total   int64
	current int64
	width   int
	message string
}

// NewSimpleProgress creates a simple progress bar
func NewSimpleProgress(total int64, message string) *SimpleProgress {
	return &SimpleProgress{
		total:   total,
		current: 0,
		width:   40,
		message: message,
	}
}

// Update updates and renders the progress bar
func (p *SimpleProgress) Update(current int64) {
	p.current = current
	p.render()
}

// Increment increments the progress
func (p *SimpleProgress) Increment(delta int64) {
	p.Update(p.current + delta)
}

func (p *SimpleProgress) render() {
	percent := float64(p.current) / float64(p.total)
	if percent > 1 {
		percent = 1
	}

	filled := int(percent * float64(p.width))
	empty := p.width - filled

	bar := lipgloss.NewStyle().Foreground(ColorPrimary).Render(strings.Repeat("█", filled)) +
		lipgloss.NewStyle().Foreground(ColorMuted).Render(strings.Repeat("░", empty))

	fmt.Printf("\r%s [%s] %.0f%%", p.message, bar, percent*100)
}

// Done completes the progress bar
func (p *SimpleProgress) Done(message string) {
	p.current = p.total
	p.render()
	fmt.Println()
	fmt.Printf("%s %s\n", Success.Render(IconSuccess), message)
}
