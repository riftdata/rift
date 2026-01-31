package ui

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Spinner wraps a bubbletea spinner for easy use
type Spinner struct {
	message string
	program *tea.Program
	model   *spinnerModel
	done    chan struct{}
}

type spinnerModel struct {
	spinner  spinner.Model
	message  string
	quitting bool
	err      error
}

type spinnerDoneMsg struct {
	err error
}

func initialSpinnerModel(message string) spinnerModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(ColorPrimary)
	return spinnerModel{
		spinner: s,
		message: message,
	}
}

func (m *spinnerModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m *spinnerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.quitting = true
			return m, tea.Quit
		}
	case spinnerDoneMsg:
		m.quitting = true
		m.err = msg.err
		return m, tea.Quit
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m *spinnerModel) View() string {
	if m.quitting {
		return ""
	}
	return fmt.Sprintf("%s %s", m.spinner.View(), m.message)
}

// NewSpinner creates a new spinner
func NewSpinner(message string) *Spinner {
	return &Spinner{
		message: message,
		done:    make(chan struct{}),
	}
}

// Start starts the spinner
func (s *Spinner) Start() {
	model := initialSpinnerModel(s.message)
	s.model = &model
	s.program = tea.NewProgram(&model)

	go func() {
		_, _ = s.program.Run()
		close(s.done)
	}()
}

// Stop stops the spinner with a success message
func (s *Spinner) Stop(message string) {
	if s.program != nil {
		s.program.Send(spinnerDoneMsg{})
		<-s.done
	}
	fmt.Printf("%s %s\n", Success.Render(IconSuccess), message)
}

// StopError stops the spinner with an error
func (s *Spinner) StopError(err error) {
	if s.program != nil {
		s.program.Send(spinnerDoneMsg{err: err})
		<-s.done
	}
	fmt.Printf("%s %s\n", Error.Render(IconError), Error.Render(err.Error()))
}

// StopFail stops the spinner with a failure message
func (s *Spinner) StopFail(message string) {
	if s.program != nil {
		s.program.Send(spinnerDoneMsg{})
		<-s.done
	}
	fmt.Printf("%s %s\n", Error.Render(IconError), Error.Render(message))
}

// UpdateMessage updates the spinner message
func (s *Spinner) UpdateMessage(message string) {
	s.message = message
	// Note: Would need custom message for live updates
}

// SimpleSpinner is a non-interactive spinner for simpler use cases
type SimpleSpinner struct {
	message string
	frames  []string
	current int
	done    chan struct{}
	ticker  *time.Ticker
}

// NewSimpleSpinner creates a simple spinner
func NewSimpleSpinner(message string) *SimpleSpinner {
	return &SimpleSpinner{
		message: message,
		frames:  []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"},
		done:    make(chan struct{}),
	}
}

// Start starts the simple spinner
func (s *SimpleSpinner) Start() {
	s.ticker = time.NewTicker(80 * time.Millisecond)
	go func() {
		for {
			select {
			case <-s.done:
				return
			case <-s.ticker.C:
				fmt.Printf("\r%s %s", SpinnerStyle.Render(s.frames[s.current]), s.message)
				s.current = (s.current + 1) % len(s.frames)
			}
		}
	}()
}

// Stop stops the simple spinner
func (s *SimpleSpinner) Stop(message string) {
	s.ticker.Stop()
	close(s.done)
	fmt.Printf("\r%s %s\n", Success.Render(IconSuccess), message)
}

// StopFail stops the simple spinner with failure
func (s *SimpleSpinner) StopFail(message string) {
	s.ticker.Stop()
	close(s.done)
	fmt.Printf("\r%s %s\n", Error.Render(IconError), message)
}
