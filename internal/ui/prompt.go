package ui

import (
	"errors"
	"fmt"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
)

// PromptTheme returns the pgbranch theme for prompts
func PromptTheme() *huh.Theme {
	t := huh.ThemeBase()

	t.Focused.Title = lipgloss.NewStyle().
		Bold(true).
		Foreground(ColorPrimary)

	t.Focused.Description = lipgloss.NewStyle().
		Foreground(ColorMuted)

	t.Focused.SelectSelector = lipgloss.NewStyle().
		Foreground(ColorPrimary).
		SetString("> ")

	t.Focused.SelectedOption = lipgloss.NewStyle().
		Foreground(ColorPrimary).
		Bold(true)

	t.Focused.UnselectedOption = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#888888"))

	return t
}

// Confirm prompts for yes/no confirmation
func Confirm(message string, defaultValue bool) (bool, error) {
	var result bool

	err := huh.NewConfirm().
		Title(message).
		Affirmative("Yes").
		Negative("No").
		Value(&result).
		WithTheme(PromptTheme()).
		Run()

	return result, err
}

// Input prompts for text input
func Input(title, placeholder string, validator func(string) error) (string, error) {
	var result string

	input := huh.NewInput().
		Title(title).
		Placeholder(placeholder).
		Value(&result)

	if validator != nil {
		input = input.Validate(validator)
	}

	err := input.WithTheme(PromptTheme()).Run()
	return result, err
}

// Select prompts for selection from options
func Select(title string, options []string) (string, error) {
	var result string

	opts := make([]huh.Option[string], len(options))
	for i, opt := range options {
		opts[i] = huh.NewOption(opt, opt)
	}

	err := huh.NewSelect[string]().
		Title(title).
		Options(opts...).
		Value(&result).
		WithTheme(PromptTheme()).
		Run()

	return result, err
}

// SelectWithKeys prompts for selection with custom keys
func SelectWithKeys(title string, options map[string]string) (string, error) {
	var result string

	opts := make([]huh.Option[string], 0, len(options))
	for key, label := range options {
		opts = append(opts, huh.NewOption(label, key))
	}

	err := huh.NewSelect[string]().
		Title(title).
		Options(opts...).
		Value(&result).
		WithTheme(PromptTheme()).
		Run()

	return result, err
}

// MultiSelect prompts for multiple selections
func MultiSelect(title string, options []string) ([]string, error) {
	var result []string

	opts := make([]huh.Option[string], len(options))
	for i, opt := range options {
		opts[i] = huh.NewOption(opt, opt)
	}

	err := huh.NewMultiSelect[string]().
		Title(title).
		Options(opts...).
		Value(&result).
		WithTheme(PromptTheme()).
		Run()

	return result, err
}

// Password prompts for password input
func Password(title string) (string, error) {
	var result string

	err := huh.NewInput().
		Title(title).
		EchoMode(huh.EchoModePassword).
		Value(&result).
		WithTheme(PromptTheme()).
		Run()

	return result, err
}

// ConnectionForm prompts for database connection details
type ConnectionDetails struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
	SSLMode  string
}

func ConnectionForm(defaults *ConnectionDetails) (*ConnectionDetails, error) {
	if defaults == nil {
		defaults = &ConnectionDetails{
			Host:    "localhost",
			Port:    "5432",
			SSLMode: "prefer",
		}
	}

	details := &ConnectionDetails{}

	form := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Host").
				Value(&details.Host).
				Placeholder(defaults.Host),

			huh.NewInput().
				Title("Port").
				Value(&details.Port).
				Placeholder(defaults.Port).
				Validate(func(s string) error {
					if s == "" {
						return nil
					}
					var port int
					if _, err := fmt.Sscanf(s, "%d", &port); err != nil {
						return errors.New("invalid port number")
					}
					if port < 1 || port > 65535 {
						return errors.New("port must be between 1 and 65535")
					}
					return nil
				}),

			huh.NewInput().
				Title("Database").
				Value(&details.Database).
				Placeholder("postgres"),

			huh.NewInput().
				Title("User").
				Value(&details.User).
				Placeholder("postgres"),

			huh.NewInput().
				Title("Password").
				Value(&details.Password).
				EchoMode(huh.EchoModePassword),

			huh.NewSelect[string]().
				Title("SSL Mode").
				Options(
					huh.NewOption("Disable", "disable"),
					huh.NewOption("Prefer", "prefer"),
					huh.NewOption("Require", "require"),
					huh.NewOption("Verify CA", "verify-ca"),
					huh.NewOption("Verify Full", "verify-full"),
				).
				Value(&details.SSLMode),
		),
	).WithTheme(PromptTheme())

	err := form.Run()
	if err != nil {
		return nil, err
	}

	// Apply defaults for empty values
	if details.Host == "" {
		details.Host = defaults.Host
	}
	if details.Port == "" {
		details.Port = defaults.Port
	}
	if details.SSLMode == "" {
		details.SSLMode = defaults.SSLMode
	}

	return details, nil
}

// BranchForm prompts for branch creation details
type BranchDetails struct {
	Name   string
	Parent string
	TTL    string
}

func BranchForm(parents []string) (*BranchDetails, error) {
	details := &BranchDetails{}

	parentOpts := make([]huh.Option[string], len(parents))
	for i, p := range parents {
		parentOpts[i] = huh.NewOption(p, p)
	}

	form := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Branch Name").
				Value(&details.Name).
				Placeholder("feature-auth").
				Validate(func(s string) error {
					if s == "" {
						return errors.New("branch name is required")
					}
					if len(s) > 63 {
						return errors.New("branch name must be 63 characters or less")
					}
					return nil
				}),

			huh.NewSelect[string]().
				Title("Parent Branch").
				Options(parentOpts...).
				Value(&details.Parent),

			huh.NewSelect[string]().
				Title("Auto-delete after (TTL)").
				Options(
					huh.NewOption("Never", ""),
					huh.NewOption("1 hour", "1h"),
					huh.NewOption("6 hours", "6h"),
					huh.NewOption("24 hours", "24h"),
					huh.NewOption("7 days", "7d"),
					huh.NewOption("30 days", "30d"),
				).
				Value(&details.TTL),
		),
	).WithTheme(PromptTheme())

	err := form.Run()
	return details, err
}
