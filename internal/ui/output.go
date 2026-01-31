package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"gopkg.in/yaml.v3"
)

// OutputFormat represents the output format
type OutputFormat string

const (
	FormatTable OutputFormat = "table"
	FormatJSON  OutputFormat = "json"
	FormatYAML  OutputFormat = "yaml"
	FormatPlain OutputFormat = "plain"
)

// Output handles formatted output
type Output struct {
	format  OutputFormat
	writer  io.Writer
	noColor bool
	quiet   bool
}

// NewOutput creates a new Output instance
func NewOutput(format OutputFormat, noColor, quiet bool) *Output {
	return &Output{
		format:  format,
		writer:  os.Stdout,
		noColor: noColor,
		quiet:   quiet,
	}
}

// SetWriter sets the output writer
func (o *Output) SetWriter(w io.Writer) {
	o.writer = w
}

// Print prints a message
func (o *Output) Print(msg string) {
	if o.quiet {
		return
	}
	_, err := fmt.Fprintln(o.writer, msg)
	if err != nil {
		return
	}
}

// Printf prints a formatted message
func (o *Output) Printf(format string, args ...interface{}) {
	if o.quiet {
		return
	}
	_, err := fmt.Fprintf(o.writer, format+"\n", args...)
	if err != nil {
		return
	}
}

// Success prints a success message
func (o *Output) Success(msg string) {
	if o.quiet {
		return
	}
	if o.noColor {
		_, err := fmt.Fprintf(o.writer, "%s %s\n", IconSuccess, msg)
		if err != nil {
			return
		}
	} else {
		_, err := fmt.Fprintln(o.writer, Success.Render(IconSuccess)+" "+msg)
		if err != nil {
			return
		}
	}
}

// Error prints an error message
func (o *Output) Error(msg string) {
	if o.noColor {
		_, err := fmt.Fprintf(os.Stderr, "%s %s\n", IconError, msg)
		if err != nil {
			return
		}
	} else {
		_, err := fmt.Fprintln(os.Stderr, Error.Render(IconError)+" "+Error.Render(msg))
		if err != nil {
			return
		}
	}
}

// Warning prints a warning message
func (o *Output) Warning(msg string) {
	if o.quiet {
		return
	}
	if o.noColor {
		_, err := fmt.Fprintf(o.writer, "%s %s\n", IconWarning, msg)
		if err != nil {
			return
		}
	} else {
		_, err := fmt.Fprintln(o.writer, Warning.Render(IconWarning)+" "+Warning.Render(msg))
		if err != nil {
			return
		}
	}
}

// Info prints an info message
func (o *Output) Info(msg string) {
	if o.quiet {
		return
	}
	if o.noColor {
		_, err := fmt.Fprintf(o.writer, "%s %s\n", IconInfo, msg)
		if err != nil {
			return
		}
	} else {
		_, err := fmt.Fprintln(o.writer, Info.Render(IconInfo)+" "+msg)
		if err != nil {
			return
		}
	}
}

// Title prints a title
func (o *Output) Title(msg string) {
	if o.quiet {
		return
	}
	if o.noColor {
		_, err := fmt.Fprintf(o.writer, "\n%s\n%s\n", msg, strings.Repeat("=", len(msg)))
		if err != nil {
			return
		}
	} else {
		_, err := fmt.Fprintln(o.writer, Title.Render(msg))
		if err != nil {
			return
		}
	}
}

// JSON outputs data as JSON
func (o *Output) JSON(data interface{}) error {
	enc := json.NewEncoder(o.writer)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}

// YAML outputs data as YAML
func (o *Output) YAML(data interface{}) error {
	enc := yaml.NewEncoder(o.writer)
	enc.SetIndent(2)
	return enc.Encode(data)
}

// Data outputs data in the configured format
func (o *Output) Data(data interface{}) error {
	switch o.format {
	case FormatJSON:
		return o.JSON(data)
	case FormatYAML:
		return o.YAML(data)
	default:
		// For table/plain, caller handles formatting
		return nil
	}
}

// IsInteractive returns true if the output is to a terminal
func (o *Output) IsInteractive() bool {
	if f, ok := o.writer.(*os.File); ok {
		stat, _ := f.Stat()
		return (stat.Mode() & os.ModeCharDevice) != 0
	}
	return false
}

// Table represents a simple table
type Table struct {
	headers []string
	rows    [][]string
	output  *Output
}

// NewTable creates a new table
func NewTable(output *Output, headers ...string) *Table {
	return &Table{
		headers: headers,
		rows:    make([][]string, 0),
		output:  output,
	}
}

// AddRow adds a row to the table
func (t *Table) AddRow(cols ...string) {
	t.rows = append(t.rows, cols)
}

// Render renders the table
func (t *Table) Render() {
	if t.output.format == FormatJSON {
		t.renderJSON()
		return
	}
	if t.output.format == FormatYAML {
		t.renderYAML()
		return
	}

	// Calculate column widths
	widths := make([]int, len(t.headers))
	for i, h := range t.headers {
		widths[i] = len(h)
	}
	for _, row := range t.rows {
		for i, col := range row {
			if i < len(widths) && len(col) > widths[i] {
				widths[i] = len(col)
			}
		}
	}

	// Render header
	headerCells := make([]string, len(t.headers))
	for i, h := range t.headers {
		if t.output.noColor {
			headerCells[i] = padRight(h, widths[i])
		} else {
			headerCells[i] = HeaderStyle.Width(widths[i]).Render(h)
		}
	}
	_, err := fmt.Fprintln(t.output.writer, strings.Join(headerCells, "  "))
	if err != nil {
		return
	}

	// Render rows
	for _, row := range t.rows {
		cells := make([]string, len(row))
		for i, col := range row {
			width := widths[0]
			if i < len(widths) {
				width = widths[i]
			}
			cells[i] = padRight(col, width)
		}
		_, err := fmt.Fprintln(t.output.writer, strings.Join(cells, "  "))
		if err != nil {
			return
		}
	}
}

func (t *Table) renderJSON() {
	data := make([]map[string]string, len(t.rows))
	for i, row := range t.rows {
		m := make(map[string]string)
		for j, col := range row {
			if j < len(t.headers) {
				m[t.headers[j]] = col
			}
		}
		data[i] = m
	}
	err := t.output.JSON(data)
	if err != nil {
		return
	}
}

func (t *Table) renderYAML() {
	data := make([]map[string]string, len(t.rows))
	for i, row := range t.rows {
		m := make(map[string]string)
		for j, col := range row {
			if j < len(t.headers) {
				m[t.headers[j]] = col
			}
		}
		data[i] = m
	}
	err := t.output.YAML(data)
	if err != nil {
		return
	}
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

// KeyValue prints a key-value pair
func (o *Output) KeyValue(key, value string) {
	if o.quiet {
		return
	}
	if o.noColor {
		_, err := fmt.Fprintf(o.writer, "  %s: %s\n", key, value)
		if err != nil {
			return
		}
	} else {
		_, err := fmt.Fprintf(o.writer, "  %s: %s\n", Muted.Render(key), value)
		if err != nil {
			return
		}
	}
}

// Box prints content in a box
func (o *Output) Box(content string) {
	if o.quiet {
		return
	}
	if o.noColor {
		lines := strings.Split(content, "\n")
		maxLen := 0
		for _, line := range lines {
			if len(line) > maxLen {
				maxLen = len(line)
			}
		}
		border := strings.Repeat("─", maxLen+2)
		_, err := fmt.Fprintf(o.writer, "┌%s┐\n", border)
		if err != nil {
			return
		}
		for _, line := range lines {
			_, err := fmt.Fprintf(o.writer, "│ %s │\n", padRight(line, maxLen))
			if err != nil {
				return
			}
		}
		_, err = fmt.Fprintf(o.writer, "└%s┘\n", border)
		if err != nil {
			return
		}
	} else {
		_, err := fmt.Fprintln(o.writer, BoxStyle.Render(content))
		if err != nil {
			return
		}
	}
}

// SpinnerStyle Spinner styles
var (
	SpinnerStyle = lipgloss.NewStyle().Foreground(ColorPrimary)
)
