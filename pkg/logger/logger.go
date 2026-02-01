package logger

import (
	"os"
	"time"

	"github.com/charmbracelet/log"
)

var defaultLogger *log.Logger

func init() {
	defaultLogger = log.NewWithOptions(os.Stderr, log.Options{
		ReportTimestamp: true,
		TimeFormat:      time.Kitchen,
	})
}

// SetLevel sets the log level
func SetLevel(level string) {
	switch level {
	case "debug":
		defaultLogger.SetLevel(log.DebugLevel)
	case "info":
		defaultLogger.SetLevel(log.InfoLevel)
	case "warn":
		defaultLogger.SetLevel(log.WarnLevel)
	case "error":
		defaultLogger.SetLevel(log.ErrorLevel)
	}
}

// Debug logs at the "debug" level
func Debug(msg string, keyvals ...interface{}) {
	defaultLogger.Debug(msg, keyvals...)
}

// Info logs at the "info" level
func Info(msg string, keyvals ...interface{}) {
	defaultLogger.Info(msg, keyvals...)
}

// Warn logs at the "warn" level
func Warn(msg string, keyvals ...interface{}) {
	defaultLogger.Warn(msg, keyvals...)
}

// Error logs at the "error" level
func Error(msg string, keyvals ...interface{}) {
	defaultLogger.Error(msg, keyvals...)
}

// Fatal logs and exits
func Fatal(msg string, keyvals ...interface{}) {
	defaultLogger.Fatal(msg, keyvals...)
}

// With returns a logger with additional context
func With(keyvals ...interface{}) *log.Logger {
	return defaultLogger.With(keyvals...)
}
