// Package config handles application configuration loading and validation.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	// Upstream database
	Upstream UpstreamConfig `mapstructure:"upstream"`

	// Proxy settings
	Proxy ProxyConfig `mapstructure:"proxy"`

	// API/Dashboard settings
	API APIConfig `mapstructure:"api"`

	// Storage settings
	Storage StorageConfig `mapstructure:"storage"`

	// Logging
	Log LogConfig `mapstructure:"log"`

	// Telemetry (opt-in)
	Telemetry TelemetryConfig `mapstructure:"telemetry"`
}

type UpstreamConfig struct {
	URL            string        `mapstructure:"url"`
	MaxConnections int           `mapstructure:"max_connections"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
	IdleTimeout    time.Duration `mapstructure:"idle_timeout"`
	SSLMode        string        `mapstructure:"ssl_mode"`
}

type ProxyConfig struct {
	ListenAddr     string        `mapstructure:"listen_addr"`
	MaxConnections int           `mapstructure:"max_connections"`
	ReadTimeout    time.Duration `mapstructure:"read_timeout"`
	WriteTimeout   time.Duration `mapstructure:"write_timeout"`
}

type APIConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	ListenAddr string `mapstructure:"listen_addr"`
	EnableCORS bool   `mapstructure:"enable_cors"`
	AuthToken  string `mapstructure:"auth_token"`
}

type StorageConfig struct {
	DataDir       string        `mapstructure:"data_dir"`
	MaxBranchSize int64         `mapstructure:"max_branch_size"`
	CompactAfter  time.Duration `mapstructure:"compact_after"`
	RetentionDays int           `mapstructure:"retention_days"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
	File   string `mapstructure:"file"`
}

type TelemetryConfig struct {
	Enabled   bool   `mapstructure:"enabled"`
	Endpoint  string `mapstructure:"endpoint"`
	Anonymous bool   `mapstructure:"anonymous"`
}

// DefaultConfig returns sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Upstream: UpstreamConfig{
			MaxConnections: 10,
			ConnectTimeout: 10 * time.Second,
			IdleTimeout:    5 * time.Minute,
			SSLMode:        "prefer",
		},
		Proxy: ProxyConfig{
			ListenAddr:     ":6432",
			MaxConnections: 100,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
		},
		API: APIConfig{
			Enabled:    true,
			ListenAddr: ":8080",
			EnableCORS: true,
		},
		Storage: StorageConfig{
			DataDir:       defaultDataDir(),
			MaxBranchSize: 10 * 1024 * 1024 * 1024, // 10GB
			CompactAfter:  24 * time.Hour,
			RetentionDays: 30,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
		},
		Telemetry: TelemetryConfig{
			Enabled:   false,
			Anonymous: true,
		},
	}
}

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".rift"
	}
	return filepath.Join(home, ".rift")
}

// Load loads configuration from file, env vars, and flags
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set defaults
	defaults := DefaultConfig()
	v.SetDefault("upstream.max_connections", defaults.Upstream.MaxConnections)
	v.SetDefault("upstream.connect_timeout", defaults.Upstream.ConnectTimeout)
	v.SetDefault("upstream.idle_timeout", defaults.Upstream.IdleTimeout)
	v.SetDefault("upstream.ssl_mode", defaults.Upstream.SSLMode)
	v.SetDefault("proxy.listen_addr", defaults.Proxy.ListenAddr)
	v.SetDefault("proxy.max_connections", defaults.Proxy.MaxConnections)
	v.SetDefault("proxy.read_timeout", defaults.Proxy.ReadTimeout)
	v.SetDefault("proxy.write_timeout", defaults.Proxy.WriteTimeout)
	v.SetDefault("api.enabled", defaults.API.Enabled)
	v.SetDefault("api.listen_addr", defaults.API.ListenAddr)
	v.SetDefault("api.enable_cors", defaults.API.EnableCORS)
	v.SetDefault("storage.data_dir", defaults.Storage.DataDir)
	v.SetDefault("storage.max_branch_size", defaults.Storage.MaxBranchSize)
	v.SetDefault("storage.compact_after", defaults.Storage.CompactAfter)
	v.SetDefault("storage.retention_days", defaults.Storage.RetentionDays)
	v.SetDefault("log.level", defaults.Log.Level)
	v.SetDefault("log.format", defaults.Log.Format)
	v.SetDefault("telemetry.enabled", defaults.Telemetry.Enabled)
	v.SetDefault("telemetry.anonymous", defaults.Telemetry.Anonymous)

	// Config file
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath(defaultDataDir())
		v.AddConfigPath("/etc/rift")
	}

	// Environment variables
	v.SetEnvPrefix("rift")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Read the config file (ignore if not found)
	if err := v.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			return nil, fmt.Errorf("reading config: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	return &cfg, nil
}

// Save writes the config to a file
func (c *Config) Save(path string) error {
	v := viper.New()
	v.Set("upstream", c.Upstream)
	v.Set("proxy", c.Proxy)
	v.Set("api", c.API)
	v.Set("storage", c.Storage)
	v.Set("log", c.Log)
	v.Set("telemetry", c.Telemetry)

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("creating config directory: %w", err)
	}

	return v.WriteConfigAs(path)
}

// Validate checks if the config is valid
func (c *Config) Validate() error {
	if c.Upstream.URL == "" {
		return fmt.Errorf("upstream.url is required")
	}
	if c.Proxy.ListenAddr == "" {
		return fmt.Errorf("proxy.listen_addr is required")
	}
	return nil
}
