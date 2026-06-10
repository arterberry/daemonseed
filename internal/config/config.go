// Package config loads daemonSeed configuration with the precedence
// defaults → config file → environment overrides (flag overrides are applied
// by the CLI layer on top of the result).
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration object.
type Config struct {
	Daemon   DaemonConfig   `yaml:"daemon"`
	Timeouts TimeoutsConfig `yaml:"timeouts"`
	Limits   LimitsConfig   `yaml:"limits"`
	Audit    AuditConfig    `yaml:"audit"`
	TUI      TUIConfig      `yaml:"tui"`
	Commands CommandsConfig `yaml:"commands"`
	Trace    TraceConfig    `yaml:"trace"`
}

// TraceConfig controls session tracing (§20.10): an OTel-flavored local log
// of MCP tool invocations and parent↔child communications, with truncated
// payload snippets. Backend "jsonl" appends lines; "sqlite" writes to a
// local database (better once the log gets big).
type TraceConfig struct {
	Enabled        bool   `yaml:"enabled"`
	Backend        string `yaml:"backend"` // jsonl | sqlite
	Path           string `yaml:"path"`
	MaxDetailChars int    `yaml:"max_detail_chars"`
	MaxSizeMB      int    `yaml:"max_size_mb"` // jsonl rotation threshold
}

// CommandsConfig gates parent-supplied slash commands surfaced into a child
// session by the inbox hook (§20.7). Default: empty, i.e. deny all.
type CommandsConfig struct {
	AllowFromParent []string `yaml:"allow_from_parent"`
}

type DaemonConfig struct {
	SocketPath string `yaml:"socket_path"`
	PIDFile    string `yaml:"pid_file"`
	AutoStart  bool   `yaml:"auto_start"`
}

type TimeoutsConfig struct {
	HandshakeSeconds         int `yaml:"handshake_seconds"`
	StatusRequestSeconds     int `yaml:"status_request_seconds"`
	ShutdownAckSeconds       int `yaml:"shutdown_ack_seconds"`
	ReconnectAttempts        int `yaml:"reconnect_attempts"`
	ReconnectBackoffMs       int `yaml:"reconnect_backoff_ms"`
	HeartbeatIntervalSeconds int `yaml:"heartbeat_interval_seconds"`
	StaleClientSeconds       int `yaml:"stale_client_seconds"`
}

type LimitsConfig struct {
	MaxMessageBytes          int `yaml:"max_message_bytes"`
	MaxClients               int `yaml:"max_clients"`
	MaxPendingTasksPerClient int `yaml:"max_pending_tasks_per_client"`
	// §20.8 scheduler guardrails. MinScheduleIntervalSeconds may be 0 to
	// disable the floor (used in tests).
	MinScheduleIntervalSeconds int `yaml:"min_schedule_interval_seconds"`
	MaxSchedules               int `yaml:"max_schedules"`
}

type AuditConfig struct {
	Enabled       bool   `yaml:"enabled"`
	LogPath       string `yaml:"log_path"`
	MaxSizeMB     int    `yaml:"max_size_mb"`
	RotateOnStart bool   `yaml:"rotate_on_start"`
	LogPayloads   bool   `yaml:"log_payloads"`
}

type TUIConfig struct {
	FeedMaxLines    int    `yaml:"feed_max_lines"`
	TimestampFormat string `yaml:"timestamp_format"`
}

// Default returns a Config populated with the spec §13.1 defaults.
func Default() *Config {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "." // fall back to relative paths; surfaced when those paths are used
	}
	return &Config{
		Daemon: DaemonConfig{
			SocketPath: "/tmp/daemonseed.sock",
			PIDFile:    "/tmp/daemonseed.pid",
			AutoStart:  false,
		},
		Timeouts: TimeoutsConfig{
			HandshakeSeconds:         5,
			StatusRequestSeconds:     10,
			ShutdownAckSeconds:       5,
			ReconnectAttempts:        3,
			ReconnectBackoffMs:       500,
			HeartbeatIntervalSeconds: 15,
			StaleClientSeconds:       45,
		},
		Limits: LimitsConfig{
			MaxMessageBytes:            1048576,
			MaxClients:                 20,
			MaxPendingTasksPerClient:   50,
			MinScheduleIntervalSeconds: 60,
			MaxSchedules:               50,
		},
		Audit: AuditConfig{
			Enabled:       true,
			LogPath:       filepath.Join(home, ".local", "share", "daemonseed", "audit.jsonl"),
			MaxSizeMB:     100,
			RotateOnStart: false,
		},
		TUI: TUIConfig{
			FeedMaxLines:    500,
			TimestampFormat: "15:04:05",
		},
		Trace: TraceConfig{
			Enabled:        true,
			Backend:        "jsonl",
			Path:           filepath.Join(home, ".local", "share", "daemonseed", "trace.jsonl"),
			MaxDetailChars: 200,
			MaxSizeMB:      100,
		},
	}
}

// DefaultPath returns the default config file location
// (~/.config/daemonseed/config.yaml), honoring the DAEMONSEED_CONFIG env var.
func DefaultPath() string {
	if p := os.Getenv("DAEMONSEED_CONFIG"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "config.yaml"
	}
	return filepath.Join(home, ".config", "daemonseed", "config.yaml")
}

// Load builds the effective config: defaults, then the YAML file at path
// (skipped without error if path is "" or the file does not exist), then
// environment variable overrides, then validation.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			dec := yaml.NewDecoder(strings.NewReader(string(data)))
			dec.KnownFields(true)
			if err := dec.Decode(cfg); err != nil {
				return nil, fmt.Errorf("parse config %s: %w", path, err)
			}
		case os.IsNotExist(err):
			// No config file is fine — defaults apply.
		default:
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
	}
	if err := cfg.applyEnv(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.expandPaths()
	return cfg, nil
}

// envOverrides maps DAEMONSEED_<SECTION>_<KEY> variables to config fields.
// Each entry parses the raw string and assigns it, returning a parse error
// with the variable name for context.
func (c *Config) applyEnv() error {
	str := func(dst *string) func(string) error {
		return func(v string) error { *dst = v; return nil }
	}
	boolean := func(dst *bool) func(string) error {
		return func(v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("expected boolean, got %q", v)
			}
			*dst = b
			return nil
		}
	}
	integer := func(dst *int) func(string) error {
		return func(v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("expected integer, got %q", v)
			}
			*dst = n
			return nil
		}
	}
	overrides := map[string]func(string) error{
		"DAEMONSEED_DAEMON_SOCKET_PATH":                   str(&c.Daemon.SocketPath),
		"DAEMONSEED_DAEMON_PID_FILE":                      str(&c.Daemon.PIDFile),
		"DAEMONSEED_DAEMON_AUTO_START":                    boolean(&c.Daemon.AutoStart),
		"DAEMONSEED_TIMEOUTS_HANDSHAKE_SECONDS":           integer(&c.Timeouts.HandshakeSeconds),
		"DAEMONSEED_TIMEOUTS_STATUS_REQUEST_SECONDS":      integer(&c.Timeouts.StatusRequestSeconds),
		"DAEMONSEED_TIMEOUTS_SHUTDOWN_ACK_SECONDS":        integer(&c.Timeouts.ShutdownAckSeconds),
		"DAEMONSEED_TIMEOUTS_RECONNECT_ATTEMPTS":          integer(&c.Timeouts.ReconnectAttempts),
		"DAEMONSEED_TIMEOUTS_RECONNECT_BACKOFF_MS":        integer(&c.Timeouts.ReconnectBackoffMs),
		"DAEMONSEED_TIMEOUTS_HEARTBEAT_INTERVAL_SECONDS":  integer(&c.Timeouts.HeartbeatIntervalSeconds),
		"DAEMONSEED_TIMEOUTS_STALE_CLIENT_SECONDS":        integer(&c.Timeouts.StaleClientSeconds),
		"DAEMONSEED_LIMITS_MAX_MESSAGE_BYTES":             integer(&c.Limits.MaxMessageBytes),
		"DAEMONSEED_LIMITS_MAX_CLIENTS":                   integer(&c.Limits.MaxClients),
		"DAEMONSEED_LIMITS_MAX_PENDING_TASKS_PER_CLIENT":  integer(&c.Limits.MaxPendingTasksPerClient),
		"DAEMONSEED_LIMITS_MIN_SCHEDULE_INTERVAL_SECONDS": integer(&c.Limits.MinScheduleIntervalSeconds),
		"DAEMONSEED_LIMITS_MAX_SCHEDULES":                 integer(&c.Limits.MaxSchedules),
		"DAEMONSEED_AUDIT_ENABLED":                        boolean(&c.Audit.Enabled),
		"DAEMONSEED_AUDIT_LOG_PATH":                       str(&c.Audit.LogPath),
		"DAEMONSEED_AUDIT_MAX_SIZE_MB":                    integer(&c.Audit.MaxSizeMB),
		"DAEMONSEED_AUDIT_ROTATE_ON_START":                boolean(&c.Audit.RotateOnStart),
		"DAEMONSEED_AUDIT_LOG_PAYLOADS":                   boolean(&c.Audit.LogPayloads),
		"DAEMONSEED_TUI_FEED_MAX_LINES":                   integer(&c.TUI.FeedMaxLines),
		"DAEMONSEED_TUI_TIMESTAMP_FORMAT":                 str(&c.TUI.TimestampFormat),
		"DAEMONSEED_TRACE_ENABLED":                        boolean(&c.Trace.Enabled),
		"DAEMONSEED_TRACE_BACKEND":                        str(&c.Trace.Backend),
		"DAEMONSEED_TRACE_PATH":                           str(&c.Trace.Path),
		"DAEMONSEED_TRACE_MAX_DETAIL_CHARS":               integer(&c.Trace.MaxDetailChars),
		"DAEMONSEED_TRACE_MAX_SIZE_MB":                    integer(&c.Trace.MaxSizeMB),
	}
	for key, apply := range overrides {
		if v, ok := os.LookupEnv(key); ok {
			if err := apply(v); err != nil {
				return fmt.Errorf("invalid %s: %w", key, err)
			}
		}
	}
	return nil
}

// Validate rejects configurations that would misbehave at runtime.
func (c *Config) Validate() error {
	positive := map[string]int{
		"timeouts.handshake_seconds":          c.Timeouts.HandshakeSeconds,
		"timeouts.status_request_seconds":     c.Timeouts.StatusRequestSeconds,
		"timeouts.shutdown_ack_seconds":       c.Timeouts.ShutdownAckSeconds,
		"timeouts.reconnect_backoff_ms":       c.Timeouts.ReconnectBackoffMs,
		"timeouts.heartbeat_interval_seconds": c.Timeouts.HeartbeatIntervalSeconds,
		"timeouts.stale_client_seconds":       c.Timeouts.StaleClientSeconds,
		"limits.max_message_bytes":            c.Limits.MaxMessageBytes,
		"limits.max_clients":                  c.Limits.MaxClients,
		"limits.max_pending_tasks_per_client": c.Limits.MaxPendingTasksPerClient,
		"tui.feed_max_lines":                  c.TUI.FeedMaxLines,
	}
	for name, v := range positive {
		if v <= 0 {
			return fmt.Errorf("invalid config: %s must be positive, got %d", name, v)
		}
	}
	if c.Timeouts.ReconnectAttempts < 0 {
		return fmt.Errorf("invalid config: timeouts.reconnect_attempts must be >= 0, got %d",
			c.Timeouts.ReconnectAttempts)
	}
	if c.Limits.MinScheduleIntervalSeconds < 0 {
		return fmt.Errorf("invalid config: limits.min_schedule_interval_seconds must be >= 0, got %d",
			c.Limits.MinScheduleIntervalSeconds)
	}
	if c.Limits.MaxSchedules <= 0 {
		return fmt.Errorf("invalid config: limits.max_schedules must be positive, got %d",
			c.Limits.MaxSchedules)
	}
	if c.Trace.Backend != "" && c.Trace.Backend != "jsonl" && c.Trace.Backend != "sqlite" {
		return fmt.Errorf("invalid config: trace.backend must be jsonl or sqlite, got %q", c.Trace.Backend)
	}
	if c.Trace.MaxDetailChars <= 0 {
		return fmt.Errorf("invalid config: trace.max_detail_chars must be positive, got %d",
			c.Trace.MaxDetailChars)
	}
	if c.Trace.MaxSizeMB <= 0 {
		return fmt.Errorf("invalid config: trace.max_size_mb must be positive, got %d", c.Trace.MaxSizeMB)
	}
	if c.Audit.MaxSizeMB <= 0 {
		return fmt.Errorf("invalid config: audit.max_size_mb must be positive, got %d", c.Audit.MaxSizeMB)
	}
	if c.Daemon.SocketPath == "" {
		return fmt.Errorf("invalid config: daemon.socket_path must not be empty")
	}
	if c.Daemon.PIDFile == "" {
		return fmt.Errorf("invalid config: daemon.pid_file must not be empty")
	}
	return nil
}

// expandPaths resolves a leading "~/" in path settings, since YAML values
// are not shell-expanded.
func (c *Config) expandPaths() {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	expand := func(p string) string {
		if strings.HasPrefix(p, "~/") {
			return filepath.Join(home, p[2:])
		}
		return p
	}
	c.Daemon.SocketPath = expand(c.Daemon.SocketPath)
	c.Daemon.PIDFile = expand(c.Daemon.PIDFile)
	c.Audit.LogPath = expand(c.Audit.LogPath)
	c.Trace.Path = expand(c.Trace.Path)
}
