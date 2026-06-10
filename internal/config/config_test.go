package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestConfig_Defaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	if cfg.Daemon.SocketPath != "/tmp/daemonseed.sock" {
		t.Errorf("socket path default: %s", cfg.Daemon.SocketPath)
	}
	if cfg.Timeouts.HandshakeSeconds != 5 {
		t.Errorf("handshake timeout default: %d", cfg.Timeouts.HandshakeSeconds)
	}
	if cfg.Limits.MaxMessageBytes != 1048576 {
		t.Errorf("max message bytes default: %d", cfg.Limits.MaxMessageBytes)
	}
	if !cfg.Audit.Enabled {
		t.Error("audit must be enabled by default")
	}
}

func TestConfig_LoadValidFile(t *testing.T) {
	path := writeTemp(t, `
daemon:
  socket_path: /tmp/custom.sock
  auto_start: true
timeouts:
  handshake_seconds: 9
limits:
  max_clients: 5
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Daemon.SocketPath != "/tmp/custom.sock" {
		t.Errorf("socket path: %s", cfg.Daemon.SocketPath)
	}
	if !cfg.Daemon.AutoStart {
		t.Error("auto_start must be true")
	}
	if cfg.Timeouts.HandshakeSeconds != 9 {
		t.Errorf("handshake: %d", cfg.Timeouts.HandshakeSeconds)
	}
	if cfg.Limits.MaxClients != 5 {
		t.Errorf("max clients: %d", cfg.Limits.MaxClients)
	}
	// Untouched sections keep defaults.
	if cfg.Timeouts.StatusRequestSeconds != 10 {
		t.Errorf("status timeout must keep default: %d", cfg.Timeouts.StatusRequestSeconds)
	}
}

func TestConfig_MissingFileUsesDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("missing file must not error: %v", err)
	}
	if cfg.Daemon.SocketPath != "/tmp/daemonseed.sock" {
		t.Errorf("defaults not applied: %s", cfg.Daemon.SocketPath)
	}
}

func TestConfig_InvalidYAML(t *testing.T) {
	path := writeTemp(t, "daemon: [this is: not valid\n  yaml: {{")
	if _, err := Load(path); err == nil {
		t.Fatal("invalid YAML must be rejected")
	}
}

func TestConfig_UnknownFieldRejected(t *testing.T) {
	path := writeTemp(t, "daemon:\n  socket_pth: /tmp/typo.sock\n")
	if _, err := Load(path); err == nil {
		t.Fatal("unknown config key (typo) must be rejected")
	}
}

func TestConfig_NegativeTimeout(t *testing.T) {
	path := writeTemp(t, "timeouts:\n  handshake_seconds: -1\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("negative timeout must be rejected")
	}
	if !strings.Contains(err.Error(), "handshake_seconds") {
		t.Errorf("error must name the offending field: %v", err)
	}
}

func TestConfig_ZeroLimitRejected(t *testing.T) {
	path := writeTemp(t, "limits:\n  max_clients: 0\n")
	if _, err := Load(path); err == nil {
		t.Fatal("zero max_clients must be rejected")
	}
}

func TestConfig_EnvOverrides(t *testing.T) {
	t.Setenv("DAEMONSEED_DAEMON_SOCKET_PATH", "/var/run/ds.sock")
	t.Setenv("DAEMONSEED_TIMEOUTS_SHUTDOWN_ACK_SECONDS", "10")
	t.Setenv("DAEMONSEED_AUDIT_ENABLED", "false")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Daemon.SocketPath != "/var/run/ds.sock" {
		t.Errorf("env socket override: %s", cfg.Daemon.SocketPath)
	}
	if cfg.Timeouts.ShutdownAckSeconds != 10 {
		t.Errorf("env timeout override: %d", cfg.Timeouts.ShutdownAckSeconds)
	}
	if cfg.Audit.Enabled {
		t.Error("env audit override must disable audit")
	}
}

func TestConfig_EnvOverridesBeatFile(t *testing.T) {
	path := writeTemp(t, "daemon:\n  socket_path: /tmp/from-file.sock\n")
	t.Setenv("DAEMONSEED_DAEMON_SOCKET_PATH", "/tmp/from-env.sock")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Daemon.SocketPath != "/tmp/from-env.sock" {
		t.Errorf("env must override file: %s", cfg.Daemon.SocketPath)
	}
}

func TestConfig_InvalidEnvValue(t *testing.T) {
	t.Setenv("DAEMONSEED_LIMITS_MAX_CLIENTS", "lots")
	if _, err := Load(""); err == nil {
		t.Fatal("non-integer env value must be rejected")
	}
}

func TestConfig_TildeExpansion(t *testing.T) {
	path := writeTemp(t, "audit:\n  log_path: ~/audit-test.jsonl\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if strings.HasPrefix(cfg.Audit.LogPath, "~") {
		t.Errorf("~ must be expanded: %s", cfg.Audit.LogPath)
	}
	home, _ := os.UserHomeDir()
	if cfg.Audit.LogPath != filepath.Join(home, "audit-test.jsonl") {
		t.Errorf("unexpected expansion: %s", cfg.Audit.LogPath)
	}
}

func TestConfig_TestdataFixtures(t *testing.T) {
	// The shared fixtures live at the repo root per spec §4.
	valid := filepath.Join("..", "..", "testdata", "configs", "valid_config.yaml")
	if _, err := Load(valid); err != nil {
		t.Errorf("valid fixture must load: %v", err)
	}
	invalid := filepath.Join("..", "..", "testdata", "configs", "invalid_config.yaml")
	if _, err := Load(invalid); err == nil {
		t.Error("invalid fixture must be rejected")
	}
}
