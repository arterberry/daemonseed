// Command daemonseed is the single entry point for the daemonSeed local
// message bus: daemon, MCP server, TUI, and management subcommands (spec §18).
package main

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/arterberry/daemonseed/internal/audit"
	"github.com/arterberry/daemonseed/internal/broker"
	"github.com/arterberry/daemonseed/internal/config"
	"github.com/arterberry/daemonseed/internal/mcp"
	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
	"github.com/arterberry/daemonseed/internal/trace"
	"github.com/arterberry/daemonseed/internal/tui"
)

// Set at build time via -ldflags (see Makefile).
var (
	Version = "1.0.0-dev"
	Commit  = "unknown"
)

// Global flag values (persistent across subcommands).
var (
	flagConfig    string
	flagSocket    string
	flagLogLevel  string
	flagLogFormat string
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "daemonseed",
		Short:         "Local message bus for orchestrating Claude Code instances",
		Long:          "daemonSeed is a local-only message broker that lets a Parent Claude Code\ninstance coordinate Child instances across repos and terminals.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&flagConfig, "config", "", "config file path (default: ~/.config/daemonseed/config.yaml)")
	root.PersistentFlags().StringVar(&flagSocket, "socket", "", "socket path override")
	root.PersistentFlags().StringVar(&flagLogLevel, "log-level", "info", "log level: debug, info, warn, error")
	root.PersistentFlags().StringVar(&flagLogFormat, "log-format", "json", "log format: json, text")

	root.AddCommand(
		newStartCmd(),
		newStopCmd(),
		newStatusCmd(),
		newMCPCmd(),
		newTUICmd(),
		newInstallCommandsCmd(),
		newInstallHooksCmd(),
		newInboxCmd(),
		newLogsCmd(),
		newTraceCmd(),
		newVersionCmd(),
	)
	return root
}

// loadConfig builds the effective config: file → env → flag overrides.
func loadConfig() (*config.Config, error) {
	path := flagConfig
	if path == "" {
		path = config.DefaultPath()
	}
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	if flagSocket != "" {
		cfg.Daemon.SocketPath = flagSocket
	}
	return cfg, nil
}

// newLogger emits structured logs to stderr (spec §14.2). Stdout is reserved
// for command output — and for JSON-RPC in `daemonseed mcp`.
func newLogger() *slog.Logger {
	var level slog.Level
	if env := os.Getenv("DAEMONSEED_LOG_LEVEL"); env != "" && flagLogLevel == "info" {
		flagLogLevel = env
	}
	switch strings.ToLower(flagLogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	if strings.ToLower(flagLogFormat) == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

// ---------------------------------------------------------------------------
// start
// ---------------------------------------------------------------------------

func newStartCmd() *cobra.Command {
	var (
		withTUI    bool
		background bool
		pidFile    string
	)
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the daemon (foreground or background)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			if pidFile != "" {
				cfg.Daemon.PIDFile = pidFile
			}
			if background {
				return startBackground(cfg)
			}
			return runDaemon(cfg, withTUI)
		},
	}
	cmd.Flags().BoolVar(&withTUI, "tui", false, "launch with TUI dashboard")
	cmd.Flags().BoolVar(&background, "background", false, "run as background process (default: foreground)")
	cmd.Flags().StringVar(&pidFile, "pidfile", "", "PID file path override")
	return cmd
}

func runDaemon(cfg *config.Config, withTUI bool) error {
	log := newLogger()

	if err := checkAndCleanPIDFile(cfg.Daemon.PIDFile, log); err != nil {
		return err
	}

	// Audit log: a failure to open it downgrades to a warning — the daemon
	// still starts, with auditing disabled (spec §17.5 TestAuditLog_FailedWrite).
	var auditLogger *audit.Logger
	if cfg.Audit.Enabled {
		auditLogger, _ = func() (*audit.Logger, error) {
			l, err := audit.New(audit.Options{
				Path:          cfg.Audit.LogPath,
				MaxSizeMB:     cfg.Audit.MaxSizeMB,
				RotateOnStart: cfg.Audit.RotateOnStart,
				LogPayloads:   cfg.Audit.LogPayloads,
			})
			if err != nil {
				log.Warn("audit log unavailable; continuing with audit disabled", "error", err)
				return nil, err
			}
			return l, nil
		}()
	}

	b := broker.New(cfg, log, auditLogger, Version)

	// Session tracing (§20.10): same downgrade-to-warning policy as audit.
	var tracer *trace.Tracer
	if cfg.Trace.Enabled {
		store, err := trace.OpenStore(cfg.Trace.Backend, cfg.Trace.Path, cfg.Trace.MaxSizeMB)
		if err != nil {
			log.Warn("trace store unavailable; continuing without tracing", "error", err)
		} else {
			tracer = trace.New(store, "daemon", cfg.Trace.MaxDetailChars)
			b.SetTracer(tracer)
		}
	}

	if err := b.Start(); err != nil {
		if tracer != nil {
			tracer.Close()
		}
		return err
	}
	if err := writePIDFile(cfg.Daemon.PIDFile); err != nil {
		b.Shutdown("startup failed", time.Second, "daemonseed", "")
		return err
	}
	fmt.Printf("daemonSeed started. Socket: %s PID: %d\n", cfg.Daemon.SocketPath, os.Getpid())

	// SIGTERM → standard cascade; SIGINT → cascade with a shorter timeout
	// (spec §11.1).
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		timeout := time.Duration(cfg.Timeouts.ShutdownAckSeconds) * time.Second
		if sig == syscall.SIGINT {
			timeout = timeout / 2
			if timeout < time.Second {
				timeout = time.Second
			}
		}
		b.Shutdown("signal received", timeout, sig.String(), "")
	}()

	if withTUI {
		events, unsub := b.Events()
		// Seed the dashboard with the current state, then stream.
		seeded := make(chan protocol.EventPayload, 257)
		seeded <- b.Snapshot()
		go func() {
			defer close(seeded)
			for ev := range events {
				seeded <- ev
			}
		}()
		err := tui.Run(tui.Options{
			Version:         Version,
			SocketPath:      cfg.Daemon.SocketPath,
			TimestampFormat: cfg.TUI.TimestampFormat,
			FeedMaxLines:    cfg.TUI.FeedMaxLines,
			Events:          seeded,
			OnQuit:          unsub,
		})
		if err != nil {
			log.Warn("TUI exited with error; daemon continues", "error", err)
		} else {
			fmt.Println("TUI closed. Daemon continues in foreground; Ctrl+C to stop.")
		}
	}

	b.Wait()
	removePIDFile(cfg.Daemon.PIDFile, log)
	if auditLogger != nil {
		if err := auditLogger.Close(); err != nil {
			log.Warn("audit log close", "error", err)
		}
	}
	if tracer != nil {
		if dropped := tracer.Dropped(); dropped > 0 {
			log.Warn("trace events lost to backpressure", "count", dropped)
		}
		if err := tracer.Close(); err != nil {
			log.Warn("trace store close", "error", err)
		}
	}
	fmt.Println("daemonSeed stopped cleanly.")
	return nil
}

// startBackground re-execs `daemonseed start` detached from the terminal,
// logging to ~/.local/share/daemonseed/daemon.log.
func startBackground(cfg *config.Config) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate own binary: %w", err)
	}
	logDir := filepath.Dir(cfg.Audit.LogPath)
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		logDir = os.TempDir()
	}
	logPath := filepath.Join(logDir, "daemon.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open daemon log %s: %w", logPath, err)
	}
	defer logFile.Close()

	args := []string{"start", "--socket", cfg.Daemon.SocketPath, "--pidfile", cfg.Daemon.PIDFile,
		"--log-level", flagLogLevel, "--log-format", flagLogFormat}
	if flagConfig != "" {
		args = append(args, "--config", flagConfig)
	}
	child := exec.Command(exe, args...)
	child.Stdout = logFile
	child.Stderr = logFile
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := child.Start(); err != nil {
		return fmt.Errorf("spawn background daemon: %w", err)
	}
	go func() { _ = child.Wait() }() // reap if it exits before we do

	// Wait for the socket so failures surface here, not silently in the log.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("unix", cfg.Daemon.SocketPath, 200*time.Millisecond); err == nil {
			conn.Close()
			fmt.Printf("daemonSeed started in background. Socket: %s PID: %d Log: %s\n",
				cfg.Daemon.SocketPath, child.Process.Pid, logPath)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("daemon did not come up within 3s; check %s", logPath)
}

// ---------------------------------------------------------------------------
// PID file handling (spec §15.5)
// ---------------------------------------------------------------------------

func readPIDFile(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("malformed PID file %s: %w", path, err)
	}
	return pid, nil
}

func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

// checkAndCleanPIDFile errors if a live daemon owns the PID file and removes
// it if the recorded process is gone (crash recovery).
func checkAndCleanPIDFile(path string, log *slog.Logger) error {
	pid, err := readPIDFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		log.Warn("removing unreadable PID file", "path", path, "error", err)
		return os.Remove(path)
	}
	if processAlive(pid) {
		return fmt.Errorf("daemonSeed is already running (pid %d). Run 'daemonseed stop' first", pid)
	}
	log.Warn("removing stale PID file from crashed daemon", "path", path, "pid", pid)
	return os.Remove(path)
}

func writePIDFile(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create PID file dir: %w", err)
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		return fmt.Errorf("write PID file: %w", err)
	}
	return nil
}

func removePIDFile(path string, log *slog.Logger) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Warn("could not remove PID file", "path", path, "error", err)
	}
}

// ---------------------------------------------------------------------------
// stop / status
// ---------------------------------------------------------------------------

func newStopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop the running daemon gracefully",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			pid, err := readPIDFile(cfg.Daemon.PIDFile)
			if err != nil {
				if os.IsNotExist(err) {
					return fmt.Errorf("%w (no PID file at %s)", protocol.ErrDaemonNotRunning, cfg.Daemon.PIDFile)
				}
				return err
			}
			if !processAlive(pid) {
				removePIDFile(cfg.Daemon.PIDFile, newLogger())
				return fmt.Errorf("%w (stale PID file removed)", protocol.ErrDaemonNotRunning)
			}
			if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
				return fmt.Errorf("signal daemon (pid %d): %w", pid, err)
			}
			fmt.Printf("sent SIGTERM to daemon (pid %d); waiting for shutdown cascade...\n", pid)
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				if !processAlive(pid) {
					fmt.Println("daemonSeed stopped.")
					return nil
				}
				time.Sleep(150 * time.Millisecond)
			}
			return fmt.Errorf("daemon (pid %d) did not stop within 30s", pid)
		},
	}
}

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show daemon status (running, socket path, connected clients)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			snapshot, err := fetchSnapshot(cfg.Daemon.SocketPath)
			if err != nil {
				fmt.Printf("daemonSeed: not running (socket %s)\n", cfg.Daemon.SocketPath)
				return nil
			}
			fmt.Printf("daemonSeed: running\n")
			fmt.Printf("  socket:   %s\n", cfg.Daemon.SocketPath)
			if pid, err := readPIDFile(cfg.Daemon.PIDFile); err == nil {
				fmt.Printf("  pid:      %d\n", pid)
			}
			fmt.Printf("  uptime:   %s\n", time.Since(snapshot.StartedAt).Round(time.Second))
			fmt.Printf("  messages: %d\n", snapshot.MsgCount)
			fmt.Printf("  clients:  %d\n", len(snapshot.Clients))
			for _, c := range snapshot.Clients {
				task := ""
				if c.CurrentTask != "" {
					task = "  task=" + c.CurrentTask
				}
				fmt.Printf("    %-8s %-20s state=%-8s id=%.8s%s\n", c.Role, c.Name, orDash(c.State), c.ClientID, task)
			}
			fmt.Printf("  schedules: %d\n", len(snapshot.Schedules))
			for _, s := range snapshot.Schedules {
				trigger := s.Trigger.Cron
				if s.Trigger.Every != "" {
					trigger = "every " + s.Trigger.Every
				} else if s.Trigger.At != "" {
					trigger = "at " + s.Trigger.At
				}
				fmt.Printf("    %-14s → %-16s %-20s next=%s fires=%d\n",
					s.ID, s.Target, trigger, s.NextFireAt.Local().Format("2006-01-02 15:04:05"), s.FireCount)
			}
			return nil
		},
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// fetchSnapshot connects as a one-shot observer and returns the snapshot
// event the daemon sends on attach.
func fetchSnapshot(socketPath string) (*protocol.EventPayload, error) {
	conn, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	hello := protocol.NewEnvelope("pending", protocol.TargetDaemon, protocol.TypeHello,
		protocol.MustEncode(protocol.HelloPayload{
			Role: "observer", Name: fmt.Sprintf("status-%d", os.Getpid()), Version: Version,
		}))
	if err := protocol.WriteMessage(conn, hello, 0); err != nil {
		return nil, err
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return nil, err
	}
	for {
		env, err := protocol.ReadMessage(conn, 0)
		if err != nil {
			return nil, err
		}
		switch env.Type {
		case protocol.TypeHelloReject:
			var rej protocol.HelloRejectPayload
			_ = env.DecodePayload(&rej)
			return nil, fmt.Errorf("daemon rejected status probe: %s", rej.Reason)
		case protocol.TypeEvent:
			var ev protocol.EventPayload
			if err := env.DecodePayload(&ev); err != nil {
				return nil, err
			}
			if ev.Kind == "snapshot" {
				return &ev, nil
			}
		}
	}
}

// ---------------------------------------------------------------------------
// mcp / tui
// ---------------------------------------------------------------------------

func newMCPCmd() *cobra.Command {
	var (
		role      string
		name      string
		autoStart bool
	)
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Start MCP server subprocess (launched by Claude Code)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			return mcp.Run(mcp.Options{
				Config:    cfg,
				Role:      roles.Role(role),
				Name:      name,
				Version:   Version,
				AutoStart: autoStart || cfg.Daemon.AutoStart,
				Log:       newLogger(), // stderr; stdout belongs to JSON-RPC
			})
		},
	}
	cmd.Flags().StringVar(&role, "role", "", "role: parent or child (required)")
	cmd.Flags().StringVar(&name, "name", "", "instance name (required)")
	cmd.Flags().BoolVar(&autoStart, "auto-start", false, "start daemon if not running")
	_ = cmd.MarkFlagRequired("role")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newTUICmd() *cobra.Command {
	return &cobra.Command{
		Use:   "tui",
		Short: "Attach TUI to running daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			return tui.RunAttached(cfg.Daemon.SocketPath, Version,
				cfg.TUI.TimestampFormat, cfg.TUI.FeedMaxLines)
		},
	}
}

// ---------------------------------------------------------------------------
// install-commands
// ---------------------------------------------------------------------------

func newInstallCommandsCmd() *cobra.Command {
	var (
		repoPath string
		role     string
		force    bool
	)
	cmd := &cobra.Command{
		Use:   "install-commands",
		Short: "Install Claude Code slash commands into a repo",
		RunE: func(cmd *cobra.Command, args []string) error {
			r := roles.Role(role)
			if r != roles.RoleParent && r != roles.RoleChild {
				return fmt.Errorf("%w: --role must be parent or child", protocol.ErrInvalidRole)
			}
			dir := filepath.Join(repoPath, ".claude", "commands")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("create %s: %w", dir, err)
			}
			installed, skipped := 0, 0
			for name, content := range slashCommands(r) {
				path := filepath.Join(dir, name)
				if _, err := os.Stat(path); err == nil && !force {
					fmt.Printf("  skip %s (exists; use --force to overwrite)\n", path)
					skipped++
					continue
				}
				if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
					return fmt.Errorf("write %s: %w", path, err)
				}
				fmt.Printf("  installed %s\n", path)
				installed++
			}
			fmt.Printf("done: %d installed, %d skipped (role: %s)\n", installed, skipped, role)
			return nil
		},
	}
	cmd.Flags().StringVar(&repoPath, "repo-path", ".", "target repo path (default: current directory)")
	cmd.Flags().StringVar(&role, "role", "", "role for these commands: parent or child")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing command files")
	_ = cmd.MarkFlagRequired("role")
	return cmd
}

// ---------------------------------------------------------------------------
// logs
// ---------------------------------------------------------------------------

func newLogsCmd() *cobra.Command {
	var (
		follow bool
		lines  int
	)
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Tail or show the audit log",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			path := cfg.Audit.LogPath
			f, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open audit log %s: %w", path, err)
			}
			defer f.Close()

			if err := printLastLines(f, lines); err != nil {
				return err
			}
			if !follow {
				return nil
			}
			// Follow mode: poll for appended lines until interrupted.
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			reader := bufio.NewReader(f)
			for {
				select {
				case <-sigCh:
					return nil
				default:
				}
				line, err := reader.ReadString('\n')
				if len(line) > 0 {
					fmt.Print(line)
				}
				if err == io.EOF {
					time.Sleep(250 * time.Millisecond)
					continue
				}
				if err != nil {
					return err
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "follow the log as it grows")
	cmd.Flags().IntVarP(&lines, "lines", "n", 50, "number of trailing lines to show")
	return cmd
}

func printLastLines(f *os.File, n int) error {
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	ring := make([]string, 0, n)
	for scanner.Scan() {
		if len(ring) == n {
			ring = ring[1:]
		}
		ring = append(ring, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	for _, line := range ring {
		fmt.Println(line)
	}
	return nil
}

// ---------------------------------------------------------------------------
// version
// ---------------------------------------------------------------------------

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("daemonseed %s (commit %s)\n", Version, Commit)
		},
	}
}
