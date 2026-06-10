package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/arterberry/daemonseed/internal/config"
	"github.com/arterberry/daemonseed/internal/protocol"
)

// newInboxCmd implements §20.7: `daemonseed inbox --drain --name api`.
// Designed to run as a Claude Code UserPromptSubmit hook in a child repo —
// stdout is injected into the session as context. Empty inbox → empty
// output, exit 0.
func newInboxCmd() *cobra.Command {
	var (
		name  string
		drain bool
	)
	cmd := &cobra.Command{
		Use:   "inbox",
		Short: "Show (and with --drain, clear) a child's pending bus messages and tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			resp, err := drainInbox(cfg, name, !drain)
			if err != nil {
				// A hook must not break the session when the daemon is down:
				// report on stderr, succeed with empty stdout.
				fmt.Fprintf(os.Stderr, "daemonseed inbox: %v\n", err)
				return nil
			}
			printInbox(os.Stdout, os.Stderr, resp, cfg.Commands.AllowFromParent)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "child name whose inbox to read (required)")
	cmd.Flags().BoolVar(&drain, "drain", false, "clear returned messages (default: peek only)")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

// drainInbox connects as a one-shot observer and performs the drain request.
func drainInbox(cfg *config.Config, name string, peek bool) (*protocol.InboxDrainResponsePayload, error) {
	conn, err := net.DialTimeout("unix", cfg.Daemon.SocketPath, time.Second)
	if err != nil {
		return nil, fmt.Errorf("%w (socket %s)", protocol.ErrDaemonNotRunning, cfg.Daemon.SocketPath)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return nil, err
	}

	hello := protocol.NewEnvelope("pending", protocol.TargetDaemon, protocol.TypeHello,
		protocol.MustEncode(protocol.HelloPayload{
			Role: "observer", Name: fmt.Sprintf("inbox-hook-%d", os.Getpid()), Version: Version,
		}))
	if err := protocol.WriteMessage(conn, hello, 0); err != nil {
		return nil, err
	}
	ack, err := protocol.ReadMessage(conn, 0)
	if err != nil {
		return nil, err
	}
	if ack.Type != protocol.TypeHelloAck {
		return nil, fmt.Errorf("handshake rejected: %s", ack.Payload)
	}
	var ackPayload protocol.HelloAckPayload
	if err := ack.DecodePayload(&ackPayload); err != nil {
		return nil, err
	}

	req := protocol.NewEnvelope(ackPayload.ClientID, protocol.TargetDaemon,
		protocol.TypeInboxDrainRequest,
		protocol.MustEncode(map[string]any{"name": name, "peek": peek}))
	if err := protocol.WriteMessage(conn, req, 0); err != nil {
		return nil, err
	}
	for {
		env, err := protocol.ReadMessage(conn, 0)
		if err != nil {
			return nil, err
		}
		if env.CorrelationID != req.ID {
			continue // snapshot EVENT etc.
		}
		if env.Type != protocol.TypeInboxDrainResponse {
			var p protocol.ErrorPayload
			_ = env.DecodePayload(&p)
			return nil, fmt.Errorf("%s: %s", env.Type, p.Reason)
		}
		var resp protocol.InboxDrainResponsePayload
		if err := env.DecodePayload(&resp); err != nil {
			return nil, err
		}
		return &resp, nil
	}
}

// printInbox renders drain results in the hook-friendly format. Slash
// commands from the parent pass through only when allowlisted in
// commands.allow_from_parent (§20.7: default deny-all); refusals are
// annotated on stdout and logged on stderr.
func printInbox(out, errOut *os.File, resp *protocol.InboxDrainResponsePayload, allowlist []string) {
	allowed := func(payload string) bool {
		command, _, _ := strings.Cut(strings.TrimSpace(payload), " ")
		for _, a := range allowlist {
			if command == a {
				return true
			}
		}
		return false
	}
	for _, m := range resp.Messages {
		text := strings.TrimSpace(m.Payload)
		from := m.From
		if from == "" {
			from = "parent"
		}
		switch {
		case strings.HasPrefix(text, "/") && allowed(text):
			fmt.Fprintf(out, "[daemonSeed] %s requests command: %s\n", from, text)
		case strings.HasPrefix(text, "/"):
			fmt.Fprintf(out, "[daemonSeed] blocked non-allowlisted command from %s: %s "+
				"(add it to commands.allow_from_parent to permit)\n", from, text)
			fmt.Fprintf(errOut, "daemonseed inbox: refused command %q from %s (not in allow_from_parent)\n",
				text, from)
		default:
			fmt.Fprintf(out, "[daemonSeed] message from %s (%s): %s\n", from, m.Type, text)
		}
	}
	for _, t := range resp.PendingTasks {
		fmt.Fprintf(out, "[daemonSeed] pending task %s (call bus_get_assignment / bus_acknowledge_task): %s\n",
			t.TaskID, t.Instruction)
	}
}

// newInstallHooksCmd writes the §20.7 UserPromptSubmit hook into a child
// repo's .claude/settings.json, merging with any existing hooks.
func newInstallHooksCmd() *cobra.Command {
	var (
		repoPath string
		name     string
	)
	cmd := &cobra.Command{
		Use:   "install-hooks",
		Short: "Install the inbox-drain hook into a child repo's .claude/settings.json",
		RunE: func(cmd *cobra.Command, args []string) error {
			settingsDir := filepath.Join(repoPath, ".claude")
			if err := os.MkdirAll(settingsDir, 0o755); err != nil {
				return fmt.Errorf("create %s: %w", settingsDir, err)
			}
			settingsPath := filepath.Join(settingsDir, "settings.json")

			settings := map[string]any{}
			if data, err := os.ReadFile(settingsPath); err == nil {
				if err := json.Unmarshal(data, &settings); err != nil {
					return fmt.Errorf("%s exists but is not valid JSON: %w", settingsPath, err)
				}
			} else if !os.IsNotExist(err) {
				return err
			}

			hookCommand := fmt.Sprintf("daemonseed inbox --drain --name %s", name)
			if installHook(settings, "UserPromptSubmit", hookCommand) {
				data, err := json.MarshalIndent(settings, "", "  ")
				if err != nil {
					return err
				}
				if err := os.WriteFile(settingsPath, append(data, '\n'), 0o644); err != nil {
					return fmt.Errorf("write %s: %w", settingsPath, err)
				}
				fmt.Printf("installed UserPromptSubmit hook in %s:\n  %s\n", settingsPath, hookCommand)
			} else {
				fmt.Printf("hook already present in %s; nothing to do\n", settingsPath)
			}
			fmt.Println("\nPending parent messages will now surface in this repo's Claude Code session.")
			fmt.Println("Allowlist slash commands in ~/.config/daemonseed/config.yaml:")
			fmt.Println("  commands:")
			fmt.Println("    allow_from_parent: [\"/bus-report\"]")
			return nil
		},
	}
	cmd.Flags().StringVar(&repoPath, "repo-path", ".", "target repo path (default: current directory)")
	cmd.Flags().StringVar(&name, "name", "", "this child's bus name (required)")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

// installHook merges a command hook into settings["hooks"][event], returning
// false if an identical command is already registered.
func installHook(settings map[string]any, event, command string) bool {
	hooks, _ := settings["hooks"].(map[string]any)
	if hooks == nil {
		hooks = map[string]any{}
		settings["hooks"] = hooks
	}
	entries, _ := hooks[event].([]any)
	for _, e := range entries {
		entry, _ := e.(map[string]any)
		inner, _ := entry["hooks"].([]any)
		for _, h := range inner {
			hook, _ := h.(map[string]any)
			if hook["command"] == command {
				return false
			}
		}
	}
	hooks[event] = append(entries, map[string]any{
		"hooks": []any{
			map[string]any{"type": "command", "command": command},
		},
	})
	return true
}
