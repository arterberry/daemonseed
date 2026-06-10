package mcp

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"context"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/arterberry/daemonseed/internal/config"
	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
	"github.com/arterberry/daemonseed/internal/trace"
)

// requestTimeout bounds ordinary request/response exchanges with the daemon.
// Status and shutdown tools compute their own, longer deadlines.
const requestTimeout = 10 * time.Second

// Options configures a daemonSeed MCP server instance.
type Options struct {
	Config    *config.Config
	Role      roles.Role
	Name      string
	Version   string
	AutoStart bool
	Log       *slog.Logger
}

// Run connects to the daemon and serves MCP over stdio until Claude Code
// closes the stream. It blocks for the lifetime of the MCP session.
func Run(opts Options) error {
	if opts.Role != roles.RoleParent && opts.Role != roles.RoleChild {
		return fmt.Errorf("%w: got %q", protocol.ErrInvalidRole, opts.Role)
	}
	if !roles.ValidName(opts.Name) {
		return fmt.Errorf("%w: got %q", protocol.ErrInvalidName, opts.Name)
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}

	bus := newBusClient(opts.Config, opts.Log, opts.Role, opts.Name, opts.Version, opts.AutoStart)
	if err := bus.Connect(); err != nil {
		return err
	}
	defer bus.Close()

	// §20.10 session trace: every tool invocation from this MCP instance is
	// recorded with duration and truncated args. A trace-store failure is a
	// warning, never fatal — the bus still works without it.
	var tracer *trace.Tracer
	if opts.Config.Trace.Enabled {
		store, err := trace.OpenStore(opts.Config.Trace.Backend, opts.Config.Trace.Path,
			opts.Config.Trace.MaxSizeMB)
		if err != nil {
			opts.Log.Warn("trace store unavailable; continuing without tracing", "error", err)
		} else {
			tracer = trace.New(store, "mcp:"+opts.Name, opts.Config.Trace.MaxDetailChars)
			defer tracer.Close()
		}
	}

	s := newServer(bus, opts, tracer)
	return server.ServeStdio(s)
}

// newServer builds the MCP server with the tool surface for the role,
// wrapping every handler in trace middleware when a tracer is present.
func newServer(bus *busClient, opts Options, tracer *trace.Tracer) *server.MCPServer {
	s := server.NewMCPServer("daemonseed", opts.Version,
		server.WithToolCapabilities(false),
		server.WithInstructions(fmt.Sprintf(
			"daemonSeed message bus. This instance is %q with role %q. "+
				"Use the bus_* tools to coordinate with other Claude Code instances.",
			opts.Name, opts.Role)),
	)
	tools := toolsForRole(bus, opts.Role)
	for i := range tools {
		tools[i] = traced(tracer, opts.Name, string(opts.Role), tools[i])
	}
	s.AddTools(tools...)
	return s
}

// traced wraps a tool handler so each invocation emits one KindTool event:
// tool name, duration, ok/error status, and a truncated argument snippet.
// Nil tracer → the original handler is returned unchanged.
func traced(tracer *trace.Tracer, session, role string, st server.ServerTool) server.ServerTool {
	if tracer == nil {
		return st
	}
	inner := st.Handler
	st.Handler = func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		start := time.Now()
		res, err := inner(ctx, req)

		status := trace.StatusOK
		detail := ""
		if args := req.GetArguments(); len(args) > 0 {
			if data, jsonErr := json.Marshal(args); jsonErr == nil {
				detail = string(data)
			}
		}
		switch {
		case err != nil:
			status = trace.StatusError
			detail = err.Error()
		case res != nil && res.IsError:
			status = trace.StatusError
			if len(res.Content) > 0 {
				if tc, ok := res.Content[0].(mcp.TextContent); ok {
					detail = tc.Text
				}
			}
		}
		tracer.Emit(trace.Event{
			Kind: trace.KindTool, Name: st.Tool.Name,
			SpanID:  "tool-" + uuid.NewString()[:8],
			Session: session, Role: role,
			DurationMs: float64(time.Since(start).Microseconds()) / 1000.0,
			Status:     status, Detail: detail,
		})
		return res, err
	}
	return st
}

// toolsForRole returns the exact tool surface for a role (spec §6.3).
func toolsForRole(bus *busClient, role roles.Role) []server.ServerTool {
	tools := commonTools(bus)
	switch role {
	case roles.RoleParent:
		tools = append(tools, parentTools(bus)...)
	case roles.RoleChild:
		tools = append(tools, childTools(bus)...)
	}
	return tools
}

// busError converts a daemon error response into a Go error, or nil if the
// response is not an error type.
func busError(resp *protocol.Envelope) error {
	switch resp.Type {
	case protocol.TypeInvalidMessage, protocol.TypePermissionDenied,
		protocol.TypeMessageTooLarge, protocol.TypeNotFound,
		protocol.TypeInternalError, protocol.TypeDeliveryFailed,
		protocol.TypeStatusTimeout:
		var p protocol.ErrorPayload
		_ = resp.DecodePayload(&p)
		return fmt.Errorf("%s: %s", resp.Type, p.Reason)
	default:
		return nil
	}
}

// textResult marshals v as indented JSON for a tool result.
func textResult(v any) (*mcp.CallToolResult, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("encode result: %v", err)), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

// errResult formats an error as a tool error result (returned to the model,
// not as a protocol failure).
func errResult(err error) (*mcp.CallToolResult, error) {
	return mcp.NewToolResultError(err.Error()), nil
}
