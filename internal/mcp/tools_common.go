package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// commonTools are available to every role (spec §6.3): bus_ping, bus_whoami,
// bus_list_all, plus bus_check_messages (spec extension: the spec gives
// children no way to read broadcasts/direct messages pushed to them, so the
// MCP buffers inbound messages and exposes them here).
func commonTools(bus *busClient) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool("bus_ping",
				mcp.WithDescription("Ping the daemonSeed daemon. Returns roundtrip time in milliseconds."),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				start := time.Now()
				resp, err := bus.request(
					bus.newEnvelope(protocol.TargetDaemon, protocol.TypePing, ""), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				return textResult(map[string]any{
					"roundtrip_ms": float64(time.Since(start).Microseconds()) / 1000.0,
				})
			},
		},
		{
			Tool: mcp.NewTool("bus_whoami",
				mcp.WithDescription("Returns this instance's client_id, name, role, connection time, and daemon version."),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				resp, err := bus.request(
					bus.newEnvelope(protocol.TargetDaemon, protocol.TypeWhoAmIRequest, ""), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				var who protocol.WhoAmIResponsePayload
				if err := resp.DecodePayload(&who); err != nil {
					return errResult(err)
				}
				return textResult(who)
			},
		},
		{
			Tool: mcp.NewTool("bus_list_all",
				mcp.WithDescription("Returns all connected clients (parent + children) with roles, names, states, and last-seen times."),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return listClients(bus, "all")
			},
		},
		{
			Tool: mcp.NewTool("bus_check_messages",
				mcp.WithDescription("Returns and clears messages received from the bus since the last check "+
					"(broadcasts, direct messages, task pushes, status reports). Call this to see what other instances sent you."),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				msgs := bus.drainInbox()
				if len(msgs) == 0 {
					return mcp.NewToolResultText(`{"messages": [], "note": "no new messages"}`), nil
				}
				out := make([]map[string]any, 0, len(msgs))
				for _, m := range msgs {
					entry := map[string]any{
						"from":        m.From,
						"type":        string(m.Type),
						"payload":     m.Payload,
						"received_at": m.ReceivedAt.Format(time.RFC3339),
					}
					if m.TaskID != "" {
						entry["task_id"] = m.TaskID
					}
					out = append(out, entry)
				}
				return textResult(map[string]any{"messages": out})
			},
		},
	}
}

// listClients implements bus_list_all and bus_list_children.
func listClients(bus *busClient, filter string) (*mcp.CallToolResult, error) {
	env := bus.newEnvelope(protocol.TargetDaemon, protocol.TypeListRequest,
		protocol.MustEncode(protocol.ListRequestPayload{Filter: filter}))
	resp, err := bus.request(env, requestTimeout)
	if err != nil {
		return errResult(err)
	}
	if err := busError(resp); err != nil {
		return errResult(err)
	}
	var list protocol.ListResponsePayload
	if err := resp.DecodePayload(&list); err != nil {
		return errResult(fmt.Errorf("decode list response: %w", err))
	}
	return textResult(list.Clients)
}
