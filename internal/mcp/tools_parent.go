package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// parentTools are exposed only when the MCP was loaded with --role parent
// (spec §6.3, §7.3). The broker independently re-checks the role on every
// message, so this is UX scoping, not the security boundary.
func parentTools(bus *busClient) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool("bus_list_children",
				mcp.WithDescription("Returns a list of all connected child instances with their names, IDs, states, and last-seen time."),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return listClients(bus, "children")
			},
		},
		{
			Tool: mcp.NewTool("bus_send",
				mcp.WithDescription("Send a direct message to a named child instance."),
				mcp.WithString("target", mcp.Required(), mcp.Description("Child name or client_id")),
				mcp.WithString("message", mcp.Required(), mcp.Description("Message content")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				target, err := req.RequireString("target")
				if err != nil {
					return errResult(err)
				}
				message, err := req.RequireString("message")
				if err != nil {
					return errResult(err)
				}
				resp, err := bus.request(
					bus.newEnvelope(target, protocol.TypeDirectMessage, message), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				var receipt protocol.DeliveryReceiptPayload
				if err := resp.DecodePayload(&receipt); err != nil {
					return errResult(err)
				}
				return textResult(map[string]any{"delivered": true, "to": receipt.DeliveredTo})
			},
		},
		{
			Tool: mcp.NewTool("bus_broadcast",
				mcp.WithDescription("Send a message to all connected child instances simultaneously."),
				mcp.WithString("message", mcp.Required(), mcp.Description("Message to broadcast")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				message, err := req.RequireString("message")
				if err != nil {
					return errResult(err)
				}
				resp, err := bus.request(
					bus.newEnvelope(protocol.TargetBroadcast, protocol.TypeBroadcast, message), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				var receipt protocol.DeliveryReceiptPayload
				if err := resp.DecodePayload(&receipt); err != nil {
					return errResult(err)
				}
				return textResult(map[string]any{
					"children_reached": receipt.Count,
					"names":            receipt.DeliveredTo,
				})
			},
		},
		{
			Tool: mcp.NewTool("bus_assign_task",
				mcp.WithDescription("Assign a structured task to a specific child. The task is a JSON object "+
					"with at minimum a task_id and instruction field (a task_id is generated if omitted)."),
				mcp.WithString("target", mcp.Required(), mcp.Description("Child name or client_id")),
				mcp.WithString("task_json", mcp.Required(),
					mcp.Description("JSON string with task_id, instruction, context, deadline_hint")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				target, err := req.RequireString("target")
				if err != nil {
					return errResult(err)
				}
				taskJSON, err := req.RequireString("task_json")
				if err != nil {
					return errResult(err)
				}
				var task protocol.TaskPayload
				if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
					return errResult(fmt.Errorf("task_json is not valid JSON: %w", err))
				}
				if task.Instruction == "" {
					return errResult(fmt.Errorf("task_json must include a non-empty instruction field"))
				}
				if task.TaskID == "" {
					task.TaskID = "task-" + uuid.NewString()[:8]
				}
				task.AssignedAt = time.Now().UTC()

				env := bus.newEnvelope(target, protocol.TypeAssignTask, protocol.MustEncode(task))
				env.TaskID = task.TaskID
				resp, err := bus.request(env, requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				return textResult(map[string]any{
					"assigned": true,
					"task_id":  task.TaskID,
					"target":   target,
				})
			},
		},
		{
			Tool: mcp.NewTool("bus_get_status",
				mcp.WithDescription("Request the current status from a specific child. Synchronous: waits up to "+
					"the configured status timeout for the child's response."),
				mcp.WithString("target", mcp.Required(), mcp.Description("Child name or client_id")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				target, err := req.RequireString("target")
				if err != nil {
					return errResult(err)
				}
				// The broker enforces the status timeout and answers with
				// STATUS_TIMEOUT; wait slightly longer so its verdict wins.
				wait := time.Duration(bus.cfg.Timeouts.StatusRequestSeconds)*time.Second + 2*time.Second
				resp, err := bus.request(
					bus.newEnvelope(target, protocol.TypeStatusRequest, ""), wait)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				var status protocol.StatusPayload
				if err := resp.DecodePayload(&status); err != nil {
					return errResult(err)
				}
				return textResult(map[string]any{"target": target, "status": status})
			},
		},
		{
			Tool: mcp.NewTool("bus_shutdown",
				mcp.WithDescription("Initiate the graceful shutdown cascade. All children are notified and given "+
					"timeout_seconds to acknowledge before force-disconnect, then the daemon stops."),
				mcp.WithNumber("timeout_seconds",
					mcp.Description("Seconds to wait for child acknowledgments (default 5)")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				timeoutSecs := req.GetInt("timeout_seconds", 5)
				bus.shuttingDown.Store(true) // expected disconnect, no reconnect attempts
				env := bus.newEnvelope(protocol.TargetDaemon, protocol.TypeShutdownRequest,
					protocol.MustEncode(protocol.ShutdownRequestPayload{TimeoutSeconds: timeoutSecs}))
				// Cascade budget: child window + parent-ack phase + slack.
				resp, err := bus.request(env, time.Duration(timeoutSecs)*time.Second+8*time.Second)
				if err != nil {
					return errResult(fmt.Errorf("shutdown initiated but result not received: %w", err))
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				var result protocol.ShutdownResultPayload
				if err := resp.DecodePayload(&result); err != nil {
					return errResult(err)
				}
				// Phase 3 ACK so the daemon can finish teardown immediately.
				if err := bus.send(bus.newEnvelope(protocol.TargetDaemon, protocol.TypeShutdownAck, "")); err != nil {
					bus.log.Warn("could not ack shutdown result", "error", err)
				}
				return textResult(map[string]any{
					"daemon_stopping": true,
					"children_acked":  result.ChildrenAcked,
					"children_forced": result.ChildrenForced,
				})
			},
		},
		{
			Tool: mcp.NewTool("bus_schedule_task",
				mcp.WithDescription("Schedule a task for a child to run at a time or on a recurrence. "+
					"The schedule lives in the daemon and fires even if this parent session closes. "+
					"when_json sets exactly one of: {\"at\": \"<RFC3339>\"} (one-shot), "+
					"{\"every\": \"15m\"} (interval), {\"cron\": \"0 2 * * *\"} (cron)."),
				mcp.WithString("target", mcp.Required(), mcp.Description("Child name")),
				mcp.WithString("task_json", mcp.Required(),
					mcp.Description("JSON task template with instruction (and optional context, deadline_hint)")),
				mcp.WithString("when_json", mcp.Required(),
					mcp.Description("JSON trigger: one of at, every, cron")),
				mcp.WithString("misfire",
					mcp.Description("Policy when the child is offline at fire time: queue (default) or skip"),
					mcp.Enum("queue", "skip")),
				mcp.WithString("ttl",
					mcp.Description("Queue-policy expiry as a Go duration, e.g. \"1h\" (default: the interval, or 24h for one-shots)")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				target, err := req.RequireString("target")
				if err != nil {
					return errResult(err)
				}
				taskJSON, err := req.RequireString("task_json")
				if err != nil {
					return errResult(err)
				}
				whenJSON, err := req.RequireString("when_json")
				if err != nil {
					return errResult(err)
				}
				var task protocol.TaskPayload
				if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
					return errResult(fmt.Errorf("task_json is not valid JSON: %w", err))
				}
				var trigger protocol.ScheduleTrigger
				if err := json.Unmarshal([]byte(whenJSON), &trigger); err != nil {
					return errResult(fmt.Errorf("when_json is not valid JSON: %w", err))
				}
				payload := protocol.ScheduleCreatePayload{
					Target:  target,
					Task:    task,
					Trigger: trigger,
					Misfire: req.GetString("misfire", ""),
					TTL:     req.GetString("ttl", ""),
				}
				resp, err := bus.request(bus.newEnvelope(protocol.TargetDaemon,
					protocol.TypeScheduleCreate, protocol.MustEncode(payload)), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				var info protocol.ScheduleInfo
				if err := resp.DecodePayload(&info); err != nil {
					return errResult(err)
				}
				return textResult(info)
			},
		},
		{
			Tool: mcp.NewTool("bus_list_schedules",
				mcp.WithDescription("List all schedules with id, target, trigger, next fire time, and fire count."),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				resp, err := bus.request(bus.newEnvelope(protocol.TargetDaemon,
					protocol.TypeScheduleList, ""), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				var list protocol.ScheduleListPayload
				if err := resp.DecodePayload(&list); err != nil {
					return errResult(err)
				}
				return textResult(list.Schedules)
			},
		},
		{
			Tool: mcp.NewTool("bus_cancel_schedule",
				mcp.WithDescription("Cancel a schedule by its schedule_id."),
				mcp.WithString("schedule_id", mcp.Required(), mcp.Description("The schedule to cancel")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				id, err := req.RequireString("schedule_id")
				if err != nil {
					return errResult(err)
				}
				resp, err := bus.request(bus.newEnvelope(protocol.TargetDaemon,
					protocol.TypeScheduleCancel,
					protocol.MustEncode(protocol.ScheduleCancelPayload{ScheduleID: id})), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				return textResult(map[string]string{"canceled": id})
			},
		},
		{
			Tool: mcp.NewTool("bus_remove_child",
				mcp.WithDescription("Disconnect a specific child from the bus. The child receives a shutdown notice first."),
				mcp.WithString("target", mcp.Required(), mcp.Description("Child name or client_id")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				target, err := req.RequireString("target")
				if err != nil {
					return errResult(err)
				}
				env := bus.newEnvelope(protocol.TargetDaemon, protocol.TypeRemoveChild,
					protocol.MustEncode(protocol.RemoveChildPayload{Target: target}))
				resp, err := bus.request(env, requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				return textResult(map[string]any{"removed": target})
			},
		},
	}
}
