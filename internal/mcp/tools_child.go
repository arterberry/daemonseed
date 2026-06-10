package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// childTools are exposed only when the MCP was loaded with --role child
// (spec §6.3, §7.3).
func childTools(bus *busClient) []server.ServerTool {
	return []server.ServerTool{
		{
			Tool: mcp.NewTool("bus_report_status",
				mcp.WithDescription("Push a status message up to the parent. Also caches the status locally "+
					"so the daemon can answer the parent's synchronous status requests."),
				mcp.WithString("message", mcp.Required(), mcp.Description("Status message")),
				mcp.WithString("state", mcp.Required(),
					mcp.Description("Current state"),
					mcp.Enum(protocol.ValidStates...)),
				mcp.WithString("current_task", mcp.Description("Task id currently being worked on, if any")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				message, err := req.RequireString("message")
				if err != nil {
					return errResult(err)
				}
				state, err := req.RequireString("state")
				if err != nil {
					return errResult(err)
				}
				if !protocol.IsValidState(state) {
					return errResult(fmt.Errorf("state must be one of %v", protocol.ValidStates))
				}
				status := protocol.StatusPayload{
					State:       state,
					Message:     message,
					CurrentTask: req.GetString("current_task", ""),
					ReportedAt:  time.Now().UTC(),
				}
				// Cache first: even if the parent is offline, future
				// STATUS_REQUESTs get answered with this.
				bus.setLastStatus(status)

				resp, err := bus.request(
					bus.newEnvelope(protocol.TargetParent, protocol.TypeStatusReport,
						protocol.MustEncode(status)), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(fmt.Errorf("status cached locally, but not delivered: %w", err))
				}
				var receipt protocol.DeliveryReceiptPayload
				_ = resp.DecodePayload(&receipt)
				if receipt.Queued {
					return textResult(map[string]any{"reported": true, "state": state,
						"note": "no parent connected; queued for the next parent"})
				}
				return textResult(map[string]any{"reported": true, "state": state})
			},
		},
		{
			Tool: mcp.NewTool("bus_send_to_parent",
				mcp.WithDescription("Send a direct message or question to the parent instance."),
				mcp.WithString("message", mcp.Required(), mcp.Description("Message content")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				message, err := req.RequireString("message")
				if err != nil {
					return errResult(err)
				}
				resp, err := bus.request(
					bus.newEnvelope(protocol.TargetParent, protocol.TypeDirectMessage, message), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				var receipt protocol.DeliveryReceiptPayload
				_ = resp.DecodePayload(&receipt)
				if receipt.Queued {
					return textResult(map[string]any{"delivered": false,
						"note": "no parent connected; queued for the next parent"})
				}
				return textResult(map[string]any{"delivered": true})
			},
		},
		{
			Tool: mcp.NewTool("bus_get_assignment",
				mcp.WithDescription("Poll for a pending task assigned by the parent. Returns immediately with "+
					"the task or {\"pending\": false} if none."),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				resp, err := bus.request(
					bus.newEnvelope(protocol.TargetDaemon, protocol.TypeGetAssignment, ""), requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				var assignment protocol.AssignmentResponsePayload
				if err := resp.DecodePayload(&assignment); err != nil {
					return errResult(err)
				}
				return textResult(assignment)
			},
		},
		{
			Tool: mcp.NewTool("bus_acknowledge_task",
				mcp.WithDescription("Acknowledge receipt of a task. Must be called after bus_get_assignment returns a task."),
				mcp.WithString("task_id", mcp.Required(), mcp.Description("The task_id to acknowledge")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				taskID, err := req.RequireString("task_id")
				if err != nil {
					return errResult(err)
				}
				env := bus.newEnvelope(protocol.TargetParent, protocol.TypeAckTask, "")
				env.TaskID = taskID
				resp, err := bus.request(env, requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				return textResult(map[string]any{"acknowledged": taskID})
			},
		},
		{
			Tool: mcp.NewTool("bus_complete_task",
				mcp.WithDescription("Report task completion to the parent."),
				mcp.WithString("task_id", mcp.Required(), mcp.Description("The completed task's id")),
				mcp.WithString("result_json", mcp.Required(), mcp.Description("JSON result payload")),
			),
			Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				taskID, err := req.RequireString("task_id")
				if err != nil {
					return errResult(err)
				}
				resultJSON, err := req.RequireString("result_json")
				if err != nil {
					return errResult(err)
				}
				env := bus.newEnvelope(protocol.TargetParent, protocol.TypeCompleteTask,
					protocol.MustEncode(protocol.CompleteTaskPayload{TaskID: taskID, ResultJSON: resultJSON}))
				env.TaskID = taskID
				resp, err := bus.request(env, requestTimeout)
				if err != nil {
					return errResult(err)
				}
				if err := busError(resp); err != nil {
					return errResult(err)
				}
				return textResult(map[string]any{"completed": taskID, "parent_notified": true})
			},
		},
	}
}
