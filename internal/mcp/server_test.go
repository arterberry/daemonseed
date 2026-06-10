package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
	"github.com/arterberry/daemonseed/internal/testutil"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// connectBus starts a bus client against a live test broker.
func connectBus(t *testing.T, socketPath string, role roles.Role, name string) *busClient {
	t.Helper()
	cfg := testutil.TestConfig(t)
	cfg.Daemon.SocketPath = socketPath
	bus := newBusClient(cfg, testLogger(), role, name, "1.0.0", false)
	if err := bus.Connect(); err != nil {
		t.Fatalf("bus connect: %v", err)
	}
	t.Cleanup(bus.Close)
	return bus
}

func names(role roles.Role, bus *busClient) []string {
	var out []string
	for _, st := range toolsForRole(bus, role) {
		out = append(out, st.Tool.Name)
	}
	sort.Strings(out)
	return out
}

func callTool(t *testing.T, bus *busClient, role roles.Role, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	for _, st := range toolsForRole(bus, role) {
		if st.Tool.Name != name {
			continue
		}
		req := mcp.CallToolRequest{}
		req.Params.Name = name
		req.Params.Arguments = args
		res, err := st.Handler(context.Background(), req)
		if err != nil {
			t.Fatalf("tool %s returned protocol error: %v", name, err)
		}
		return res
	}
	t.Fatalf("tool %s not registered for role %s", name, role)
	return nil
}

func resultText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if len(res.Content) == 0 {
		t.Fatal("tool result has no content")
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("content is %T, want TextContent", res.Content[0])
	}
	return tc.Text
}

func TestMCPServer_ParentToolsAvailable(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	bus := connectBus(t, socket, roles.RoleParent, "orchestrator")

	got := names(roles.RoleParent, bus)
	want := []string{
		"bus_assign_task", "bus_broadcast", "bus_cancel_schedule", "bus_check_messages",
		"bus_get_status", "bus_list_all", "bus_list_children", "bus_list_schedules",
		"bus_ping", "bus_remove_child", "bus_schedule_task", "bus_send",
		"bus_shutdown", "bus_whoami",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("parent tools:\n got %v\nwant %v", got, want)
	}
	for _, forbidden := range []string{"bus_report_status", "bus_get_assignment", "bus_complete_task"} {
		for _, n := range got {
			if n == forbidden {
				t.Errorf("parent surface must not include child tool %s", forbidden)
			}
		}
	}
}

func TestMCPServer_ChildToolsAvailable(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	bus := connectBus(t, socket, roles.RoleChild, "api")

	got := names(roles.RoleChild, bus)
	want := []string{
		"bus_acknowledge_task", "bus_check_messages", "bus_complete_task",
		"bus_get_assignment", "bus_list_all", "bus_ping", "bus_report_status",
		"bus_send_to_parent", "bus_whoami",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("child tools:\n got %v\nwant %v", got, want)
	}
}

func TestMCPServer_PingReturnsLatency(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	bus := connectBus(t, socket, roles.RoleParent, "orchestrator")

	res := callTool(t, bus, roles.RoleParent, "bus_ping", nil)
	if res.IsError {
		t.Fatalf("ping errored: %s", resultText(t, res))
	}
	var out struct {
		RoundtripMs float64 `json:"roundtrip_ms"`
	}
	if err := json.Unmarshal([]byte(resultText(t, res)), &out); err != nil {
		t.Fatalf("ping result not JSON: %v", err)
	}
	if out.RoundtripMs <= 0 {
		t.Errorf("roundtrip_ms = %v, want > 0", out.RoundtripMs)
	}
}

func TestMCPServer_WhoAmI(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	bus := connectBus(t, socket, roles.RoleChild, "api")

	res := callTool(t, bus, roles.RoleChild, "bus_whoami", nil)
	var who protocol.WhoAmIResponsePayload
	if err := json.Unmarshal([]byte(resultText(t, res)), &who); err != nil {
		t.Fatal(err)
	}
	if who.Name != "api" || who.Role != "child" || who.ClientID != bus.ClientID() {
		t.Errorf("whoami = %+v", who)
	}
}

func TestMCPServer_SendAndCheckMessages(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := connectBus(t, socket, roles.RoleParent, "orchestrator")
	child := connectBus(t, socket, roles.RoleChild, "api")

	res := callTool(t, parent, roles.RoleParent, "bus_send",
		map[string]any{"target": "api", "message": "hello api"})
	if res.IsError {
		t.Fatalf("bus_send: %s", resultText(t, res))
	}

	// The child's inbox fills asynchronously; poll bus_check_messages.
	deadline := time.Now().Add(2 * time.Second)
	for {
		res := callTool(t, child, roles.RoleChild, "bus_check_messages", nil)
		text := resultText(t, res)
		if strings.Contains(text, "hello api") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("message never reached child inbox; last result: %s", text)
		}
	}
}

func TestMCPServer_TaskRoundTrip(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := connectBus(t, socket, roles.RoleParent, "orchestrator")
	child := connectBus(t, socket, roles.RoleChild, "api")

	res := callTool(t, parent, roles.RoleParent, "bus_assign_task", map[string]any{
		"target":    "api",
		"task_json": `{"task_id": "auth-001", "instruction": "extract the auth module"}`,
	})
	if res.IsError {
		t.Fatalf("assign: %s", resultText(t, res))
	}

	res = callTool(t, child, roles.RoleChild, "bus_get_assignment", nil)
	var assignment protocol.AssignmentResponsePayload
	if err := json.Unmarshal([]byte(resultText(t, res)), &assignment); err != nil {
		t.Fatal(err)
	}
	if !assignment.Pending || assignment.Task == nil || assignment.Task.TaskID != "auth-001" {
		t.Fatalf("assignment = %+v", assignment)
	}

	res = callTool(t, child, roles.RoleChild, "bus_acknowledge_task",
		map[string]any{"task_id": "auth-001"})
	if res.IsError {
		t.Fatalf("ack: %s", resultText(t, res))
	}

	res = callTool(t, child, roles.RoleChild, "bus_complete_task",
		map[string]any{"task_id": "auth-001", "result_json": `{"files": 3}`})
	if res.IsError {
		t.Fatalf("complete: %s", resultText(t, res))
	}
}

// TestMCPServer_GetStatusAutoReply: the child MCP answers the parent's
// synchronous status request from its cached bus_report_status state.
func TestMCPServer_GetStatusAutoReply(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := connectBus(t, socket, roles.RoleParent, "orchestrator")
	child := connectBus(t, socket, roles.RoleChild, "api")

	res := callTool(t, child, roles.RoleChild, "bus_report_status",
		map[string]any{"message": "extracting files", "state": "working"})
	if res.IsError {
		t.Fatalf("report: %s", resultText(t, res))
	}

	res = callTool(t, parent, roles.RoleParent, "bus_get_status",
		map[string]any{"target": "api"})
	if res.IsError {
		t.Fatalf("get_status: %s", resultText(t, res))
	}
	text := resultText(t, res)
	if !strings.Contains(text, "working") || !strings.Contains(text, "extracting files") {
		t.Errorf("status result = %s", text)
	}
}

func TestMCPServer_InvalidStateRejected(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	child := connectBus(t, socket, roles.RoleChild, "api")

	res := callTool(t, child, roles.RoleChild, "bus_report_status",
		map[string]any{"message": "zzz", "state": "napping"})
	if !res.IsError {
		t.Error("invalid state must produce a tool error")
	}
}

func TestMCP_DaemonNotRunning(t *testing.T) {
	cfg := testutil.TestConfig(t)
	cfg.Daemon.SocketPath = filepath.Join(t.TempDir(), "nope.sock")
	bus := newBusClient(cfg, testLogger(), roles.RoleParent, "orchestrator", "1.0.0", false)
	err := bus.Connect()
	if err == nil {
		bus.Close()
		t.Fatal("connect must fail when the daemon is down")
	}
	if !errors.Is(err, protocol.ErrDaemonNotRunning) {
		t.Errorf("error must wrap ErrDaemonNotRunning: %v", err)
	}
	if !strings.Contains(err.Error(), "daemonseed start") {
		t.Errorf("error must tell the user how to start the daemon: %v", err)
	}
}

func TestMCP_SecondParentRejectedAtConnect(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	connectBus(t, socket, roles.RoleParent, "orchestrator")

	cfg := testutil.TestConfig(t)
	cfg.Daemon.SocketPath = socket
	second := newBusClient(cfg, testLogger(), roles.RoleParent, "usurper", "1.0.0", false)
	err := second.Connect()
	if err == nil {
		second.Close()
		t.Fatal("second parent must be rejected")
	}
	if !strings.Contains(err.Error(), "parent is already connected") {
		t.Errorf("unexpected error: %v", err)
	}
}
