package mcp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/arterberry/daemonseed/internal/trace"
)

func TestTraced_EmitsToolEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	store, err := trace.NewJSONLStore(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	tracer := trace.New(store, "mcp:api", 100)

	okTool := traced(tracer, "api", "child", server.ServerTool{
		Tool: mcp.NewTool("bus_report_status"),
		Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("done"), nil
		},
	})
	errTool := traced(tracer, "api", "child", server.ServerTool{
		Tool: mcp.NewTool("bus_send_to_parent"),
		Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultError("no parent connected"), nil
		},
	})

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"message": "working", "state": "working"}
	if _, err := okTool.Handler(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := errTool.Handler(context.Background(), mcp.CallToolRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := tracer.Close(); err != nil {
		t.Fatal(err)
	}

	events, err := trace.ReadJSONL(path, 10, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}

	ok := events[0]
	if ok.Kind != trace.KindTool || ok.Name != "bus_report_status" || ok.Status != trace.StatusOK {
		t.Errorf("ok event = %+v", ok)
	}
	if ok.Session != "api" || ok.Role != "child" || ok.Source != "mcp:api" {
		t.Errorf("ok event identity = %+v", ok)
	}
	if ok.DurationMs <= 0 {
		t.Errorf("duration = %v, want > 0", ok.DurationMs)
	}
	if !strings.Contains(ok.Detail, "working") {
		t.Errorf("detail must carry args snippet: %q", ok.Detail)
	}

	failed := events[1]
	if failed.Status != trace.StatusError || !strings.Contains(failed.Detail, "no parent connected") {
		t.Errorf("error event = %+v", failed)
	}
}

func TestTraced_NilTracerIsPassthrough(t *testing.T) {
	st := server.ServerTool{
		Tool: mcp.NewTool("bus_ping"),
		Handler: func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("pong"), nil
		},
	}
	wrapped := traced(nil, "api", "child", st)
	res, err := wrapped.Handler(context.Background(), mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("passthrough failed: %v %v", err, res)
	}
}
