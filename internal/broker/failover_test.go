// Integration tests for §20.9 (parent failover) and §20.10 (session trace).
package broker_test

import (
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arterberry/daemonseed/internal/broker"
	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/testutil"
	"github.com/arterberry/daemonseed/internal/trace"
)

func TestFailover_ChildToParentBufferedWhileNoParent(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	// No parent connected: a status report is queued, not failed.
	report := api.NewEnvelope(protocol.TargetParent, protocol.TypeStatusReport,
		protocol.MustEncode(protocol.StatusPayload{State: "working", Message: "halfway", ReportedAt: time.Now()}))
	api.Send(report)
	receipt := api.MustReceiveType(t, protocol.TypeDeliveryReceipt, tick)
	var r protocol.DeliveryReceiptPayload
	if err := receipt.DecodePayload(&r); err != nil {
		t.Fatal(err)
	}
	if !r.Queued || r.Count != 0 {
		t.Fatalf("receipt = %+v, want queued", r)
	}

	// A direct message and a task completion queue the same way.
	api.Send(api.NewEnvelope(protocol.TargetParent, protocol.TypeDirectMessage, "are you there?"))
	api.MustReceiveType(t, protocol.TypeDeliveryReceipt, tick)

	// The next parent receives the backlog in order, plus children are told.
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	first := parent.MustReceiveType(t, protocol.TypeStatusReport, tick)
	if first.From != api.ID {
		t.Errorf("backlog report from = %q, want %q", first.From, api.ID)
	}
	var status protocol.StatusPayload
	if err := first.DecodePayload(&status); err != nil {
		t.Fatal(err)
	}
	if status.Message != "halfway" {
		t.Errorf("status = %+v", status)
	}
	second := parent.MustReceiveType(t, protocol.TypeDirectMessage, tick)
	if second.Payload != "are you there?" {
		t.Errorf("backlog message = %q", second.Payload)
	}

	notice := api.MustReceiveType(t, protocol.TypeDirectMessage, tick)
	if !strings.Contains(notice.Payload, "parent connected") {
		t.Errorf("children must learn a parent arrived: %q", notice.Payload)
	}
}

func TestFailover_NewParentTakesOverAfterCrash(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	first := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	first.Close()                                                     // crash, no goodbye
	api.MustReceiveType(t, protocol.TypeDirectMessage, 2*time.Second) // "parent disconnected"

	// While orphaned, the child completes a task: it must be buffered.
	complete := api.NewEnvelope(protocol.TargetParent, protocol.TypeCompleteTask,
		protocol.MustEncode(protocol.CompleteTaskPayload{TaskID: "auth-001", ResultJSON: `{"files":3}`}))
	complete.TaskID = "auth-001"
	api.Send(complete)
	receipt := api.MustReceiveType(t, protocol.TypeDeliveryReceipt, tick)
	var r protocol.DeliveryReceiptPayload
	if err := receipt.DecodePayload(&r); err != nil {
		t.Fatal(err)
	}
	if !r.Queued {
		t.Fatalf("completion while orphaned must be queued: %+v", r)
	}

	// A successor parent (the slot is free again) inherits the completion.
	deadline := time.Now().Add(tick)
	var successor *testutil.TestClient
	for successor == nil {
		c, _ := testutil.TryHandshake(t, socket, "parent", "orchestrator-2")
		if c != nil {
			successor = c
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("successor parent could not connect")
		}
	}
	done := successor.MustReceiveType(t, protocol.TypeCompleteTask, tick)
	if done.TaskID != "auth-001" {
		t.Errorf("inherited completion = %+v", done)
	}
}

func TestTrace_BrokerEmitsMessageAndLifecycleEvents(t *testing.T) {
	cfg := testutil.TestConfig(t)
	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")
	store, err := trace.NewJSONLStore(tracePath, 10)
	if err != nil {
		t.Fatal(err)
	}
	tracer := trace.New(store, "daemon", 50)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := broker.New(cfg, log, nil, "test")
	b.SetTracer(tracer)
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Shutdown("test cleanup", time.Second, "test", "") })

	parent := testutil.ConnectTestClient(t, b.SocketPath(), "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, b.SocketPath(), "child", "api")

	env := parent.NewEnvelope("api", protocol.TypeDirectMessage,
		"this payload is quite long and will be truncated by the configured detail limit for traces")
	parent.Send(env)
	api.MustReceiveType(t, protocol.TypeDirectMessage, tick)

	b.Shutdown("test done", time.Second, "test", "")
	if err := tracer.Close(); err != nil { // flush
		t.Fatal(err)
	}

	events, err := trace.ReadJSONL(tracePath, 100, "", "")
	if err != nil {
		t.Fatal(err)
	}
	var sawConnect, sawMessage bool
	for _, ev := range events {
		if ev.Kind == trace.KindLifecycle && ev.Name == "client_connected" && ev.Session == "api" {
			sawConnect = true
		}
		if ev.Kind == trace.KindMessage && ev.Name == "DIRECT_MESSAGE" &&
			ev.From == "orchestrator" && ev.To == "api" {
			sawMessage = true
			if ev.SpanID != env.ID {
				t.Errorf("span id = %q, want envelope id %q", ev.SpanID, env.ID)
			}
			if !strings.Contains(ev.Detail, "…(+") {
				t.Errorf("payload must be truncated in trace: %q", ev.Detail)
			}
		}
	}
	if !sawConnect || !sawMessage {
		t.Errorf("missing events: connect=%v message=%v (total %d)", sawConnect, sawMessage, len(events))
	}
}
