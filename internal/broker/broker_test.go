// Integration tests: a real broker on a temp Unix socket, raw protocol
// clients, full message flows (spec §17.3–§17.5).
package broker_test

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/testutil"
)

func TestMain(m *testing.M) {
	// Spec Appendix B.5: the broker must shut down with zero goroutine leaks.
	goleak.VerifyTestMain(m)
}

const tick = 2 * time.Second // generous receive timeout for CI machines

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

func TestBroker_ParentChildConnect(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	child := testutil.ConnectTestClient(t, socket, "child", "api-service")

	if parent.ID == "" || child.ID == "" {
		t.Fatal("both clients must receive client_ids in HELLO_ACK")
	}
	if parent.ID == child.ID {
		t.Fatal("client_ids must be unique")
	}

	// Both are visible in LIST.
	parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeListRequest,
		protocol.MustEncode(protocol.ListRequestPayload{Filter: "all"})))
	resp := parent.MustReceiveType(t, protocol.TypeListResponse, tick)
	var list protocol.ListResponsePayload
	if err := resp.DecodePayload(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Clients) != 2 {
		t.Fatalf("list shows %d clients, want 2: %+v", len(list.Clients), list.Clients)
	}
}

func TestBroker_Broadcast(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	ui := testutil.ConnectTestClient(t, socket, "child", "ui")

	env := parent.NewEnvelope(protocol.TargetBroadcast, protocol.TypeBroadcast, "begin task: auth module")
	parent.Send(env)

	for _, c := range []*testutil.TestClient{api, ui} {
		got := c.MustReceiveType(t, protocol.TypeBroadcast, tick)
		if got.Payload != "begin task: auth module" || got.From != parent.ID {
			t.Errorf("child %s got %+v", c.Name, got)
		}
	}
	receipt := parent.MustReceiveType(t, protocol.TypeDeliveryReceipt, tick)
	var r protocol.DeliveryReceiptPayload
	if err := receipt.DecodePayload(&r); err != nil {
		t.Fatal(err)
	}
	if r.Count != 2 {
		t.Errorf("receipt count = %d, want 2", r.Count)
	}
}

func TestBroker_DirectMessage(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	ui := testutil.ConnectTestClient(t, socket, "child", "ui")

	parent.Send(parent.NewEnvelope("api", protocol.TypeDirectMessage, "just for api"))

	got := api.MustReceiveType(t, protocol.TypeDirectMessage, tick)
	if got.Payload != "just for api" {
		t.Errorf("api got %q", got.Payload)
	}
	ui.MustNotReceiveType(t, protocol.TypeDirectMessage, 300*time.Millisecond)
}

func TestBroker_AssignTask(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	task := protocol.TaskPayload{TaskID: "auth-001", Instruction: "extract auth module"}
	parent.Send(parent.NewEnvelope("api", protocol.TypeAssignTask, protocol.MustEncode(task)))

	got := api.MustReceiveType(t, protocol.TypeAssignTask, tick)
	var received protocol.TaskPayload
	if err := got.DecodePayload(&received); err != nil {
		t.Fatal(err)
	}
	if received.TaskID != "auth-001" || received.Instruction != "extract auth module" {
		t.Errorf("task = %+v", received)
	}
	if received.AssignedAt.IsZero() {
		t.Error("broker must stamp assigned_at")
	}
	receipt := parent.MustReceiveType(t, protocol.TypeDeliveryReceipt, tick)
	if receipt.TaskID != "auth-001" {
		t.Errorf("receipt must echo task_id, got %q", receipt.TaskID)
	}

	// The task is also queued for polling.
	api.Send(api.NewEnvelope(protocol.TargetDaemon, protocol.TypeGetAssignment, ""))
	poll := api.MustReceiveType(t, protocol.TypeAssignmentResult, tick)
	var assignment protocol.AssignmentResponsePayload
	if err := poll.DecodePayload(&assignment); err != nil {
		t.Fatal(err)
	}
	if !assignment.Pending || assignment.Task == nil || assignment.Task.TaskID != "auth-001" {
		t.Errorf("assignment = %+v", assignment)
	}
}

func TestBroker_TaskLifecycle(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	// Assign.
	task := protocol.TaskPayload{TaskID: "auth-001", Instruction: "do the thing"}
	parent.Send(parent.NewEnvelope("api", protocol.TypeAssignTask, protocol.MustEncode(task)))
	api.MustReceiveType(t, protocol.TypeAssignTask, tick)

	// Acknowledge → parent is notified.
	ack := api.NewEnvelope(protocol.TargetParent, protocol.TypeAckTask, "")
	ack.TaskID = "auth-001"
	api.Send(ack)
	gotAck := parent.MustReceiveType(t, protocol.TypeAckTask, tick)
	if gotAck.TaskID != "auth-001" || gotAck.From != api.ID {
		t.Errorf("parent ack = %+v", gotAck)
	}

	// Queue is now empty.
	api.Send(api.NewEnvelope(protocol.TargetDaemon, protocol.TypeGetAssignment, ""))
	poll := api.MustReceiveType(t, protocol.TypeAssignmentResult, tick)
	var assignment protocol.AssignmentResponsePayload
	if err := poll.DecodePayload(&assignment); err != nil {
		t.Fatal(err)
	}
	if assignment.Pending {
		t.Error("acknowledged task must leave the pending queue")
	}

	// Complete → parent is notified.
	complete := api.NewEnvelope(protocol.TargetParent, protocol.TypeCompleteTask,
		protocol.MustEncode(protocol.CompleteTaskPayload{TaskID: "auth-001", ResultJSON: `{"files":3}`}))
	complete.TaskID = "auth-001"
	api.Send(complete)
	gotDone := parent.MustReceiveType(t, protocol.TypeCompleteTask, tick)
	if gotDone.TaskID != "auth-001" {
		t.Errorf("complete = %+v", gotDone)
	}
}

func TestBroker_StatusRequest(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	req := parent.NewEnvelope("api", protocol.TypeStatusRequest, "")
	parent.Send(req)

	// Child receives the relayed request and answers with the correlation id.
	gotReq := api.MustReceiveType(t, protocol.TypeStatusRequest, tick)
	if gotReq.CorrelationID == "" {
		t.Fatal("relayed STATUS_REQUEST must carry a correlation id")
	}
	report := api.NewEnvelope(protocol.TargetParent, protocol.TypeStatusReport,
		protocol.MustEncode(protocol.StatusPayload{State: "working", Message: "extracting files", ReportedAt: time.Now()}))
	report.CorrelationID = gotReq.CorrelationID
	api.Send(report)

	gotReport := parent.MustReceiveType(t, protocol.TypeStatusReport, tick)
	if gotReport.CorrelationID != req.ID {
		t.Errorf("report correlation = %q, want %q", gotReport.CorrelationID, req.ID)
	}
	var status protocol.StatusPayload
	if err := gotReport.DecodePayload(&status); err != nil {
		t.Fatal(err)
	}
	if status.State != "working" {
		t.Errorf("status = %+v", status)
	}
}

func TestBroker_StatusRequestTimeout(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil) // status timeout = 1s in test config
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	_ = api // never answers

	req := parent.NewEnvelope("api", protocol.TypeStatusRequest, "")
	parent.Send(req)
	timeout := parent.MustReceiveType(t, protocol.TypeStatusTimeout, 3*time.Second)
	if timeout.CorrelationID != req.ID {
		t.Errorf("timeout correlation = %q, want %q", timeout.CorrelationID, req.ID)
	}
}

func TestBroker_GracefulShutdown(t *testing.T) {
	cfg := testutil.TestConfig(t)
	b := testutil.StartTestBrokerInstance(t, cfg)
	socket := b.SocketPath()
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	ui := testutil.ConnectTestClient(t, socket, "child", "ui")

	// Children ACK their shutdown notices as they arrive.
	var wg sync.WaitGroup
	for _, c := range []*testutil.TestClient{api, ui} {
		wg.Add(1)
		go func(c *testutil.TestClient) {
			defer wg.Done()
			notice := c.MustReceiveType(t, protocol.TypeShutdownNotice, tick)
			var p protocol.ShutdownNoticePayload
			if err := notice.DecodePayload(&p); err != nil {
				t.Errorf("notice payload: %v", err)
				return
			}
			c.Send(c.NewEnvelope(protocol.TargetDaemon, protocol.TypeShutdownAck, ""))
		}(c)
	}

	req := parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeShutdownRequest,
		protocol.MustEncode(protocol.ShutdownRequestPayload{TimeoutSeconds: 5}))
	parent.Send(req)

	result := parent.MustReceiveType(t, protocol.TypeShutdownResult, 5*time.Second)
	if result.CorrelationID != req.ID {
		t.Errorf("result correlation = %q, want %q", result.CorrelationID, req.ID)
	}
	var res protocol.ShutdownResultPayload
	if err := result.DecodePayload(&res); err != nil {
		t.Fatal(err)
	}
	if len(res.ChildrenAcked) != 2 || len(res.ChildrenForced) != 0 {
		t.Errorf("cascade result = %+v", res)
	}
	parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeShutdownAck, ""))
	wg.Wait()

	b.Wait() // daemon must terminate fully
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Error("socket file must be removed on clean shutdown")
	}
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

func TestBroker_MessageToNonexistentTarget(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	env := parent.NewEnvelope("ghost", protocol.TypeDirectMessage, "anyone there?")
	parent.Send(env)
	failed := parent.MustReceiveType(t, protocol.TypeDeliveryFailed, tick)
	if failed.CorrelationID != env.ID {
		t.Errorf("DELIVERY_FAILED must correlate to the message, got %q", failed.CorrelationID)
	}
	var p protocol.ErrorPayload
	if err := failed.DecodePayload(&p); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.Reason, "ghost") {
		t.Errorf("reason must name the target: %s", p.Reason)
	}
}

func TestBroker_ParentDisconnectsUnexpectedly(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	parent.Close() // no shutdown, no goodbye

	// Spec §17.4: children must be notified within 2 seconds.
	notice := api.MustReceiveType(t, protocol.TypeDirectMessage, 2*time.Second)
	if notice.From != protocol.DaemonSenderID || !strings.Contains(notice.Payload, "parent disconnected") {
		t.Errorf("notice = %+v", notice)
	}
}

func TestBroker_ChildConnectsBeforeParent(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	// The child is first told a parent arrived (§20.9), then both
	// directions work.
	if got := api.MustReceiveType(t, protocol.TypeDirectMessage, tick); got.Payload != "parent connected" {
		t.Errorf("expected the failover notice first, got %q", got.Payload)
	}
	parent.Send(parent.NewEnvelope("api", protocol.TypeDirectMessage, "hello child"))
	if got := api.MustReceiveType(t, protocol.TypeDirectMessage, tick); got.Payload != "hello child" {
		t.Errorf("child got %q", got.Payload)
	}
	api.Send(api.NewEnvelope(protocol.TargetParent, protocol.TypeDirectMessage, "hello parent"))
	if got := parent.MustReceiveType(t, protocol.TypeDirectMessage, tick); got.Payload != "hello parent" {
		t.Errorf("parent got %q", got.Payload)
	}
}

func TestBroker_MaxClientsReached(t *testing.T) {
	cfg := testutil.TestConfig(t)
	cfg.Limits.MaxClients = 20
	socket, _ := testutil.StartTestBroker(t, cfg)

	for i := 0; i < 20; i++ {
		testutil.ConnectTestClient(t, socket, "child", testutil.Fmt("worker-%d", i))
	}
	c, reject := testutil.TryHandshake(t, socket, "child", "one-too-many")
	if c != nil {
		t.Fatal("21st client must be rejected")
	}
	if reason := testutil.DecodeReject(t, reject); !strings.Contains(reason, "maximum client limit") {
		t.Errorf("reject reason = %q", reason)
	}
}

func TestBroker_DuplicateName(t *testing.T) {
	// Documented choice (registry.Add): duplicates are rejected, not
	// disambiguated, so a name always maps to exactly one session.
	socket, _ := testutil.StartTestBroker(t, nil)
	testutil.ConnectTestClient(t, socket, "child", "api-service")
	c, reject := testutil.TryHandshake(t, socket, "child", "api-service")
	if c != nil {
		t.Fatal("duplicate name must be rejected")
	}
	if reason := testutil.DecodeReject(t, reject); !strings.Contains(reason, "already in use") {
		t.Errorf("reject reason = %q", reason)
	}
}

func TestBroker_EmptyPayloadMessage(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	parent.Send(parent.NewEnvelope("api", protocol.TypeDirectMessage, ""))
	got := api.MustReceiveType(t, protocol.TypeDirectMessage, tick)
	if got.Payload != "" {
		t.Errorf("payload = %q, want empty", got.Payload)
	}
}

func TestBroker_UnicodePayload(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	payload := "完了 ✅ デプロイ成功 🚀 — ça marche! Привет"
	parent.Send(parent.NewEnvelope("api", protocol.TypeDirectMessage, payload))
	if got := api.MustReceiveType(t, protocol.TypeDirectMessage, tick); got.Payload != payload {
		t.Errorf("unicode payload corrupted: %q", got.Payload)
	}
}

func TestBroker_SimultaneousConnections(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			testutil.ConnectTestClient(t, socket, "child", testutil.Fmt("burst-%d", i))
		}(i)
	}
	wg.Wait()

	parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeListRequest,
		protocol.MustEncode(protocol.ListRequestPayload{Filter: "children"})))
	resp := parent.MustReceiveType(t, protocol.TypeListResponse, tick)
	var list protocol.ListResponsePayload
	if err := resp.DecodePayload(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.Clients) != 10 {
		t.Errorf("registered %d children, want 10", len(list.Clients))
	}
}

// envelopeWireSize returns the marshaled size of env.
func envelopeWireSize(t *testing.T, env *protocol.Envelope) int {
	t.Helper()
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return len(data)
}

func TestBroker_LargePayload_AtLimit(t *testing.T) {
	cfg := testutil.TestConfig(t)
	cfg.Limits.MaxMessageBytes = 8192
	socket, _ := testutil.StartTestBroker(t, cfg)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	// Pad the payload so the marshaled envelope is exactly at the limit.
	env := parent.NewEnvelope("api", protocol.TypeDirectMessage, "")
	pad := cfg.Limits.MaxMessageBytes - envelopeWireSize(t, env)
	env.Payload = strings.Repeat("x", pad)
	if got := envelopeWireSize(t, env); got != cfg.Limits.MaxMessageBytes {
		t.Fatalf("test setup: envelope is %d bytes, want %d", got, cfg.Limits.MaxMessageBytes)
	}

	parent.Send(env)
	if got := api.MustReceiveType(t, protocol.TypeDirectMessage, tick); len(got.Payload) != pad {
		t.Errorf("at-limit message corrupted: %d bytes", len(got.Payload))
	}
}

func TestBroker_LargePayload_OverLimit(t *testing.T) {
	cfg := testutil.TestConfig(t)
	cfg.Limits.MaxMessageBytes = 8192
	socket, _ := testutil.StartTestBroker(t, cfg)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	env := parent.NewEnvelope("api", protocol.TypeDirectMessage, "")
	pad := cfg.Limits.MaxMessageBytes - envelopeWireSize(t, env) + 1 // one byte over
	env.Payload = strings.Repeat("x", pad)

	parent.Send(env)
	parent.MustReceiveType(t, protocol.TypeMessageTooLarge, tick)

	// The stream stays framed: the same connection keeps working.
	parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypePing, ""))
	parent.MustReceiveType(t, protocol.TypePong, tick)
}

func TestShutdown_ChildDisconnectedDuringCascade(t *testing.T) {
	cfg := testutil.TestConfig(t)
	b := testutil.StartTestBrokerInstance(t, cfg)
	socket := b.SocketPath()
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	flaky := testutil.ConnectTestClient(t, socket, "child", "flaky")

	go func() {
		api.MustReceiveType(t, protocol.TypeShutdownNotice, tick)
		api.Send(api.NewEnvelope(protocol.TargetDaemon, protocol.TypeShutdownAck, ""))
	}()
	go func() {
		flaky.MustReceiveType(t, protocol.TypeShutdownNotice, tick)
		flaky.Close() // drops instead of ACKing
	}()

	req := parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeShutdownRequest,
		protocol.MustEncode(protocol.ShutdownRequestPayload{TimeoutSeconds: 30}))
	parent.Send(req)

	// Despite the 30s ACK window, the cascade completes promptly because the
	// disconnect releases the wait.
	result := parent.MustReceiveType(t, protocol.TypeShutdownResult, 5*time.Second)
	var res protocol.ShutdownResultPayload
	if err := result.DecodePayload(&res); err != nil {
		t.Fatal(err)
	}
	if len(res.ChildrenAcked) != 1 || res.ChildrenAcked[0] != "api" {
		t.Errorf("acked = %v", res.ChildrenAcked)
	}
	if len(res.ChildrenForced) != 1 || res.ChildrenForced[0] != "flaky" {
		t.Errorf("forced = %v", res.ChildrenForced)
	}
	parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeShutdownAck, ""))
	b.Wait()
}

func TestShutdown_NoChildrenConnected(t *testing.T) {
	cfg := testutil.TestConfig(t)
	b := testutil.StartTestBrokerInstance(t, cfg)
	parent := testutil.ConnectTestClient(t, b.SocketPath(), "parent", "orchestrator")

	req := parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeShutdownRequest,
		protocol.MustEncode(protocol.ShutdownRequestPayload{TimeoutSeconds: 30}))
	parent.Send(req)
	result := parent.MustReceiveType(t, protocol.TypeShutdownResult, tick)
	var res protocol.ShutdownResultPayload
	if err := result.DecodePayload(&res); err != nil {
		t.Fatal(err)
	}
	if len(res.ChildrenAcked) != 0 || len(res.ChildrenForced) != 0 {
		t.Errorf("result = %+v, want empty", res)
	}
	parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeShutdownAck, ""))
	b.Wait()
}

func TestHealth_ClientReconnects(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	first := testutil.ConnectTestClient(t, socket, "child", "api")
	firstID := first.ID
	first.Close()

	// The name frees up as soon as the broker processes the disconnect;
	// retry until then (no unconditional sleeps).
	deadline := time.Now().Add(tick)
	for {
		c, reject := testutil.TryHandshake(t, socket, "child", "api")
		if c != nil {
			if c.ID == firstID {
				t.Error("reconnected client must receive a new client_id")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("could not reconnect: %s", testutil.DecodeReject(t, reject))
		}
	}
}

func TestBroker_StaleClientDisconnected(t *testing.T) {
	cfg := testutil.TestConfig(t)
	cfg.Timeouts.StaleClientSeconds = 1
	socket, _ := testutil.StartTestBroker(t, cfg)
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	// Send nothing: the broker must cut the connection after ~1–2s.
	api.WaitDisconnect(t, 5*time.Second)
}

// ---------------------------------------------------------------------------
// Negative tests
// ---------------------------------------------------------------------------

func TestBroker_NoHello_ConnectionDropped(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil) // handshake timeout = 1s
	conn := testutil.Dial(t, socket)

	// Say nothing. The broker must drop us; detect via read returning EOF.
	if err := conn.SetReadDeadline(time.Now().Add(4 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadMessage(conn, 0); err == nil {
		t.Fatal("broker must not send anything to a silent client")
	}

	// And the registry must not contain it.
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeListRequest, ""))
	resp := parent.MustReceiveType(t, protocol.TypeListResponse, tick)
	var list protocol.ListResponsePayload
	if err := resp.DecodePayload(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.Clients) != 1 {
		t.Errorf("registry must hold only the parent, got %+v", list.Clients)
	}
}

func TestBroker_SecondParent_Rejected(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	c, reject := testutil.TryHandshake(t, socket, "parent", "usurper")
	if c != nil {
		t.Fatal("second parent must be rejected")
	}
	if reason := testutil.DecodeReject(t, reject); !strings.Contains(reason, "parent is already connected") {
		t.Errorf("reject reason = %q", reason)
	}
}

func TestBroker_InvalidRole_Rejected(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	c, reject := testutil.TryHandshake(t, socket, "superadmin", "wannabe")
	if c != nil {
		t.Fatal("invalid role must be rejected")
	}
	if reason := testutil.DecodeReject(t, reject); !strings.Contains(reason, "role must be") {
		t.Errorf("reject reason = %q", reason)
	}
}

func TestBroker_InvalidName_Rejected(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	c, reject := testutil.TryHandshake(t, socket, "child", "bad name!")
	if c != nil {
		t.Fatal("invalid name must be rejected")
	}
	if reason := testutil.DecodeReject(t, reject); !strings.Contains(reason, "name") {
		t.Errorf("reject reason = %q", reason)
	}
}

func TestBroker_MalformedJSON_Rejected(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	// A well-framed message whose body is binary garbage.
	body := []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0x01}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	if _, err := api.Conn.Write(append(header[:], body...)); err != nil {
		t.Fatal(err)
	}

	api.MustReceiveType(t, protocol.TypeInvalidMessage, tick)

	// The broker survives and the same connection still works.
	api.Send(api.NewEnvelope(protocol.TargetDaemon, protocol.TypePing, ""))
	api.MustReceiveType(t, protocol.TypePong, tick)
}

func TestBroker_TruncatedFrame_Handled(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	// Promise 100 bytes, deliver none, hang up.
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 100)
	if _, err := api.Conn.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	api.Close()

	// The broker must shrug it off: a fresh client works fine.
	probe := testutil.ConnectTestClient(t, socket, "child", "probe")
	probe.Send(probe.NewEnvelope(protocol.TargetDaemon, protocol.TypePing, ""))
	probe.MustReceiveType(t, protocol.TypePong, tick)
}

func TestBroker_ChildCallsParentTool(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	env := api.NewEnvelope(protocol.TargetBroadcast, protocol.TypeBroadcast, "I am in charge now")
	api.Send(env)
	denied := api.MustReceiveType(t, protocol.TypePermissionDenied, tick)
	if denied.CorrelationID != env.ID {
		t.Errorf("denial must correlate to the offending message")
	}
}

func TestBroker_ParentCallsChildTool(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	env := parent.NewEnvelope(protocol.TargetParent, protocol.TypeStatusReport,
		protocol.MustEncode(protocol.StatusPayload{State: "working", Message: "pretending to be a child"}))
	parent.Send(env)
	parent.MustReceiveType(t, protocol.TypePermissionDenied, tick)
}

func TestBroker_EmptyFromField(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	env := protocol.NewEnvelope("", protocol.TargetParent, protocol.TypeDirectMessage, "anonymous")
	api.Send(env)
	api.MustReceiveType(t, protocol.TypeInvalidMessage, tick)
}

func TestBroker_SpoofedFromField(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	env := protocol.NewEnvelope(parent.ID, protocol.TargetBroadcast, protocol.TypeBroadcast, "spoofed")
	api.Send(env)
	api.MustReceiveType(t, protocol.TypeInvalidMessage, tick)
}

func TestBroker_FutureProtocolVersion(t *testing.T) {
	// Documented choice (broker handshake): unknown client versions are
	// accepted with a logged warning, not rejected — the envelope format is
	// versioned separately and v1 has no incompatible variants.
	socket, _ := testutil.StartTestBroker(t, nil)
	conn := testutil.Dial(t, socket)
	hello := protocol.NewEnvelope("pending", protocol.TargetDaemon, protocol.TypeHello,
		protocol.MustEncode(protocol.HelloPayload{Role: "child", Name: "futuristic", Version: "99"}))
	if err := protocol.WriteMessage(conn, hello, 0); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(tick)); err != nil {
		t.Fatal(err)
	}
	resp, err := protocol.ReadMessage(conn, 0)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Type != protocol.TypeHelloAck {
		t.Errorf("version 99 must be accepted with a warning, got %s", resp.Type)
	}
}

func TestBroker_ChildDirectMessageToSiblingDenied(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	testutil.ConnectTestClient(t, socket, "child", "ui")

	env := api.NewEnvelope("ui", protocol.TypeDirectMessage, "psst")
	api.Send(env)
	api.MustReceiveType(t, protocol.TypePermissionDenied, tick)
}

func TestBroker_SecondDaemonOnSameSocket(t *testing.T) {
	cfg := testutil.TestConfig(t)
	b := testutil.StartTestBrokerInstance(t, cfg)
	_ = b
	second := testutil.StartTestBrokerNoStart(t, cfg)
	if err := second.Start(); err == nil {
		t.Fatal("second daemon on the same socket must refuse to start")
		second.Shutdown("test", time.Second, "test", "")
	}
}
