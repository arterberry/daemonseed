// Integration tests for §20.7 (inbox drain) and §20.8 (scheduler).
package broker_test

import (
	"strings"
	"testing"
	"time"

	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/testutil"
)

// ---------------------------------------------------------------------------
// §20.7 — named inbox & drain
// ---------------------------------------------------------------------------

func drainRequest(c *testutil.TestClient, name string, peek bool) *protocol.Envelope {
	return c.NewEnvelope(protocol.TargetDaemon, protocol.TypeInboxDrainRequest,
		protocol.MustEncode(map[string]any{"name": name, "peek": peek}))
}

func TestInbox_DrainReturnsAndClearsMessages(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	observer := testutil.ConnectTestClient(t, socket, "observer", "hook-cli")

	parent.Send(parent.NewEnvelope("api", protocol.TypeDirectMessage, "/bus-report please"))
	api.MustReceiveType(t, protocol.TypeDirectMessage, tick) // live delivery still happens

	observer.Send(drainRequest(observer, "api", false))
	resp := observer.MustReceiveType(t, protocol.TypeInboxDrainResponse, tick)
	var drained protocol.InboxDrainResponsePayload
	if err := resp.DecodePayload(&drained); err != nil {
		t.Fatal(err)
	}
	if len(drained.Messages) != 1 || drained.Messages[0].Payload != "/bus-report please" {
		t.Fatalf("drained = %+v", drained.Messages)
	}
	if drained.Messages[0].From != "orchestrator" {
		t.Errorf("sender name = %q", drained.Messages[0].From)
	}

	// Second drain is empty: the first cleared it.
	observer.Send(drainRequest(observer, "api", false))
	resp = observer.MustReceiveType(t, protocol.TypeInboxDrainResponse, tick)
	if err := resp.DecodePayload(&drained); err != nil {
		t.Fatal(err)
	}
	if len(drained.Messages) != 0 {
		t.Errorf("second drain must be empty, got %+v", drained.Messages)
	}
}

func TestInbox_PeekDoesNotClear(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	observer := testutil.ConnectTestClient(t, socket, "observer", "hook-cli")

	parent.Send(parent.NewEnvelope("api", protocol.TypeDirectMessage, "hello"))
	api.MustReceiveType(t, protocol.TypeDirectMessage, tick)

	observer.Send(drainRequest(observer, "api", true)) // peek
	observer.MustReceiveType(t, protocol.TypeInboxDrainResponse, tick)

	observer.Send(drainRequest(observer, "api", false)) // real drain still sees it
	resp := observer.MustReceiveType(t, protocol.TypeInboxDrainResponse, tick)
	var drained protocol.InboxDrainResponsePayload
	if err := resp.DecodePayload(&drained); err != nil {
		t.Fatal(err)
	}
	if len(drained.Messages) != 1 {
		t.Errorf("peek must not clear: drain got %d messages", len(drained.Messages))
	}
}

func TestInbox_IncludesPendingTasks(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	observer := testutil.ConnectTestClient(t, socket, "observer", "hook-cli")

	taskJSON := protocol.MustEncode(protocol.TaskPayload{TaskID: "auth-001", Instruction: "extract auth"})
	parent.Send(parent.NewEnvelope("api", protocol.TypeAssignTask, taskJSON))
	api.MustReceiveType(t, protocol.TypeAssignTask, tick)

	observer.Send(drainRequest(observer, "api", false))
	resp := observer.MustReceiveType(t, protocol.TypeInboxDrainResponse, tick)
	var drained protocol.InboxDrainResponsePayload
	if err := resp.DecodePayload(&drained); err != nil {
		t.Fatal(err)
	}
	if len(drained.PendingTasks) != 1 || drained.PendingTasks[0].TaskID != "auth-001" {
		t.Fatalf("pending tasks = %+v", drained.PendingTasks)
	}

	// Pending tasks are a peek: they resurface until acknowledged.
	observer.Send(drainRequest(observer, "api", false))
	resp = observer.MustReceiveType(t, protocol.TypeInboxDrainResponse, tick)
	if err := resp.DecodePayload(&drained); err != nil {
		t.Fatal(err)
	}
	if len(drained.PendingTasks) != 1 {
		t.Error("unacknowledged task must keep resurfacing in drains")
	}
}

func TestInbox_ChildMayOnlyDrainItself(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	api := testutil.ConnectTestClient(t, socket, "child", "api")
	testutil.ConnectTestClient(t, socket, "child", "ui")

	api.Send(drainRequest(api, "ui", false))
	api.MustReceiveType(t, protocol.TypePermissionDenied, tick)

	api.Send(drainRequest(api, "api", false))
	api.MustReceiveType(t, protocol.TypeInboxDrainResponse, tick)
}

func TestTasks_SurviveReconnect(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	taskJSON := protocol.MustEncode(protocol.TaskPayload{TaskID: "auth-001", Instruction: "extract auth"})
	parent.Send(parent.NewEnvelope("api", protocol.TypeAssignTask, taskJSON))
	api.MustReceiveType(t, protocol.TypeAssignTask, tick)
	api.Close()

	// Reconnect under the same name (retry until the broker frees it).
	deadline := time.Now().Add(tick)
	var again *testutil.TestClient
	for again == nil {
		c, _ := testutil.TryHandshake(t, socket, "child", "api")
		if c != nil {
			again = c
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("could not reconnect")
		}
	}

	again.Send(again.NewEnvelope(protocol.TargetDaemon, protocol.TypeGetAssignment, ""))
	resp := again.MustReceiveType(t, protocol.TypeAssignmentResult, tick)
	var assignment protocol.AssignmentResponsePayload
	if err := resp.DecodePayload(&assignment); err != nil {
		t.Fatal(err)
	}
	if !assignment.Pending || assignment.Task.TaskID != "auth-001" {
		t.Errorf("task must survive reconnect, got %+v", assignment)
	}
}

// ---------------------------------------------------------------------------
// §20.8 — scheduler
// ---------------------------------------------------------------------------

func createSchedule(t *testing.T, parent *testutil.TestClient, payload protocol.ScheduleCreatePayload) protocol.ScheduleInfo {
	t.Helper()
	parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeScheduleCreate,
		protocol.MustEncode(payload)))
	resp := parent.MustReceiveType(t, protocol.TypeScheduleCreated, tick)
	var info protocol.ScheduleInfo
	if err := resp.DecodePayload(&info); err != nil {
		t.Fatal(err)
	}
	return info
}

func TestScheduler_IntervalFiresAndDelivers(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	info := createSchedule(t, parent, protocol.ScheduleCreatePayload{
		Target:  "api",
		Task:    protocol.TaskPayload{Instruction: "run the audit"},
		Trigger: protocol.ScheduleTrigger{Every: "100ms"},
	})
	if info.ID == "" || info.Misfire != "queue" {
		t.Fatalf("schedule info = %+v", info)
	}

	// The child receives a pushed ASSIGN_TASK carrying schedule context.
	got := api.MustReceiveType(t, protocol.TypeAssignTask, 3*time.Second)
	var task protocol.TaskPayload
	if err := got.DecodePayload(&task); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(task.TaskID, info.ID+"-") {
		t.Errorf("task_id = %q, want prefix %q", task.TaskID, info.ID)
	}
	if task.Context["schedule_id"] != info.ID || task.Context["scheduled_by"] != "parent:orchestrator" {
		t.Errorf("context = %+v", task.Context)
	}

	// It keeps firing until canceled.
	api.MustReceiveType(t, protocol.TypeAssignTask, 3*time.Second)
	parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeScheduleCancel,
		protocol.MustEncode(protocol.ScheduleCancelPayload{ScheduleID: info.ID})))
	parent.MustReceiveType(t, protocol.TypeScheduleCanceled, tick)
}

func TestScheduler_OneShotFiresOnceAndRetires(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	at := time.Now().Add(150 * time.Millisecond).Format(time.RFC3339Nano)
	createSchedule(t, parent, protocol.ScheduleCreatePayload{
		Target:  "api",
		Task:    protocol.TaskPayload{Instruction: "one shot"},
		Trigger: protocol.ScheduleTrigger{At: at},
	})

	api.MustReceiveType(t, protocol.TypeAssignTask, 3*time.Second)

	// After firing, the schedule is gone.
	deadline := time.Now().Add(tick)
	for {
		parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeScheduleList, ""))
		resp := parent.MustReceiveType(t, protocol.TypeScheduleListResp, tick)
		var list protocol.ScheduleListPayload
		if err := resp.DecodePayload(&list); err != nil {
			t.Fatal(err)
		}
		if len(list.Schedules) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("one-shot schedule must retire after firing: %+v", list.Schedules)
		}
	}
}

func TestScheduler_QueuePolicyDeliversOnReconnect(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	// Schedule for a child that is not connected; queue policy with a long TTL.
	at := time.Now().Add(100 * time.Millisecond).Format(time.RFC3339Nano)
	createSchedule(t, parent, protocol.ScheduleCreatePayload{
		Target:  "late-riser",
		Task:    protocol.TaskPayload{Instruction: "waiting for you"},
		Trigger: protocol.ScheduleTrigger{At: at},
		Misfire: "queue",
		TTL:     "1h",
	})

	// Connect after the fire time and poll: the task must be waiting.
	deadline := time.Now().Add(3 * time.Second)
	child := testutil.ConnectTestClient(t, socket, "child", "late-riser")
	for {
		child.Send(child.NewEnvelope(protocol.TargetDaemon, protocol.TypeGetAssignment, ""))
		resp := child.MustReceiveType(t, protocol.TypeAssignmentResult, tick)
		var assignment protocol.AssignmentResponsePayload
		if err := resp.DecodePayload(&assignment); err != nil {
			t.Fatal(err)
		}
		if assignment.Pending {
			if assignment.Task.Instruction != "waiting for you" {
				t.Errorf("task = %+v", assignment.Task)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("queued scheduled task never became available")
		}
	}
}

func TestScheduler_SkipPolicyDropsOccurrence(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	at := time.Now().Add(100 * time.Millisecond).Format(time.RFC3339Nano)
	info := createSchedule(t, parent, protocol.ScheduleCreatePayload{
		Target:  "ghost",
		Task:    protocol.TaskPayload{Instruction: "nobody home"},
		Trigger: protocol.ScheduleTrigger{At: at},
		Misfire: "skip",
	})

	// Wait until the one-shot has fired (schedule list becomes empty).
	deadline := time.Now().Add(3 * time.Second)
	for {
		parent.Send(parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeScheduleList, ""))
		resp := parent.MustReceiveType(t, protocol.TypeScheduleListResp, tick)
		var list protocol.ScheduleListPayload
		if err := resp.DecodePayload(&list); err != nil {
			t.Fatal(err)
		}
		if len(list.Schedules) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("schedule %s never fired", info.ID)
		}
	}

	// The occurrence was skipped: a late-connecting child finds nothing.
	child := testutil.ConnectTestClient(t, socket, "child", "ghost")
	child.Send(child.NewEnvelope(protocol.TargetDaemon, protocol.TypeGetAssignment, ""))
	resp := child.MustReceiveType(t, protocol.TypeAssignmentResult, tick)
	var assignment protocol.AssignmentResponsePayload
	if err := resp.DecodePayload(&assignment); err != nil {
		t.Fatal(err)
	}
	if assignment.Pending {
		t.Errorf("skip policy must not queue: %+v", assignment.Task)
	}
}

func TestScheduler_ValidationAndGuardrails(t *testing.T) {
	cfg := testutil.TestConfig(t)
	cfg.Limits.MinScheduleIntervalSeconds = 60
	cfg.Limits.MaxSchedules = 2
	socket, _ := testutil.StartTestBroker(t, cfg)
	parent := testutil.ConnectTestClient(t, socket, "parent", "orchestrator")

	send := func(p protocol.ScheduleCreatePayload) *protocol.Envelope {
		env := parent.NewEnvelope(protocol.TargetDaemon, protocol.TypeScheduleCreate, protocol.MustEncode(p))
		parent.Send(env)
		return env
	}
	expectInvalid := func(p protocol.ScheduleCreatePayload, wantSubstr string) {
		t.Helper()
		send(p)
		resp := parent.MustReceiveType(t, protocol.TypeInvalidMessage, tick)
		var e protocol.ErrorPayload
		if err := resp.DecodePayload(&e); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(e.Reason, wantSubstr) {
			t.Errorf("reason = %q, want substring %q", e.Reason, wantSubstr)
		}
	}

	task := protocol.TaskPayload{Instruction: "x"}
	// Below the minimum interval.
	expectInvalid(protocol.ScheduleCreatePayload{
		Target: "api", Task: task, Trigger: protocol.ScheduleTrigger{Every: "1s"},
	}, "below the minimum")
	// Sub-minimum cron (fires every minute < 60s is fine — use a 1-minute cron
	// against a 60s floor: gap is exactly 60s, allowed; instead reject via
	// missing trigger).
	expectInvalid(protocol.ScheduleCreatePayload{Target: "api", Task: task}, "exactly one")
	// Two triggers at once.
	expectInvalid(protocol.ScheduleCreatePayload{
		Target: "api", Task: task,
		Trigger: protocol.ScheduleTrigger{Every: "5m", Cron: "* * * * *"},
	}, "exactly one")
	// Past one-shot.
	expectInvalid(protocol.ScheduleCreatePayload{
		Target: "api", Task: task,
		Trigger: protocol.ScheduleTrigger{At: time.Now().Add(-time.Hour).Format(time.RFC3339)},
	}, "in the past")
	// Missing instruction.
	expectInvalid(protocol.ScheduleCreatePayload{
		Target: "api", Trigger: protocol.ScheduleTrigger{Every: "5m"},
	}, "instruction")
	// Bad cron.
	expectInvalid(protocol.ScheduleCreatePayload{
		Target: "api", Task: task, Trigger: protocol.ScheduleTrigger{Cron: "not a cron"},
	}, "cron")

	// Max schedules.
	createSchedule(t, parent, protocol.ScheduleCreatePayload{
		Target: "api", Task: task, Trigger: protocol.ScheduleTrigger{Every: "5m"}})
	createSchedule(t, parent, protocol.ScheduleCreatePayload{
		Target: "api", Task: task, Trigger: protocol.ScheduleTrigger{Every: "10m"}})
	expectInvalid(protocol.ScheduleCreatePayload{
		Target: "api", Task: task, Trigger: protocol.ScheduleTrigger{Every: "15m"},
	}, "limit")
}

func TestScheduler_ChildCannotCreateSchedules(t *testing.T) {
	socket, _ := testutil.StartTestBroker(t, nil)
	api := testutil.ConnectTestClient(t, socket, "child", "api")

	api.Send(api.NewEnvelope(protocol.TargetDaemon, protocol.TypeScheduleCreate,
		protocol.MustEncode(protocol.ScheduleCreatePayload{
			Target:  "api",
			Task:    protocol.TaskPayload{Instruction: "self-serve"},
			Trigger: protocol.ScheduleTrigger{Every: "5m"},
		})))
	api.MustReceiveType(t, protocol.TypePermissionDenied, tick)
}

func TestScheduler_VisibleInSnapshot(t *testing.T) {
	cfg := testutil.TestConfig(t)
	b := testutil.StartTestBrokerInstance(t, cfg)
	parent := testutil.ConnectTestClient(t, b.SocketPath(), "parent", "orchestrator")

	info := createSchedule(t, parent, protocol.ScheduleCreatePayload{
		Target:  "api",
		Task:    protocol.TaskPayload{Instruction: "audit"},
		Trigger: protocol.ScheduleTrigger{Every: "5m"},
	})
	snap := b.Snapshot()
	if len(snap.Schedules) != 1 || snap.Schedules[0].ID != info.ID {
		t.Errorf("snapshot schedules = %+v", snap.Schedules)
	}
}
