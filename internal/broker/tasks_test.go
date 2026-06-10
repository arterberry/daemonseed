package broker

import (
	"testing"
	"time"

	"github.com/arterberry/daemonseed/internal/protocol"
)

func task(id string) protocol.TaskPayload {
	return protocol.TaskPayload{TaskID: id, Instruction: "do " + id}
}

func TestTaskStore_FIFOAndAcknowledge(t *testing.T) {
	s := newTaskStore(10)
	_ = s.assign("api", task("t1"), time.Time{})
	_ = s.assign("api", task("t2"), time.Time{})

	if next, ok := s.next("api"); !ok || next.TaskID != "t1" {
		t.Fatalf("next = %+v, %v", next, ok)
	}
	// Peek does not remove.
	if next, _ := s.next("api"); next.TaskID != "t1" {
		t.Fatal("next must not consume the task")
	}
	if !s.acknowledge("api", "t1") {
		t.Fatal("acknowledge t1 must succeed")
	}
	if next, _ := s.next("api"); next.TaskID != "t2" {
		t.Fatalf("after ack, next = %s", next.TaskID)
	}
	if s.acknowledge("api", "t1") {
		t.Fatal("double acknowledge must fail")
	}
}

func TestTaskStore_QueueLimit(t *testing.T) {
	s := newTaskStore(2)
	_ = s.assign("api", task("t1"), time.Time{})
	_ = s.assign("api", task("t2"), time.Time{})
	if err := s.assign("api", task("t3"), time.Time{}); err == nil {
		t.Fatal("assignment beyond the limit must fail")
	}
}

func TestTaskStore_TTLExpiry(t *testing.T) {
	s := newTaskStore(10)
	_ = s.assign("api", task("fleeting"), time.Now().Add(20*time.Millisecond))
	_ = s.assign("api", task("durable"), time.Time{})

	if all := s.all("api"); len(all) != 2 {
		t.Fatalf("before expiry: %d tasks", len(all))
	}
	// Wait out the TTL via the condition, not a fixed sleep budget.
	deadline := time.Now().Add(2 * time.Second)
	for {
		all := s.all("api")
		if len(all) == 1 && all[0].TaskID == "durable" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired task still present: %+v", all)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTaskStore_PersistsAcrossNames(t *testing.T) {
	// Queues are name-keyed: nothing is dropped when a connection goes away,
	// so a reconnecting child sees its pending work (§20.8 queue policy).
	s := newTaskStore(10)
	_ = s.assign("api", task("t1"), time.Time{})
	if all := s.all("api"); len(all) != 1 {
		t.Fatalf("tasks = %d", len(all))
	}
	if _, ok := s.next("other"); ok {
		t.Fatal("queues must be isolated by name")
	}
}
