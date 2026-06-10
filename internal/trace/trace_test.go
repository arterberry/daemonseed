package trace

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testEvent(name, session, traceID string) Event {
	return Event{
		Kind: KindMessage, Name: name, Session: session, TraceID: traceID,
		From: "orchestrator", To: session, Status: StatusOK, Detail: "payload",
	}
}

// stores under test, by constructor.
func eachStore(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Helper()
	t.Run("jsonl", func(t *testing.T) {
		s, err := NewJSONLStore(filepath.Join(t.TempDir(), "trace.jsonl"), 10)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		fn(t, s)
	})
	t.Run("sqlite", func(t *testing.T) {
		s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "trace.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		fn(t, s)
	})
}

func TestStore_WriteAndRecent(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		for i := 0; i < 5; i++ {
			ev := testEvent("DIRECT_MESSAGE", "api", "trace-1")
			ev.TS = time.Now().UTC()
			if err := s.Write(ev); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		_ = s.Write(Event{TS: time.Now().UTC(), Kind: KindTool, Name: "bus_ping", Session: "ui"})

		all, err := s.Recent(50, "", "")
		if err != nil {
			t.Fatalf("recent: %v", err)
		}
		if len(all) != 6 {
			t.Fatalf("recent len = %d, want 6", len(all))
		}

		// Filter by session.
		api, err := s.Recent(50, "api", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(api) != 5 {
			t.Errorf("session filter: %d events, want 5", len(api))
		}

		// Filter by trace id.
		chain, err := s.Recent(50, "", "trace-1")
		if err != nil {
			t.Fatal(err)
		}
		if len(chain) != 5 {
			t.Errorf("trace filter: %d events, want 5", len(chain))
		}

		// Limit returns the most recent, oldest first.
		last, err := s.Recent(2, "", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(last) != 2 || last[1].Name != "bus_ping" {
			t.Errorf("limit window wrong: %+v", last)
		}
	})
}

func TestStore_ConcurrentWrites(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 20; j++ {
					_ = s.Write(testEvent("PING", "api", ""))
				}
			}()
		}
		wg.Wait()
		got, err := s.Recent(500, "", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 160 {
			t.Errorf("events = %d, want 160", len(got))
		}
	})
}

func TestJSONL_Rotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	s, err := NewJSONLStore(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.maxBytes = 500 // shrink for the test; rotation logic is what matters
	for i := 0; i < 10; i++ {
		ev := testEvent("BROADCAST", "api", "")
		ev.Detail = strings.Repeat("x", 100)
		if err := s.Write(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Errorf("rotation must produce a .1 backup: %v", err)
	}
}

func TestTracer_AsyncAndNilSafe(t *testing.T) {
	s, err := NewJSONLStore(filepath.Join(t.TempDir(), "trace.jsonl"), 10)
	if err != nil {
		t.Fatal(err)
	}
	tr := New(s, "daemon", 50)
	tr.Emit(Event{Kind: KindMessage, Name: "PING", Detail: strings.Repeat("y", 500)})
	if err := tr.Close(); err != nil { // Close flushes the queue
		t.Fatal(err)
	}
	events, err := ReadJSONL(s.path, 10, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if !strings.Contains(events[0].Detail, "…(+") || len(events[0].Detail) > 80 {
		t.Errorf("detail must be truncated: %q", events[0].Detail)
	}
	if events[0].Source != "daemon" || events[0].TS.IsZero() {
		t.Errorf("source/ts not stamped: %+v", events[0])
	}

	// A nil tracer is a working no-op.
	var nop *Tracer
	nop.Emit(Event{Kind: KindTool, Name: "bus_ping"})
	if err := nop.Close(); err != nil {
		t.Fatal(err)
	}
	if nop.Dropped() != 0 {
		t.Error("nil tracer drops nothing")
	}
}

func TestOpenStore_BackendSelection(t *testing.T) {
	dir := t.TempDir()
	jsonl, err := OpenStore("jsonl", filepath.Join(dir, "t.jsonl"), 10)
	if err != nil {
		t.Fatal(err)
	}
	jsonl.Close()
	if _, ok := jsonl.(*JSONLStore); !ok {
		t.Errorf("jsonl backend = %T", jsonl)
	}

	// sqlite swaps the default .jsonl suffix for .db.
	sq, err := OpenStore("sqlite", filepath.Join(dir, "t.jsonl"), 10)
	if err != nil {
		t.Fatal(err)
	}
	sq.Close()
	if _, err := os.Stat(filepath.Join(dir, "t.db")); err != nil {
		t.Errorf("sqlite path must use .db suffix: %v", err)
	}

	if _, err := OpenStore("parquet", filepath.Join(dir, "t"), 10); err == nil {
		t.Error("unknown backend must be rejected")
	}
}
