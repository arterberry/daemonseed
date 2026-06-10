package audit

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func newTestLogger(t *testing.T, opts Options) (*Logger, string) {
	t.Helper()
	if opts.Path == "" {
		opts.Path = filepath.Join(t.TempDir(), "audit.jsonl")
	}
	l, err := New(opts)
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l, opts.Path
}

func readEntries(t *testing.T, path string) []Entry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	defer f.Close()
	var entries []Entry
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", len(entries)+1, err)
		}
		entries = append(entries, e)
	}
	return entries
}

func TestLogger_WritesJSONL(t *testing.T) {
	l, path := newTestLogger(t, Options{})
	for i := 0; i < 3; i++ {
		err := l.Log(Entry{
			MessageID: "msg-1", From: "p1", FromName: "orchestrator",
			To: "broadcast", Type: "BROADCAST", PayloadSizeBytes: 42, DeliveryCount: 2,
		})
		if err != nil {
			t.Fatalf("log: %v", err)
		}
	}
	entries := readEntries(t, path)
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if e.Seq != uint64(i+1) {
			t.Errorf("entry %d seq = %d, want %d", i, e.Seq, i+1)
		}
		if e.LoggedAt.IsZero() {
			t.Errorf("entry %d missing logged_at", i)
		}
		if e.FromName != "orchestrator" || e.Type != "BROADCAST" {
			t.Errorf("entry %d fields wrong: %+v", i, e)
		}
	}
}

func TestLogger_PayloadOmittedByDefault(t *testing.T) {
	l, path := newTestLogger(t, Options{})
	if err := l.Log(Entry{MessageID: "m", From: "a", Type: "BROADCAST", Payload: "secret-content"}); err != nil {
		t.Fatalf("log: %v", err)
	}
	data, _ := os.ReadFile(path)
	if strings.Contains(string(data), "secret-content") {
		t.Error("payload content must not be logged by default")
	}
}

func TestLogger_PayloadIncludedWhenEnabled(t *testing.T) {
	l, path := newTestLogger(t, Options{LogPayloads: true})
	if err := l.Log(Entry{MessageID: "m", From: "a", Type: "BROADCAST", Payload: "debug-content"}); err != nil {
		t.Fatalf("log: %v", err)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "debug-content") {
		t.Error("payload must be logged when log_payloads is enabled")
	}
}

func TestLogger_AppendsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l1, _ := newTestLogger(t, Options{Path: path})
	_ = l1.Log(Entry{MessageID: "first", From: "a", Type: "PING"})
	l1.Close()

	l2, _ := newTestLogger(t, Options{Path: path})
	_ = l2.Log(Entry{MessageID: "second", From: "a", Type: "PING"})
	l2.Close()

	if got := len(readEntries(t, path)); got != 2 {
		t.Errorf("reopen must append, want 2 entries got %d", got)
	}
}

func TestLogger_RotateOnStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l1, _ := newTestLogger(t, Options{Path: path})
	_ = l1.Log(Entry{MessageID: "old", From: "a", Type: "PING"})
	l1.Close()

	l2, _ := newTestLogger(t, Options{Path: path, RotateOnStart: true})
	_ = l2.Log(Entry{MessageID: "new", From: "a", Type: "PING"})
	l2.Close()

	entries := readEntries(t, path)
	if len(entries) != 1 || entries[0].MessageID != "new" {
		t.Errorf("rotate_on_start must start a fresh file: %+v", entries)
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Errorf("previous log must be kept as .1 backup: %v", err)
	}
}

func TestLogger_SizeRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(Options{Path: path, MaxSizeMB: 1})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer l.Close()
	// Force the threshold low by writing entries with large From fields is
	// slow at 1MB; instead shrink maxBytes directly — rotation logic is what
	// is under test, not the arithmetic on MaxSizeMB.
	l.maxBytes = 400

	for i := 0; i < 5; i++ {
		if err := l.Log(Entry{MessageID: strings.Repeat("x", 100), From: "a", Type: "PING"}); err != nil {
			t.Fatalf("log %d: %v", i, err)
		}
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Errorf("size rotation must produce a .1 backup: %v", err)
	}
}

func TestLogger_UnwritableDirectory(t *testing.T) {
	if runtime.GOOS != "windows" && os.Geteuid() == 0 {
		t.Skip("running as root; permission bits are not enforced")
	}
	dir := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(dir, 0o500); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	_, err := New(Options{Path: filepath.Join(dir, "sub", "audit.jsonl")})
	if err == nil {
		t.Error("unwritable directory must return an error")
	}
}

func TestLogger_ConcurrentWrites(t *testing.T) {
	l, path := newTestLogger(t, Options{})
	var wg sync.WaitGroup
	const writers, each = 8, 25
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				_ = l.Log(Entry{MessageID: "m", From: "a", Type: "PING"})
			}
		}()
	}
	wg.Wait()
	entries := readEntries(t, path)
	if len(entries) != writers*each {
		t.Fatalf("want %d entries, got %d", writers*each, len(entries))
	}
	seen := make(map[uint64]bool)
	for _, e := range entries {
		if seen[e.Seq] {
			t.Fatalf("duplicate seq %d", e.Seq)
		}
		seen[e.Seq] = true
	}
}
