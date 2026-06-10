package trace

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// JSONLStore appends one JSON object per line. Multiple processes (the
// daemon plus MCP servers) append to the same file: each Write is a single
// O_APPEND write of one line, which the OS keeps intact for typical line
// sizes. Rotation moves the file to <path>.1 when it exceeds maxBytes.
type JSONLStore struct {
	mu       sync.Mutex
	f        *os.File
	path     string
	size     int64
	maxBytes int64
}

// NewJSONLStore opens (creating if needed) the trace log at path.
func NewJSONLStore(path string, maxSizeMB int) (*JSONLStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create trace dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open trace log: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if maxSizeMB <= 0 {
		maxSizeMB = 100
	}
	return &JSONLStore{
		f:        f,
		path:     path,
		size:     st.Size(),
		maxBytes: int64(maxSizeMB) * 1024 * 1024,
	}, nil
}

// Write appends one event line.
func (s *JSONLStore) Write(ev Event) error {
	line, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal trace event: %w", err)
	}
	line = append(line, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return fmt.Errorf("trace store closed")
	}
	if s.size+int64(len(line)) > s.maxBytes {
		if err := s.rotateLocked(); err != nil {
			return err
		}
	}
	n, err := s.f.Write(line)
	s.size += int64(n)
	return err
}

// Recent reads the file and returns the last n matching events, oldest
// first. Lines another process wrote are picked up because reads go through
// a fresh handle.
func (s *JSONLStore) Recent(n int, session, traceID string) ([]Event, error) {
	return ReadJSONL(s.path, n, session, traceID)
}

// ReadJSONL implements Recent over any JSONL trace file (also used by the
// CLI viewer, which has no Store open).
func ReadJSONL(path string, n int, session, traceID string) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ring []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		var ev Event
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue // tolerate a torn line from a concurrent writer
		}
		if session != "" && ev.Session != session && ev.From != session && ev.To != session {
			continue
		}
		if traceID != "" && ev.TraceID != traceID {
			continue
		}
		if n > 0 && len(ring) == n {
			ring = ring[1:]
		}
		ring = append(ring, ev)
	}
	return ring, sc.Err()
}

// Close closes the file.
func (s *JSONLStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

func (s *JSONLStore) rotateLocked() error {
	if err := s.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(s.path, s.path+".1"); err != nil {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	s.f = f
	s.size = 0
	return nil
}
