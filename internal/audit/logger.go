// Package audit implements the append-only JSONL audit trail. Every message
// routed through the broker produces exactly one entry (spec §14.1).
package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry is one audit record. Payload content is omitted unless the logger
// was constructed with logPayloads=true (spec §14.1: payloads may contain
// sensitive task content).
type Entry struct {
	Seq              uint64    `json:"seq"`
	LoggedAt         time.Time `json:"logged_at"`
	MessageID        string    `json:"message_id"`
	From             string    `json:"from"`
	FromName         string    `json:"from_name"`
	To               string    `json:"to"`
	Type             string    `json:"type"`
	PayloadSizeBytes int       `json:"payload_size_bytes"`
	DeliveryCount    int       `json:"delivery_count"`
	DeliveryFailed   bool      `json:"delivery_failed"`
	Payload          string    `json:"payload,omitempty"`
}

// Logger writes JSONL entries to an append-only file with size-based
// rotation. Safe for concurrent use.
type Logger struct {
	mu          sync.Mutex
	f           *os.File
	path        string
	size        int64
	maxBytes    int64
	seq         uint64
	logPayloads bool
}

// Options configures a Logger.
type Options struct {
	Path          string
	MaxSizeMB     int
	RotateOnStart bool
	LogPayloads   bool
}

// New opens (creating if needed) the audit log at opts.Path. The parent
// directory is created with 0700. Returns an error if the location is not
// writable — callers decide whether that is fatal (the daemon downgrades to
// audit-disabled with a warning, per spec §17.5 TestAuditLog_FailedWrite).
func New(opts Options) (*Logger, error) {
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o700); err != nil {
		return nil, fmt.Errorf("create audit dir: %w", err)
	}
	if opts.RotateOnStart {
		if _, err := os.Stat(opts.Path); err == nil {
			if err := rotateFile(opts.Path); err != nil {
				return nil, fmt.Errorf("rotate on start: %w", err)
			}
		}
	}
	f, err := os.OpenFile(opts.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat audit log: %w", err)
	}
	maxMB := opts.MaxSizeMB
	if maxMB <= 0 {
		maxMB = 100
	}
	return &Logger{
		f:           f,
		path:        opts.Path,
		size:        st.Size(),
		maxBytes:    int64(maxMB) * 1024 * 1024,
		logPayloads: opts.LogPayloads,
	}, nil
}

// Log assigns the next sequence number and timestamp to e and appends it.
func (l *Logger) Log(e Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	e.Seq = l.seq
	e.LoggedAt = time.Now().UTC()
	if !l.logPayloads {
		e.Payload = ""
	}
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal audit entry: %w", err)
	}
	line = append(line, '\n')
	if l.size+int64(len(line)) > l.maxBytes {
		if err := l.rotateLocked(); err != nil {
			return err
		}
	}
	n, err := l.f.Write(line)
	l.size += int64(n)
	if err != nil {
		return fmt.Errorf("write audit entry: %w", err)
	}
	return nil
}

// LogPayloads reports whether payload content is included in entries.
func (l *Logger) LogPayloads() bool { return l.logPayloads }

// Path returns the active log file path.
func (l *Logger) Path() string { return l.path }

// Close flushes and closes the underlying file.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

// rotateLocked moves the current file to <path>.1 (replacing any previous
// backup) and starts a fresh file. Caller holds l.mu.
func (l *Logger) rotateLocked() error {
	if err := l.f.Close(); err != nil {
		return fmt.Errorf("close for rotation: %w", err)
	}
	if err := rotateFile(l.path); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopen after rotation: %w", err)
	}
	l.f = f
	l.size = 0
	return nil
}

func rotateFile(path string) error {
	backup := path + ".1"
	if err := os.Rename(path, backup); err != nil {
		return fmt.Errorf("rotate %s: %w", path, err)
	}
	return nil
}
