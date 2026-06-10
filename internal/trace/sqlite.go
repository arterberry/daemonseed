package trace

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"
)

// SQLiteStore persists events in a local SQLite database. WAL mode plus a
// busy timeout makes concurrent writers (the daemon and several MCP
// processes) safe; indexes on ts and trace_id keep the viewer fast even
// when the log gets big — the reason this backend exists.
type SQLiteStore struct {
	db *sql.DB
}

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS trace_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    source      TEXT NOT NULL,
    kind        TEXT NOT NULL,
    name        TEXT NOT NULL,
    trace_id    TEXT,
    span_id     TEXT,
    session     TEXT,
    role        TEXT,
    from_name   TEXT,
    to_name     TEXT,
    duration_ms REAL,
    status      TEXT,
    detail      TEXT
);
CREATE INDEX IF NOT EXISTS idx_trace_ts ON trace_events(ts);
CREATE INDEX IF NOT EXISTS idx_trace_trace_id ON trace_events(trace_id);
CREATE INDEX IF NOT EXISTS idx_trace_session ON trace_events(session);
`

// NewSQLiteStore opens (creating if needed) the trace database at path.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create trace dir: %w", err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open trace db: %w", err)
	}
	// One writer connection per process keeps lock contention predictable.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init trace schema: %w", err)
	}
	return &SQLiteStore{db: db}, nil
}

// Write inserts one event.
func (s *SQLiteStore) Write(ev Event) error {
	_, err := s.db.Exec(`
		INSERT INTO trace_events
		  (ts, source, kind, name, trace_id, span_id, session, role, from_name, to_name, duration_ms, status, detail)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.TS.UTC().Format(time.RFC3339Nano), ev.Source, ev.Kind, ev.Name,
		ev.TraceID, ev.SpanID, ev.Session, ev.Role, ev.From, ev.To,
		ev.DurationMs, ev.Status, ev.Detail)
	if err != nil {
		return fmt.Errorf("insert trace event: %w", err)
	}
	return nil
}

// Recent returns the last n matching events, oldest first.
func (s *SQLiteStore) Recent(n int, session, traceID string) ([]Event, error) {
	query := `
		SELECT ts, source, kind, name, trace_id, span_id, session, role,
		       from_name, to_name, duration_ms, status, detail
		FROM trace_events WHERE 1=1`
	var args []any
	if session != "" {
		query += " AND (session = ? OR from_name = ? OR to_name = ?)"
		args = append(args, session, session, session)
	}
	if traceID != "" {
		query += " AND trace_id = ?"
		args = append(args, traceID)
	}
	query += " ORDER BY id DESC LIMIT ?"
	if n <= 0 {
		n = 50
	}
	args = append(args, n)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query trace events: %w", err)
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var ev Event
		var ts string
		if err := rows.Scan(&ts, &ev.Source, &ev.Kind, &ev.Name, &ev.TraceID, &ev.SpanID,
			&ev.Session, &ev.Role, &ev.From, &ev.To, &ev.DurationMs, &ev.Status, &ev.Detail); err != nil {
			return nil, err
		}
		ev.TS, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Reverse to oldest-first to match the JSONL backend.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// Close closes the database.
func (s *SQLiteStore) Close() error { return s.db.Close() }
