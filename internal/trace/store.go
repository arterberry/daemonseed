package trace

import (
	"fmt"
	"strings"
)

// Backends.
const (
	BackendJSONL  = "jsonl"
	BackendSQLite = "sqlite"
)

// OpenStore opens the configured backend. When the backend is sqlite but
// the path still carries the JSONL default's .jsonl suffix, the suffix is
// swapped for .db so switching backends needs only one config line.
func OpenStore(backend, path string, maxSizeMB int) (Store, error) {
	switch backend {
	case BackendJSONL, "":
		return NewJSONLStore(path, maxSizeMB)
	case BackendSQLite:
		if strings.HasSuffix(path, ".jsonl") {
			path = strings.TrimSuffix(path, ".jsonl") + ".db"
		}
		return NewSQLiteStore(path)
	default:
		return nil, fmt.Errorf("unknown trace backend %q (want %q or %q)", backend, BackendJSONL, BackendSQLite)
	}
}

// ResolvePath returns the effective storage path for a backend (mirrors the
// suffix swap in OpenStore; used by the CLI viewer).
func ResolvePath(backend, path string) string {
	if backend == BackendSQLite && strings.HasSuffix(path, ".jsonl") {
		return strings.TrimSuffix(path, ".jsonl") + ".db"
	}
	return path
}
