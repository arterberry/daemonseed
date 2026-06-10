package protocol

import "errors"

// Sentinel errors used across daemonSeed. Wrap with fmt.Errorf("...: %w", err)
// to add context; check with errors.Is.
var (
	ErrMessageTooLarge   = errors.New("message exceeds maximum size")
	ErrHandshakeTimeout  = errors.New("handshake not completed within deadline")
	ErrParentExists      = errors.New("a parent client is already connected")
	ErrClientNotFound    = errors.New("target client not found")
	ErrPermissionDenied  = errors.New("role not permitted to use this tool")
	ErrInvalidRole       = errors.New("role must be 'parent' or 'child'")
	ErrMalformedEnvelope = errors.New("message envelope failed validation")
	ErrDaemonNotRunning  = errors.New("daemon is not running")
	ErrMaxClientsReached = errors.New("maximum client limit reached")
	ErrNameTaken         = errors.New("client name is already in use")
	ErrInvalidName       = errors.New("client name must match [a-zA-Z0-9_-]+")
)
