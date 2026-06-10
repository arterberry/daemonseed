package protocol

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Envelope is the universal message wrapper for all daemonSeed communication.
//
// CorrelationID is a spec extension: the spec mandates synchronous flows
// (delivery receipts, status requests, daemon queries) but the envelope in
// §10.1 has no correlation mechanism. Responses set CorrelationID to the ID
// of the request envelope they answer. It is omitted from JSON when empty,
// so envelopes for the message shapes defined in the spec serialize exactly
// as specified.
type Envelope struct {
	ID            string      `json:"id"`      // UUID v4
	From          string      `json:"from"`    // client_id of sender
	To            string      `json:"to"`      // routing target (see spec §5.4)
	Type          MessageType `json:"type"`    // message type constant
	Payload       string      `json:"payload"` // JSON string or plain text
	TaskID        string      `json:"task_id,omitempty"`
	Timestamp     time.Time   `json:"timestamp"`
	Version       string      `json:"version"` // protocol version, e.g. "1"
	CorrelationID string      `json:"correlation_id,omitempty"`
}

// NewEnvelope builds an envelope with a fresh UUID, the current timestamp,
// and the current protocol version.
func NewEnvelope(from, to string, typ MessageType, payload string) *Envelope {
	return &Envelope{
		ID:        uuid.NewString(),
		From:      from,
		To:        to,
		Type:      typ,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
		Version:   Version,
	}
}

// Validate checks the structural invariants every inbound envelope must
// satisfy before the broker will route it.
func (e *Envelope) Validate() error {
	if e == nil {
		return fmt.Errorf("%w: nil envelope", ErrMalformedEnvelope)
	}
	if e.From == "" {
		return fmt.Errorf("%w: empty from field", ErrMalformedEnvelope)
	}
	if e.Type == "" {
		return fmt.Errorf("%w: empty type field", ErrMalformedEnvelope)
	}
	return nil
}

// DecodePayload unmarshals the envelope's JSON payload into v.
func (e *Envelope) DecodePayload(v any) error {
	if err := json.Unmarshal([]byte(e.Payload), v); err != nil {
		return fmt.Errorf("decode %s payload: %w", e.Type, err)
	}
	return nil
}

// MustEncode marshals v to a JSON string for use as an envelope payload.
// The payload structs below contain no unmarshalable types, so an error here
// is a programming bug; it is surfaced as a panic in tests via mustJSON.
func MustEncode(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		// Marshaling a plain struct of strings/times cannot fail at runtime;
		// guard anyway so a future field change never silently corrupts a message.
		panic(fmt.Sprintf("protocol: encode payload: %v", err))
	}
	return string(data)
}

// HelloPayload is carried by HELLO.
type HelloPayload struct {
	Role    string `json:"role"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

// HelloAckPayload is carried by HELLO_ACK.
type HelloAckPayload struct {
	ClientID      string `json:"client_id"`
	DaemonVersion string `json:"daemon_version"`
}

// HelloRejectPayload is carried by HELLO_REJECT.
type HelloRejectPayload struct {
	Reason string `json:"reason"`
}

// TaskPayload is carried by ASSIGN_TASK and GET_ASSIGNMENT_RESPONSE.
type TaskPayload struct {
	TaskID       string            `json:"task_id"`
	Instruction  string            `json:"instruction"`
	Context      map[string]string `json:"context,omitempty"`
	DeadlineHint string            `json:"deadline_hint,omitempty"`
	AssignedAt   time.Time         `json:"assigned_at"`
}

// StatusPayload is carried by STATUS_REPORT.
type StatusPayload struct {
	State       string    `json:"state"` // idle|working|blocked|complete|error
	Message     string    `json:"message"`
	CurrentTask string    `json:"current_task,omitempty"`
	ReportedAt  time.Time `json:"reported_at"`
}

// ValidStates enumerates the allowed StatusPayload.State values.
var ValidStates = []string{"idle", "working", "blocked", "complete", "error"}

// IsValidState reports whether s is an allowed status state.
func IsValidState(s string) bool {
	for _, v := range ValidStates {
		if s == v {
			return true
		}
	}
	return false
}

// ErrorPayload is carried by the error message types (INVALID_MESSAGE,
// PERMISSION_DENIED, MESSAGE_TOO_LARGE, NOT_FOUND, INTERNAL_ERROR,
// DELIVERY_FAILED).
type ErrorPayload struct {
	Reason string `json:"reason"`
}

// ShutdownNoticePayload is carried by SHUTDOWN_NOTICE sent to children.
type ShutdownNoticePayload struct {
	Reason         string `json:"reason"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	InitiatedBy    string `json:"initiated_by"`
}

// ShutdownResultPayload is carried by SHUTDOWN_NOTICE/SHUTDOWN_RESULT sent
// to the parent at the end of the cascade.
type ShutdownResultPayload struct {
	ChildrenAcked  []string `json:"children_acked"`
	ChildrenForced []string `json:"children_forced"`
}

// ShutdownRequestPayload is carried by SHUTDOWN_REQUEST (parent → daemon).
type ShutdownRequestPayload struct {
	TimeoutSeconds int `json:"timeout_seconds"`
}

// RemoveChildPayload is carried by REMOVE_CHILD_REQUEST (parent → daemon).
type RemoveChildPayload struct {
	Target string `json:"target"`
}

// DeliveryReceiptPayload is carried by DELIVERY_RECEIPT (daemon → sender).
// Queued reports parent-failover buffering (§20.9): no parent was connected,
// so the message waits in the daemon and is flushed to the next parent.
type DeliveryReceiptPayload struct {
	DeliveredTo []string `json:"delivered_to"`
	Count       int      `json:"count"`
	Queued      bool     `json:"queued,omitempty"`
}

// ClientInfo describes one connected client in LIST_RESPONSE and
// WHOAMI_RESPONSE payloads.
type ClientInfo struct {
	ClientID    string    `json:"client_id"`
	Name        string    `json:"name"`
	Role        string    `json:"role"`
	State       string    `json:"state,omitempty"`
	CurrentTask string    `json:"current_task,omitempty"`
	ConnectedAt time.Time `json:"connected_at"`
	LastSeen    time.Time `json:"last_seen"`
}

// ListRequestPayload is carried by LIST_REQUEST. Filter is "all" or "children".
type ListRequestPayload struct {
	Filter string `json:"filter"`
}

// ListResponsePayload is carried by LIST_RESPONSE.
type ListResponsePayload struct {
	Clients []ClientInfo `json:"clients"`
}

// WhoAmIResponsePayload is carried by WHOAMI_RESPONSE.
type WhoAmIResponsePayload struct {
	ClientInfo
	DaemonVersion string `json:"daemon_version"`
}

// AssignmentResponsePayload is carried by GET_ASSIGNMENT_RESPONSE.
type AssignmentResponsePayload struct {
	Pending bool         `json:"pending"`
	Task    *TaskPayload `json:"task,omitempty"`
}

// CompleteTaskPayload is carried by COMPLETE_TASK.
type CompleteTaskPayload struct {
	TaskID     string `json:"task_id"`
	ResultJSON string `json:"result_json"`
}

// InboxEntry is one buffered message in a child's broker-side named inbox
// (§20.7). The broker records messages routed to a child so a hook process
// can surface them into the child's Claude session.
type InboxEntry struct {
	From       string      `json:"from"` // sender name
	Type       MessageType `json:"type"`
	Payload    string      `json:"payload"`
	TaskID     string      `json:"task_id,omitempty"`
	ReceivedAt time.Time   `json:"received_at"`
}

// InboxDrainRequestPayload is carried by INBOX_DRAIN_REQUEST.
type InboxDrainRequestPayload struct {
	Name string `json:"name"` // child name whose inbox to drain
}

// InboxDrainResponsePayload is carried by INBOX_DRAIN_RESPONSE. Messages are
// cleared by the drain; PendingTasks are a non-destructive peek (cleared by
// bus_acknowledge_task) so unhandled tasks keep resurfacing as reminders.
type InboxDrainResponsePayload struct {
	Messages     []InboxEntry  `json:"messages"`
	PendingTasks []TaskPayload `json:"pending_tasks"`
}

// ScheduleTrigger describes when a schedule fires (§20.8). Exactly one
// field must be set.
type ScheduleTrigger struct {
	At    string `json:"at,omitempty"`    // RFC 3339 one-shot
	Every string `json:"every,omitempty"` // Go duration, e.g. "15m"
	Cron  string `json:"cron,omitempty"`  // standard 5-field cron expression
}

// ScheduleCreatePayload is carried by SCHEDULE_CREATE_REQUEST.
type ScheduleCreatePayload struct {
	Target  string          `json:"target"` // child name
	Task    TaskPayload     `json:"task"`   // template; task_id is generated per fire
	Trigger ScheduleTrigger `json:"trigger"`
	Misfire string          `json:"misfire,omitempty"` // "queue" (default) or "skip"
	TTL     string          `json:"ttl,omitempty"`     // Go duration; queue-policy expiry
}

// ScheduleInfo describes one schedule in responses and snapshots.
type ScheduleInfo struct {
	ID         string          `json:"id"`
	Target     string          `json:"target"`
	Trigger    ScheduleTrigger `json:"trigger"`
	Misfire    string          `json:"misfire"`
	CreatedBy  string          `json:"created_by"`
	CreatedAt  time.Time       `json:"created_at"`
	NextFireAt time.Time       `json:"next_fire_at"`
	FireCount  int             `json:"fire_count"`
}

// ScheduleListPayload is carried by SCHEDULE_LIST_RESPONSE.
type ScheduleListPayload struct {
	Schedules []ScheduleInfo `json:"schedules"`
}

// ScheduleCancelPayload is carried by SCHEDULE_CANCEL_REQUEST.
type ScheduleCancelPayload struct {
	ScheduleID string `json:"schedule_id"`
}

// EventPayload is carried by EVENT envelopes sent to observer connections
// (TUI attach mode, `daemonseed status`). Spec extension: §12.1 requires the
// TUI to attach to a running daemon via the daemon's event stream, which the
// spec does not otherwise define.
type EventPayload struct {
	Kind      string         `json:"kind"` // client_connected|client_disconnected|message|snapshot
	Clients   []ClientInfo   `json:"clients,omitempty"`
	From      string         `json:"from,omitempty"`
	FromName  string         `json:"from_name,omitempty"`
	To        string         `json:"to,omitempty"`
	Type      MessageType    `json:"type,omitempty"`
	Summary   string         `json:"summary,omitempty"`
	Raw       string         `json:"raw,omitempty"`
	At        time.Time      `json:"at"`
	MsgCount  uint64         `json:"msg_count,omitempty"`
	StartedAt time.Time      `json:"started_at,omitempty"`
	Schedules []ScheduleInfo `json:"schedules,omitempty"` // snapshot only (§20.8)
}
