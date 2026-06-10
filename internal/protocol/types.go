// Package protocol defines the wire format shared by the daemonSeed broker
// and its clients: the message envelope, message type constants, payload
// schemas, sentinel errors, and length-prefixed framing.
package protocol

// MessageType identifies the kind of message carried by an Envelope.
type MessageType string

const (
	// Handshake
	TypeHello       MessageType = "HELLO"
	TypeHelloAck    MessageType = "HELLO_ACK"
	TypeHelloReject MessageType = "HELLO_REJECT"

	// Routing
	TypeBroadcast      MessageType = "BROADCAST"
	TypeDirectMessage  MessageType = "DIRECT_MESSAGE"
	TypeDeliveryFailed MessageType = "DELIVERY_FAILED"

	// Task lifecycle
	TypeAssignTask   MessageType = "ASSIGN_TASK"
	TypeAckTask      MessageType = "ACK_TASK"
	TypeCompleteTask MessageType = "COMPLETE_TASK"

	// Status
	TypeStatusRequest MessageType = "STATUS_REQUEST"
	TypeStatusReport  MessageType = "STATUS_REPORT"
	TypeStatusTimeout MessageType = "STATUS_TIMEOUT"

	// Health
	TypeHeartbeat    MessageType = "HEARTBEAT"
	TypeHeartbeatAck MessageType = "HEARTBEAT_ACK"
	TypePing         MessageType = "PING"
	TypePong         MessageType = "PONG"

	// Shutdown
	TypeShutdownNotice MessageType = "SHUTDOWN_NOTICE"
	TypeShutdownAck    MessageType = "SHUTDOWN_ACK"
	TypeShutdownForce  MessageType = "SHUTDOWN_FORCE"

	// Errors
	TypeInvalidMessage   MessageType = "INVALID_MESSAGE"
	TypePermissionDenied MessageType = "PERMISSION_DENIED"
	TypeMessageTooLarge  MessageType = "MESSAGE_TOO_LARGE"
	TypeNotFound         MessageType = "NOT_FOUND"
	TypeInternalError    MessageType = "INTERNAL_ERROR"
)

// Spec extension: the spec (§7.3) requires synchronous, broker-mediated
// request/response flows (list clients, whoami, task polling, delivery
// confirmation, parent-initiated shutdown, child removal) but defines no
// message types for them. These constants fill that gap. They are routed
// with To set to TargetDaemon and answered by the broker directly; the
// response carries the request envelope's ID in CorrelationID.
const (
	TypeListRequest      MessageType = "LIST_REQUEST"
	TypeListResponse     MessageType = "LIST_RESPONSE"
	TypeWhoAmIRequest    MessageType = "WHOAMI_REQUEST"
	TypeWhoAmIResponse   MessageType = "WHOAMI_RESPONSE"
	TypeGetAssignment    MessageType = "GET_ASSIGNMENT_REQUEST"
	TypeAssignmentResult MessageType = "GET_ASSIGNMENT_RESPONSE"
	TypeDeliveryReceipt  MessageType = "DELIVERY_RECEIPT"
	TypeShutdownRequest  MessageType = "SHUTDOWN_REQUEST"
	TypeShutdownResult   MessageType = "SHUTDOWN_RESULT"
	TypeRemoveChild      MessageType = "REMOVE_CHILD_REQUEST"
	TypeEvent            MessageType = "EVENT"

	// §20.7 hook-based command injection: a hook process drains the named
	// inbox the broker keeps per child. Allowed from observers — the drain
	// CLI cannot connect as the child itself (duplicate name) and the 0600
	// socket is the security boundary.
	TypeInboxDrainRequest  MessageType = "INBOX_DRAIN_REQUEST"
	TypeInboxDrainResponse MessageType = "INBOX_DRAIN_RESPONSE"

	// §20.8 task scheduler (parent-only).
	TypeScheduleCreate   MessageType = "SCHEDULE_CREATE_REQUEST"
	TypeScheduleCreated  MessageType = "SCHEDULE_CREATE_RESPONSE"
	TypeScheduleList     MessageType = "SCHEDULE_LIST_REQUEST"
	TypeScheduleListResp MessageType = "SCHEDULE_LIST_RESPONSE"
	TypeScheduleCancel   MessageType = "SCHEDULE_CANCEL_REQUEST"
	TypeScheduleCanceled MessageType = "SCHEDULE_CANCEL_RESPONSE"
)

// Routing targets understood by the broker (see spec §5.4). Any other To
// value is resolved as a client_id first, then as a registered client name.
const (
	TargetBroadcast = "broadcast"
	TargetParent    = "parent"
	TargetChildren  = "children"
	// TargetDaemon addresses the broker itself (spec extension): used for
	// request/response messages the broker answers rather than routes.
	TargetDaemon = "daemon"
)

// Version is the wire protocol version stamped on every envelope.
const Version = "1"

// DaemonSenderID is the From value used by the broker for messages it
// originates (errors, receipts, notices). It is reserved: client HELLOs
// can never claim it because it contains characters outside the allowed
// name charset semantics enforced at handshake.
const DaemonSenderID = "daemon"
