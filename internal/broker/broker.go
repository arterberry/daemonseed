// Package broker implements the daemonSeed message broker: Unix-socket
// listener, client handshake, role enforcement, message routing, audit
// logging, and the graceful shutdown cascade.
package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/arterberry/daemonseed/internal/audit"
	"github.com/arterberry/daemonseed/internal/config"
	"github.com/arterberry/daemonseed/internal/health"
	"github.com/arterberry/daemonseed/internal/protocol"
	"github.com/arterberry/daemonseed/internal/roles"
	"github.com/arterberry/daemonseed/internal/shutdown"
	"github.com/arterberry/daemonseed/internal/trace"
)

// maxParentInbox bounds the child→parent backlog buffered while no parent
// is connected (§20.9 parent failover). Oldest entries are dropped first.
const maxParentInbox = 500

// parentAckTimeout is how long phase 3 of the cascade waits for the parent's
// SHUTDOWN_ACK (fixed at 3s by spec §11.2).
const parentAckTimeout = 3 * time.Second

// pendingStatus tracks one in-flight STATUS_REQUEST (spec §7.3
// bus_get_status: the broker relays and waits).
type pendingStatus struct {
	requesterID string
	timer       *time.Timer
}

// Broker is the daemonSeed message broker.
type Broker struct {
	cfg     *config.Config
	log     *slog.Logger
	audit   *audit.Logger // nil when auditing is disabled
	tracer  *trace.Tracer // nil when tracing is disabled (nil-safe)
	version string

	ln      net.Listener
	reg     *Registry
	roleReg *roles.RoleRegistry
	health  *health.Monitor
	tasks   *taskStore
	inboxes *namedInboxes
	sched   *scheduler
	events  *eventHub

	pendingMu     sync.Mutex
	pendingStatus map[string]*pendingStatus

	cascadeMu sync.Mutex
	cascade   *shutdown.Cascade
	parentAck chan string

	// parentInbox buffers child→parent envelopes while no parent is
	// connected; flushed to the next parent on connect (§20.9).
	parentInboxMu sync.Mutex
	parentInbox   []*protocol.Envelope

	observerMu sync.Mutex
	observers  map[string]func() // client_id → unsubscribe

	msgCount  atomic.Uint64
	startedAt time.Time
	stopping  atomic.Bool

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopped  chan struct{}
}

// New creates a Broker. auditLogger may be nil (auditing disabled).
func New(cfg *config.Config, log *slog.Logger, auditLogger *audit.Logger, version string) *Broker {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		cfg:           cfg,
		log:           log,
		audit:         auditLogger,
		version:       version,
		reg:           NewRegistry(cfg.Limits.MaxClients),
		roleReg:       roles.NewRegistry(),
		tasks:         newTaskStore(cfg.Limits.MaxPendingTasksPerClient),
		inboxes:       newNamedInboxes(),
		events:        newEventHub(),
		pendingStatus: make(map[string]*pendingStatus),
		parentAck:     make(chan string, 4),
		observers:     make(map[string]func()),
		ctx:           ctx,
		cancel:        cancel,
		stopped:       make(chan struct{}),
	}
	b.health = health.New(
		time.Duration(cfg.Timeouts.StaleClientSeconds)*time.Second,
		time.Second,
		b.onStaleClient,
	)
	b.sched = newScheduler(
		time.Duration(cfg.Limits.MinScheduleIntervalSeconds)*time.Second,
		cfg.Limits.MaxSchedules,
		log,
		b.fireSchedule,
	)
	return b
}

// SetTracer attaches a session tracer. Must be called before Start; a nil
// tracer disables tracing (all emit calls are nil-safe).
func (b *Broker) SetTracer(t *trace.Tracer) { b.tracer = t }

// Start binds the socket (spec §5.1) and launches the accept loop and
// health monitor. It returns once the daemon is accepting connections.
func (b *Broker) Start() error {
	path := b.cfg.Daemon.SocketPath
	if _, err := os.Stat(path); err == nil {
		// Something already occupies the path: probe whether a live daemon
		// is listening, otherwise clean up the stale socket (§5.1 step 2).
		conn, err := net.DialTimeout("unix", path, time.Second)
		if err == nil {
			conn.Close()
			return fmt.Errorf("daemonSeed is already running at %s. Run 'daemonseed stop' first", path)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove stale socket %s: %w", path, err)
		}
		b.log.Warn("removed stale socket", "path", path)
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("bind socket %s: %w", path, err)
	}
	// Owner-only access (spec §15.1).
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return fmt.Errorf("chmod socket: %w", err)
	}
	b.ln = ln
	b.startedAt = time.Now().UTC()

	b.wg.Add(3)
	go func() {
		defer b.wg.Done()
		b.health.Start(b.ctx)
	}()
	go func() {
		defer b.wg.Done()
		b.sched.run(b.ctx)
	}()
	go b.acceptLoop()

	b.log.Info("daemonSeed started", "socket", path, "pid", os.Getpid(), "version", b.version)
	return nil
}

// Wait blocks until the broker has fully stopped.
func (b *Broker) Wait() { <-b.stopped }

// Events subscribes to the broker's event stream (used by the in-process
// TUI). The returned cancel function must be called to unsubscribe.
func (b *Broker) Events() (<-chan protocol.EventPayload, func()) {
	return b.events.Subscribe()
}

// Snapshot returns the current client list, schedules, message count, and
// start time.
func (b *Broker) Snapshot() protocol.EventPayload {
	return protocol.EventPayload{
		Kind:      "snapshot",
		Clients:   b.clientInfos(false),
		Schedules: b.sched.list(),
		MsgCount:  b.msgCount.Load(),
		StartedAt: b.startedAt,
		At:        time.Now().UTC(),
	}
}

// SocketPath returns the bound socket path.
func (b *Broker) SocketPath() string { return b.cfg.Daemon.SocketPath }

// ---------------------------------------------------------------------------
// Accept loop & handshake
// ---------------------------------------------------------------------------

func (b *Broker) acceptLoop() {
	defer b.wg.Done()
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return // graceful shutdown, not a crash
			}
			b.log.Error("accept error", "error", err)
			continue
		}
		b.wg.Add(1)
		go b.handleConn(conn)
	}
}

// rejectConn writes HELLO_REJECT directly (the client has no write loop yet)
// and closes the connection.
func (b *Broker) rejectConn(conn net.Conn, reason string) {
	env := protocol.NewEnvelope(protocol.DaemonSenderID, "", protocol.TypeHelloReject,
		protocol.MustEncode(protocol.HelloRejectPayload{Reason: reason}))
	if err := protocol.WriteMessage(conn, env, b.cfg.Limits.MaxMessageBytes); err != nil {
		b.log.Warn("failed to send HELLO_REJECT", "error", err)
	}
	conn.Close()
	b.log.Warn("connection rejected", "reason", reason)
}

func (b *Broker) handleConn(conn net.Conn) {
	defer b.wg.Done()

	// The client must complete HELLO within the handshake timeout (§5.3).
	deadline := time.Now().Add(time.Duration(b.cfg.Timeouts.HandshakeSeconds) * time.Second)
	if err := conn.SetReadDeadline(deadline); err != nil {
		conn.Close()
		return
	}
	env, err := protocol.ReadMessage(conn, b.cfg.Limits.MaxMessageBytes)
	if err != nil {
		b.log.Warn("handshake failed before HELLO", "error", err)
		conn.Close()
		return
	}
	if env.Type != protocol.TypeHello {
		b.rejectConn(conn, "first message must be HELLO")
		return
	}
	var hello protocol.HelloPayload
	if err := env.DecodePayload(&hello); err != nil {
		b.rejectConn(conn, "malformed HELLO payload")
		return
	}
	role := roles.Role(hello.Role)
	if !role.Valid() {
		b.rejectConn(conn, protocol.ErrInvalidRole.Error())
		return
	}
	if !roles.ValidName(hello.Name) {
		b.rejectConn(conn, protocol.ErrInvalidName.Error())
		return
	}
	// Client version policy (documented choice for §17.5
	// TestBroker_FutureProtocolVersion): any version is accepted; an
	// unexpected one is logged as a warning rather than rejected, since the
	// wire format is versioned per-envelope and v1 has no breaking variants.
	if hello.Version != "" && hello.Version != b.version && env.Version != protocol.Version {
		b.log.Warn("client protocol version differs", "client_version", hello.Version,
			"envelope_version", env.Version, "name", hello.Name)
	}

	id := uuid.NewString()
	client := newClient(id, conn, hello, role, b.cfg.Limits.MaxMessageBytes)

	// One-parent rule: claimed under the role registry's mutex (§6.2,
	// Appendix B.8), rolled back if registry admission then fails.
	if role == roles.RoleParent {
		if err := b.roleReg.RegisterParent(id); err != nil {
			b.rejectConn(conn, "a parent is already connected")
			return
		}
	}
	if err := b.reg.Add(client); err != nil {
		if role == roles.RoleParent {
			b.roleReg.Deregister(id)
		}
		b.rejectConn(conn, err.Error())
		return
	}
	if role == roles.RoleChild {
		b.roleReg.RegisterChild(id, hello.Name)
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		b.cleanupClient(client)
		return
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		client.writeLoop()
	}()

	ack := protocol.NewEnvelope(protocol.DaemonSenderID, id, protocol.TypeHelloAck,
		protocol.MustEncode(protocol.HelloAckPayload{ClientID: id, DaemonVersion: b.version}))
	ack.CorrelationID = env.ID
	client.Send(ack)

	b.health.Touch(id)
	b.log.Info("client connected", "client_id", id, "name", hello.Name, "role", string(role))

	if role == roles.RoleObserver {
		b.attachObserver(client)
	} else {
		b.publishClientEvent("client_connected")
		b.tracer.Emit(trace.Event{
			Kind: trace.KindLifecycle, Name: "client_connected",
			Session: client.Name, Role: string(role), Status: trace.StatusOK,
		})
	}
	if role == roles.RoleParent {
		b.onParentConnected(client)
	}

	b.readLoop(client)
	b.cleanupClient(client)
}

// onParentConnected implements parent failover (§20.9): the new parent
// receives the child→parent backlog buffered while no parent was connected,
// and children are told a parent is available again.
func (b *Broker) onParentConnected(parent *Client) {
	b.parentInboxMu.Lock()
	backlog := b.parentInbox
	b.parentInbox = nil
	b.parentInboxMu.Unlock()

	for _, env := range backlog {
		if !parent.Send(env) {
			b.log.Warn("parent backlog message dropped on flush", "message_id", env.ID, "type", string(env.Type))
		}
	}
	if len(backlog) > 0 {
		b.log.Info("flushed buffered child messages to new parent",
			"parent", parent.Name, "count", len(backlog))
	}
	b.tracer.Emit(trace.Event{
		Kind: trace.KindLifecycle, Name: "parent_takeover",
		Session: parent.Name, Role: "parent", Status: trace.StatusOK,
		Detail: fmt.Sprintf("flushed %d buffered messages", len(backlog)),
	})

	notice := protocol.NewEnvelope(protocol.DaemonSenderID, protocol.TargetChildren,
		protocol.TypeDirectMessage, "parent connected")
	for _, child := range b.reg.Children() {
		if !child.Send(notice) {
			b.log.Warn("could not notify child of parent connect", "child", child.Name)
		}
	}
}

// bufferForParent queues a child→parent envelope while no parent is
// connected and acknowledges the sender with a queued receipt.
func (b *Broker) bufferForParent(c *Client, env *protocol.Envelope) {
	fwd := *env
	fwd.CorrelationID = "" // child-side correlation must not leak to the parent
	b.parentInboxMu.Lock()
	if len(b.parentInbox) >= maxParentInbox {
		dropped := b.parentInbox[0]
		b.parentInbox = b.parentInbox[1:]
		b.log.Warn("parent inbox full; dropping oldest", "message_id", dropped.ID)
	}
	b.parentInbox = append(b.parentInbox, &fwd)
	b.parentInboxMu.Unlock()

	b.tracer.Emit(trace.Event{
		Kind: trace.KindMessage, Name: string(env.Type),
		TraceID: env.ID, SpanID: env.ID,
		From: c.Name, To: protocol.TargetParent,
		Status: trace.StatusQueued, Detail: env.Payload,
	})
	b.auditEnvelope(env, c.Name, 0, false)
	b.reply(c, env.ID, protocol.TypeDeliveryReceipt,
		protocol.MustEncode(protocol.DeliveryReceiptPayload{Queued: true}), env.TaskID)
}

// attachObserver streams broker events to an observer connection as EVENT
// envelopes, starting with a snapshot.
func (b *Broker) attachObserver(c *Client) {
	c.Send(b.eventEnvelope(c.ID, b.Snapshot()))
	ch, unsub := b.events.Subscribe()
	b.observerMu.Lock()
	b.observers[c.ID] = unsub
	b.observerMu.Unlock()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for ev := range ch {
			if !c.Send(b.eventEnvelope(c.ID, ev)) && c.closed.Load() {
				// Observer is gone; drain until unsubscribed at cleanup.
				continue
			}
		}
	}()
}

func (b *Broker) eventEnvelope(to string, ev protocol.EventPayload) *protocol.Envelope {
	return protocol.NewEnvelope(protocol.DaemonSenderID, to, protocol.TypeEvent, protocol.MustEncode(ev))
}

func (b *Broker) readLoop(c *Client) {
	for {
		env, err := protocol.ReadMessage(c.conn, b.cfg.Limits.MaxMessageBytes)
		if err != nil {
			switch {
			case errors.Is(err, protocol.ErrMalformedEnvelope):
				// Frame consumed; the stream is intact (§16.1 rule 4).
				b.sendError(c, protocol.TypeInvalidMessage, "", "malformed message: not valid JSON")
				continue
			case errors.Is(err, protocol.ErrMessageTooLarge):
				// ReadMessage drained the oversized body, so the stream is
				// still framed: report and keep serving the connection.
				b.sendError(c, protocol.TypeMessageTooLarge, "",
					fmt.Sprintf("message exceeds limit of %d bytes", b.cfg.Limits.MaxMessageBytes))
				continue
			case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
				return // clean disconnect or local close
			case errors.Is(err, io.ErrUnexpectedEOF):
				b.log.Warn("client sent truncated frame", "client_id", c.ID, "name", c.Name)
				return
			default:
				if !b.stopping.Load() {
					b.log.Warn("read error", "client_id", c.ID, "error", err)
				}
				return
			}
		}
		b.health.Touch(c.ID)
		b.dispatch(c, env)
	}
}

func (b *Broker) cleanupClient(c *Client) {
	b.reg.Remove(c.ID)
	b.roleReg.Deregister(c.ID)
	b.health.Remove(c.ID)
	// Pending tasks deliberately survive the disconnect: queues are keyed by
	// name, so a reconnecting child (or the scheduler's queue policy) picks
	// up where it left off.

	b.observerMu.Lock()
	if unsub, ok := b.observers[c.ID]; ok {
		delete(b.observers, c.ID)
		unsub()
	}
	b.observerMu.Unlock()

	b.cascadeMu.Lock()
	if b.cascade != nil {
		b.cascade.ClientGone(c.ID)
	}
	b.cascadeMu.Unlock()

	c.Close()
	if c.Role != roles.RoleObserver {
		b.log.Info("client disconnected", "client_id", c.ID, "name", c.Name, "role", string(c.Role))
		b.publishClientEvent("client_disconnected")
		b.tracer.Emit(trace.Event{
			Kind: trace.KindLifecycle, Name: "client_disconnected",
			Session: c.Name, Role: string(c.Role), Status: trace.StatusOK,
		})
	}

	// Spec §11.1: when the parent vanishes unexpectedly, children are told.
	if c.Role == roles.RoleParent && !b.stopping.Load() {
		notice := protocol.NewEnvelope(protocol.DaemonSenderID, protocol.TargetChildren,
			protocol.TypeDirectMessage, "parent disconnected")
		for _, child := range b.reg.Children() {
			if !child.Send(notice) {
				b.log.Warn("could not notify child of parent disconnect", "child", child.Name)
			}
		}
	}
}

func (b *Broker) onStaleClient(clientID string) {
	if c, ok := b.reg.Get(clientID); ok {
		b.log.Warn("disconnecting stale client", "client_id", clientID, "name", c.Name)
		c.Close() // read loop unblocks; cleanupClient does the rest
	}
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

// typeRoles maps role-restricted message types to the role allowed to send
// them. Enforcement happens here in the broker — not in the MCP server — so
// crafted raw socket messages cannot bypass it (spec §15.3).
var typeRoles = map[protocol.MessageType]roles.Role{
	protocol.TypeBroadcast:       roles.RoleParent,
	protocol.TypeAssignTask:      roles.RoleParent,
	protocol.TypeStatusRequest:   roles.RoleParent,
	protocol.TypeShutdownRequest: roles.RoleParent,
	protocol.TypeRemoveChild:     roles.RoleParent,
	protocol.TypeStatusReport:    roles.RoleChild,
	protocol.TypeAckTask:         roles.RoleChild,
	protocol.TypeCompleteTask:    roles.RoleChild,
	protocol.TypeGetAssignment:   roles.RoleChild,
	protocol.TypeScheduleCreate:  roles.RoleParent,
	protocol.TypeScheduleList:    roles.RoleParent,
	protocol.TypeScheduleCancel:  roles.RoleParent,
}

// observerAllowed lists the only message types an observer may send.
// INBOX_DRAIN_REQUEST is included because the §20.7 hook CLI connects as an
// observer (it cannot claim the child's name while the child is connected).
var observerAllowed = map[protocol.MessageType]bool{
	protocol.TypePing:              true,
	protocol.TypeHeartbeat:         true,
	protocol.TypeListRequest:       true,
	protocol.TypeWhoAmIRequest:     true,
	protocol.TypeInboxDrainRequest: true,
}

func (b *Broker) dispatch(c *Client, env *protocol.Envelope) {
	b.msgCount.Add(1)

	if err := env.Validate(); err != nil {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID, err.Error())
		b.auditEnvelope(env, c.Name, 0, true)
		return
	}
	// Anti-spoofing: From must be the sender's own assigned client_id.
	if env.From != c.ID {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID,
			"from field must match your assigned client_id")
		b.auditEnvelope(env, c.Name, 0, true)
		return
	}
	if c.Role == roles.RoleObserver && !observerAllowed[env.Type] {
		b.sendError(c, protocol.TypePermissionDenied, env.ID, "observers are read-only")
		return
	}
	if want, restricted := typeRoles[env.Type]; restricted && c.Role != want {
		b.sendError(c, protocol.TypePermissionDenied, env.ID,
			fmt.Sprintf("%s requires role %q, connection has role %q", env.Type, want, c.Role))
		b.auditEnvelope(env, c.Name, 0, true)
		return
	}

	switch env.Type {
	case protocol.TypePing:
		b.reply(c, env.ID, protocol.TypePong, "", "")
	case protocol.TypeHeartbeat:
		b.reply(c, env.ID, protocol.TypeHeartbeatAck, "", "")
	case protocol.TypeListRequest:
		b.handleList(c, env)
	case protocol.TypeWhoAmIRequest:
		b.handleWhoAmI(c, env)
	case protocol.TypeGetAssignment:
		b.handleGetAssignment(c, env)
	case protocol.TypeBroadcast:
		b.handleBroadcast(c, env)
	case protocol.TypeDirectMessage:
		b.handleDirect(c, env)
	case protocol.TypeAssignTask:
		b.handleAssignTask(c, env)
	case protocol.TypeAckTask, protocol.TypeCompleteTask:
		b.handleTaskUpdate(c, env)
	case protocol.TypeStatusReport:
		b.handleStatusReport(c, env)
	case protocol.TypeStatusRequest:
		b.handleStatusRequest(c, env)
	case protocol.TypeShutdownRequest:
		b.handleShutdownRequest(c, env)
	case protocol.TypeRemoveChild:
		b.handleRemoveChild(c, env)
	case protocol.TypeShutdownAck:
		b.handleShutdownAck(c)
	case protocol.TypeInboxDrainRequest:
		b.handleInboxDrain(c, env)
	case protocol.TypeScheduleCreate:
		b.handleScheduleCreate(c, env)
	case protocol.TypeScheduleList:
		b.reply(c, env.ID, protocol.TypeScheduleListResp,
			protocol.MustEncode(protocol.ScheduleListPayload{Schedules: b.sched.list()}), "")
	case protocol.TypeScheduleCancel:
		b.handleScheduleCancel(c, env)
	case protocol.TypePong, protocol.TypeHeartbeatAck:
		// Valid but informational; last-seen was already refreshed.
	default:
		b.sendError(c, protocol.TypeInvalidMessage, env.ID,
			fmt.Sprintf("unsupported message type %q", env.Type))
	}
}

// reply sends a daemon-originated response correlated to a request.
func (b *Broker) reply(c *Client, corrID string, typ protocol.MessageType, payload, taskID string) {
	env := protocol.NewEnvelope(protocol.DaemonSenderID, c.ID, typ, payload)
	env.CorrelationID = corrID
	env.TaskID = taskID
	if !c.Send(env) {
		b.log.Warn("could not deliver reply", "client_id", c.ID, "type", string(typ))
	}
}

func (b *Broker) sendError(c *Client, typ protocol.MessageType, corrID, reason string) {
	b.log.Warn("rejecting message", "client_id", c.ID, "name", c.Name,
		"error_type", string(typ), "reason", reason)
	b.tracer.Emit(trace.Event{
		Kind: trace.KindMessage, Name: string(typ),
		TraceID: corrID, From: protocol.DaemonSenderID, To: c.Name,
		Status: trace.StatusError, Detail: reason,
	})
	b.reply(c, corrID, typ, protocol.MustEncode(protocol.ErrorPayload{Reason: reason}), "")
}

func (b *Broker) clientInfos(childrenOnly bool) []protocol.ClientInfo {
	var clients []*Client
	if childrenOnly {
		clients = b.reg.Children()
	} else {
		clients = b.reg.All()
	}
	infos := make([]protocol.ClientInfo, 0, len(clients))
	for _, c := range clients {
		lastSeen, _ := b.health.LastSeen(c.ID)
		infos = append(infos, c.Info(lastSeen))
	}
	return infos
}

func (b *Broker) handleList(c *Client, env *protocol.Envelope) {
	var req protocol.ListRequestPayload
	_ = env.DecodePayload(&req) // empty payload means "all"
	payload := protocol.MustEncode(protocol.ListResponsePayload{
		Clients: b.clientInfos(req.Filter == "children"),
	})
	b.reply(c, env.ID, protocol.TypeListResponse, payload, "")
}

func (b *Broker) handleWhoAmI(c *Client, env *protocol.Envelope) {
	lastSeen, _ := b.health.LastSeen(c.ID)
	payload := protocol.MustEncode(protocol.WhoAmIResponsePayload{
		ClientInfo:    c.Info(lastSeen),
		DaemonVersion: b.version,
	})
	b.reply(c, env.ID, protocol.TypeWhoAmIResponse, payload, "")
}

func (b *Broker) handleGetAssignment(c *Client, env *protocol.Envelope) {
	resp := protocol.AssignmentResponsePayload{}
	if task, ok := b.tasks.next(c.Name); ok {
		resp.Pending = true
		resp.Task = &task
	}
	b.reply(c, env.ID, protocol.TypeAssignmentResult, protocol.MustEncode(resp), "")
}

// deliver fans env out to targets, returning the names reached. Failures
// (full outbound buffers) are logged and reported to the sender via
// DELIVERY_FAILED, never dropped silently (spec §5.4, Appendix B.3).
func (b *Broker) deliver(sender *Client, env *protocol.Envelope, targets []*Client) []string {
	var delivered []string
	var failed []string
	for _, t := range targets {
		if t.Send(env) {
			delivered = append(delivered, t.Name)
			// §20.7: mirror messages routed to children into their named
			// inbox so the hook CLI can surface them into the session.
			if t.Role == roles.RoleChild {
				switch env.Type {
				case protocol.TypeDirectMessage, protocol.TypeBroadcast, protocol.TypeAssignTask:
					b.inboxes.record(t.Name, sender.Name, env)
				}
			}
		} else {
			failed = append(failed, t.Name)
			b.log.Warn("dropping message for slow or closed client",
				"target", t.Name, "type", string(env.Type), "message_id", env.ID)
		}
	}
	if len(failed) > 0 {
		b.sendError(sender, protocol.TypeDeliveryFailed, env.ID,
			fmt.Sprintf("could not deliver to: %v (outbound queue full or closing)", failed))
	}
	b.publishMessageEvent(sender, env, delivered)
	b.auditEnvelope(env, sender.Name, len(delivered), len(delivered) == 0 && len(targets) > 0)

	// §20.10 session trace: one event per routed communication. TraceID
	// follows the request/response chain when one exists.
	traceID := env.CorrelationID
	if traceID == "" {
		traceID = env.ID
	}
	status := trace.StatusOK
	if len(delivered) == 0 && len(targets) > 0 {
		status = trace.StatusDropped
	}
	b.tracer.Emit(trace.Event{
		Kind: trace.KindMessage, Name: string(env.Type),
		TraceID: traceID, SpanID: env.ID,
		From: sender.Name, To: strings.Join(delivered, ","),
		Status: status, Detail: env.Payload,
	})
	return delivered
}

// resolveTargets maps env.To to concrete clients per the §5.4 routing table.
func (b *Broker) resolveTargets(sender *Client, to string) ([]*Client, error) {
	switch to {
	case protocol.TargetBroadcast:
		var out []*Client
		for _, c := range b.reg.All() {
			if c.ID != sender.ID {
				out = append(out, c)
			}
		}
		return out, nil
	case protocol.TargetParent:
		pid, ok := b.roleReg.ParentID()
		if !ok {
			return nil, fmt.Errorf("%w: no parent connected", protocol.ErrClientNotFound)
		}
		p, ok := b.reg.Get(pid)
		if !ok {
			return nil, fmt.Errorf("%w: no parent connected", protocol.ErrClientNotFound)
		}
		return []*Client{p}, nil
	case protocol.TargetChildren:
		return b.reg.Children(), nil
	case "", protocol.TargetDaemon:
		return nil, fmt.Errorf("%w: %q is not a routable target", protocol.ErrClientNotFound, to)
	default:
		c, ok := b.reg.Resolve(to)
		if !ok || c.Role == roles.RoleObserver {
			return nil, fmt.Errorf("%w: %q", protocol.ErrClientNotFound, to)
		}
		return []*Client{c}, nil
	}
}

func (b *Broker) handleBroadcast(c *Client, env *protocol.Envelope) {
	targets, _ := b.resolveTargets(c, protocol.TargetBroadcast)
	delivered := b.deliver(c, env, targets)
	b.reply(c, env.ID, protocol.TypeDeliveryReceipt,
		protocol.MustEncode(protocol.DeliveryReceiptPayload{DeliveredTo: delivered, Count: len(delivered)}), "")
}

func (b *Broker) handleDirect(c *Client, env *protocol.Envelope) {
	// Children may only message the parent (§6.3 exposes no child→child
	// tool); the broker enforces it so raw socket clients cannot bypass it.
	if c.Role == roles.RoleChild && env.To != protocol.TargetParent {
		if pid, ok := b.roleReg.ParentID(); !ok || env.To != pid {
			b.sendError(c, protocol.TypePermissionDenied, env.ID,
				"children may only send direct messages to the parent")
			return
		}
	}
	targets, err := b.resolveTargets(c, env.To)
	if err != nil {
		// §20.9 parent failover: a child's message to an absent parent is
		// buffered for the next parent instead of failing.
		if c.Role == roles.RoleChild && env.To == protocol.TargetParent {
			b.bufferForParent(c, env)
			return
		}
		b.sendError(c, protocol.TypeDeliveryFailed, env.ID, err.Error())
		b.auditEnvelope(env, c.Name, 0, true)
		return
	}
	delivered := b.deliver(c, env, targets)
	if len(delivered) > 0 {
		b.reply(c, env.ID, protocol.TypeDeliveryReceipt,
			protocol.MustEncode(protocol.DeliveryReceiptPayload{DeliveredTo: delivered, Count: len(delivered)}), "")
	}
}

func (b *Broker) handleAssignTask(c *Client, env *protocol.Envelope) {
	var task protocol.TaskPayload
	if err := env.DecodePayload(&task); err != nil || task.TaskID == "" || task.Instruction == "" {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID,
			"task payload must be JSON with non-empty task_id and instruction")
		return
	}
	if task.AssignedAt.IsZero() {
		task.AssignedAt = time.Now().UTC()
	}
	target, ok := b.reg.Resolve(env.To)
	if !ok || target.Role != roles.RoleChild {
		b.sendError(c, protocol.TypeDeliveryFailed, env.ID,
			fmt.Sprintf("target child %q not found", env.To))
		b.auditEnvelope(env, c.Name, 0, true)
		return
	}
	if err := b.tasks.assign(target.Name, task, time.Time{}); err != nil {
		b.sendError(c, protocol.TypeDeliveryFailed, env.ID, err.Error())
		return
	}
	fwd := *env
	fwd.To = target.ID
	fwd.TaskID = task.TaskID
	fwd.Payload = protocol.MustEncode(task) // normalized (assigned_at stamped)
	delivered := b.deliver(c, &fwd, []*Client{target})
	if len(delivered) > 0 {
		b.reply(c, env.ID, protocol.TypeDeliveryReceipt,
			protocol.MustEncode(protocol.DeliveryReceiptPayload{DeliveredTo: delivered, Count: 1}),
			task.TaskID)
	}
}

// handleTaskUpdate forwards ACK_TASK / COMPLETE_TASK from a child to the
// parent and updates the pending-task store.
func (b *Broker) handleTaskUpdate(c *Client, env *protocol.Envelope) {
	if env.TaskID == "" {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID, "task_id is required")
		return
	}
	b.tasks.acknowledge(c.Name, env.TaskID)
	targets, err := b.resolveTargets(c, protocol.TargetParent)
	if err != nil {
		// Acknowledged locally; the update waits for the next parent (§20.9).
		b.bufferForParent(c, env)
		return
	}
	fwd := *env
	fwd.CorrelationID = "" // child-side correlation must not leak to the parent
	delivered := b.deliver(c, &fwd, targets)
	if len(delivered) > 0 {
		b.reply(c, env.ID, protocol.TypeDeliveryReceipt,
			protocol.MustEncode(protocol.DeliveryReceiptPayload{DeliveredTo: delivered, Count: 1}),
			env.TaskID)
	}
}

func (b *Broker) handleStatusReport(c *Client, env *protocol.Envelope) {
	var status protocol.StatusPayload
	if err := env.DecodePayload(&status); err != nil || !protocol.IsValidState(status.State) {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID,
			fmt.Sprintf("status payload must be JSON with state one of %v", protocol.ValidStates))
		return
	}
	c.SetStatus(status.State, status.CurrentTask)

	// A correlated report answers a pending STATUS_REQUEST (§7.3).
	if env.CorrelationID != "" {
		b.pendingMu.Lock()
		pend, ok := b.pendingStatus[env.CorrelationID]
		if ok {
			delete(b.pendingStatus, env.CorrelationID)
			pend.timer.Stop()
		}
		b.pendingMu.Unlock()
		if ok {
			if requester, found := b.reg.Get(pend.requesterID); found {
				b.deliver(c, env, []*Client{requester})
			}
			return
		}
		// Fall through: a stale correlation is treated as a plain report.
	}

	targets, err := b.resolveTargets(c, protocol.TargetParent)
	if err != nil {
		// State is cached on the connection; the report itself waits for
		// the next parent (§20.9).
		b.bufferForParent(c, env)
		return
	}
	delivered := b.deliver(c, env, targets)
	if len(delivered) > 0 {
		b.reply(c, env.ID, protocol.TypeDeliveryReceipt,
			protocol.MustEncode(protocol.DeliveryReceiptPayload{DeliveredTo: delivered, Count: 1}), "")
	}
}

func (b *Broker) handleStatusRequest(c *Client, env *protocol.Envelope) {
	target, ok := b.reg.Resolve(env.To)
	if !ok || target.Role != roles.RoleChild {
		b.sendError(c, protocol.TypeDeliveryFailed, env.ID,
			fmt.Sprintf("target child %q not found", env.To))
		return
	}
	timeout := time.Duration(b.cfg.Timeouts.StatusRequestSeconds) * time.Second
	reqID := env.ID
	requesterID := c.ID

	b.pendingMu.Lock()
	b.pendingStatus[reqID] = &pendingStatus{
		requesterID: requesterID,
		timer: time.AfterFunc(timeout, func() {
			b.pendingMu.Lock()
			_, still := b.pendingStatus[reqID]
			delete(b.pendingStatus, reqID)
			b.pendingMu.Unlock()
			if !still {
				return
			}
			b.log.Warn("status request timed out", "target", target.Name, "request_id", reqID)
			if requester, found := b.reg.Get(requesterID); found {
				b.reply(requester, reqID, protocol.TypeStatusTimeout,
					protocol.MustEncode(protocol.ErrorPayload{
						Reason: fmt.Sprintf("no status from %q within %s", target.Name, timeout),
					}), "")
			}
		}),
	}
	b.pendingMu.Unlock()

	fwd := *env
	fwd.To = target.ID
	fwd.CorrelationID = reqID // the child echoes this on its STATUS_REPORT
	if delivered := b.deliver(c, &fwd, []*Client{target}); len(delivered) == 0 {
		// Could not even queue the request; cancel the pending wait now.
		b.pendingMu.Lock()
		if pend, ok := b.pendingStatus[reqID]; ok {
			pend.timer.Stop()
			delete(b.pendingStatus, reqID)
		}
		b.pendingMu.Unlock()
	}
}

func (b *Broker) handleShutdownRequest(c *Client, env *protocol.Envelope) {
	var req protocol.ShutdownRequestPayload
	_ = env.DecodePayload(&req)
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(b.cfg.Timeouts.ShutdownAckSeconds) * time.Second
	}
	b.auditEnvelope(env, c.Name, 0, false)
	// Run asynchronously: this dispatch must return so the parent's read
	// loop can deliver its SHUTDOWN_ACK during phase 3. Deliberately NOT
	// tracked in b.wg — Shutdown itself waits on that group in phase 4, and
	// its completion is already observable via the stopped channel.
	go b.Shutdown("parent requested shutdown", timeout, "parent:"+c.Name, env.ID)
}

func (b *Broker) handleRemoveChild(c *Client, env *protocol.Envelope) {
	var req protocol.RemoveChildPayload
	if err := env.DecodePayload(&req); err != nil || req.Target == "" {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID, "payload must be JSON with a target field")
		return
	}
	target, ok := b.reg.Resolve(req.Target)
	if !ok || target.Role != roles.RoleChild {
		b.sendError(c, protocol.TypeNotFound, env.ID,
			fmt.Sprintf("child %q not found", req.Target))
		return
	}
	notice := protocol.NewEnvelope(protocol.DaemonSenderID, target.ID, protocol.TypeShutdownNotice,
		protocol.MustEncode(protocol.ShutdownNoticePayload{
			Reason:      "removed by parent",
			InitiatedBy: "parent:" + c.Name,
		}))
	target.Send(notice)
	target.Close() // write loop flushes the notice before the socket closes
	b.auditEnvelope(env, c.Name, 1, false)
	b.reply(c, env.ID, protocol.TypeDeliveryReceipt,
		protocol.MustEncode(protocol.DeliveryReceiptPayload{DeliveredTo: []string{target.Name}, Count: 1}), "")
}

// handleInboxDrain serves the §20.7 hook CLI: returns (and by default
// clears) the named inbox plus a peek at pending tasks. Observers and the
// parent may drain any child; a child may only drain itself.
func (b *Broker) handleInboxDrain(c *Client, env *protocol.Envelope) {
	var req struct {
		protocol.InboxDrainRequestPayload
		Peek bool `json:"peek,omitempty"`
	}
	if err := env.DecodePayload(&req); err != nil || req.Name == "" {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID, "payload must be JSON with a name field")
		return
	}
	if c.Role == roles.RoleChild && req.Name != c.Name {
		b.sendError(c, protocol.TypePermissionDenied, env.ID, "children may only drain their own inbox")
		return
	}
	resp := protocol.InboxDrainResponsePayload{
		PendingTasks: b.tasks.all(req.Name),
	}
	if req.Peek {
		// Peek is implemented as drain-and-restore-free: messages stay put.
		// (The named inbox only mutates on a real drain.)
		resp.Messages = nil
	} else {
		resp.Messages = b.inboxes.drain(req.Name)
	}
	b.log.Info("inbox drained", "name", req.Name, "messages", len(resp.Messages),
		"pending_tasks", len(resp.PendingTasks), "by_role", string(c.Role), "peek", req.Peek)
	b.reply(c, env.ID, protocol.TypeInboxDrainResponse, protocol.MustEncode(resp), "")
}

func (b *Broker) handleScheduleCreate(c *Client, env *protocol.Envelope) {
	var req protocol.ScheduleCreatePayload
	if err := env.DecodePayload(&req); err != nil {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID, "payload must be a JSON schedule definition")
		return
	}
	if !roles.ValidName(req.Target) {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID, protocol.ErrInvalidName.Error())
		return
	}
	info, err := b.sched.add(req, "parent:"+c.Name)
	if err != nil {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID, err.Error())
		return
	}
	b.auditEnvelope(env, c.Name, 0, false)
	b.log.Info("schedule created", "schedule_id", info.ID, "target", info.Target,
		"trigger", info.Trigger, "created_by", info.CreatedBy)
	b.reply(c, env.ID, protocol.TypeScheduleCreated, protocol.MustEncode(info), "")
}

func (b *Broker) handleScheduleCancel(c *Client, env *protocol.Envelope) {
	var req protocol.ScheduleCancelPayload
	if err := env.DecodePayload(&req); err != nil || req.ScheduleID == "" {
		b.sendError(c, protocol.TypeInvalidMessage, env.ID, "payload must be JSON with a schedule_id field")
		return
	}
	if !b.sched.cancel(req.ScheduleID) {
		b.sendError(c, protocol.TypeNotFound, env.ID,
			fmt.Sprintf("schedule %q not found", req.ScheduleID))
		return
	}
	b.auditEnvelope(env, c.Name, 0, false)
	b.log.Info("schedule canceled", "schedule_id", req.ScheduleID, "by", c.Name)
	b.reply(c, env.ID, protocol.TypeScheduleCanceled,
		protocol.MustEncode(map[string]string{"canceled": req.ScheduleID}), "")
}

// fireSchedule delivers one schedule occurrence (§20.8): the task is queued
// (or skipped, per misfire policy) and pushed as a normal ASSIGN_TASK when
// the child is connected. Every fire is audited.
func (b *Broker) fireSchedule(s *schedule, task protocol.TaskPayload) {
	target, connected := b.reg.GetByName(s.info.Target)
	if !connected && s.info.Misfire == misfireSkip {
		b.log.Warn("schedule fired but child offline; skipping occurrence",
			"schedule_id", s.info.ID, "target", s.info.Target, "task_id", task.TaskID)
		return
	}
	expiry := time.Now().Add(s.queueTTL())
	if err := b.tasks.assign(s.info.Target, task, expiry); err != nil {
		b.log.Error("scheduled task dropped: queue full",
			"schedule_id", s.info.ID, "target", s.info.Target, "error", err)
		return
	}

	env := protocol.NewEnvelope(protocol.DaemonSenderID, s.info.Target,
		protocol.TypeAssignTask, protocol.MustEncode(task))
	env.TaskID = task.TaskID
	if connected {
		if target.Send(env) {
			b.inboxes.record(target.Name, "scheduler", env)
		} else {
			b.log.Warn("scheduled task queued but push undeliverable",
				"schedule_id", s.info.ID, "target", s.info.Target)
		}
	} else {
		b.log.Info("scheduled task queued for offline child",
			"schedule_id", s.info.ID, "target", s.info.Target, "task_id", task.TaskID)
	}
	b.auditEnvelope(env, "scheduler:"+s.info.ID, boolToInt(connected), false)
	status := trace.StatusOK
	if !connected {
		status = trace.StatusQueued
	}
	b.tracer.Emit(trace.Event{
		Kind: trace.KindFire, Name: string(protocol.TypeAssignTask),
		TraceID: s.info.ID, SpanID: task.TaskID,
		From: "scheduler", To: s.info.Target,
		Status: status, Detail: task.Instruction,
	})
	b.events.Publish(protocol.EventPayload{
		Kind:     "message",
		From:     protocol.DaemonSenderID,
		FromName: "scheduler",
		To:       s.info.Target,
		Type:     protocol.TypeAssignTask,
		Summary:  fmt.Sprintf("schedule %s fired: %s", s.info.ID, task.Instruction),
		Raw:      env.Payload,
		At:       time.Now().UTC(),
		MsgCount: b.msgCount.Load(),
	})
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (b *Broker) handleShutdownAck(c *Client) {
	if c.Role == roles.RoleParent {
		select {
		case b.parentAck <- c.ID:
		default:
		}
		return
	}
	b.cascadeMu.Lock()
	cascade := b.cascade
	b.cascadeMu.Unlock()
	if cascade != nil {
		cascade.Ack(c.ID)
	}
}

// ---------------------------------------------------------------------------
// Shutdown (spec §11.2)
// ---------------------------------------------------------------------------

// cascadeBus adapts Broker to shutdown.Notifier.
type cascadeBus struct{ b *Broker }

func (cb cascadeBus) Children() []shutdown.Target {
	children := cb.b.reg.Children()
	targets := make([]shutdown.Target, 0, len(children))
	for _, c := range children {
		targets = append(targets, shutdown.Target{ID: c.ID, Name: c.Name})
	}
	return targets
}

func (cb cascadeBus) Send(clientID string, env *protocol.Envelope) bool {
	c, ok := cb.b.reg.Get(clientID)
	return ok && c.Send(env)
}

func (cb cascadeBus) ForceDisconnect(clientID string) {
	if c, ok := cb.b.reg.Get(clientID); ok {
		c.Close()
	}
}

// Shutdown runs the full four-phase cascade exactly once and tears the
// daemon down. corrID is non-empty when a parent's SHUTDOWN_REQUEST
// initiated it, so the result can be correlated to the requesting tool call.
// Safe to call from any goroutine; later calls block until teardown is done.
func (b *Broker) Shutdown(reason string, ackTimeout time.Duration, initiatedBy, corrID string) {
	b.stopOnce.Do(func() {
		b.stopping.Store(true)
		b.log.Info("shutdown cascade starting", "reason", reason, "initiated_by", initiatedBy)

		// Phases 1–2: notify children, collect ACKs.
		cascade := shutdown.New(cascadeBus{b}, ackTimeout, b.log)
		b.cascadeMu.Lock()
		b.cascade = cascade
		b.cascadeMu.Unlock()
		result := cascade.Run(b.ctx, reason, initiatedBy)
		b.cascadeMu.Lock()
		b.cascade = nil
		b.cascadeMu.Unlock()

		// Phase 3: report the outcome to the parent and wait for its ACK.
		if pid, ok := b.roleReg.ParentID(); ok {
			if parent, found := b.reg.Get(pid); found {
				typ := protocol.TypeShutdownNotice
				if corrID != "" {
					typ = protocol.TypeShutdownResult
				}
				payload := protocol.MustEncode(protocol.ShutdownResultPayload{
					ChildrenAcked:  append([]string{}, result.Acked...),
					ChildrenForced: append([]string{}, result.Forced...),
				})
				b.reply(parent, corrID, typ, payload, "")
				select {
				case <-b.parentAck:
					b.log.Info("parent acknowledged shutdown")
				case <-time.After(parentAckTimeout):
					b.log.Warn("parent did not acknowledge shutdown in time")
				}
			}
		}

		// Phase 4: daemon teardown.
		if b.ln != nil {
			b.ln.Close()
		}
		for _, c := range b.reg.All() {
			c.Close()
		}
		for _, c := range b.reg.Observers() {
			c.Close()
		}
		b.cancel()
		b.wg.Wait()

		// Cancel any still-armed status timers so no goroutine outlives us.
		b.pendingMu.Lock()
		for id, pend := range b.pendingStatus {
			pend.timer.Stop()
			delete(b.pendingStatus, id)
		}
		b.pendingMu.Unlock()

		b.events.CloseAll()
		if b.ln != nil {
			if err := os.Remove(b.cfg.Daemon.SocketPath); err != nil && !os.IsNotExist(err) {
				b.log.Warn("could not remove socket file", "error", err)
			}
		}
		b.log.Info("shutdown cascade complete",
			"children_acked", result.Acked, "children_forced", result.Forced)
		close(b.stopped)
	})
	<-b.stopped
}

// ---------------------------------------------------------------------------
// Events & audit
// ---------------------------------------------------------------------------

func (b *Broker) publishClientEvent(kind string) {
	b.events.Publish(protocol.EventPayload{
		Kind:      kind,
		Clients:   b.clientInfos(false),
		MsgCount:  b.msgCount.Load(),
		StartedAt: b.startedAt,
		At:        time.Now().UTC(),
	})
}

func (b *Broker) publishMessageEvent(sender *Client, env *protocol.Envelope, delivered []string) {
	summary := env.Payload
	if len(summary) > 120 {
		summary = summary[:120] + "…"
	}
	b.events.Publish(protocol.EventPayload{
		Kind:     "message",
		From:     env.From,
		FromName: sender.Name,
		To:       env.To,
		Type:     env.Type,
		Summary:  summary,
		Raw:      env.Payload,
		At:       time.Now().UTC(),
		MsgCount: b.msgCount.Load(),
	})
}

func (b *Broker) auditEnvelope(env *protocol.Envelope, fromName string, deliveryCount int, failed bool) {
	if b.audit == nil {
		return
	}
	err := b.audit.Log(audit.Entry{
		MessageID:        env.ID,
		From:             env.From,
		FromName:         fromName,
		To:               env.To,
		Type:             string(env.Type),
		PayloadSizeBytes: len(env.Payload),
		DeliveryCount:    deliveryCount,
		DeliveryFailed:   failed,
		Payload:          env.Payload,
	})
	if err != nil {
		b.log.Error("audit write failed", "error", err)
	}
}
