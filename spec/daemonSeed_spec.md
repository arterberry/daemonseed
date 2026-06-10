# daemonSeed — Full Engineering Specification

**Version:** 1.0.0-spec  
**Language:** Go 1.22+  
**Target:** macOS (primary), Linux (supported)  
**Purpose:** A local inter-process message bus that enables a Parent Claude Code instance to orchestrate one or more Child Claude Code instances across any number of repos and terminals — via an MCP server and optional slash command interface.

---

## Table of Contents

1. [Project Vision](#1-project-vision)
2. [System Architecture](#2-system-architecture)
3. [Component Breakdown](#3-component-breakdown)
4. [Directory Structure](#4-directory-structure)
5. [Daemon (Broker)](#5-daemon-broker)
6. [Role System: Parent & Child](#6-role-system-parent--child)
7. [MCP Server Integration](#7-mcp-server-integration)
8. [Slash Command Interface](#8-slash-command-interface)
9. [Wire Protocol](#9-wire-protocol)
10. [Message Types & Envelope](#10-message-types--envelope)
11. [Graceful Shutdown Cascade](#11-graceful-shutdown-cascade)
12. [TUI Dashboard (Bubble Tea)](#12-tui-dashboard-bubble-tea)
13. [Configuration](#13-configuration)
14. [Logging & Audit Trail](#14-logging--audit-trail)
15. [Security Model](#15-security-model)
16. [Error Handling Philosophy](#16-error-handling-philosophy)
17. [Testing Strategy](#17-testing-strategy)
18. [CLI Reference](#18-cli-reference)
19. [Build & Release](#19-build--release)
20. [Future Extensions](#20-future-extensions)

---

## 1. Project Vision

`daemonSeed` is a lightweight, fast, local-only message broker written in Go. It allows a developer to open multiple Claude Code sessions — each in its own terminal tab, each working on a different repo — and coordinate them from a single **Parent** instance. Children receive tasks, report status, and can be shut down cleanly, all through the Claude Code MCP toolset or via `/` slash commands.

### Core Principles

- **Local only.** No network exposure. Unix Domain Socket transport only. Fast and private by design.
- **Single parent.** The broker enforces exactly one Parent connection at any time. A second parent attempt is rejected with a clear error.
- **Role-declared at load time.** Each Claude Code session loads the MCP with `--role parent` or `--role child`. Role cannot be changed without reconnecting.
- **Accountability first.** Every message is logged with timestamp, sender, receiver, type, and payload. Nothing is lost silently.
- **Graceful by default.** Shutdown is always a cascade — no instance is abandoned. Timeouts are configurable and enforced.
- **No shortcuts in the implementation.** This spec describes exactly what to build. If something is ambiguous, the implementor should ask or document their assumption explicitly in code comments.

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Developer Machine                     │
│                                                             │
│  iTerm Tab 1 (Parent)    iTerm Tab 2 (Child)   Tab 3 (Child)│
│  ┌──────────────────┐   ┌─────────────────┐  ┌───────────┐ │
│  │  Claude Code     │   │  Claude Code    │  │ Claude    │ │
│  │  repo: platform  │   │  repo: api      │  │ Code      │ │
│  │  role: parent    │   │  role: child    │  │ repo: ui  │ │
│  │  MCP: claudebus  │   │  MCP: claudebus │  │ role:child│ │
│  └────────┬─────────┘   └────────┬────────┘  └─────┬─────┘ │
│           │                      │                  │       │
│           └──────────────────────┼──────────────────┘       │
│                                  │                          │
│                     Unix Domain Socket                      │
│                   /tmp/daemonseed.sock                      │
│                                  │                          │
│                    ┌─────────────▼──────────────┐           │
│                    │      daemonSeed Broker      │           │
│                    │  ─ role registry            │           │
│                    │  ─ message router           │           │
│                    │  ─ subscription manager     │           │
│                    │  ─ audit log writer         │           │
│                    │  ─ shutdown cascade mgr     │           │
│                    │  ─ health monitor           │           │
│                    └─────────────┬──────────────┘           │
│                                  │                          │
│                    ┌─────────────▼──────────────┐           │
│                    │   TUI Dashboard (optional)  │           │
│                    │   bubbletea - live view     │           │
│                    └────────────────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Parent Claude Code
  │
  │  bus_broadcast("begin task: extract auth module")
  ▼
MCP Server (in-process with broker)
  │
  ▼
Broker Router
  ├──► Child "api"     receives task envelope
  ├──► Child "ui"      receives task envelope
  └──► Child "worker"  receives task envelope

Child Claude Code
  │
  │  bus_report_status("auth extraction complete, 3 files modified")
  ▼
Broker Router
  │
  ▼
Parent MCP receives status update → Claude Code notified via tool result
```

---

## 3. Component Breakdown

| Component | Binary / Package | Responsibility |
|---|---|---|
| `cmd/daemonseed` | `daemonseed` binary | Entry point, CLI subcommands |
| `internal/broker` | Library | Socket listener, client registry, message routing |
| `internal/roles` | Library | Role enforcement: parent/child, one-parent rule |
| `internal/protocol` | Library | Wire format: encode/decode message envelopes |
| `internal/mcp` | Library | MCP server exposing bus tools to Claude Code |
| `internal/tui` | Library | Bubble Tea live dashboard |
| `internal/audit` | Library | Append-only structured log writer |
| `internal/config` | Library | Config file parsing, env overrides, defaults |
| `internal/health` | Library | Client heartbeat tracking, stale connection detection |
| `internal/shutdown` | Library | Graceful shutdown cascade logic |

---

## 4. Directory Structure

```
daemonSeed/
├── cmd/
│   └── daemonseed/
│       └── main.go                  # CLI entry point (cobra)
├── internal/
│   ├── broker/
│   │   ├── broker.go                # Core broker: listen, register, route
│   │   ├── broker_test.go
│   │   ├── client.go                # Connected client representation
│   │   ├── client_test.go
│   │   ├── registry.go              # Thread-safe client registry
│   │   └── registry_test.go
│   ├── roles/
│   │   ├── roles.go                 # Role constants, validation, enforcement
│   │   └── roles_test.go
│   ├── protocol/
│   │   ├── envelope.go              # Message struct, JSON encode/decode
│   │   ├── envelope_test.go
│   │   ├── types.go                 # All MessageType constants
│   │   └── framing.go               # Length-prefixed framing for socket writes
│   ├── mcp/
│   │   ├── server.go                # MCP server (mark3labs/mcp-go)
│   │   ├── tools_parent.go          # Tools available only to parent role
│   │   ├── tools_child.go           # Tools available only to child role
│   │   ├── tools_common.go          # Tools available to all roles
│   │   └── server_test.go
│   ├── tui/
│   │   ├── app.go                   # Bubble Tea application root
│   │   ├── model.go                 # TUI model (state)
│   │   ├── update.go                # TUI update logic
│   │   ├── view.go                  # TUI render functions
│   │   ├── components/
│   │   │   ├── clientlist.go        # Connected clients panel
│   │   │   ├── messagefeed.go       # Live message feed panel
│   │   │   └── statusbar.go         # Bottom status bar
│   │   └── tui_test.go
│   ├── audit/
│   │   ├── logger.go                # Append-only JSONL log writer
│   │   └── logger_test.go
│   ├── config/
│   │   ├── config.go                # Config struct + loader
│   │   └── config_test.go
│   ├── health/
│   │   ├── monitor.go               # Heartbeat tracker, stale detection
│   │   └── monitor_test.go
│   └── shutdown/
│       ├── cascade.go               # Shutdown sequence manager
│       └── cascade_test.go
├── scripts/
│   ├── install.sh                   # Install binary + MCP config helper
│   └── uninstall.sh
├── testdata/
│   ├── configs/
│   │   ├── valid_config.yaml
│   │   └── invalid_config.yaml
│   └── messages/
│       ├── valid_envelope.json
│       └── malformed_envelope.json
├── .mcp.json.parent.example         # Example MCP config for parent repos
├── .mcp.json.child.example          # Example MCP config for child repos
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

---

## 5. Daemon (Broker)

### 5.1 Startup Sequence

```
1. Parse config (file → env overrides → flag overrides)
2. Check if socket path already exists:
   a. If yes, attempt a test dial — if something is already listening, exit with error:
      "daemonSeed is already running at /tmp/daemonseed.sock. Run 'daemonseed stop' first."
   b. If yes but nothing answers, remove the stale socket file.
3. Create and bind Unix Domain Socket with permissions 0600 (owner only)
4. Initialize audit logger
5. Initialize health monitor
6. Initialize role registry
7. Initialize message router
8. Start MCP server (if --with-mcp flag set or config enables it)
9. Register SIGTERM + SIGINT handlers → graceful shutdown cascade
10. If --tui flag: launch Bubble Tea TUI in same process
11. Start Accept loop (see 5.2)
12. Log: "daemonSeed started. Socket: /tmp/daemonseed.sock PID: <pid>"
```

### 5.2 Accept Loop

The accept loop runs in a goroutine. For each new connection:

```go
// Pseudocode — implement precisely
for {
    conn, err := listener.Accept()
    if err != nil {
        if isClosed(err) { return } // graceful shutdown, not a crash
        log.Error("accept error", err)
        continue
    }
    go broker.handleClient(conn)
}
```

Each client connection is handled in its own goroutine. The broker is designed for low concurrency (typical: 2–10 Claude Code sessions), so goroutine-per-connection is appropriate and readable.

### 5.3 Client Handshake

On first connection, the client must send a `HELLO` message within `config.HandshakeTimeoutSeconds` (default: 5s). If it does not, the connection is dropped and the event is logged.

The `HELLO` message contains:
- `role`: `"parent"` or `"child"`
- `name`: human-readable identifier (e.g., `"api-service"`, `"ui-frontend"`)
- `version`: client version string (for future compatibility checks)

The broker responds with either:
- `HELLO_ACK` containing the client's assigned `client_id` (UUID)
- `HELLO_REJECT` with a human-readable reason (e.g., `"a parent is already connected"`)

After `HELLO_ACK`, the client is registered and begins normal operation.

### 5.4 Message Routing

The broker routes messages based on `envelope.To`:

| `To` value | Routing behavior |
|---|---|
| `"broadcast"` | Delivered to all connected clients except sender |
| `"parent"` | Delivered to the currently registered parent client |
| `"children"` | Delivered to all registered child clients |
| `"<client_id>"` | Delivered to that specific client by UUID |
| `"<name>"` | Delivered to client registered with that name |

If `To` does not resolve to any connected client, the broker returns a `DELIVERY_FAILED` message to the sender with the reason. This is never silently dropped.

### 5.5 Broker Invariants

These must always be true. The test suite must verify each:

- There is **never** more than one parent connected at a time.
- A client that fails the handshake is **never** added to the registry.
- A message with a missing or empty `From` field is **always** rejected with `INVALID_MESSAGE`.
- The broker **never** panics on malformed input — all JSON decode errors are caught and logged.
- Stale/disconnected clients are removed from the registry within `config.StaleClientTimeoutSeconds`.

---

## 6. Role System: Parent & Child

### 6.1 Role Constants

```go
package roles

type Role string

const (
    RoleParent Role = "parent"
    RoleChild  Role = "child"
)

// Unset is the state before handshake completes
const RoleUnset Role = ""
```

### 6.2 Role Registry

The `RoleRegistry` is a thread-safe struct that tracks:
- Which `client_id` is the current parent (nil if none)
- All child `client_id`s and their names

```go
type RoleRegistry struct {
    mu       sync.RWMutex
    parent   *string        // client_id, nil if no parent
    children map[string]string // client_id → name
}
```

Methods:
- `RegisterParent(clientID string) error` — returns error if parent already registered
- `RegisterChild(clientID, name string)`
- `Deregister(clientID string)` — removes from whichever role it held
- `ParentID() (string, bool)`
- `ChildIDs() []string`
- `ChildByName(name string) (string, bool)` — returns client_id

### 6.3 Tool Surface by Role

The MCP server exposes **different tool sets** depending on the role of the connecting instance. This is enforced at the broker level — tool calls arriving from the wrong role are rejected with `PERMISSION_DENIED`.

**Parent-only tools:**

```
bus_list_children()
bus_send(target string, message string)
bus_broadcast(message string)
bus_assign_task(target string, task_json string)
bus_get_status(target string)
bus_shutdown(timeout_seconds int)
bus_remove_child(target string)
```

**Child-only tools:**

```
bus_report_status(message string)
bus_send_to_parent(message string)
bus_get_assignment()
bus_acknowledge_task(task_id string)
bus_complete_task(task_id string, result_json string)
```

**Common tools (all roles):**

```
bus_ping()
bus_whoami()
bus_list_all()
```

---

## 7. MCP Server Integration

### 7.1 Overview

The MCP server runs as a subprocess launched by Claude Code. It connects to the daemon's Unix socket as a client, performs the handshake with its declared role, and then bridges between Claude Code's tool calls and the broker's message protocol.

The MCP server is the **only interface** Claude Code uses. There is no shell scripting required once the MCP is loaded.

### 7.2 MCP Server Startup

```
Claude Code launches: daemonseed mcp --role parent --name "orchestrator"
  │
  ├── Connect to /tmp/daemonseed.sock
  ├── Send HELLO {role: "parent", name: "orchestrator", version: "1.0.0"}
  ├── Receive HELLO_ACK {client_id: "uuid-..."}
  ├── Store client_id in process memory
  └── Start MCP stdio server (JSON-RPC over stdin/stdout per MCP spec)
```

If the daemon is not running when the MCP server starts:

```
Option A (default): Fail with clear error message:
  "daemonSeed daemon is not running. Start it with: daemonseed start"

Option B (config: auto_start: true): Automatically start the daemon,
  wait up to 3 seconds for socket to appear, then connect.
```

### 7.3 MCP Tool Definitions (precise)

Each tool definition below is the exact schema the MCP server must register.

#### `bus_list_children`
- **Role:** Parent only
- **Description:** Returns a list of all connected child instances with their names, IDs, and last-seen time.
- **Input schema:** `{}` (no parameters)
- **Returns:** JSON array of child objects

#### `bus_send`
- **Role:** Parent only
- **Description:** Send a direct message to a named child instance.
- **Input schema:**
  ```json
  {
    "target": { "type": "string", "description": "Child name or client_id" },
    "message": { "type": "string", "description": "Message content" }
  }
  ```
- **Returns:** Delivery confirmation or error

#### `bus_broadcast`
- **Role:** Parent only
- **Description:** Send a message to all connected child instances simultaneously.
- **Input schema:**
  ```json
  {
    "message": { "type": "string", "description": "Message to broadcast" }
  }
  ```
- **Returns:** Count of children reached

#### `bus_assign_task`
- **Role:** Parent only
- **Description:** Assign a structured task to a specific child. The task is a JSON object with at minimum a `task_id` and `instruction` field.
- **Input schema:**
  ```json
  {
    "target": { "type": "string" },
    "task_json": {
      "type": "string",
      "description": "JSON string with task_id, instruction, context, deadline_hint"
    }
  }
  ```
- **Returns:** Task envelope sent to child, with task_id echoed

#### `bus_get_status`
- **Role:** Parent only
- **Description:** Request the current status from a specific child. This is a synchronous call — the broker relays the request and waits up to `config.StatusTimeoutSeconds` (default: 10s) for the child's response.
- **Input schema:**
  ```json
  {
    "target": { "type": "string" }
  }
  ```
- **Returns:** Child's status report or timeout error

#### `bus_shutdown`
- **Role:** Parent only
- **Description:** Initiate graceful shutdown cascade. All children are notified and given `timeout_seconds` to acknowledge before force-disconnect.
- **Input schema:**
  ```json
  {
    "timeout_seconds": {
      "type": "integer",
      "default": 5,
      "description": "Seconds to wait for child acknowledgments"
    }
  }
  ```
- **Returns:** Shutdown cascade result (children ACKed, children timed out, daemon stopping)

#### `bus_report_status`
- **Role:** Child only
- **Description:** Push a status message up to the parent.
- **Input schema:**
  ```json
  {
    "message": { "type": "string" },
    "state": {
      "type": "string",
      "enum": ["idle", "working", "blocked", "complete", "error"]
    }
  }
  ```

#### `bus_send_to_parent`
- **Role:** Child only
- **Description:** Send a direct message or question to the parent instance.
- **Input schema:**
  ```json
  {
    "message": { "type": "string" }
  }
  ```

#### `bus_get_assignment`
- **Role:** Child only
- **Description:** Poll for a pending task assigned by the parent. Returns immediately with the task or an empty result if none pending.
- **Input schema:** `{}`
- **Returns:** Task envelope or `{"pending": false}`

#### `bus_acknowledge_task`
- **Role:** Child only
- **Description:** Acknowledge receipt of a task. Must be called after `bus_get_assignment` returns a task.
- **Input schema:**
  ```json
  {
    "task_id": { "type": "string" }
  }
  ```

#### `bus_complete_task`
- **Role:** Child only
- **Description:** Report task completion to the parent.
- **Input schema:**
  ```json
  {
    "task_id": { "type": "string" },
    "result_json": { "type": "string", "description": "JSON result payload" }
  }
  ```

#### `bus_ping`
- **Role:** All
- **Description:** Ping the daemon. Returns roundtrip time in milliseconds.
- **Input schema:** `{}`

#### `bus_whoami`
- **Role:** All
- **Description:** Returns this instance's client_id, name, role, and connection time.
- **Input schema:** `{}`

#### `bus_list_all`
- **Role:** All
- **Description:** Returns all connected clients (parent + children) with roles and names.
- **Input schema:** `{}`

---

## 8. Slash Command Interface

Claude Code supports `/` slash commands via the `.claude/commands/` directory in a repo. `daemonSeed` ships with a set of pre-built slash command scripts that wrap the MCP tools for direct invocation.

### 8.1 Installation

```bash
daemonseed install-commands --repo-path /path/to/repo --role parent
# or
daemonseed install-commands --repo-path /path/to/repo --role child
```

This creates:

```
.claude/
└── commands/
    ├── bus-list.md           # /bus-list
    ├── bus-send.md           # /bus-send
    ├── bus-broadcast.md      # /bus-broadcast
    ├── bus-status.md         # /bus-status
    ├── bus-assign.md         # /bus-assign
    ├── bus-shutdown.md       # /bus-shutdown (parent only)
    ├── bus-report.md         # /bus-report (child only)
    └── bus-whoami.md         # /bus-whoami
```

### 8.2 Slash Command Definitions

Each `.md` file in `.claude/commands/` is a Claude Code slash command prompt. These instruct Claude to use the MCP tools.

**`bus-list.md`:**
```markdown
List all connected daemonSeed clients. Use the bus_list_all MCP tool and
present the results in a clean table showing: name, role, client_id (short),
state, and last seen time.
```

**`bus-send.md`:**
```markdown
Send a direct message via daemonSeed. Ask the user for the target (name or ID)
and the message content if not already provided. Use the bus_send MCP tool.
Confirm delivery or report the error clearly.
```

**`bus-broadcast.md`:**
```markdown
Broadcast a message to all connected child instances via daemonSeed.
Ask for the message content if not provided. Use bus_broadcast MCP tool.
Report how many children received the message.
```

**`bus-status.md`:**
```markdown
Request the current status from a specific child instance. Ask for the
target name if not provided. Use bus_get_status MCP tool. Present the
returned status clearly, noting if the request timed out.
```

**`bus-assign.md`:**
```markdown
Assign a task to a child instance via daemonSeed. Ask the user for:
1. Target child name
2. Task instruction (what to do)
3. Any context needed
Then format as a task JSON and call bus_assign_task MCP tool. Report the
assigned task_id for tracking.
```

**`bus-shutdown.md`:**
```markdown
Initiate a graceful shutdown of all daemonSeed connections. Confirm with
the user before proceeding. Use bus_shutdown MCP tool with a 5-second
timeout. Report which children acknowledged and which timed out.
```

**`bus-report.md`:**
```markdown
Report this instance's current status to the parent. Ask for a brief
status message and current state (idle/working/blocked/complete/error)
if not provided. Use bus_report_status MCP tool.
```

**`bus-whoami.md`:**
```markdown
Show this instance's daemonSeed identity. Use bus_whoami MCP tool and
display: name, role, client_id, connection time, and daemon version.
```

---

## 9. Wire Protocol

### 9.1 Transport

Unix Domain Socket at path `/tmp/daemonseed.sock` (configurable).

All messages are **length-prefixed JSON**:

```
┌─────────────────────┬──────────────────────────────┐
│  4-byte length (BE) │  JSON payload (UTF-8)         │
└─────────────────────┴──────────────────────────────┘
```

The 4-byte big-endian unsigned integer encodes the byte length of the following JSON payload. Maximum message size: `config.MaxMessageBytes` (default: 1MB). Messages exceeding this are rejected with `MESSAGE_TOO_LARGE`.

### 9.2 Framing Implementation

```go
// WriteMessage writes a length-prefixed JSON message to a writer.
func WriteMessage(w io.Writer, env *Envelope) error {
    data, err := json.Marshal(env)
    if err != nil {
        return fmt.Errorf("marshal: %w", err)
    }
    if len(data) > MaxMessageBytes {
        return ErrMessageTooLarge
    }
    length := uint32(len(data))
    if err := binary.Write(w, binary.BigEndian, length); err != nil {
        return fmt.Errorf("write length: %w", err)
    }
    _, err = w.Write(data)
    return err
}

// ReadMessage reads a length-prefixed JSON message from a reader.
func ReadMessage(r io.Reader) (*Envelope, error) {
    var length uint32
    if err := binary.Read(r, binary.BigEndian, &length); err != nil {
        return nil, fmt.Errorf("read length: %w", err)
    }
    if length > uint32(MaxMessageBytes) {
        return nil, ErrMessageTooLarge
    }
    buf := make([]byte, length)
    if _, err := io.ReadFull(r, buf); err != nil {
        return nil, fmt.Errorf("read body: %w", err)
    }
    var env Envelope
    if err := json.Unmarshal(buf, &env); err != nil {
        return nil, fmt.Errorf("unmarshal: %w", err)
    }
    return &env, nil
}
```

---

## 10. Message Types & Envelope

### 10.1 Envelope

```go
package protocol

import "time"

// Envelope is the universal message wrapper for all daemonSeed communication.
type Envelope struct {
    ID        string      `json:"id"`         // UUID v4
    From      string      `json:"from"`       // client_id of sender
    To        string      `json:"to"`         // routing target (see §5.4)
    Type      MessageType `json:"type"`       // message type constant
    Payload   string      `json:"payload"`    // JSON string or plain text
    TaskID    string      `json:"task_id,omitempty"`
    Timestamp time.Time   `json:"timestamp"`
    Version   string      `json:"version"`    // protocol version, e.g. "1"
}
```

### 10.2 Message Types

```go
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
    TypeAssignTask    MessageType = "ASSIGN_TASK"
    TypeAckTask       MessageType = "ACK_TASK"
    TypeCompleteTask  MessageType = "COMPLETE_TASK"

    // Status
    TypeStatusRequest  MessageType = "STATUS_REQUEST"
    TypeStatusReport   MessageType = "STATUS_REPORT"
    TypeStatusTimeout  MessageType = "STATUS_TIMEOUT"

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
    TypeInvalidMessage  MessageType = "INVALID_MESSAGE"
    TypePermissionDenied MessageType = "PERMISSION_DENIED"
    TypeMessageTooLarge  MessageType = "MESSAGE_TOO_LARGE"
    TypeNotFound         MessageType = "NOT_FOUND"
    TypeInternalError    MessageType = "INTERNAL_ERROR"
)
```

### 10.3 Payload Schemas

Task payload (used in `ASSIGN_TASK`):

```go
type TaskPayload struct {
    TaskID       string            `json:"task_id"`
    Instruction  string            `json:"instruction"`
    Context      map[string]string `json:"context,omitempty"`
    DeadlineHint string            `json:"deadline_hint,omitempty"`
    AssignedAt   time.Time         `json:"assigned_at"`
}
```

Status report payload (used in `STATUS_REPORT`):

```go
type StatusPayload struct {
    State       string    `json:"state"` // idle|working|blocked|complete|error
    Message     string    `json:"message"`
    CurrentTask string    `json:"current_task,omitempty"`
    ReportedAt  time.Time `json:"reported_at"`
}
```

Hello payload (used in `HELLO`):

```go
type HelloPayload struct {
    Role    string `json:"role"`
    Name    string `json:"name"`
    Version string `json:"version"`
}
```

Hello ACK payload (used in `HELLO_ACK`):

```go
type HelloAckPayload struct {
    ClientID      string `json:"client_id"`
    DaemonVersion string `json:"daemon_version"`
}
```

---

## 11. Graceful Shutdown Cascade

This is a first-class feature. The sequence must be deterministic and tested.

### 11.1 Trigger Sources

| Trigger | Action |
|---|---|
| Parent calls `bus_shutdown` | Standard cascade |
| `SIGTERM` to daemon process | Standard cascade |
| `SIGINT` to daemon process | Standard cascade with shorter timeout |
| `daemonseed stop` CLI command | Sends SIGTERM to daemon via PID file |
| Parent MCP disconnects unexpectedly | Children are notified: `"parent disconnected"` |

### 11.2 Cascade Sequence

```
Phase 1 — Notify (parallel)
  For each connected child:
    Send SHUTDOWN_NOTICE { reason, timeout_seconds, initiated_by }
    Start per-child ACK timer

Phase 2 — Wait for ACKs
  Collect SHUTDOWN_ACK messages until either:
    a. All children have ACKed → proceed to Phase 3
    b. timeout_seconds elapses → force-disconnect non-ACKing children,
       log which ones timed out

Phase 3 — Notify parent
  Send parent: SHUTDOWN_NOTICE { children_acked: [...], children_forced: [...] }
  Wait for parent SHUTDOWN_ACK (3s timeout)

Phase 4 — Daemon teardown
  Close socket listener
  Flush and close audit log
  Remove socket file
  Write to stdout: "daemonSeed stopped cleanly."
  Exit 0
```

### 11.3 Unexpected Daemon Death

If the daemon is killed with `SIGKILL` or crashes, clients will detect this via their next read/write attempt (connection refused / broken pipe). The MCP server must handle this cleanly:

```go
// MCP server connection error handling:
// On any socket error after successful handshake:
//   1. Log the error
//   2. Attempt reconnect up to config.ReconnectAttempts times
//      with config.ReconnectBackoffMs delay
//   3. If reconnect fails, return structured error to Claude Code:
//      "daemonSeed daemon connection lost. Restart with: daemonseed start"
```

---

## 12. TUI Dashboard (Bubble Tea)

### 12.1 Launch Modes

```bash
# Launch daemon with TUI in same process (foreground)
daemonseed start --tui

# Attach TUI to already-running daemon (reads from daemon's event stream)
daemonseed tui
```

### 12.2 Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  daemonSeed  v1.0.0       socket: /tmp/daemonseed.sock   ▲ LIVE │
├───────────────────────┬─────────────────────────────────────────┤
│  CONNECTED CLIENTS    │  MESSAGE FEED                           │
│  ─────────────────    │  ──────────────────────────────────     │
│  ● orchestrator       │  10:42:31 [parent→children] BROADCAST   │
│    role: parent       │            "begin task: auth module"    │
│    id: a3f2...        │                                         │
│    connected: 8m ago  │  10:42:33 [api→parent] STATUS_REPORT   │
│                       │            state=working               │
│  ○ api-service        │            "starting file extraction"   │
│    role: child        │                                         │
│    state: working     │  10:42:45 [ui→parent] STATUS_REPORT    │
│    task: auth-001     │            state=idle                   │
│                       │            "no relevant files in scope" │
│  ○ ui-frontend        │                                         │
│    role: child        │  10:43:01 [api→parent] COMPLETE_TASK   │
│    state: idle        │            task_id=auth-001             │
│                       │            "3 files modified"           │
├───────────────────────┴─────────────────────────────────────────┤
│  [Q] Quit  [C] Clear feed  [P] Pause  [F] Filter  [?] Help      │
│  uptime: 8m 32s   msgs: 47   clients: 3 (1P 2C)   log: ON      │
└─────────────────────────────────────────────────────────────────┘
```

### 12.3 TUI Features

| Key | Action |
|---|---|
| `Q` / `Ctrl+C` | Quit TUI (daemon keeps running) |
| `C` | Clear message feed display |
| `P` | Pause/resume live feed scrolling |
| `F` | Open filter dialog (by client, by message type) |
| `?` | Help overlay |
| `↑/↓` | Scroll message feed |
| `Tab` | Switch focus between panels |
| `D` | Toggle detail view for selected message |

### 12.4 TUI Model

```go
type Model struct {
    clients     []ClientView
    messages    []MessageView
    paused      bool
    filter      FilterState
    focusedPane Pane
    width       int
    height      int
    statusBar   StatusBar
}

type ClientView struct {
    ID          string
    Name        string
    Role        roles.Role
    State       string
    CurrentTask string
    ConnectedAt time.Time
    LastSeen    time.Time
}

type MessageView struct {
    Timestamp time.Time
    From      string
    To        string
    Type      protocol.MessageType
    Summary   string
    Raw       string // full JSON, shown in detail view
}
```

### 12.5 Color Scheme

Use lipgloss for styling. The TUI should feel like a terminal tool, not a toy:

- Background: terminal default (transparent)
- Parent client: `#00D7FF` (cyan)
- Child client: `#87FF5F` (green)
- Error messages: `#FF5F5F` (red)
- Warning messages: `#FFD75F` (amber)
- Info messages: `#878787` (gray)
- Borders: `#444444`
- Status bar: `#1C1C1C` background, `#AAAAAA` text

---

## 13. Configuration

### 13.1 Config File

Default location: `~/.config/daemonseed/config.yaml`  
Override with: `--config /path/to/config.yaml` or `DAEMONSEED_CONFIG` env var.

```yaml
# daemonSeed configuration
daemon:
  socket_path: /tmp/daemonseed.sock
  pid_file: /tmp/daemonseed.pid
  auto_start: false               # MCP auto-starts daemon if not running

timeouts:
  handshake_seconds: 5
  status_request_seconds: 10
  shutdown_ack_seconds: 5
  reconnect_attempts: 3
  reconnect_backoff_ms: 500
  heartbeat_interval_seconds: 15
  stale_client_seconds: 45

limits:
  max_message_bytes: 1048576      # 1MB
  max_clients: 20
  max_pending_tasks_per_client: 50

audit:
  enabled: true
  log_path: ~/.local/share/daemonseed/audit.jsonl
  max_size_mb: 100
  rotate_on_start: false

tui:
  feed_max_lines: 500
  timestamp_format: "15:04:05"
```

### 13.2 Environment Overrides

All config values can be overridden with environment variables using the pattern `DAEMONSEED_<SECTION>_<KEY>` in SCREAMING_SNAKE_CASE:

```bash
DAEMONSEED_DAEMON_SOCKET_PATH=/var/run/daemonseed.sock
DAEMONSEED_TIMEOUTS_SHUTDOWN_ACK_SECONDS=10
DAEMONSEED_AUDIT_ENABLED=false
```

### 13.3 Per-Repo MCP Config Examples

**Parent repo (`.mcp.json`):**
```json
{
  "mcpServers": {
    "daemonseed": {
      "command": "daemonseed",
      "args": ["mcp", "--role", "parent", "--name", "orchestrator"],
      "env": {}
    }
  }
}
```

**Child repo (`.mcp.json`):**
```json
{
  "mcpServers": {
    "daemonseed": {
      "command": "daemonseed",
      "args": ["mcp", "--role", "child", "--name", "api-service"],
      "env": {}
    }
  }
}
```

---

## 14. Logging & Audit Trail

### 14.1 Audit Log Format

Every message routed through the broker is written to the audit log as JSONL (one JSON object per line):

```json
{
  "seq": 1,
  "logged_at": "2025-01-15T10:42:31.123456Z",
  "message_id": "550e8400-e29b-41d4-a716-446655440000",
  "from": "client-id-parent",
  "from_name": "orchestrator",
  "to": "broadcast",
  "type": "BROADCAST",
  "payload_size_bytes": 42,
  "delivery_count": 2,
  "delivery_failed": false
}
```

Note: **Payload content is NOT logged by default** to avoid logging potentially sensitive task content. Set `audit.log_payloads: true` in config to include full payload (useful for debugging).

### 14.2 Structured Application Log

The daemon emits structured logs (JSON) to stderr by default. Format:

```json
{"level":"INFO","ts":"2025-01-15T10:42:31Z","msg":"client connected","client_id":"abc","name":"api-service","role":"child"}
{"level":"WARN","ts":"2025-01-15T10:42:45Z","msg":"status request timed out","client_id":"xyz","target":"ui-frontend"}
{"level":"ERROR","ts":"2025-01-15T10:43:01Z","msg":"failed to route message","error":"client not found","to":"nonexistent"}
```

Use `--log-format text` for human-readable output during development.

Log levels: `DEBUG`, `INFO`, `WARN`, `ERROR`. Set with `--log-level` flag or `DAEMONSEED_LOG_LEVEL` env var.

---

## 15. Security Model

This is a local-only tool. The security model reflects that:

### 15.1 Socket Permissions

The Unix Domain Socket is created with `0600` permissions. Only the process owner can connect. This is enforced at the OS level.

```go
listener, err := net.Listen("unix", socketPath)
if err != nil { ... }
// Immediately restrict permissions
if err := os.Chmod(socketPath, 0600); err != nil { ... }
```

### 15.2 No Authentication Tokens

No token/auth scheme is implemented in v1.0. The assumption is that only the developer's own processes connect to their own socket. If multi-user support is needed in the future, add `--token` flag to both daemon and MCP server, with HMAC verification in the handshake.

### 15.3 Role Enforcement

Role-based tool restrictions are enforced by the **broker**, not the MCP server. Even if someone crafts a raw socket message pretending to be a parent calling a parent-only tool from a child connection, the broker checks the registered role for that `client_id` and rejects it with `PERMISSION_DENIED`.

### 15.4 No External Network Access

`daemonSeed` must **never** open a TCP socket, make an HTTP request, or communicate outside the local machine. This must be enforced in code review and verified by test.

### 15.5 PID File

On start, the daemon writes its PID to `config.PIDFile`. On clean exit, the PID file is removed. The `daemonseed stop` command reads this file and sends SIGTERM. If the PID file exists but the process is gone (crash), `daemonseed start` detects this and cleans up automatically.

---

## 16. Error Handling Philosophy

### 16.1 Rules

1. **No silent failures.** Every error is either returned to the caller, logged, or both. `_ = someErr` is forbidden.
2. **Errors are typed, not stringly-typed.** Use sentinel errors and `fmt.Errorf("%w", err)` wrapping. Define error types in `internal/protocol/errors.go`.
3. **Panics are bugs.** The broker must never panic on external input. All JSON parsing, network reads, and type assertions must be protected.
4. **Client errors don't crash the broker.** A malformed message from one client results in an `INVALID_MESSAGE` response to that client and a log entry. Other clients are unaffected.
5. **Context everywhere.** All blocking operations accept a `context.Context`. Cancellation is respected promptly.

### 16.2 Error Types

```go
package protocol

import "errors"

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
)
```

---

## 17. Testing Strategy

### 17.1 Philosophy

Tests are not optional and not an afterthought. Every package has a `_test.go` file. The test suite must pass with `go test -race ./...`. No test may sleep unconditionally — use channels or condition variables for synchronization.

### 17.2 Test Categories

#### Unit Tests (per package)
Cover the logic of each package in isolation, using mocks or fakes for dependencies.

#### Integration Tests (`internal/broker/integration_test.go`)
Spin up a real broker on a temp socket, connect real clients, test the full message flow.

#### Edge Case Tests
See §17.4 for explicit list.

#### Negative Tests
See §17.5 for explicit list.

### 17.3 Happy Path Tests

```
broker_test.go:
  TestBroker_ParentChildConnect         — parent connects, child connects, both get HELLO_ACK
  TestBroker_Broadcast                  — parent broadcasts, all children receive
  TestBroker_DirectMessage              — parent sends to named child, only that child receives
  TestBroker_AssignTask                 — parent assigns task, child gets assignment
  TestBroker_TaskLifecycle              — assign → ack → complete, parent notified
  TestBroker_StatusRequest              — parent requests status, child responds
  TestBroker_GracefulShutdown           — shutdown cascade, all ACK, daemon exits 0

registry_test.go:
  TestRegistry_RegisterAndRetrieve
  TestRegistry_Deregister
  TestRegistry_ChildByName

protocol_test.go:
  TestEnvelope_MarshalUnmarshal
  TestFraming_WriteRead
  TestFraming_RoundTrip_LargeMessage

mcp_test.go:
  TestMCPServer_ParentToolsAvailable
  TestMCPServer_ChildToolsAvailable
  TestMCPServer_PingReturnsLatency

shutdown_test.go:
  TestCascade_AllACK
  TestCascade_SomeMissing_ForceDisconnect

health_test.go:
  TestHeartbeat_ClientDroppedAfterTimeout
```

### 17.4 Edge Case Tests

```
broker_test.go:
  TestBroker_MessageToNonexistentTarget
    → Must return DELIVERY_FAILED to sender, not silently drop

  TestBroker_ParentDisconnectsUnexpectedly
    → Children must receive notification within 2 seconds

  TestBroker_ChildConnectsBeforeParent
    → Child should connect successfully; parent connects later, both functional

  TestBroker_MaxClientsReached
    → 21st client must receive HELLO_REJECT with ErrMaxClientsReached

  TestBroker_DuplicateName
    → Two children with same name: second one gets a disambiguated name (e.g., "api-service-2")
    → Or reject with clear error — document the choice

  TestBroker_EmptyPayloadMessage
    → Must be accepted (payload is optional)

  TestBroker_UnicodePayload
    → Payload with emoji, CJK characters — must round-trip cleanly

  TestBroker_SimultaneousConnections
    → 10 children connecting within 100ms — all must register correctly

  TestBroker_LargePayload_AtLimit
    → Message exactly at MaxMessageBytes — must succeed

  TestBroker_LargePayload_OverLimit
    → Message 1 byte over MaxMessageBytes — must return MESSAGE_TOO_LARGE

  TestShutdown_ChildDisconnectedDuringCascade
    → Child disconnects between SHUTDOWN_NOTICE and expected ACK
    → Cascade must not hang; must complete normally

  TestShutdown_NoChildrenConnected
    → Shutdown with only parent connected — must complete immediately

  TestHealth_ClientReconnects
    → Client drops and reconnects within reconnect window — new client_id issued
```

### 17.5 Negative Tests

```
broker_test.go:
  TestBroker_NoHello_ConnectionDropped
    → Client connects but sends no HELLO within HandshakeTimeout
    → Connection must be dropped, no entry in registry

  TestBroker_SecondParent_Rejected
    → Two clients both request role=parent
    → First gets HELLO_ACK, second gets HELLO_REJECT

  TestBroker_InvalidRole_Rejected
    → Client sends role="superadmin" in HELLO
    → Must receive HELLO_REJECT with ErrInvalidRole

  TestBroker_MalformedJSON_Rejected
    → Client sends binary garbage instead of JSON
    → Must receive INVALID_MESSAGE, broker must not crash

  TestBroker_TruncatedFrame_Handled
    → Client sends 4-byte length header then closes connection
    → Broker handles io.ErrUnexpectedEOF cleanly

  TestBroker_ChildCallsParentTool
    → Child sends a message attempting to call bus_broadcast
    → Must receive PERMISSION_DENIED

  TestBroker_ParentCallsChildTool
    → Parent sends bus_report_status
    → Must receive PERMISSION_DENIED

  TestBroker_EmptyFromField
    → Message with empty "from" field
    → Must be rejected with INVALID_MESSAGE

  TestBroker_FutureProtocolVersion
    → Client sends version="99" in HELLO
    → Either accept with warning (log) or reject — document and test the choice

  TestMCP_DaemonNotRunning
    → MCP server starts when daemon is down (auto_start=false)
    → Must return clear error, not panic

  TestAuditLog_FailedWrite
    → Audit log directory is not writable
    → Daemon must start with audit disabled and log a warning, not crash

  TestConfig_InvalidYAML
    → Config file contains invalid YAML
    → Must exit with clear error message, non-zero exit code

  TestConfig_NegativeTimeout
    → Config has timeout: -1
    → Must be rejected as invalid config
```

### 17.6 Test Helpers

Create `internal/testutil/testutil.go` with:

```go
// StartTestBroker starts a broker on a temp socket and returns the socket path
// and a cleanup function. Cleans up automatically when test ends.
func StartTestBroker(t *testing.T, cfg *config.Config) (socketPath string, cleanup func())

// ConnectTestClient connects a raw client to the broker socket,
// performs HELLO handshake, and returns a connected client for use in tests.
func ConnectTestClient(t *testing.T, socketPath, role, name string) *TestClient

// TestClient wraps a net.Conn with helper methods for test assertions.
type TestClient struct { ... }
func (c *TestClient) Send(env *protocol.Envelope)
func (c *TestClient) Receive(timeout time.Duration) *protocol.Envelope
func (c *TestClient) MustReceiveType(t *testing.T, typ protocol.MessageType, timeout time.Duration) *protocol.Envelope
```

---

## 18. CLI Reference

The `daemonseed` binary is the sole entry point. It uses `cobra` for subcommands.

```
daemonseed [flags] <command>

Commands:
  start             Start the daemon (foreground or background)
  stop              Stop the running daemon gracefully
  status            Show daemon status (running, socket path, connected clients)
  mcp               Start MCP server subprocess (launched by Claude Code)
  tui               Attach TUI to running daemon
  install-commands  Install Claude Code slash commands into a repo
  logs              Tail or show the audit log
  version           Print version information

Flags:
  --config string       Config file path (default: ~/.config/daemonseed/config.yaml)
  --socket string       Socket path override
  --log-level string    Log level: debug, info, warn, error (default: info)
  --log-format string   Log format: json, text (default: json)

start flags:
  --tui               Launch with TUI dashboard
  --background        Run as background process (default: foreground)
  --pidfile string    PID file path override

mcp flags:
  --role string       Role: parent or child (required)
  --name string       Instance name (required)
  --auto-start        Start daemon if not running

install-commands flags:
  --repo-path string  Target repo path (default: current directory)
  --role string       Role for these commands: parent or child
  --force             Overwrite existing command files
```

---

## 19. Build & Release

### 19.1 Makefile Targets

```makefile
.PHONY: build test lint clean install

build:
	go build -ldflags="-X main.Version=$(VERSION) -X main.Commit=$(COMMIT)" \
	  -o bin/daemonseed ./cmd/daemonseed

test:
	go test -race -count=1 -timeout=60s ./...

test-verbose:
	go test -race -count=1 -timeout=60s -v ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

install:
	go install ./cmd/daemonseed

coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
```

### 19.2 Go Modules

```
module github.com/yourusername/daemonseed

go 1.22

require (
    github.com/charmbracelet/bubbletea    v0.27.x
    github.com/charmbracelet/bubbles      v0.19.x
    github.com/charmbracelet/lipgloss     v1.0.x
    github.com/google/uuid                v1.6.x
    github.com/mark3labs/mcp-go           v0.x.x
    github.com/spf13/cobra                v1.8.x
    github.com/spf13/viper                v1.19.x
    go.etcd.io/bbolt                      v1.3.x   // optional: durable task queue
    go.uber.org/zap                       v1.27.x  // structured logging
)
```

### 19.3 Binary Size and Performance Targets

- Binary size: under 20MB (no CGo by default)
- Broker message routing latency: under 1ms for local socket at p99
- Startup time (daemon ready): under 200ms
- Memory footprint (idle, 5 clients): under 20MB RSS

---

## 20. Future Extensions

These are out of scope for v1.0 but the architecture must not preclude them:

### 20.1 Persistent Task Queue

Replace the in-memory task queue with bbolt. Tasks survive daemon restarts. Children can reconnect and pick up where they left off.

### 20.2 Task Result Storage

Store task results in bbolt for later retrieval. Parent can call `bus_get_result(task_id)` days later.

### 20.3 Multi-Token Authentication

Add `--token` to both daemon start and MCP connect. HMAC-SHA256 challenge/response in handshake. For shared machines or CI environments.

### 20.4 Named Channels / Topics

Extend routing with named pub/sub topics beyond the parent/child model. Children can subscribe to specific topics and only receive broadcasts on those topics.

### 20.5 HTTP REST API

Expose a local-only HTTP server (localhost only, random ephemeral port) for tooling that can't use Unix sockets — e.g., browser extensions, VS Code extensions.

### 20.6 daemonSeed Cloud (explicitly out of scope)

This project is intentionally local-only. Any cloud or network feature would be a separate project with a separate threat model.

### 20.7 Hook-Based Command Injection (Parent → Child Slash Commands)

> **Status: IMPLEMENTED.** `daemonseed inbox --drain`, `daemonseed
> install-hooks`, the broker-side named inbox (`INBOX_DRAIN_REQUEST`), and
> the `commands.allow_from_parent` allowlist shipped as specified below.

The bus can already carry a slash command as a task or message payload (e.g.,
`bus_send` with `"/bus-report"` or `bus_assign_task` whose instruction is
`"/fix-tests --target auth"`). What v1.0 lacks is the last hop: delivering it
*into* the child's running Claude Code session. MCP is pull-based — the child
only sees bus traffic when its model calls `bus_get_assignment` or
`bus_check_messages` — and no MCP mechanism can push a prompt into a session.

This extension closes the gap with Claude Code **hooks**, which can inject
content into a session at defined lifecycle points:

1. **New CLI subcommand:** `daemonseed inbox --drain --role child --name <name>`
   connects to the daemon, drains any pending messages/assignments addressed
   to that child, prints them to stdout in a hook-friendly format, and exits.
   Empty inbox → empty output, exit 0.

2. **Shipped hook snippet:** `daemonseed install-commands --role child` (or a
   new `install-hooks` subcommand) additionally writes a `UserPromptSubmit`
   and/or `Stop` hook into the child repo's `.claude/settings.json` that runs
   the drain command. Pending parent instructions then surface in the child's
   session as injected context ("parent requests: /bus-report") the next time
   the session finishes a turn or receives input — no manual polling.

3. **Command allowlist:** parent-supplied command execution is remote code
   execution by design, even on a single-user machine. The child's config
   gains:

   ```yaml
   commands:
     allow_from_parent: ["/bus-report", "/bus-whoami"]   # default: empty (deny all)
   ```

   The drain command annotates (or refuses to emit) instructions whose leading
   slash command is not allowlisted, and the refusal is logged.

4. **Audit:** injected command payloads follow the existing audit rules —
   message metadata always logged, payload content only with
   `audit.log_payloads: true`. Every drain (count of messages delivered into a
   session) is logged as a structured event.

Out of scope for this extension: autonomous execution. The hook surfaces the
parent's request to the child's session; the child's model (and its human)
still decides whether to act. A fully autonomous headless runner
(`daemonseed run` spawning `claude -p "/command"`) is a separate, larger
extension with a meaningfully different risk profile.

### 20.8 Task Scheduler (Cron / Intervals / One-Shot)

> **Status: IMPLEMENTED** (in-memory; bbolt persistence still pending per
> §20.1). One deviation: cron parsing uses the in-house `internal/cron`
> package (standard 5-field semantics) because the build environment could
> not fetch `robfig/cron`; it is designed to be swapped for the library.

Lets the parent schedule tasks for children that fire at a time or on a
recurrence — "run the dependency audit on `api` nightly at 02:00" — without
the parent session needing to stay open.

**Ownership.** The scheduler lives in the **daemon**, not the parent. The
broker is the only always-on process; schedules must fire whether or not the
authoring parent session is still connected. The parent is the author and
manager of schedules, never the executor.

**Parent tools:**

```
bus_schedule_task(target string, task_json string, when_json string)
    → returns schedule_id
bus_list_schedules()
    → all schedules with id, target, trigger, next_fire_at, created_by, fire_count
bus_cancel_schedule(schedule_id string)
```

**Trigger shapes** (`when_json`, exactly one of):

```json
{ "at":    "2026-06-10T02:00:00Z" }       // one-shot
{ "every": "15m" }                        // fixed interval (Go duration)
{ "cron":  "0 2 * * *" }                  // standard 5-field cron expression
```

Cron parsing uses an established library (e.g., `robfig/cron`), not a
hand-rolled parser. Times are evaluated in the daemon's local timezone unless
the expression carries an explicit `TZ=` prefix.

**Firing semantics.** When a schedule fires, the daemon enqueues a normal
`ASSIGN_TASK` to the target child — the existing task store, delivery path,
receipts, and audit trail are reused unchanged. The generated task carries
`task_id = "<schedule_id>-<fire_seq>"` and a `context` entry identifying the
schedule and the parent that created it. Nothing changes on the child side;
delivery into the child's session is the same aperture as §20.7 (polling or
hook injection), and the two extensions compose: cron fires → task queued →
hook surfaces it.

**Misfire policy** (per schedule, set at creation):

| Policy | Child offline at fire time |
|---|---|
| `queue` (default) | Task is queued; delivered when the child reconnects. Expires after `ttl` (default: the schedule's interval, or 24h for one-shots) |
| `skip` | Occurrence is dropped and logged; next occurrence unaffected |

If the parent is offline when a child completes a scheduled task, the
`COMPLETE_TASK` result is undeliverable in v1 semantics; full support
depends on §20.2 (task result storage) so results can be fetched later.

**Persistence.** In-memory schedules are acceptable for a first cut, but a
daemon restart silently dropping a nightly job violates the project's
no-silent-loss principle — durable storage via bbolt (§20.1) is the intended
end state, with schedules reloaded and `next_fire_at` recomputed on startup.

**Guardrails:**

- `limits.min_schedule_interval` (default: 60s) — sub-minute recurrences are
  rejected at creation.
- `limits.max_schedules` (default: 50).
- Every fire is written to the audit log with `schedule_id`, target, and the
  creating parent's identity; creation and cancellation are audited too.
- Schedules are visible in `daemonseed status` and the TUI — nothing runs
  invisibly.
- Only a parent may create or cancel schedules (broker-enforced, as with all
  parent-only types). Schedules outlive the parent's connection by design,
  but a daemon shutdown cascade cancels all pending fires cleanly.

### 20.9 Parent Failover

> **Status: IMPLEMENTED.**

The daemon is the control plane: all authoritative state (registry, roles,
task queues, schedules, inboxes) lives in the broker, so a parent session is
a replaceable controller, not a single point of state. Failover makes that
explicit:

- **The parent slot re-arbitrates.** When the parent disconnects (cleanly or
  by crash), the slot frees immediately; any new parent connection — same
  name or a different one — claims it. The one-parent invariant (§5.5) is
  unchanged: never more than one at a time.
- **Child→parent traffic is buffered, not lost.** While no parent is
  connected, `STATUS_REPORT`, `ACK_TASK`, `COMPLETE_TASK`, and
  `DIRECT_MESSAGE` envelopes addressed to `parent` are held in a bounded
  daemon-side queue (500 entries, oldest dropped with a log entry) instead
  of failing with `DELIVERY_FAILED`. The sender receives a
  `DELIVERY_RECEIPT` with `queued: true` so the child knows the message
  waits rather than arrived.
- **The successor inherits the backlog.** On parent connect, the buffered
  envelopes are flushed to the new parent in arrival order, then children
  are notified with a `DIRECT_MESSAGE` from `daemon` (`parent connected`) —
  the counterpart of the existing `parent disconnected` notice.
- Correlated replies (a child answering a specific parent's
  `STATUS_REQUEST`) are NOT buffered: the requester is gone and its request
  died with it.

### 20.10 Session Tracing (Local OTel-Style)

> **Status: IMPLEMENTED.**

A local-only trace of *what talked to what, when, and how long it took* —
distinct from the audit log (§14), which is the compliance record of broker
deliveries. The trace is the debugging/observability record and includes
events the audit log never sees (MCP tool invocations with durations).

**Event model** (`internal/trace`): timestamped events with `kind`
(`message` | `tool` | `fire` | `lifecycle`), `trace_id` (follows a
request/response chain via envelope correlation), `span_id`, source
(`daemon` or `mcp:<name>`), from/to, status (`ok`/`error`/`queued`/
`dropped`), duration for tool calls, and a **truncated** payload snippet
(`trace.max_detail_chars`, default 200) — never full message bodies.

**What is traced:**

- Every parent↔child communication routed by the broker (per delivery,
  with the delivered-to set).
- Every MCP tool invocation, from every connected instance, with duration
  and truncated arguments (middleware around all registered tools).
- Scheduler fires, lifecycle markers (connect/disconnect/parent takeover),
  and error responses (`PERMISSION_DENIED`, `DELIVERY_FAILED`, …).

**Backends** (`trace.backend`):

- `jsonl` (default): append-only lines at
  `~/.local/share/daemonseed/trace.jsonl`, size-rotated.
- `sqlite`: a local database via `modernc.org/sqlite` (pure Go, WAL mode,
  safe for the daemon plus several MCP processes writing concurrently),
  indexed by time, session, and trace id — the right choice once the log
  gets big.

**Non-blocking by construction:** events flow through a bounded async queue;
a slow disk drops trace events (counted and reported) rather than stalling
message routing — same philosophy as the TUI event fan-out (Appendix B.4).

**Viewer:** `daemonseed trace [-n N] [--session NAME] [--trace-id ID]
[--kind tool|message|fire|lifecycle]` reads either backend directly, daemon
running or not.

---

## Appendix A — Implementation Checklist for Claude Code

This checklist is ordered. Do not skip ahead. Each item should be complete before moving to the next.

- [ ] `go.mod` and `go.sum` initialized
- [ ] `internal/protocol/types.go` — all MessageType constants
- [ ] `internal/protocol/envelope.go` — Envelope struct + all payload structs
- [ ] `internal/protocol/framing.go` — WriteMessage + ReadMessage with tests
- [ ] `internal/protocol/errors.go` — all sentinel error values
- [ ] `internal/config/config.go` — Config struct, loader, env override
- [ ] `internal/config/config_test.go` — happy + negative config tests
- [ ] `internal/roles/roles.go` — Role type, RoleRegistry, all methods
- [ ] `internal/roles/roles_test.go` — all role tests including concurrent access
- [ ] `internal/audit/logger.go` — JSONL audit writer, rotation
- [ ] `internal/audit/logger_test.go`
- [ ] `internal/health/monitor.go` — heartbeat loop, stale client detection
- [ ] `internal/health/monitor_test.go`
- [ ] `internal/broker/client.go` — Client struct, read/write loops
- [ ] `internal/broker/registry.go` — thread-safe client registry
- [ ] `internal/broker/broker.go` — Accept loop, handshake, routing
- [ ] `internal/broker/broker_test.go` — all happy, edge, negative tests
- [ ] `internal/testutil/testutil.go` — test helpers
- [ ] `internal/shutdown/cascade.go` — full cascade logic
- [ ] `internal/shutdown/cascade_test.go`
- [ ] `internal/mcp/server.go` — MCP server scaffolding
- [ ] `internal/mcp/tools_parent.go` — all parent tools
- [ ] `internal/mcp/tools_child.go` — all child tools
- [ ] `internal/mcp/tools_common.go` — ping, whoami, list_all
- [ ] `internal/mcp/server_test.go`
- [ ] `internal/tui/` — all TUI components
- [ ] `cmd/daemonseed/main.go` — CLI with all subcommands
- [ ] `Makefile`
- [ ] `.mcp.json.parent.example`
- [ ] `.mcp.json.child.example`
- [ ] Slash command `.md` files
- [ ] `README.md` with quickstart
- [ ] `go test -race ./...` passes with zero failures

---

## Appendix B — Notes to the Implementor

1. **Do not use `interface{}` / `any` in the hot path.** The envelope payload is `string` (JSON-encoded or plain text). Callers decode the payload into typed structs. This keeps the broker fast and simple.

2. **Do not use `time.Sleep` in production code paths.** Use `time.After` or `time.NewTimer` with proper cleanup, or context deadlines.

3. **The broker's routing goroutines must not block each other.** Each client has its own outbound channel (buffered). If a client's outbound channel is full (slow consumer), the message is dropped with a log entry and a `DELIVERY_FAILED` is sent to the original sender. Never let one slow client stall the entire broker.

4. **Bubble Tea TUI must not block the broker.** The TUI subscribes to a separate events channel that the broker fans out to. TUI lag does not affect message routing.

5. **Every `goroutine` must have a clear owner and a clear shutdown path.** No fire-and-forget goroutines without a `sync.WaitGroup` or context cancellation path. The broker must be able to shut down cleanly with zero goroutine leaks (verify with `goleak` in tests).

6. **Socket cleanup on startup is mandatory.** See §5.1 step 2. A stale socket from a previous crash must not prevent restart.

7. **Client names are case-sensitive.** `"API-Service"` and `"api-service"` are different names. Reject names containing characters outside `[a-zA-Z0-9_-]` with a clear error at handshake time.

8. **The one-parent rule is enforced with a mutex, not a channel.** It must be impossible for two parents to register simultaneously even under high concurrency.

9. **Use `log/slog` (stdlib, Go 1.21+) or `go.uber.org/zap`.** Do not use `log.Printf`. All log entries must include at minimum: level, timestamp, message, and relevant key-value fields.

10. **The MCP server is a subprocess of Claude Code.** It communicates with Claude Code via stdin/stdout (JSON-RPC). It communicates with the daemon via the Unix socket. These are two separate I/O streams — keep them carefully separated to avoid cross-contamination.
