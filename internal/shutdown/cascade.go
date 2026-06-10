// Package shutdown implements the graceful shutdown cascade (spec §11):
// notify all children in parallel, collect SHUTDOWN_ACKs until everyone has
// answered or the timeout elapses, then force-disconnect stragglers.
package shutdown

import (
	"context"
	"log/slog"
	"time"

	"github.com/arterberry/daemonseed/internal/protocol"
)

// Target identifies one child to be notified.
type Target struct {
	ID   string
	Name string
}

// Notifier is the cascade's view of the broker. It is an interface so the
// cascade logic is testable with a fake bus.
type Notifier interface {
	// Children returns the children connected at cascade start.
	Children() []Target
	// Send delivers env to the client; returns false if it could not be queued.
	Send(clientID string, env *protocol.Envelope) bool
	// ForceDisconnect closes a client that did not acknowledge in time.
	ForceDisconnect(clientID string)
}

// Result reports the outcome of phases 1–2 of the cascade. Names are used
// (not client_ids) because the result is surfaced to the operator.
type Result struct {
	Acked  []string
	Forced []string
}

// Cascade coordinates one shutdown sequence. Create with New, feed events
// with Ack/ClientGone (from the broker's dispatch loop), and drive it with
// Run. A Cascade is single-use.
type Cascade struct {
	notifier Notifier
	timeout  time.Duration
	log      *slog.Logger
	acks     chan string
	gone     chan string
}

// New creates a cascade that waits up to timeout for child acknowledgments.
func New(n Notifier, timeout time.Duration, log *slog.Logger) *Cascade {
	if log == nil {
		log = slog.Default()
	}
	return &Cascade{
		notifier: n,
		timeout:  timeout,
		log:      log,
		// Buffers sized generously so feeders never block even if Run has
		// already returned (late ACKs are simply ignored).
		acks: make(chan string, 128),
		gone: make(chan string, 128),
	}
}

// Ack records a SHUTDOWN_ACK from clientID. Never blocks.
func (c *Cascade) Ack(clientID string) {
	select {
	case c.acks <- clientID:
	default:
	}
}

// ClientGone records that clientID disconnected mid-cascade, so it must not
// be waited for (spec §17.4 TestShutdown_ChildDisconnectedDuringCascade).
func (c *Cascade) ClientGone(clientID string) {
	select {
	case c.gone <- clientID:
	default:
	}
}

// Run executes phases 1 and 2: notify every child, then collect ACKs until
// all children have answered, the timeout elapses, or ctx is canceled.
// Children that never ACKed (including those whose notice could not be
// queued, and those that disconnected on their own mid-cascade) are
// force-disconnected and reported in Result.Forced.
func (c *Cascade) Run(ctx context.Context, reason, initiatedBy string) Result {
	children := c.notifier.Children()
	var res Result
	if len(children) == 0 {
		return res // §17.4 TestShutdown_NoChildrenConnected: completes immediately
	}

	payload := protocol.MustEncode(protocol.ShutdownNoticePayload{
		Reason:         reason,
		TimeoutSeconds: int(c.timeout / time.Second),
		InitiatedBy:    initiatedBy,
	})

	awaiting := make(map[string]string, len(children)) // id → name
	for _, ch := range children {
		env := protocol.NewEnvelope(protocol.DaemonSenderID, ch.ID, protocol.TypeShutdownNotice, payload)
		if !c.notifier.Send(ch.ID, env) {
			c.log.Warn("shutdown notice undeliverable", "client_id", ch.ID, "name", ch.Name)
			res.Forced = append(res.Forced, ch.Name)
			c.notifier.ForceDisconnect(ch.ID)
			continue
		}
		awaiting[ch.ID] = ch.Name
	}

	timer := time.NewTimer(c.timeout)
	defer timer.Stop()
	for len(awaiting) > 0 {
		select {
		case id := <-c.acks:
			if name, ok := awaiting[id]; ok {
				delete(awaiting, id)
				res.Acked = append(res.Acked, name)
				c.log.Info("shutdown acknowledged", "client_id", id, "name", name)
			}
		case id := <-c.gone:
			if name, ok := awaiting[id]; ok {
				delete(awaiting, id)
				res.Forced = append(res.Forced, name)
				c.log.Warn("client disconnected during cascade", "client_id", id, "name", name)
			}
		case <-timer.C:
			for id, name := range awaiting {
				res.Forced = append(res.Forced, name)
				c.log.Warn("shutdown ack timed out, forcing disconnect", "client_id", id, "name", name)
				c.notifier.ForceDisconnect(id)
			}
			return res
		case <-ctx.Done():
			for id, name := range awaiting {
				res.Forced = append(res.Forced, name)
				c.notifier.ForceDisconnect(id)
			}
			return res
		}
	}
	return res
}
