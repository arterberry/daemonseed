package broker

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/arterberry/daemonseed/internal/cron"
	"github.com/arterberry/daemonseed/internal/protocol"
)

// Misfire policies (§20.8): what happens when a schedule fires while the
// target child is offline.
const (
	misfireQueue = "queue" // default: queue with TTL, delivered on reconnect
	misfireSkip  = "skip"  // drop the occurrence, log it
)

// defaultOneShotTTL bounds how long a queued one-shot task waits for an
// offline child before expiring.
const defaultOneShotTTL = 24 * time.Hour

// schedule is one registered schedule with its parsed trigger.
type schedule struct {
	info protocol.ScheduleInfo
	task protocol.TaskPayload // template; per-fire task_id is generated

	oneShot  time.Time      // set when trigger is "at"
	interval time.Duration  // set when trigger is "every"
	cronExpr *cron.Schedule // set when trigger is "cron"

	ttl  time.Duration
	next time.Time
}

// advance computes the fire after `now`, returning false when the schedule
// is exhausted (one-shots, unsatisfiable cron).
func (s *schedule) advance(now time.Time) bool {
	switch {
	case !s.oneShot.IsZero():
		return false
	case s.interval > 0:
		s.next = now.Add(s.interval)
		return true
	default:
		s.next = s.cronExpr.Next(now)
		return !s.next.IsZero()
	}
}

// queueTTL is the expiry applied when the fired task must wait in the queue.
func (s *schedule) queueTTL() time.Duration {
	if s.ttl > 0 {
		return s.ttl
	}
	if s.interval > 0 {
		return s.interval // default: superseded by the next occurrence
	}
	if s.cronExpr != nil {
		if next := s.cronExpr.Next(time.Now()); !next.IsZero() {
			if gap := time.Until(next); gap > 0 {
				return gap
			}
		}
	}
	return defaultOneShotTTL
}

// fireFunc delivers one occurrence. Implemented by the broker.
type fireFunc func(s *schedule, task protocol.TaskPayload)

// scheduler owns the timer loop. It lives in the daemon so schedules fire
// whether or not the authoring parent session is still connected (§20.8).
type scheduler struct {
	mu        sync.Mutex
	schedules map[string]*schedule
	wake      chan struct{}
	fire      fireFunc
	log       *slog.Logger

	minInterval  time.Duration
	maxSchedules int
}

func newScheduler(minInterval time.Duration, maxSchedules int, log *slog.Logger, fire fireFunc) *scheduler {
	return &scheduler{
		schedules:    make(map[string]*schedule),
		wake:         make(chan struct{}, 1),
		fire:         fire,
		log:          log,
		minInterval:  minInterval,
		maxSchedules: maxSchedules,
	}
}

// add validates and registers a schedule, returning its public info.
func (sc *scheduler) add(req protocol.ScheduleCreatePayload, createdBy string) (protocol.ScheduleInfo, error) {
	set := 0
	for _, v := range []string{req.Trigger.At, req.Trigger.Every, req.Trigger.Cron} {
		if v != "" {
			set++
		}
	}
	if set != 1 {
		return protocol.ScheduleInfo{}, fmt.Errorf("trigger must set exactly one of at, every, cron")
	}
	if req.Task.Instruction == "" {
		return protocol.ScheduleInfo{}, fmt.Errorf("task must include a non-empty instruction")
	}
	if req.Target == "" {
		return protocol.ScheduleInfo{}, fmt.Errorf("target child name is required")
	}
	misfire := req.Misfire
	if misfire == "" {
		misfire = misfireQueue
	}
	if misfire != misfireQueue && misfire != misfireSkip {
		return protocol.ScheduleInfo{}, fmt.Errorf("misfire must be %q or %q", misfireQueue, misfireSkip)
	}

	s := &schedule{task: req.Task}
	now := time.Now()
	switch {
	case req.Trigger.At != "":
		at, err := time.Parse(time.RFC3339, req.Trigger.At)
		if err != nil {
			return protocol.ScheduleInfo{}, fmt.Errorf("invalid at timestamp (want RFC 3339): %w", err)
		}
		if !at.After(now) {
			return protocol.ScheduleInfo{}, fmt.Errorf("at timestamp %s is in the past", req.Trigger.At)
		}
		s.oneShot = at
		s.next = at
	case req.Trigger.Every != "":
		every, err := time.ParseDuration(req.Trigger.Every)
		if err != nil {
			return protocol.ScheduleInfo{}, fmt.Errorf("invalid every duration: %w", err)
		}
		if every <= 0 {
			return protocol.ScheduleInfo{}, fmt.Errorf("every must be positive")
		}
		if every < sc.minInterval {
			return protocol.ScheduleInfo{}, fmt.Errorf("interval %s is below the minimum %s", every, sc.minInterval)
		}
		s.interval = every
		s.next = now.Add(every)
	default:
		expr, err := cron.Parse(req.Trigger.Cron)
		if err != nil {
			return protocol.ScheduleInfo{}, err
		}
		first := expr.Next(now)
		if first.IsZero() {
			return protocol.ScheduleInfo{}, fmt.Errorf("cron expression %q never fires", req.Trigger.Cron)
		}
		// Guardrail: the gap between the first two fires must respect the
		// minimum interval (approximation; exact minimum gap is unbounded
		// to compute).
		if second := expr.Next(first); !second.IsZero() && second.Sub(first) < sc.minInterval {
			return protocol.ScheduleInfo{}, fmt.Errorf("cron fires every %s, below the minimum %s",
				second.Sub(first), sc.minInterval)
		}
		s.cronExpr = expr
		s.next = first
	}
	if req.TTL != "" {
		ttl, err := time.ParseDuration(req.TTL)
		if err != nil || ttl <= 0 {
			return protocol.ScheduleInfo{}, fmt.Errorf("invalid ttl duration %q", req.TTL)
		}
		s.ttl = ttl
	}

	s.info = protocol.ScheduleInfo{
		ID:         "sched-" + uuid.NewString()[:8],
		Target:     req.Target,
		Trigger:    req.Trigger,
		Misfire:    misfire,
		CreatedBy:  createdBy,
		CreatedAt:  now.UTC(),
		NextFireAt: s.next.UTC(),
	}

	sc.mu.Lock()
	if len(sc.schedules) >= sc.maxSchedules {
		sc.mu.Unlock()
		return protocol.ScheduleInfo{}, fmt.Errorf("schedule limit reached (%d)", sc.maxSchedules)
	}
	sc.schedules[s.info.ID] = s
	info := s.info // copy under the lock: fireDue mutates info.FireCount
	sc.mu.Unlock()
	sc.kick()
	return info, nil
}

// cancel removes a schedule. Returns false if the id is unknown.
func (sc *scheduler) cancel(id string) bool {
	sc.mu.Lock()
	_, ok := sc.schedules[id]
	delete(sc.schedules, id)
	sc.mu.Unlock()
	if ok {
		sc.kick()
	}
	return ok
}

// list returns all schedules sorted by next fire time.
func (sc *scheduler) list() []protocol.ScheduleInfo {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	out := make([]protocol.ScheduleInfo, 0, len(sc.schedules))
	for _, s := range sc.schedules {
		info := s.info
		info.NextFireAt = s.next.UTC()
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NextFireAt.Before(out[j].NextFireAt) })
	return out
}

// kick wakes the run loop after a mutation. Never blocks.
func (sc *scheduler) kick() {
	select {
	case sc.wake <- struct{}{}:
	default:
	}
}

// run is the timer loop: sleep until the soonest next fire, fire everything
// due, repeat. A daemon shutdown cancels ctx, which cancels all pending
// fires cleanly (§20.8).
func (sc *scheduler) run(ctx context.Context) {
	for {
		next, any := sc.soonest()
		var timerCh <-chan time.Time
		if any {
			timer := time.NewTimer(time.Until(next))
			timerCh = timer.C
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-sc.wake:
				timer.Stop()
				continue // recompute after add/cancel
			case <-timerCh:
				sc.fireDue(time.Now())
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-sc.wake:
		}
	}
}

func (sc *scheduler) soonest() (time.Time, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	var soonest time.Time
	for _, s := range sc.schedules {
		if soonest.IsZero() || s.next.Before(soonest) {
			soonest = s.next
		}
	}
	return soonest, !soonest.IsZero()
}

// fireDue fires every schedule whose time has arrived and advances or
// retires it. The fire sequence is captured under the lock; the callbacks
// run outside it so a slow delivery never blocks schedule mutation.
func (sc *scheduler) fireDue(now time.Time) {
	type firing struct {
		s   *schedule
		seq int
	}
	sc.mu.Lock()
	var due []firing
	for _, s := range sc.schedules {
		if !s.next.After(now) {
			s.info.FireCount++
			due = append(due, firing{s: s, seq: s.info.FireCount})
			if !s.advance(now) {
				delete(sc.schedules, s.info.ID)
			}
		}
	}
	sc.mu.Unlock()

	for _, f := range due {
		task := f.s.task
		task.TaskID = fmt.Sprintf("%s-%d", f.s.info.ID, f.seq)
		task.AssignedAt = now.UTC()
		// Copy the context map: the template is shared across fires.
		ctx := make(map[string]string, len(f.s.task.Context)+2)
		for k, v := range f.s.task.Context {
			ctx[k] = v
		}
		ctx["schedule_id"] = f.s.info.ID
		ctx["scheduled_by"] = f.s.info.CreatedBy
		task.Context = ctx
		sc.fire(f.s, task)
	}
}
