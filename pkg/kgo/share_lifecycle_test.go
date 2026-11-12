package kgo

import (
	"context"
	"sync"
	"testing"
)

// shareLifecycleEvent is one fired OnPartitions{Assigned,Revoked,Lost}
// callback, in firing order.
type shareLifecycleEvent struct {
	kind string // "assigned", "revoked", or "lost"
	m    map[string][]int32
}

// shareLifecycleRecorder records lifecycle callbacks in order so tests
// can assert both the payload and the revoked-before-assigned ordering
// of a single assignment change.
type shareLifecycleRecorder struct {
	events []shareLifecycleEvent
}

func (r *shareLifecycleRecorder) assigned(_ context.Context, _ *Client, m map[string][]int32) {
	r.events = append(r.events, shareLifecycleEvent{"assigned", m})
}

func (r *shareLifecycleRecorder) revoked(_ context.Context, _ *Client, m map[string][]int32) {
	r.events = append(r.events, shareLifecycleEvent{"revoked", m})
}

func (r *shareLifecycleRecorder) lost(_ context.Context, _ *Client, m map[string][]int32) {
	r.events = append(r.events, shareLifecycleEvent{"lost", m})
}

// newShareLifecycleTestConsumer builds a share consumer with the given
// topics (and their partition counts) loaded in tps, without any broker.
// Cursor sources point at a dummy source whose fetch loop is permanently
// "already running", so the source wake at the end of updateAssignment
// is a no-op and no goroutines are spawned.
func newShareLifecycleTestConsumer(rec *shareLifecycleRecorder, topics map[string]int32) *shareConsumer {
	cfg := defaultCfg()
	cfg.shareGroup = "g"
	cfg.onAssigned = rec.assigned
	cfg.onRevoked = rec.revoked
	cfg.onLost = rec.lost
	cl := &Client{cfg: cfg, ctx: context.Background()}
	cl.cfg.logWrapPartitionCallbacks() // same wrapping initShare applies

	sc := &shareConsumer{
		cl:     cl,
		cfg:    &cl.cfg,
		tps:    newTopicsPartitions(),
		reSeen: make(map[string]bool),
		left:   make(chan struct{}),
	}

	src := &source{}
	src.fetchState.state.Store(stateContinueWorking) // maybeShareConsume no-ops

	names := make([]string, 0, len(topics))
	for topic := range topics {
		names = append(names, topic)
	}
	sc.tps.storeTopics(names)
	for topic, n := range topics {
		td := &topicPartitionsData{topic: topic}
		for p := int32(0); p < n; p++ {
			cursor := &shareCursor{topic: topic, partition: p, cursorsIdx: -1}
			cursor.source.Store(src)
			td.partitions = append(td.partitions, &topicPartition{shareCursor: cursor})
		}
		sc.tps.load()[topic].v.Store(td)
	}
	return sc
}

func (sc *shareConsumer) testCursor(t *testing.T, topic string, p int32) *shareCursor {
	t.Helper()
	td := sc.tps.load()[topic].load()
	if int(p) >= len(td.partitions) {
		t.Fatalf("topic %s has %d partitions, no partition %d", topic, len(td.partitions), p)
	}
	return td.partitions[p].shareCursor
}

func checkEvents(t *testing.T, rec *shareLifecycleRecorder, want []shareLifecycleEvent) {
	t.Helper()
	if len(rec.events) != len(want) {
		t.Fatalf("got %d lifecycle events %v, want %d %v", len(rec.events), rec.events, len(want), want)
	}
	for i := range want {
		got := rec.events[i]
		if got.kind != want[i].kind || !mapi32sDeepEq(got.m, want[i].m) {
			t.Errorf("event %d: got %s %v, want %s %v", i, got.kind, got.m, want[i].kind, want[i].m)
		}
	}
}

// TestShareAssignPartitionsLifecycle walks the assignment lifecycle:
// initial assign, no-op reassign, grow, shrink, and a mixed
// revoke+assign, checking which callbacks fire and with what.
func TestShareAssignPartitionsLifecycle(t *testing.T) {
	var rec shareLifecycleRecorder
	sc := newShareLifecycleTestConsumer(&rec, map[string]int32{"t": 3})

	// Initial assignment: everything is newly activated.
	sc.assignPartitions(map[string][]int32{"t": {0, 1}})
	checkEvents(t, &rec, []shareLifecycleEvent{
		{"assigned", map[string][]int32{"t": {0, 1}}},
	})

	// The broker re-sends the same assignment (e.g. a full heartbeat
	// after an error): nothing changed, no callbacks.
	rec.events = nil
	sc.assignPartitions(map[string][]int32{"t": {0, 1}})
	checkEvents(t, &rec, nil)

	// Grow: only the newly activated partition fires assigned.
	rec.events = nil
	sc.assignPartitions(map[string][]int32{"t": {0, 1, 2}})
	checkEvents(t, &rec, []shareLifecycleEvent{
		{"assigned", map[string][]int32{"t": {2}}},
	})

	// Shrink: the dropped partitions fire revoked, and revoked runs
	// before any assigned from the same pass.
	rec.events = nil
	sc.assignPartitions(map[string][]int32{"t": {1}})
	checkEvents(t, &rec, []shareLifecycleEvent{
		{"revoked", map[string][]int32{"t": {0, 2}}},
	})
	for _, p := range []int32{0, 2} {
		if sc.testCursor(t, "t", p).assigned.Load() {
			t.Errorf("partition %d still active after revoke", p)
		}
	}

	// Mixed: one partition leaves, another joins, in one assignment.
	// The revoke must complete before the assign fires.
	rec.events = nil
	sc.assignPartitions(map[string][]int32{"t": {0}})
	checkEvents(t, &rec, []shareLifecycleEvent{
		{"revoked", map[string][]int32{"t": {1}}},
		{"assigned", map[string][]int32{"t": {0}}},
	})
	if rec.events[0].kind != "revoked" {
		t.Errorf("revoke must fire before assign, got order %v", rec.events)
	}
}

// TestShareAssignPartitionsMetadataLag: a partition the broker assigns
// before our metadata knows it is not activated and fires no callback;
// once metadata catches up, the retry activates it and fires assigned.
func TestShareAssignPartitionsMetadataLag(t *testing.T) {
	var rec shareLifecycleRecorder
	sc := newShareLifecycleTestConsumer(&rec, map[string]int32{"t": 1})

	sc.assignPartitions(map[string][]int32{"t": {0, 3}})
	checkEvents(t, &rec, []shareLifecycleEvent{
		{"assigned", map[string][]int32{"t": {0}}},
	})
	if !sc.pendingAssigns {
		t.Error("pendingAssigns not set for a partition our metadata does not know")
	}

	// Metadata catches up: partition 3 now exists (partitions were
	// added). The retry activates it and fires assigned for it alone.
	rec.events = nil
	src := &source{}
	src.fetchState.state.Store(stateContinueWorking)
	td := sc.tps.load()["t"].load()
	for p := int32(1); p <= 3; p++ {
		cursor := &shareCursor{topic: "t", partition: p, cursorsIdx: -1}
		cursor.source.Store(src)
		td.partitions = append(td.partitions, &topicPartition{shareCursor: cursor})
	}
	sc.tps.load()["t"].v.Store(td)

	sc.assignPartitions(map[string][]int32{"t": {0, 3}})
	checkEvents(t, &rec, []shareLifecycleEvent{
		{"assigned", map[string][]int32{"t": {3}}},
	})
	if sc.pendingAssigns {
		t.Error("pendingAssigns still set after all assigned partitions activated")
	}
}

// TestShareLoseAssignment: membership loss (fence / unknown member)
// fires OnPartitionsLost with the full active assignment, never
// OnPartitionsRevoked, and deactivates every cursor.
func TestShareLoseAssignment(t *testing.T) {
	var rec shareLifecycleRecorder
	sc := newShareLifecycleTestConsumer(&rec, map[string]int32{"t": 3})

	sc.assignPartitions(map[string][]int32{"t": {0, 1}})
	rec.events = nil

	sc.loseAssignment()
	checkEvents(t, &rec, []shareLifecycleEvent{
		{"lost", map[string][]int32{"t": {0, 1}}},
	})
	for _, p := range []int32{0, 1} {
		if sc.testCursor(t, "t", p).assigned.Load() {
			t.Errorf("partition %d still active after membership loss", p)
		}
	}
	if got := sc.nowAssigned.read(); got != nil {
		t.Errorf("nowAssigned = %v after membership loss, want nil", got)
	}

	// Losing with nothing assigned fires nothing.
	rec.events = nil
	sc.loseAssignment()
	checkEvents(t, &rec, nil)
}

// TestShareLeaveFiresRevoked: a graceful leave fires OnPartitionsRevoked
// for everything still assigned, after which nowAssigned is cleared.
func TestShareLeaveFiresRevoked(t *testing.T) {
	var rec shareLifecycleRecorder
	sc := newShareLifecycleTestConsumer(&rec, map[string]int32{"t": 2})

	// leave needs the consumer and fetch manager that initShare wires up.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &consumer{cl: sc.cl}
	sc.c = c
	sc.fm = newFetchManager(ctx, cancel, &c.pollActive, c.pollWake, -1)
	sc.cond = sync.NewCond(&sc.mu)
	sc.ackC = sync.NewCond(&sc.ackMu)

	sc.assignPartitions(map[string][]int32{"t": {0, 1}})
	rec.events = nil

	sc.leave(context.Background())
	select {
	case <-sc.left:
	default:
		t.Fatal("leave returned without closing sc.left")
	}
	checkEvents(t, &rec, []shareLifecycleEvent{
		{"revoked", map[string][]int32{"t": {0, 1}}},
	})
	if got := sc.nowAssigned.read(); got != nil {
		t.Errorf("nowAssigned = %v after leave, want nil", got)
	}

	// A second leave is a no-op (single-shot guard), and leaving with
	// nothing assigned fires no callback.
	rec.events = nil
	sc.leave(context.Background())
	checkEvents(t, &rec, nil)
}

// TestShareLifecycleConfigValidation: partition lifecycle callbacks are
// valid with a share group (they used to be rejected), still invalid
// with no group at all, and still valid with a classic group.
func TestShareLifecycleConfigValidation(t *testing.T) {
	noop := func(context.Context, *Client, map[string][]int32) {}

	if _, _, err := validateCfg(
		ShareGroup("g"),
		OnPartitionsAssigned(noop),
		OnPartitionsRevoked(noop),
		OnPartitionsLost(noop),
	); err != nil {
		t.Errorf("share group with lifecycle callbacks: unexpected error: %v", err)
	}

	if _, _, err := validateCfg(OnPartitionsAssigned(noop)); err == nil {
		t.Error("expected an error for lifecycle callbacks without any group, got nil")
	}

	if _, _, err := validateCfg(
		ConsumerGroup("g"),
		OnPartitionsAssigned(noop),
		OnPartitionsRevoked(noop),
		OnPartitionsLost(noop),
	); err != nil {
		t.Errorf("classic group with lifecycle callbacks: unexpected error: %v", err)
	}
}
