package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/rules"
	"github.com/Lauiskk/vigil/internal/state"
	"github.com/Lauiskk/vigil/internal/window"
)

var base = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

// clock is a hand-cranked time source: the engine's cooldown and detection
// stamps are behaviour worth asserting, and sleeping to observe them would
// make the suite slow and flaky at once.
type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newEngine() (*Engine, *clock) {
	c := &clock{t: base}
	return New(rules.DefaultProfiles(), WithClock(c.now)), c
}

func payment(id string, off time.Duration, v float64) domain.Event {
	return domain.Event{
		ID: id, Stream: domain.StreamPayments, Key: "card-1",
		At: base.Add(off), Value: v, Unit: "BRL",
	}
}

// tripVelocity is one more authorisation than the payments profile allows in
// a minute. Derived rather than hard-coded so retuning the profile does not
// silently stop these tests from testing anything.
var tripVelocity = func() int {
	for _, r := range rules.DefaultProfiles()[domain.StreamPayments].Rules {
		if v, ok := r.(rules.Velocity); ok {
			return v.Max + 1
		}
	}
	panic("payments profile has no velocity rule")
}()

// burst files n authorisations one second apart.
func burst(e *Engine, n int, from time.Duration) []domain.Alert {
	var out []domain.Alert
	for i := 0; i < n; i++ {
		alerts, err := e.Ingest(payment(
			"ev-"+time.Duration(i).String()+from.String(),
			from+time.Duration(i)*time.Second, 1))
		if err != nil {
			panic(err)
		}
		out = append(out, alerts...)
	}
	return out
}

func TestIngestRejectsUnprocessableRecords(t *testing.T) {
	e, _ := newEngine()

	tests := []struct {
		name string
		ev   domain.Event
	}{
		{"empty id", domain.Event{Stream: domain.StreamPayments, Key: "k", At: base}},
		{"zero time", domain.Event{ID: "x", Stream: domain.StreamPayments, Key: "k"}},
		{"unknown stream", domain.Event{ID: "x", Stream: "telemetry", Key: "k", At: base}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			alerts, err := e.Ingest(tc.ev)
			if err == nil {
				t.Fatal("expected an error so the record can be dead-lettered")
			}
			if !errors.Is(err, domain.ErrInvalidEvent) {
				t.Errorf("error %v is not routable to the DLQ", err)
			}
			if alerts != nil {
				t.Error("a rejected record must not raise alerts")
			}
		})
	}
	if got := e.Stats().Invalid.Load(); got != uint64(len(tests)) {
		t.Errorf("Invalid = %d, want %d", got, len(tests))
	}
}

// A stream with no profile is unprocessable even though the event itself is
// well-formed, and must be distinguishable from a malformed record.
func TestIngestRejectsStreamsWithoutAProfile(t *testing.T) {
	e := New(map[domain.Stream]rules.Profile{}, WithClock(func() time.Time { return base }))
	_, err := e.Ingest(payment("a", 0, 1))
	if err == nil || !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatalf("err = %v, want a DLQ-routable error", err)
	}
}

func TestIngestRaisesAndThenSuppresses(t *testing.T) {
	e, c := newEngine()

	got := burst(e, tripVelocity, 0)
	if len(got) != 1 {
		t.Fatalf("first burst raised %d alerts, want exactly 1", len(got))
	}
	if got[0].Rule != "velocity" || got[0].ID == "" {
		t.Errorf("alert = %+v, want a velocity alert with an id", got[0])
	}
	if got[0].DetectedAt != base {
		t.Error("DetectedAt did not come from the injected clock")
	}

	// More of the same inside the cooldown is the same fact, not new alerts.
	if extra := burst(e, 6, time.Duration(tripVelocity)*time.Second); len(extra) != 0 {
		t.Errorf("cooldown let %d repeat alerts through", len(extra))
	}
	if e.Stats().Suppressed.Load() == 0 {
		t.Error("suppression was not counted")
	}

	// Past the cooldown the same condition is worth reporting again.
	c.advance(2 * time.Minute)
	if again := burst(e, tripVelocity, 3*time.Minute); len(again) != 1 {
		t.Errorf("after the cooldown expired, got %d alerts, want 1", len(again))
	}
}

func TestAlertIDsAreUnique(t *testing.T) {
	e, c := newEngine()
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		for _, a := range burst(e, tripVelocity, time.Duration(i)*10*time.Minute) {
			if seen[a.ID] {
				t.Fatalf("duplicate alert id %q", a.ID)
			}
			seen[a.ID] = true
		}
		c.advance(time.Minute)
	}
	if len(seen) != 5 {
		t.Errorf("collected %d distinct alerts, want 5", len(seen))
	}
}

func TestLateRecordsAreCountedNotFailed(t *testing.T) {
	e, _ := newEngine()
	if _, err := e.Ingest(payment("a", 0, 1)); err != nil {
		t.Fatal(err)
	}
	// Push the watermark far ahead, then file something long past the grace.
	if _, err := e.Ingest(payment("b", 10*time.Minute, 1)); err != nil {
		t.Fatal(err)
	}
	alerts, err := e.Ingest(payment("c", time.Second, 1))
	if err != nil {
		t.Fatalf("a late record is not an error: %v", err)
	}
	if alerts != nil {
		t.Error("a late record must not raise alerts")
	}
	if got := e.Stats().Late.Load(); got != 1 {
		t.Errorf("Late = %d, want 1", got)
	}
}

func TestTerminalEventClosesTheKey(t *testing.T) {
	e, _ := newEngine()
	job := func(id string, off time.Duration, status string) domain.Event {
		ev := domain.Event{
			ID: id, Stream: domain.StreamVideo, Key: "job-1",
			At: base.Add(off), Value: 30, Unit: "fps",
		}
		if status != "" {
			ev.Labels = map[string]string{domain.LabelStatus: status}
		}
		return ev
	}

	if _, err := e.Ingest(job("a", 0, "running")); err != nil {
		t.Fatal(err)
	}
	if e.Store().Len() != 1 {
		t.Fatal("the job should be tracked while it runs")
	}

	if _, err := e.Ingest(job("b", time.Second, "done")); err != nil {
		t.Fatal(err)
	}
	if e.Store().Len() != 0 {
		t.Error("a finished job must be forgotten, or stall will fire on it forever")
	}

	// And with the key gone, sweeping must not invent a stall for it.
	e.now = func() time.Time { return base.Add(time.Hour) }
	if alerts := e.Sweep(); len(alerts) != 0 {
		t.Errorf("sweep raised %d alerts for a completed job", len(alerts))
	}
}

func TestSweepFiresStallForAnAbandonedJob(t *testing.T) {
	e, c := newEngine()
	_, err := e.Ingest(domain.Event{
		ID: "a", Stream: domain.StreamVideo, Key: "job-2",
		At: base, Value: 30, Unit: "fps",
		Labels: map[string]string{domain.LabelStatus: "running"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if alerts := e.Sweep(); len(alerts) != 0 {
		t.Fatalf("swept too early and raised %d alerts", len(alerts))
	}

	c.advance(2 * time.Minute) // past Stall.After of 90s
	alerts := e.Sweep()
	if len(alerts) != 1 {
		t.Fatalf("got %d alerts, want 1 stall", len(alerts))
	}
	if alerts[0].Rule != "stall" || alerts[0].Key != "job-2" {
		t.Errorf("alert = %+v", alerts[0])
	}
	// Swept alerts must declare themselves, or their multi-minute interval
	// pollutes every latency percentile downstream.
	if !alerts[0].Absence {
		t.Error("a swept alert was not marked as absence-raised")
	}
	if _, ok := alerts[0].PipelineLatency(); ok {
		t.Error("an absence alert reported a usable pipeline latency")
	}

	// The cooldown applies to swept alerts too.
	if repeat := e.Sweep(); len(repeat) != 0 {
		t.Errorf("sweep repeated the stall %d times", len(repeat))
	}

	// And once reported, the key is gone — so it does not re-raise the same
	// alert every cooldown for as long as it stays silent.
	if e.Store().Len() != 0 {
		t.Error("a key reported as stalled is still being tracked")
	}
	c.advance(time.Hour)
	if repeat := e.Sweep(); len(repeat) != 0 {
		t.Errorf("an hour later the dead key raised %d more alerts", len(repeat))
	}
}

// A permanently dead entity must be reported once, not once per cooldown.
func TestASilentKeyIsReportedOnlyOnce(t *testing.T) {
	e, c := newEngine()
	for i := 0; i < 5; i++ {
		_, err := e.Ingest(domain.Event{
			ID: "j" + time.Duration(i).String(), Stream: domain.StreamVideo,
			Key: "job-dead", At: base, Value: 30, Unit: "fps",
			Labels: map[string]string{domain.LabelStatus: "running"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	total := 0
	// Twenty minutes of sweeping, well past several cooldown periods.
	for i := 0; i < 40; i++ {
		c.advance(30 * time.Second)
		total += len(e.Sweep())
	}
	if total != 1 {
		t.Errorf("one permanently silent job produced %d alerts over twenty minutes, want 1", total)
	}
}

// Payments has no sweep interval — a card that goes quiet is not a fault —
// so sweeping must leave it alone however long it idles.
func TestSweepSkipsStreamsWithoutASweepInterval(t *testing.T) {
	e, c := newEngine()
	if _, err := e.Ingest(payment("a", 0, 1)); err != nil {
		t.Fatal(err)
	}
	c.advance(24 * time.Hour)
	if alerts := e.Sweep(); len(alerts) != 0 {
		t.Errorf("a quiet card raised %d alerts", len(alerts))
	}
}

func TestEvictIdleAndGeometry(t *testing.T) {
	e, c := newEngine()
	if _, err := e.Ingest(payment("a", 0, 1)); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Hour)
	if got := e.EvictIdle(30 * time.Minute); got != 1 {
		t.Errorf("evicted %d, want 1", got)
	}

	g, ok := e.Geometry(domain.StreamPayments)
	if !ok || g.Window != time.Minute {
		t.Errorf("geometry = %+v (ok=%v)", g, ok)
	}
	if _, ok := e.Geometry("nope"); ok {
		t.Error("an unknown stream should have no geometry")
	}
}

// The cooldown map must not accumulate one entry per key that ever alerted.
func TestSweepPrunesExpiredCooldowns(t *testing.T) {
	e, c := newEngine()
	burst(e, tripVelocity, 0)
	e.mu.Lock()
	held := len(e.cooldown)
	e.mu.Unlock()
	if held == 0 {
		t.Fatal("expected a cooldown entry after an alert")
	}

	c.advance(time.Hour)
	e.Sweep()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.cooldown) != 0 {
		t.Errorf("%d expired cooldown entries survived the sweep", len(e.cooldown))
	}
}

func TestRandomIDIsHex(t *testing.T) {
	id := randomID()
	if len(id) != 8 {
		t.Errorf("randomID = %q, want 8 hex chars", id)
	}
	if a, b := randomID(), randomID(); a == b {
		t.Error("randomID produced the same value twice")
	}
}

func TestWithStoreAdoptsExistingState(t *testing.T) {
	restored := state.New()
	restored.Add(
		state.Key{Stream: domain.StreamPayments, ID: "card-1"},
		state.Geometry{Window: time.Minute, MaxPoints: 64},
		window.Point{ID: "old", At: base, Value: 1},
		base,
	)

	e := New(rules.DefaultProfiles(), WithClock(func() time.Time { return base }), WithStore(restored))
	if e.Store().Len() != 1 {
		t.Fatal("the engine did not adopt the store it was given")
	}
	// The replayed observation must count toward the window, which is the
	// entire point of restoring from the changelog after a rebalance.
	if w := e.Store().Window(state.Key{Stream: domain.StreamPayments, ID: "card-1"}); w == nil || w.Count() != 1 {
		t.Error("restored state was not visible to the engine")
	}
}

// A rule listed in a profile but scoped to another stream must be skipped
// rather than evaluated against events it was never meant to see.
func TestIngestSkipsRulesScopedToAnotherStream(t *testing.T) {
	profiles := map[domain.Stream]rules.Profile{
		domain.StreamPayments: {
			Stream: domain.StreamPayments, Window: time.Minute, MaxPoints: 16,
			Cooldown: time.Minute,
			Rules: []rules.Rule{
				rules.Velocity{Max: 1, Per: time.Minute, On: []domain.Stream{domain.StreamVideo}},
			},
		},
	}
	e := New(profiles, WithClock(func() time.Time { return base }))
	for i := 0; i < 5; i++ {
		alerts, err := e.Ingest(payment("p-"+time.Duration(i).String(), time.Duration(i)*time.Second, 1))
		if err != nil {
			t.Fatal(err)
		}
		if len(alerts) != 0 {
			t.Fatalf("a video-scoped rule fired on a payment: %+v", alerts)
		}
	}
}

func TestTerminalEventClearsTheCooldown(t *testing.T) {
	e, _ := newEngine()
	job := func(id string, off time.Duration, status string) domain.Event {
		return domain.Event{
			ID: id, Stream: domain.StreamVideo, Key: "job-3",
			At: base.Add(off), Value: 30, Unit: "fps",
			Labels: map[string]string{domain.LabelStatus: status},
		}
	}
	// Clear the video velocity limit, whatever the profile sets it to.
	var raised int
	for i := 0; i < 40; i++ {
		alerts, err := e.Ingest(job("j-"+time.Duration(i).String(), time.Duration(i)*time.Second, "running"))
		if err != nil {
			t.Fatal(err)
		}
		raised += len(alerts)
	}
	if raised == 0 {
		t.Fatal("expected a retry-storm alert to seed a cooldown entry")
	}
	e.mu.Lock()
	before := len(e.cooldown)
	e.mu.Unlock()
	if before == 0 {
		t.Fatal("no cooldown entry was recorded")
	}

	if _, err := e.Ingest(job("j-final", 11*time.Second, "done")); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.cooldown) != 0 {
		t.Errorf("%d cooldown entries survived the job finishing", len(e.cooldown))
	}
}

func TestRandomIDFallsBackWhenEntropyFails(t *testing.T) {
	id := randomIDFrom(func([]byte) (int, error) { return 0, errors.New("no entropy") })
	if len(id) != 8 {
		t.Errorf("fallback id = %q, want 8 characters", id)
	}
}
