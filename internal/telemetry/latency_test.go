package telemetry

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLatencyPercentiles(t *testing.T) {
	l := NewLatency(1000)
	// 1ms through 100ms, so the percentiles are known by construction.
	for i := 1; i <= 100; i++ {
		l.Observe(time.Duration(i) * time.Millisecond)
	}

	s := l.Snapshot()
	if s.Count != 100 {
		t.Errorf("Count = %d, want 100", s.Count)
	}
	if s.P50 != 51*time.Millisecond {
		t.Errorf("P50 = %v, want 51ms", s.P50)
	}
	if s.P99 != 100*time.Millisecond {
		t.Errorf("P99 = %v, want 100ms", s.P99)
	}
	if s.Max != 100*time.Millisecond {
		t.Errorf("Max = %v, want 100ms", s.Max)
	}
}

// The ring is the reason the dashboard recovers after a surge instead of
// reporting the surge's p99 forever.
func TestLatencyForgetsOldSamples(t *testing.T) {
	l := NewLatency(10)
	for i := 0; i < 10; i++ {
		l.Observe(time.Second) // a slow period
	}
	if got := l.Snapshot().P50; got != time.Second {
		t.Fatalf("P50 = %v, want 1s", got)
	}

	for i := 0; i < 10; i++ {
		l.Observe(time.Millisecond) // recovery
	}
	s := l.Snapshot()
	if s.P50 != time.Millisecond {
		t.Errorf("P50 = %v, want the slow period to have rolled out of the ring", s.P50)
	}
	// The all-time maximum is deliberately not forgotten.
	if s.Max != time.Second {
		t.Errorf("Max = %v, want the peak retained", s.Max)
	}
	if s.Count != 20 {
		t.Errorf("Count = %d, want every observation counted", s.Count)
	}
}

func TestLatencyEmptyAndDegenerate(t *testing.T) {
	if got := NewLatency(100).Snapshot(); got.Count != 0 || got.P99 != 0 {
		t.Errorf("empty snapshot = %+v, want zeroes", got)
	}
	// A nonsensical size must not panic on the first write.
	l := NewLatency(0)
	l.Observe(5 * time.Millisecond)
	if got := l.Snapshot().P50; got != 5*time.Millisecond {
		t.Errorf("P50 = %v, want 5ms", got)
	}
}

func TestSnapshotMarshalsAsMilliseconds(t *testing.T) {
	l := NewLatency(10)
	l.Observe(1500 * time.Microsecond)

	body, err := json.Marshal(l.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["p50Ms"] != 1.5 {
		t.Errorf("p50Ms = %v, want 1.5 — the browser should not be dividing nanoseconds", got["p50Ms"])
	}
}

func TestRate(t *testing.T) {
	base := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	r := NewRate(base)

	r.Add(120)
	if got := r.Roll(base.Add(2 * time.Second)); got != 60 {
		t.Errorf("rate = %v, want 60/s over two seconds", got)
	}

	per, total := r.Per()
	if per != 60 || total != 120 {
		t.Errorf("Per = (%v, %d), want (60, 120)", per, total)
	}

	// The interval resets, so an idle window reports zero rather than
	// repeating the previous burst.
	if got := r.Roll(base.Add(3 * time.Second)); got != 0 {
		t.Errorf("idle interval rate = %v, want 0", got)
	}
	// And the running total is cumulative, not per interval.
	if _, total := r.Per(); total != 120 {
		t.Errorf("total = %d, want 120", total)
	}
}

func TestRateIgnoresANonAdvancingClock(t *testing.T) {
	base := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	r := NewRate(base)
	r.Add(10)
	if got := r.Roll(base); got != 0 {
		t.Errorf("a zero-length interval yielded %v, want the previous rate (0)", got)
	}
}
