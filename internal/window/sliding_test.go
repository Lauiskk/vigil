package window

import (
	"math"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return t0.Add(d) }

// add is a helper for the common "one value at one offset" case.
func add(s *Sliding, d time.Duration, v float64) bool {
	return s.Add(Point{ID: d.String(), At: at(d), Value: v})
}

func TestWatermarkAdvancesAndEvicts(t *testing.T) {
	s := New(60*time.Second, 0, 1000)
	add(s, 0, 1)
	add(s, 30*time.Second, 2)
	add(s, 60*time.Second, 3)

	if got := s.Count(); got != 3 {
		t.Fatalf("Count = %d, want 3 (all inside a 60s window ending at +60s)", got)
	}
	if got := s.Watermark(); !got.Equal(at(60 * time.Second)) {
		t.Errorf("Watermark = %v, want +60s", got)
	}

	// Advancing the watermark past +60s pushes the first point out.
	add(s, 71*time.Second, 4)
	if got := s.Count(); got != 3 {
		t.Errorf("Count = %d, want 3 after the +0s point aged out", got)
	}
	if p, _ := s.Last(); p.Value != 4 {
		t.Errorf("Last value = %v, want 4", p.Value)
	}
}

// The grace period is the whole point of separating retention from the query
// window, so the two boundaries are pinned explicitly.
func TestLateArrivals(t *testing.T) {
	const size, grace = 60 * time.Second, 10 * time.Second
	s := New(size, grace, 1000)
	add(s, 0, 1)
	add(s, 71*time.Second, 2) // mark=+71s, window starts +11s, retention starts +1s

	tests := []struct {
		name         string
		offset       time.Duration
		wantAccepted bool
		wantInWindow bool
	}{
		{"far too late, beyond grace", 500 * time.Millisecond, false, false},
		{"late but inside grace, outside window", 5 * time.Second, true, false},
		{"late but still inside the window", 30 * time.Second, true, true},
		{"current", 70 * time.Second, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(size, grace, 1000)
			add(s, 0, 1)
			add(s, 71*time.Second, 2)
			beforeLate := s.Late()
			beforeWindow := s.Count()

			if got := add(s, tc.offset, 99); got != tc.wantAccepted {
				t.Fatalf("Add accepted = %v, want %v", got, tc.wantAccepted)
			}
			if !tc.wantAccepted && s.Late() != beforeLate+1 {
				t.Errorf("Late = %d, want it to increment to %d", s.Late(), beforeLate+1)
			}
			if got := s.Count() > beforeWindow; got != tc.wantInWindow {
				t.Errorf("point visible in window = %v, want %v", got, tc.wantInWindow)
			}
		})
	}
}

func TestOutOfOrderInsertStaysSorted(t *testing.T) {
	s := New(time.Minute, time.Minute, 1000)
	for _, d := range []time.Duration{0, 40 * time.Second, 10 * time.Second, 30 * time.Second, 20 * time.Second} {
		add(s, d, float64(d/time.Second))
	}
	pts := s.InWindow()
	if len(pts) != 5 {
		t.Fatalf("len = %d, want 5", len(pts))
	}
	for i := 1; i < len(pts); i++ {
		if pts[i].At.Before(pts[i-1].At) {
			t.Fatalf("points out of order at %d: %v before %v", i, pts[i].At, pts[i-1].At)
		}
	}
}

func TestMaxBoundDropsOldest(t *testing.T) {
	s := New(time.Hour, 0, 4)
	for i := 0; i < 20; i++ {
		add(s, time.Duration(i)*time.Second, float64(i))
	}
	if got := s.Retained(); got > 4 {
		t.Errorf("Retained = %d, want the cap of 4 to hold", got)
	}
	// The cap must keep the newest, not the oldest.
	if p, _ := s.Last(); p.Value != 19 {
		t.Errorf("Last value = %v, want 19", p.Value)
	}
}

func TestNewClampsMaxSoPreviousAlwaysWorks(t *testing.T) {
	s := New(time.Minute, 0, 1) // geovelocity needs two points
	add(s, 0, 1)
	add(s, time.Second, 2)
	if _, ok := s.Previous(); !ok {
		t.Error("Previous unavailable: max was not clamped to 2")
	}
}

func TestMeanAndStdDev(t *testing.T) {
	s := New(time.Hour, 0, 1000)
	// Textbook set: mean 5, population standard deviation exactly 2.
	for i, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		add(s, time.Duration(i)*time.Second, v)
	}
	if got := s.Mean(); math.Abs(got-5) > 1e-9 {
		t.Errorf("Mean = %v, want 5", got)
	}
	if got := s.StdDev(); math.Abs(got-2) > 1e-9 {
		t.Errorf("StdDev = %v, want 2", got)
	}
}

func TestEmptyAndSingletonEdges(t *testing.T) {
	s := New(time.Minute, 0, 10)

	if s.Count() != 0 || s.Mean() != 0 || s.StdDev() != 0 || s.Span() != 0 {
		t.Error("an empty window should report zero for every aggregate")
	}
	if _, ok := s.Last(); ok {
		t.Error("Last on an empty window should report false")
	}
	if _, ok := s.Previous(); ok {
		t.Error("Previous on an empty window should report false")
	}

	add(s, 0, 42)
	if s.StdDev() != 0 {
		t.Error("StdDev of a single point should be 0, not NaN")
	}
	if s.Span() != 0 {
		t.Error("Span of a single point should be 0")
	}
	if _, ok := s.Previous(); ok {
		t.Error("Previous with one point should report false")
	}
}

func TestSpanAndWindowStart(t *testing.T) {
	s := New(time.Minute, 0, 100)
	add(s, 0, 1)
	add(s, 25*time.Second, 2)
	if got := s.Span(); got != 25*time.Second {
		t.Errorf("Span = %v, want 25s", got)
	}
	if got := s.WindowStart(); !got.Equal(at(-35 * time.Second)) {
		t.Errorf("WindowStart = %v, want mark-60s", got)
	}
}

func TestLastAndPrevious(t *testing.T) {
	s := New(time.Minute, 0, 100)
	add(s, 0, 1)
	add(s, time.Second, 2)
	add(s, 2*time.Second, 3)

	last, ok := s.Last()
	if !ok || last.Value != 3 {
		t.Errorf("Last = %v (ok=%v), want 3", last.Value, ok)
	}
	prev, ok := s.Previous()
	if !ok || prev.Value != 2 {
		t.Errorf("Previous = %v (ok=%v), want 2", prev.Value, ok)
	}
}

func TestPreceding(t *testing.T) {
	s := New(time.Hour, time.Hour, 100)
	s.Add(Point{ID: "a", At: at(0), Value: 1})
	s.Add(Point{ID: "b", At: at(30 * time.Second), Value: 2})
	s.Add(Point{ID: "c", At: at(60 * time.Second), Value: 3})

	tests := []struct {
		name    string
		when    time.Duration
		exclude string
		wantID  string
		wantOK  bool
	}{
		{"predecessor of the newest", 60 * time.Second, "c", "b", true},
		{"predecessor of a middle point", 30 * time.Second, "b", "a", true},
		{"nothing precedes the oldest", 0, "a", "", false},
		{"a point at the same instant is eligible", 30 * time.Second, "zz", "b", true},
		{"future timestamps are ignored", 45 * time.Second, "", "b", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := s.Preceding(at(tc.when), tc.exclude)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && got.ID != tc.wantID {
				t.Errorf("id = %q, want %q", got.ID, tc.wantID)
			}
		})
	}
}
