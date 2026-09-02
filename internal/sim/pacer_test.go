package sim

import (
	"testing"
	"time"
)

func TestPacerHonoursFractionalRates(t *testing.T) {
	tests := []struct {
		name      string
		perSecond float64
		tick      time.Duration
		ticks     int
		want      int
	}{
		{"six per second over one second", 6, 100 * time.Millisecond, 10, 6},
		{"a rate below one per tick still produces", 1, 100 * time.Millisecond, 10, 1},
		{"a rate below one per second still produces", 0.5, 100 * time.Millisecond, 20, 1},
		{"two thousand per second", 2000, 100 * time.Millisecond, 10, 2000},
		{"an awkward fraction does not drift", 7.3, 100 * time.Millisecond, 100, 73},
		{"zero produces nothing", 0, 100 * time.Millisecond, 50, 0},
		{"a negative rate produces nothing", -5, 100 * time.Millisecond, 50, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var p Pacer
			total := 0
			for i := 0; i < tc.ticks; i++ {
				total += p.Take(tc.perSecond, tc.tick)
			}
			// One event of slack for the fraction still in the accumulator.
			if total < tc.want-1 || total > tc.want {
				t.Errorf("produced %d over %d ticks, want %d", total, tc.ticks, tc.want)
			}
		})
	}
}

func TestPacerResetsWhenStopped(t *testing.T) {
	var p Pacer
	p.Take(100, 50*time.Millisecond) // leaves a fraction behind
	if got := p.Take(0, time.Second); got != 0 {
		t.Errorf("a zero rate produced %d events", got)
	}
	// The stale fraction must not leak into the next burst.
	if got := p.Take(1, 100*time.Millisecond); got != 0 {
		t.Errorf("carried a stale fraction across a stop: %d", got)
	}
}
