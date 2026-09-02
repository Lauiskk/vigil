package rules

import (
	"fmt"
	"math"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/window"
)

// ZScore fires when a value departs from what this particular key normally
// does, measured in standard deviations of its own recent history.
//
// Per-key rather than global on purpose: a R$ 4.000 charge is unremarkable on
// a card that habitually spends that and glaring on one that never exceeds
// R$ 90. A global threshold cannot express that difference.
type ZScore struct {
	K          float64 // deviations past which the value is an outlier
	MinSamples int     // below this the baseline is not worth trusting

	// MinDelta is an absolute floor the deviation must also clear.
	//
	// Sigma is estimated from a few dozen samples, so it carries a standard
	// error of its own — around 13% at n=28. A key whose variance happens to
	// come out low will otherwise report ordinary readings as extreme, and
	// across a mesh of sensors producing thousands of readings an hour, that
	// is not a rare event but a steady drip of false positives. Raising K
	// treats the symptom; requiring the deviation to be materially large in
	// the unit being measured treats the cause. Zero disables the floor.
	MinDelta float64

	// Below, when false, means only values above the mean are anomalies.
	//
	// Symmetric detection is right for money — an unusually small charge on a
	// card that habitually spends is its own signal — and wrong for a
	// threshold-shaped measurement, where a reading falling back to baseline
	// after an elevated period is a large deviation and also just recovery.
	// Alerting on it says "Reading spike" about a number going down.
	Below bool

	On []domain.Stream
}

func (r ZScore) Name() string                   { return "zscore" }
func (r ZScore) AppliesTo(s domain.Stream) bool { return appliesToSet(r.On).has(s) }

func (r ZScore) Eval(ev domain.Event, w *window.Sliding, now time.Time) *domain.Alert {
	pts := w.InWindow()
	mean, stddev, n := statsExcludingLast(pts)
	if n < r.MinSamples {
		return nil
	}
	// A flat history has no scale to measure deviation against. Dividing by
	// it would make every subsequent value infinitely anomalous, so the
	// honest answer is to withhold judgement until the key has some variance.
	if stddev <= 0 {
		return nil
	}

	if !r.Below && ev.Value < mean {
		return nil
	}
	delta := math.Abs(ev.Value - mean)
	if delta < r.MinDelta {
		return nil
	}
	z := delta / stddev
	if z <= r.K {
		return nil
	}

	direction := "above"
	if ev.Value < mean {
		direction = "below"
	}
	return &domain.Alert{
		Rule:       r.Name(),
		Stream:     ev.Stream,
		Key:        ev.Key,
		At:         ev.At,
		DetectedAt: now,
		Severity:   severityFor(z, r.K),
		Title:      titleFor(ev.Stream, r.Name()),
		Detail: fmt.Sprintf("%s %s is %.1fσ %s this key's mean of %s (n=%d)",
			compactFloat(ev.Value), ev.Unit, z, direction, compactFloat(mean), n),
		Evidence: map[string]any{
			"value":    ev.Value,
			"mean":     math.Round(mean*100) / 100,
			"stddev":   math.Round(stddev*100) / 100,
			"z":        math.Round(z*100) / 100,
			"k":        r.K,
			"delta":    math.Round(delta*100) / 100,
			"minDelta": r.MinDelta,
			"samples":  n,
		},
		EventIDs: ids(pts, 8),
	}
}
