package rules

import (
	"fmt"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/window"
)

// Threshold fires when a key's value stays past a bound for long enough.
//
// The sustain requirement is what separates this from a naive comparison. A
// single sample over the line is noise; an air sensor that has read above the
// limit continuously for two minutes is an event. Alerting on the first
// sample would make the rule useless in exactly the conditions it is for.
type Threshold struct {
	Bound     float64
	Above     bool // true: alert above Bound; false: alert below
	Sustained time.Duration
	On        []domain.Stream
}

func (r Threshold) Name() string                   { return "threshold" }
func (r Threshold) AppliesTo(s domain.Stream) bool { return appliesToSet(r.On).has(s) }

func (r Threshold) beyond(v float64) bool {
	if r.Above {
		return v > r.Bound
	}
	return v < r.Bound
}

func (r Threshold) Eval(ev domain.Event, w *window.Sliding, now time.Time) *domain.Alert {
	if !r.beyond(ev.Value) {
		return nil
	}

	// Walk back over the contiguous run of samples that are also beyond the
	// bound. The moment one is not, the streak is broken and the clock on
	// "sustained" restarts.
	pts := w.InWindow()
	i := len(pts) - 1
	for i > 0 && r.beyond(pts[i-1].Value) {
		i--
	}
	run := pts[i:]
	held := run[len(run)-1].At.Sub(run[0].At)
	if held < r.Sustained {
		return nil
	}

	direction := "above"
	if !r.Above {
		direction = "below"
	}
	return &domain.Alert{
		Rule:       r.Name(),
		Stream:     ev.Stream,
		Key:        ev.Key,
		At:         ev.At,
		DetectedAt: now,
		Severity:   severityFor(held.Seconds(), r.Sustained.Seconds()),
		Title:      titleFor(ev.Stream, r.Name()),
		Detail: fmt.Sprintf("%s %s — %s %s for %s across %d samples",
			compactFloat(ev.Value), ev.Unit, direction,
			compactFloat(r.Bound), compactDuration(held), len(run)),
		Evidence: map[string]any{
			"value":       ev.Value,
			"bound":       r.Bound,
			"above":       r.Above,
			"heldMs":      held.Milliseconds(),
			"sustainedMs": r.Sustained.Milliseconds(),
			"samples":     len(run),
		},
		EventIDs: ids(run, 12),
	}
}
