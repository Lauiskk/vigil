package rules

import (
	"fmt"
	"math"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/window"
)

// Velocity fires when a key produces more events inside the window than it
// should. It is the same arithmetic in all three streams and means something
// different in each: a card being tested with rapid low-value authorisations,
// a transcode job thrashing through retries, a sensor flapping.
type Velocity struct {
	Max int           // strictly more than this many events is an alert
	Per time.Duration // reported in the detail line; the window itself is configured on the store
	On  []domain.Stream
}

func (r Velocity) Name() string                   { return "velocity" }
func (r Velocity) AppliesTo(s domain.Stream) bool { return appliesToSet(r.On).has(s) }

func (r Velocity) Eval(ev domain.Event, w *window.Sliding, now time.Time) *domain.Alert {
	pts := w.InWindow()
	count := len(pts)
	if count <= r.Max {
		return nil
	}

	// Grade by rate rather than by count. The rule fires the instant the
	// limit is crossed — which is the point, detection should not wait — so
	// the count at that moment is always limit+1 and would always grade as
	// mild. The rate says how hard the key is being hammered, which is the
	// difference between a busy customer and an attack.
	span := w.Span()
	observedPerSecond := ratePerSecond(count, span)
	allowedPerSecond := 0.0
	if r.Per > 0 {
		allowedPerSecond = float64(r.Max) / r.Per.Seconds()
	}

	return &domain.Alert{
		Rule:       r.Name(),
		Stream:     ev.Stream,
		Key:        ev.Key,
		At:         ev.At,
		DetectedAt: now,
		Severity:   severityFor(observedPerSecond, allowedPerSecond),
		Title:      titleFor(ev.Stream, r.Name()),
		Detail: fmt.Sprintf("%d events in %s (limit %d per %s)",
			count, compactDuration(span), r.Max, compactDuration(r.Per)),
		Evidence: map[string]any{
			"count":            count,
			"limit":            r.Max,
			"spanMs":           span.Milliseconds(),
			"windowMs":         r.Per.Milliseconds(),
			"perSecond":        observedPerSecond,
			"allowedPerSecond": math.Round(allowedPerSecond*100) / 100,
		},
		EventIDs: ids(pts, 12),
	}
}
