package rules

import (
	"fmt"
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

	span := w.Span()
	return &domain.Alert{
		Rule:       r.Name(),
		Stream:     ev.Stream,
		Key:        ev.Key,
		At:         ev.At,
		DetectedAt: now,
		Severity:   severityFor(float64(count), float64(r.Max)),
		Title:      titleFor(ev.Stream, r.Name()),
		Detail: fmt.Sprintf("%d events in %s (limit %d per %s)",
			count, compactDuration(span), r.Max, compactDuration(r.Per)),
		Evidence: map[string]any{
			"count":     count,
			"limit":     r.Max,
			"spanMs":    span.Milliseconds(),
			"windowMs":  r.Per.Milliseconds(),
			"perSecond": ratePerSecond(count, span),
		},
		EventIDs: ids(pts, 12),
	}
}
