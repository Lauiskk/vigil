package rules

import (
	"fmt"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/window"
)

// Stall fires when a key stops producing events altogether.
//
// It is the one rule that cannot be driven by an arriving event, because the
// symptom *is* the absence of one: a transcode job that wedges stops emitting
// progress, and a sensor that loses power stops reporting. Eval therefore
// always returns nil and the work happens in Sweep, which the engine calls on
// a timer over every live key.
type Stall struct {
	After time.Duration
	On    []domain.Stream
}

func (r Stall) Name() string                   { return "stall" }
func (r Stall) AppliesTo(s domain.Stream) bool { return appliesToSet(r.On).has(s) }

// Eval never fires. See the type comment.
func (r Stall) Eval(domain.Event, *window.Sliding, time.Time) *domain.Alert { return nil }

func (r Stall) Sweep(stream domain.Stream, key string, w *window.Sliding, now time.Time) *domain.Alert {
	last, ok := w.Last()
	if !ok {
		return nil
	}
	idle := now.Sub(last.At)
	if idle < r.After {
		return nil
	}

	return &domain.Alert{
		Rule:       r.Name(),
		Stream:     stream,
		Key:        key,
		At:         last.At,
		DetectedAt: now,
		Severity:   severityFor(idle.Seconds(), r.After.Seconds()),
		Title:      titleFor(stream, r.Name()),
		Detail: fmt.Sprintf("no events for %s (expected within %s)",
			compactDuration(idle), compactDuration(r.After)),
		Evidence: map[string]any{
			"idleMs":      idle.Milliseconds(),
			"thresholdMs": r.After.Milliseconds(),
			"lastValue":   last.Value,
		},
		EventIDs: []string{last.ID},
	}
}
