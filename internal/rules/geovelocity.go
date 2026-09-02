package rules

import (
	"fmt"
	"math"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/window"
)

// GeoVelocity fires when the implied speed between a key's last two
// observations exceeds what physics allows — a card used in São Paulo and
// then in Tokyo forty seconds later.
//
// This is the rule the demo leads with, because it needs no explanation.
type GeoVelocity struct {
	MaxKmh float64 // a commercial flight cruises near 900 km/h
	MinKm  float64 // below this, treat the difference as location noise
	On     []domain.Stream
}

func (r GeoVelocity) Name() string                   { return "geovelocity" }
func (r GeoVelocity) AppliesTo(s domain.Stream) bool { return appliesToSet(r.On).has(s) }

func (r GeoVelocity) Eval(ev domain.Event, w *window.Sliding, now time.Time) *domain.Alert {
	if ev.Geo == nil {
		return nil
	}
	// Preceding, not Previous: an out-of-order arrival is not the newest
	// point in the window, so its predecessor is not the second-newest one.
	prev, ok := w.Preceding(ev.At, ev.ID)
	if !ok || prev.Geo == nil {
		return nil
	}

	dist := prev.Geo.DistanceKm(*ev.Geo)
	if dist < r.MinKm {
		return nil
	}

	// Preceding guarantees dt >= 0, so the pair is always judged forwards.
	dt := ev.At.Sub(prev.At)

	// Two locations at the same instant is not a fast journey, it is an
	// impossible one. Infinity is the honest answer and formats as such.
	kmh := math.Inf(1)
	if dt > 0 {
		kmh = dist / dt.Hours()
	}
	if kmh <= r.MaxKmh {
		return nil
	}

	return &domain.Alert{
		Rule:       r.Name(),
		Stream:     ev.Stream,
		Key:        ev.Key,
		At:         ev.At,
		DetectedAt: now,
		Severity:   severityFor(kmh, r.MaxKmh),
		Title:      titleFor(ev.Stream, r.Name()),
		Detail: fmt.Sprintf("%s → %s · %s km in %s · %s km/h",
			placeOf(prev.Geo), placeOf(ev.Geo),
			compactFloat(dist), compactDuration(dt), compactFloat(kmh)),
		Evidence: map[string]any{
			"fromPlace":  placeOf(prev.Geo),
			"toPlace":    placeOf(ev.Geo),
			"distanceKm": math.Round(dist*10) / 10,
			"seconds":    dt.Seconds(),
			"impliedKmh": compactFloat(kmh),
			"limitKmh":   r.MaxKmh,
		},
		EventIDs: []string{prev.ID, ev.ID},
	}
}

func placeOf(g *domain.Geo) string {
	if g == nil {
		return "?"
	}
	if g.Place != "" {
		return g.Place
	}
	return fmt.Sprintf("%.2f,%.2f", g.Lat, g.Lon)
}
