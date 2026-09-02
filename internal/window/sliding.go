// Package window provides the time-windowed bookkeeping the rules run on.
//
// This is the piece Kafka Streams would have supplied and does not, because
// Kafka Streams is JVM-only. It is deliberately small: one sliding window per
// key, advanced by *event* time rather than wall-clock time, with an explicit
// grace period for out-of-order arrivals. Tumbling aggregation is not here —
// that is the Java topology's job (see docs/decisions/0001).
package window

import (
	"math"
	"sort"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
)

// Point is one observation retained inside a window.
type Point struct {
	ID    string
	At    time.Time
	Value float64
	Geo   *domain.Geo
}

// Sliding retains the recent observations for a single key.
//
// Time advances on a watermark — the highest event time observed — so the
// window behaves identically whether the pipeline is reading live traffic or
// replaying a fixture at a thousand times speed. Nothing here reads the clock.
//
// Retention spans [mark-size-grace, mark], but only [mark-size, mark] is
// visible to InWindow. The difference is the grace period: a record that
// arrives out of order is still accepted and still counts toward the window it
// belongs to, rather than being silently dropped because a later record had
// already advanced the watermark past it.
type Sliding struct {
	size  time.Duration
	grace time.Duration
	max   int

	pts  []Point // ascending by At
	mark time.Time
	late int
}

// New returns a window of the given size. Grace may be zero. The max bound
// caps how many observations a single key may retain: one pathologically hot
// key would otherwise be able to exhaust memory, and for every rule here the
// most recent max observations are sufficient to decide.
func New(size, grace time.Duration, max int) *Sliding {
	if max < 2 {
		max = 2 // geovelocity needs a previous point to compare against
	}
	return &Sliding{size: size, grace: grace, max: max}
}

// Add files an observation. It reports false when the record is too old to be
// worth keeping — beyond the grace period behind the watermark — in which case
// the caller should count it as late rather than treat it as an error.
func (s *Sliding) Add(p Point) bool {
	if p.At.After(s.mark) {
		s.mark = p.At
	}
	if p.At.Before(s.retentionStart()) {
		s.late++
		return false
	}

	// The common case is strictly increasing event time, so try the append
	// before paying for a search.
	if n := len(s.pts); n == 0 || !p.At.Before(s.pts[n-1].At) {
		s.pts = append(s.pts, p)
	} else {
		i := sort.Search(len(s.pts), func(i int) bool { return s.pts[i].At.After(p.At) })
		s.pts = append(s.pts, Point{})
		copy(s.pts[i+1:], s.pts[i:])
		s.pts[i] = p
	}

	s.evict()
	return true
}

func (s *Sliding) retentionStart() time.Time { return s.mark.Add(-s.size - s.grace) }

// WindowStart is the earliest event time the rules consider current.
func (s *Sliding) WindowStart() time.Time { return s.mark.Add(-s.size) }

func (s *Sliding) evict() {
	start := s.retentionStart()
	drop := 0
	for drop < len(s.pts) && s.pts[drop].At.Before(start) {
		drop++
	}
	// Drop the oldest beyond the hard cap as well, so a hot key degrades to
	// its most recent observations instead of growing without bound.
	if over := len(s.pts) - drop - s.max; over > 0 {
		drop += over
	}
	if drop > 0 {
		s.pts = append(s.pts[:0], s.pts[drop:]...)
	}
}

// InWindow returns the observations inside [mark-size, mark], oldest first.
// The slice aliases internal storage and must not be retained by the caller
// across the next Add.
func (s *Sliding) InWindow() []Point {
	start := s.WindowStart()
	i := sort.Search(len(s.pts), func(i int) bool { return !s.pts[i].At.Before(start) })
	return s.pts[i:]
}

// Count is the number of observations in the window.
func (s *Sliding) Count() int { return len(s.InWindow()) }

// Last returns the most recent retained observation, which may sit outside the
// window proper — geovelocity compares against the previous observation
// whenever it happened, not only when it was recent.
func (s *Sliding) Last() (Point, bool) {
	if len(s.pts) == 0 {
		return Point{}, false
	}
	return s.pts[len(s.pts)-1], true
}

// Previous returns the observation before the most recent one.
func (s *Sliding) Previous() (Point, bool) {
	if len(s.pts) < 2 {
		return Point{}, false
	}
	return s.pts[len(s.pts)-2], true
}

// Watermark is the highest event time observed for this key.
func (s *Sliding) Watermark() time.Time { return s.mark }

// Late counts records rejected for arriving beyond the grace period.
func (s *Sliding) Late() int { return s.late }

// Retained is the total number of points held, including those kept only for
// the grace period. Used by the memory-pressure metric.
func (s *Sliding) Retained() int { return len(s.pts) }

// Mean is the arithmetic mean of the values in the window; zero when empty.
func (s *Sliding) Mean() float64 {
	pts := s.InWindow()
	if len(pts) == 0 {
		return 0
	}
	var sum float64
	for _, p := range pts {
		sum += p.Value
	}
	return sum / float64(len(pts))
}

// StdDev is the population standard deviation over the window.
//
// Recomputed in two passes rather than maintained incrementally: the window is
// bounded by max, so this is cheap, and it avoids the catastrophic
// cancellation that incremental variance suffers when observations leave the
// window in a different order than they arrived.
func (s *Sliding) StdDev() float64 {
	pts := s.InWindow()
	if len(pts) < 2 {
		return 0
	}
	mean := s.Mean()
	var ss float64
	for _, p := range pts {
		d := p.Value - mean
		ss += d * d
	}
	return math.Sqrt(ss / float64(len(pts)))
}

// Span is the elapsed event time between the oldest and newest observation in
// the window.
func (s *Sliding) Span() time.Duration {
	pts := s.InWindow()
	if len(pts) < 2 {
		return 0
	}
	return pts[len(pts)-1].At.Sub(pts[0].At)
}

// Preceding returns the most recent retained observation at or before t,
// ignoring the point identified by excludeID.
//
// This is what a rule comparing an event against "the one before it" actually
// needs. Previous returns the second-newest point in the window, which is only
// the same thing while records arrive in order — and the grace period exists
// precisely because they sometimes do not. Points sharing t are eligible, so a
// rule can still reason about two observations at the same instant.
//
// This relies on point ids being unique, which domain.Event.Validate enforces
// as non-empty and the producers generate per record.
func (s *Sliding) Preceding(t time.Time, excludeID string) (Point, bool) {
	for i := len(s.pts) - 1; i >= 0; i-- {
		p := s.pts[i]
		if p.At.After(t) || p.ID == excludeID {
			continue
		}
		return p, true
	}
	return Point{}, false
}
