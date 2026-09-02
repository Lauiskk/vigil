// Package rules holds the detection logic: five generic primitives over one
// event type, which is what lets a single pipeline serve payments, video and
// sensor streams without three parallel implementations.
//
// Every rule is pure. It receives the arriving event and the key's window,
// and returns an alert or nil. It never reads the clock, never performs I/O
// and never allocates an identifier — the engine stamps those — so each rule
// is exhaustively testable from a table.
package rules

import (
	"math"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/window"
)

// Rule evaluates a key's recent history when an event arrives.
type Rule interface {
	// Name is the stable identifier written to the alert and used for the
	// per-rule cooldown. Never change one without a migration note.
	Name() string

	// AppliesTo reports whether this rule runs for a given stream.
	AppliesTo(s domain.Stream) bool

	// Eval runs after the event has been added to w. It returns nil when
	// nothing is wrong, which is the overwhelmingly common case.
	Eval(ev domain.Event, w *window.Sliding, now time.Time) *domain.Alert
}

// Sweeper is additionally evaluated on a timer.
//
// It exists for one reason: a rule that fires on the *absence* of events has
// no arriving event to trigger it. A transcode job that stalls stops
// producing progress records altogether, so nothing would ever call Eval.
type Sweeper interface {
	Rule
	Sweep(stream domain.Stream, key string, w *window.Sliding, now time.Time) *domain.Alert
}

// severityFor grades an alert by how far past its threshold the observation
// sits, so the dashboard can rank a card tested 40 times above one tested 11.
func severityFor(observed, threshold float64) domain.Severity {
	switch {
	case threshold <= 0 || observed >= threshold*2:
		return domain.SeverityCritical
	case observed >= threshold*1.25:
		return domain.SeverityWarn
	default:
		return domain.SeverityInfo
	}
}

// stats returns the mean and population standard deviation of the given
// points, excluding the final one.
//
// The exclusion matters. By the time a rule runs, the arriving event is
// already in the window, so comparing it against a mean that includes it
// drags the baseline toward the outlier and suppresses exactly the anomalies
// the rule exists to catch.
func statsExcludingLast(pts []window.Point) (mean, stddev float64, n int) {
	if len(pts) < 2 {
		return 0, 0, 0
	}
	hist := pts[:len(pts)-1]
	n = len(hist)

	for _, p := range hist {
		mean += p.Value
	}
	mean /= float64(n)

	if n < 2 {
		return mean, 0, n
	}
	var ss float64
	for _, p := range hist {
		d := p.Value - mean
		ss += d * d
	}
	return mean, math.Sqrt(ss / float64(n)), n
}

// ids collects the event ids behind an alert, newest last, bounded so a burst
// of ten thousand does not produce a ten-thousand-element alert.
func ids(pts []window.Point, limit int) []string {
	if len(pts) > limit {
		pts = pts[len(pts)-limit:]
	}
	out := make([]string, 0, len(pts))
	for _, p := range pts {
		out = append(out, p.ID)
	}
	return out
}

// appliesToSet is the shared AppliesTo implementation. A nil or empty set
// means the rule runs for every stream.
type appliesToSet []domain.Stream

func (a appliesToSet) has(s domain.Stream) bool {
	if len(a) == 0 {
		return true
	}
	for _, want := range a {
		if want == s {
			return true
		}
	}
	return false
}
