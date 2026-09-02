package domain

import "time"

// Severity orders alerts for the dashboard. It is derived by each rule from
// how far past its threshold the observation sits, not configured per rule.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarn     Severity = "warn"
	SeverityCritical Severity = "critical"
)

// Rank gives a sortable weight; the dashboard shows critical first.
func (s Severity) Rank() int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarn:
		return 2
	default:
		return 1
	}
}

// Alert is what a rule emits onto `vigil.alerts`.
//
// The two timestamps are the point of the whole project. At is the event time
// the producer stamped; DetectedAt is when the processor concluded something
// was wrong. Their difference is the end-to-end pipeline latency the dashboard
// plots, and it is measured rather than asserted.
type Alert struct {
	ID         string    `json:"id"`
	Rule       string    `json:"rule"`
	Stream     Stream    `json:"stream"`
	Key        string    `json:"key"`
	At         time.Time `json:"at"`
	DetectedAt time.Time `json:"detectedAt"`
	Severity   Severity  `json:"severity"`

	// Title is a short line for the feed; Detail is the human sentence that
	// explains the arithmetic ("18.500 km in 41s — 1.6M km/h").
	Title  string `json:"title"`
	Detail string `json:"detail"`

	// Evidence carries the rule's numbers so the dashboard can render them
	// without re-deriving anything, and so a reviewer can check the maths.
	Evidence map[string]any `json:"evidence,omitempty"`

	// EventIDs are the records that produced this alert, newest last.
	EventIDs []string `json:"eventIds,omitempty"`
}

// Latency is the time from the triggering event to the detection decision.
func (a Alert) Latency() time.Duration { return a.DetectedAt.Sub(a.At) }
