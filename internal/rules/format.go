package rules

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
)

// titles maps a stream and rule to the phrase a human reads in the feed. The
// same arithmetic means something different per stream, and saying "velocity"
// to a recruiter explains nothing while "card testing" explains everything.
var titles = map[domain.Stream]map[string]string{
	domain.StreamPayments: {
		"velocity":    "Card testing",
		"geovelocity": "Impossible travel",
		"zscore":      "Amount anomaly",
	},
	domain.StreamVideo: {
		"velocity":  "Retry storm",
		"stall":     "Job stalled",
		"threshold": "SLA breach",
	},
	domain.StreamSensors: {
		"velocity":    "Sensor flapping",
		"geovelocity": "Impossible relocation",
		"zscore":      "Reading spike",
		"stall":       "Sensor offline",
		"threshold":   "Threshold exceeded",
	},
}

func titleFor(s domain.Stream, rule string) string {
	if byRule, ok := titles[s]; ok {
		if title, ok := byRule[rule]; ok {
			return title
		}
	}
	return rule
}

// compactFloat renders a number for a dense dashboard row: enough precision to
// be meaningful, short enough not to wrap.
func compactFloat(v float64) string {
	switch {
	case math.IsInf(v, 0):
		return "∞"
	case math.IsNaN(v):
		return "—"
	}
	abs := math.Abs(v)
	switch {
	case abs >= 1e6:
		return trimZero(fmt.Sprintf("%.1f", v/1e6)) + "M"
	case abs >= 1e3:
		return trimZero(fmt.Sprintf("%.1f", v/1e3)) + "K"
	case abs >= 100:
		return fmt.Sprintf("%.0f", v)
	default:
		return trimZero(fmt.Sprintf("%.2f", v))
	}
}

func trimZero(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

// compactDuration prefers the largest unit that still reads precisely.
func compactDuration(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return trimZero(fmt.Sprintf("%.1f", d.Seconds())) + "s"
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

func ratePerSecond(count int, span time.Duration) float64 {
	if span <= 0 {
		return 0
	}
	return math.Round(float64(count)/span.Seconds()*100) / 100
}
