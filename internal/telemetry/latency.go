// Package telemetry holds the small measurement helpers the services share.
package telemetry

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// Latency keeps a bounded sample of recent measurements and reports
// percentiles over them.
//
// A ring rather than a histogram: the dashboard shows what the pipeline is
// doing *now*, and a cumulative histogram would keep reporting a p99 set
// during a surge long after the surge ended. Sorting a few thousand values on
// read costs less than the HTTP request that asked for them.
type Latency struct {
	mu     sync.Mutex
	ring   []time.Duration
	cursor int
	filled bool

	count uint64
	max   time.Duration
}

// NewLatency returns a tracker over the most recent size samples.
func NewLatency(size int) *Latency {
	if size < 1 {
		size = 1
	}
	return &Latency{ring: make([]time.Duration, size)}
}

// Observe records one measurement.
func (l *Latency) Observe(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.ring[l.cursor] = d
	l.cursor++
	if l.cursor == len(l.ring) {
		l.cursor = 0
		l.filled = true
	}
	l.count++
	if d > l.max {
		l.max = d
	}
}

// Snapshot reports the percentiles over the retained samples. All values are
// zero when nothing has been observed.
func (l *Latency) Snapshot() Snapshot {
	l.mu.Lock()
	samples := l.retained()
	total, max := l.count, l.max
	l.mu.Unlock()

	if len(samples) == 0 {
		return Snapshot{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	return Snapshot{
		Count:  total,
		P50:    quantile(samples, 0.50),
		P95:    quantile(samples, 0.95),
		P99:    quantile(samples, 0.99),
		Max:    max,
		Recent: samples[len(samples)-1],
	}
}

// retained copies the live portion of the ring. Called with the lock held.
func (l *Latency) retained() []time.Duration {
	n := l.cursor
	if l.filled {
		n = len(l.ring)
	}
	out := make([]time.Duration, n)
	copy(out, l.ring[:n])
	return out
}

// Snapshot is a read of the tracker at one moment.
type Snapshot struct {
	Count  uint64        `json:"count"`
	P50    time.Duration `json:"-"`
	P95    time.Duration `json:"-"`
	P99    time.Duration `json:"-"`
	Max    time.Duration `json:"-"`
	Recent time.Duration `json:"-"`
}

// MarshalJSON renders the durations as milliseconds, which is the unit the
// dashboard displays and avoids shipping nanosecond integers to a browser
// that would only divide them again.
func (s Snapshot) MarshalJSON() ([]byte, error) {
	type wire struct {
		Count    uint64  `json:"count"`
		P50Ms    float64 `json:"p50Ms"`
		P95Ms    float64 `json:"p95Ms"`
		P99Ms    float64 `json:"p99Ms"`
		MaxMs    float64 `json:"maxMs"`
		RecentMs float64 `json:"recentMs"`
	}
	return json.Marshal(wire{
		Count:    s.Count,
		P50Ms:    ms(s.P50),
		P95Ms:    ms(s.P95),
		P99Ms:    ms(s.P99),
		MaxMs:    ms(s.Max),
		RecentMs: ms(s.Recent),
	})
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

// quantile picks the nearest-rank value from an ascending slice.
func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

// Rate counts events and reports how many arrived per second over the last
// completed interval.
type Rate struct {
	mu       sync.Mutex
	current  uint64
	lastRate float64
	since    time.Time
	total    uint64
}

func NewRate(now time.Time) *Rate { return &Rate{since: now} }

// Add records n events.
func (r *Rate) Add(n uint64) {
	r.mu.Lock()
	r.current += n
	r.total += n
	r.mu.Unlock()
}

// Roll closes the current interval and returns the rate over it. Called on a
// ticker; measuring against real elapsed time rather than the nominal tick
// keeps the number honest when the loop runs late.
func (r *Rate) Roll(now time.Time) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	elapsed := now.Sub(r.since).Seconds()
	if elapsed <= 0 {
		return r.lastRate
	}
	r.lastRate = float64(r.current) / elapsed
	r.current = 0
	r.since = now
	return r.lastRate
}

// Per returns the most recent completed rate and the running total.
func (r *Rate) Per() (perSecond float64, total uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastRate, r.total
}
