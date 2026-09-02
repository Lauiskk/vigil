// Package sim generates the traffic the pipeline runs on.
//
// It is not a load generator bolted on the side — it is how the demo is
// legible. Ordinary traffic has to look ordinary enough that the alerts mean
// something, and each fault has to produce a signature a visitor can recognise
// within a second or two of pressing the button.
package sim

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
)

// Fault is an anomaly a visitor can inject from the dashboard.
type Fault string

const (
	FaultImpossibleTravel Fault = "impossible-travel"
	FaultCardTesting      Fault = "card-testing"
	FaultAmountAnomaly    Fault = "amount-anomaly"

	FaultJobStall   Fault = "job-stall"
	FaultSLABreach  Fault = "sla-breach"
	FaultRetryStorm Fault = "retry-storm"

	FaultAirQuality    Fault = "air-quality"
	FaultSensorSpike   Fault = "sensor-spike"
	FaultSensorOffline Fault = "sensor-offline"
)

// ErrUnknownFault is returned when a source is asked for a fault it does not
// implement, which is how the HTTP layer turns a bad request into a 400.
var ErrUnknownFault = fmt.Errorf("unknown fault")

// Source produces events for one stream.
type Source interface {
	Stream() domain.Stream

	// Tick returns the events due at now: `rate` ordinary events, plus any
	// previously scheduled fault events that have come due.
	Tick(now time.Time, rate int) []domain.Event

	// Inject schedules a fault and reports the key it will affect, so the
	// dashboard can highlight the entity about to misbehave.
	Inject(f Fault, now time.Time) (string, error)

	// Faults lists what this source can inject.
	Faults() []Fault
}

// pending is one fault event and the moment it should be emitted.
//
// The release time and the event's own timestamp are deliberately separate.
// An event is stamped at the instant it is emitted, never in advance, because
// time.Now().Add(d) produces a wall-clock reading that assumes the wall clock
// advances at the same rate as the monotonic clock. Under NTP slew it does
// not — measurably so under virtualisation — and a timestamp computed that
// way, once serialised, is in the future. Downstream that arrives as an
// alert detected before the event that caused it.
type pending struct {
	releaseAt time.Time
	ev        domain.Event
}

// scheduled is a fault's worth of events waiting for their moment.
//
// Faults are queued rather than emitted immediately because the interesting
// ones are shapes over time: a card tested two dozen times in eight seconds,
// an encoder starved for a minute. Emitting them at once would trip a
// different rule than the one being demonstrated. The shape survives because
// each event is released on its own tick and stamped then, so the intervals
// between them are preserved without any of them being stamped ahead.
type scheduled struct {
	mu     sync.Mutex
	queue  []pending
	silent map[string]time.Time
}

func newScheduled() *scheduled {
	return &scheduled{silent: make(map[string]time.Time)}
}

// add queues an event for release at the given time, keeping the queue ordered.
func (s *scheduled) add(releaseAt time.Time, ev domain.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, pending{releaseAt: releaseAt, ev: ev})
	// Insertion sort: the queue is short and almost always already ordered.
	for i := len(s.queue) - 1; i > 0 && s.queue[i].releaseAt.Before(s.queue[i-1].releaseAt); i-- {
		s.queue[i], s.queue[i-1] = s.queue[i-1], s.queue[i]
	}
}

// due removes and returns everything scheduled at or before now, stamping each
// event with now as it goes.
func (s *scheduled) due(now time.Time) []domain.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	i := 0
	for i < len(s.queue) && !s.queue[i].releaseAt.After(now) {
		i++
	}
	if i == 0 {
		return nil
	}
	out := make([]domain.Event, i)
	for j := 0; j < i; j++ {
		out[j] = s.queue[j].ev
		out[j].At = now
	}
	s.queue = append([]pending(nil), s.queue[i:]...)
	return out
}

// mute stops ordinary traffic for a key until the deadline. It is how the
// absence faults work: a stalled job is one that stops reporting.
func (s *scheduled) mute(key string, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.silent[key] = until
}

func (s *scheduled) muted(key string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	until, ok := s.silent[key]
	if !ok {
		return false
	}
	if now.After(until) {
		delete(s.silent, key)
		return false
	}
	return true
}

// ids issues unique event identifiers. Uniqueness is relied upon by the
// window's Preceding lookup, so it is a property of the producer, not a
// convenience.
type ids struct {
	prefix string
	n      uint64
	mu     sync.Mutex
}

func (i *ids) next() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.n++
	return fmt.Sprintf("%s-%d", i.prefix, i.n)
}

// lockedRand wraps a deterministic generator for concurrent use: Tick runs on
// the producer goroutine while Inject arrives from an HTTP handler.
type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newRand(seed int64) *lockedRand {
	return &lockedRand{r: rand.New(rand.NewSource(seed))}
}

func (l *lockedRand) intn(n int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Intn(n)
}

func (l *lockedRand) float() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Float64()
}

// normal returns a sample from a normal distribution, clamped to be positive —
// no simulated transaction should have a negative amount just because the tail
// went that way.
func (l *lockedRand) normal(mean, stddev float64) float64 {
	l.mu.Lock()
	v := l.r.NormFloat64()
	l.mu.Unlock()
	out := mean + v*stddev
	if out < 0.01 {
		return 0.01
	}
	return out
}

// pick returns a uniformly chosen element.
func pick[T any](l *lockedRand, xs []T) T { return xs[l.intn(len(xs))] }
