// Package engine is the detection loop: it owns the window store, runs the
// rules for each arriving event, and suppresses repeats.
//
// It knows nothing about Kafka. The processor feeds it decoded events and
// publishes whatever alerts come back, which is what lets the whole detection
// path be tested without a broker.
package engine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/rules"
	"github.com/Lauiskk/vigil/internal/state"
	"github.com/Lauiskk/vigil/internal/window"
)

// Engine evaluates events against a set of per-stream profiles.
type Engine struct {
	profiles map[domain.Stream]rules.Profile
	store    *state.Store

	// now is injected so tests drive time explicitly rather than sleeping.
	now func() time.Time

	mu       sync.Mutex
	cooldown map[cooldownKey]time.Time

	runID string
	seq   atomic.Uint64

	stats Stats
}

type cooldownKey struct {
	stream domain.Stream
	key    string
	rule   string
}

// Stats is a snapshot of what the engine has done, for the metrics endpoint
// and the dashboard header.
type Stats struct {
	Processed  atomic.Uint64
	Alerted    atomic.Uint64
	Suppressed atomic.Uint64
	Late       atomic.Uint64
	Invalid    atomic.Uint64
	Swept      atomic.Uint64
}

// Option configures an Engine.
type Option func(*Engine)

// WithClock replaces the time source. Tests use it to make detection
// timestamps and cooldown expiry deterministic.
func WithClock(f func() time.Time) Option { return func(e *Engine) { e.now = f } }

// WithStore supplies a pre-populated store, as used after a changelog replay.
func WithStore(s *state.Store) Option { return func(e *Engine) { e.store = s } }

func New(profiles map[domain.Stream]rules.Profile, opts ...Option) *Engine {
	e := &Engine{
		profiles: profiles,
		store:    state.New(),
		now:      time.Now,
		cooldown: make(map[cooldownKey]time.Time),
		runID:    randomID(),
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Store exposes the window store so the processor can snapshot it to the
// changelog and restore it after a rebalance.
func (e *Engine) Store() *state.Store { return e.store }

// Stats returns the live counters. The struct contains atomics, so callers
// read fields directly rather than copying it.
func (e *Engine) Stats() *Stats { return &e.stats }

// Geometry returns the window shape configured for a stream.
func (e *Engine) Geometry(s domain.Stream) (state.Geometry, bool) {
	p, ok := e.profiles[s]
	if !ok {
		return state.Geometry{}, false
	}
	return geometryOf(p), true
}

func geometryOf(p rules.Profile) state.Geometry {
	return state.Geometry{Window: p.Window, Grace: p.Grace, MaxPoints: p.MaxPoints}
}

// Ingest files one event and returns the alerts it raised.
//
// An error means the record is unprocessable and belongs in the dead-letter
// topic; it is never a reason to stop consuming. A late record is not an
// error — it is counted and dropped, because by definition nothing downstream
// can still act on it.
func (e *Engine) Ingest(ev domain.Event) ([]domain.Alert, error) {
	if err := ev.Validate(); err != nil {
		e.stats.Invalid.Add(1)
		return nil, err
	}
	profile, ok := e.profiles[ev.Stream]
	if !ok {
		e.stats.Invalid.Add(1)
		return nil, fmt.Errorf("%w: no profile for stream %q", domain.ErrInvalidEvent, ev.Stream)
	}

	now := e.now()
	e.stats.Processed.Add(1)

	key := state.Key{Stream: ev.Stream, ID: ev.Key}
	accepted := e.store.Add(key, geometryOf(profile), window.Point{
		ID: ev.ID, At: ev.At, Value: ev.Value, Geo: ev.Geo,
	}, now)
	if !accepted {
		e.stats.Late.Add(1)
		return nil, nil
	}

	w := e.store.Window(key)
	var out []domain.Alert
	for _, r := range profile.Rules {
		if !r.AppliesTo(ev.Stream) {
			continue
		}
		alert := r.Eval(ev, w, now)
		if alert == nil {
			continue
		}
		if a, ok := e.admit(*alert, profile.Cooldown, now); ok {
			out = append(out, a)
		}
	}

	// A terminal event closes the key. Done last so the rules still see the
	// final observation before the window is discarded.
	if ev.Terminal() {
		e.store.Drop(key)
		e.clearCooldown(ev.Stream, ev.Key)
	}
	return out, nil
}

// Sweep evaluates the absence-based rules across every live key.
//
// The processor calls this on a ticker. Alerts are collected while the store
// lock is held and admitted afterwards, because admit takes a different lock
// and holding both invites a deadlock the first time someone reorders a call.
func (e *Engine) Sweep() []domain.Alert {
	now := e.now()
	e.stats.Swept.Add(1)

	type candidate struct {
		alert    domain.Alert
		cooldown time.Duration
	}
	var found []candidate

	e.store.Range(func(k state.Key, w *window.Sliding) bool {
		profile, ok := e.profiles[k.Stream]
		if !ok || profile.SweepEvery <= 0 {
			return true
		}
		for _, r := range profile.Rules {
			sweeper, ok := r.(rules.Sweeper)
			if !ok || !r.AppliesTo(k.Stream) {
				continue
			}
			if alert := sweeper.Sweep(k.Stream, k.ID, w, now); alert != nil {
				found = append(found, candidate{*alert, profile.Cooldown})
			}
		}
		return true
	})

	out := make([]domain.Alert, 0, len(found))
	for _, c := range found {
		if a, ok := e.admit(c.alert, c.cooldown, now); ok {
			out = append(out, a)
		}
	}
	e.pruneCooldown(now)
	return out
}

// admit stamps an identifier on an alert and applies the per-rule cooldown.
// A card being tested forty times is one fact, not thirty alerts.
func (e *Engine) admit(a domain.Alert, cooldown time.Duration, now time.Time) (domain.Alert, bool) {
	ck := cooldownKey{stream: a.Stream, key: a.Key, rule: a.Rule}

	e.mu.Lock()
	if until, ok := e.cooldown[ck]; ok && now.Before(until) {
		e.mu.Unlock()
		e.stats.Suppressed.Add(1)
		return domain.Alert{}, false
	}
	e.cooldown[ck] = now.Add(cooldown)
	e.mu.Unlock()

	a.ID = fmt.Sprintf("%s-%d", e.runID, e.seq.Add(1))
	e.stats.Alerted.Add(1)
	return a, true
}

func (e *Engine) clearCooldown(stream domain.Stream, key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for ck := range e.cooldown {
		if ck.stream == stream && ck.key == key {
			delete(e.cooldown, ck)
		}
	}
}

// pruneCooldown drops expired entries so the map does not grow for the life of
// the process, one entry per key that ever alerted.
func (e *Engine) pruneCooldown(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for ck, until := range e.cooldown {
		if now.After(until) {
			delete(e.cooldown, ck)
		}
	}
}

// EvictIdle forgets keys that have not been seen for the given duration.
func (e *Engine) EvictIdle(idle time.Duration) int {
	return e.store.EvictIdle(e.now().Add(-idle))
}

// randomID gives each process a short prefix so alert ids stay unique across
// restarts and across the several processor instances in a consumer group.
func randomID() string { return randomIDFrom(rand.Read) }

// randomIDFrom takes the entropy source as a parameter so the fallback below
// can be exercised. A defensive branch nobody has ever run is a guess, not a
// safeguard.
func randomIDFrom(read func([]byte) (int, error)) string {
	var b [4]byte
	if _, err := read(b[:]); err != nil {
		// The only documented failure is an unusable system entropy source.
		// A timestamp is a poorer but adequate fallback, and beats panicking
		// inside a constructor.
		return fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff)
	}
	return hex.EncodeToString(b[:])
}
