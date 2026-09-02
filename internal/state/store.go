// Package state holds the processor's working memory: one window per key,
// plus the snapshot format that lets a rebalanced consumer rebuild it from the
// changelog topic instead of replaying the whole input.
package state

import (
	"sync"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/window"
)

// Key identifies one tracked entity. The stream is part of the identity
// because a card id and a sensor id could collide, and because each stream
// gets its own window geometry.
type Key struct {
	Stream domain.Stream `json:"stream"`
	ID     string        `json:"id"`
}

// Geometry is the window shape for a stream. It is passed in rather than
// configured here so that this package does not need to know the rules exist.
type Geometry struct {
	Window    time.Duration
	Grace     time.Duration
	MaxPoints int
}

type entry struct {
	w        *window.Sliding
	lastSeen time.Time // wall clock, for idle eviction only
}

// Store is the set of live windows.
//
// It is guarded by a mutex rather than sharded per partition. The processor
// writes from one goroutine per partition, but the metrics endpoint and the
// changelog writer read concurrently, and a mutex around a map operation is
// far below the cost of the Kafka round trip that produced the event.
type Store struct {
	mu      sync.Mutex
	entries map[Key]*entry
}

func New() *Store { return &Store{entries: make(map[Key]*entry)} }

// Add files an observation, creating the key's window on first sight. It
// reports whether the record was accepted; false means it arrived too late.
func (s *Store) Add(k Key, g Geometry, p window.Point, seen time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[k]
	if !ok {
		e = &entry{w: window.New(g.Window, g.Grace, g.MaxPoints)}
		s.entries[k] = e
	}
	e.lastSeen = seen
	return e.w.Add(p)
}

// Window returns a key's window, or nil when the key is not tracked.
func (s *Store) Window(k Key) *window.Sliding {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[k]; ok {
		return e.w
	}
	return nil
}

// Drop forgets a key. The processor calls this when an entity reaches a
// terminal state — a finished transcode job raises no further events, so
// keeping its window alive would only give the stall rule something to fire
// on forever.
func (s *Store) Drop(k Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, k)
}

// Range visits every live key. The callback returns false to stop early.
//
// It holds the store lock throughout, so the callback must not call back into
// the store. Sweeping collects alerts and emits them afterwards for exactly
// this reason.
func (s *Store) Range(f func(Key, *window.Sliding) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.entries {
		if !f(k, e.w) {
			return
		}
	}
}

// Len is the number of tracked keys.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// EvictIdle forgets keys untouched since the cutoff and reports how many went.
//
// Without this the store is a memory leak with extra steps: every card that
// ever transacts would be retained for the life of the process, even though
// its window emptied minutes ago.
func (s *Store) EvictIdle(before time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, e := range s.entries {
		if e.lastSeen.Before(before) {
			delete(s.entries, k)
			n++
		}
	}
	return n
}
