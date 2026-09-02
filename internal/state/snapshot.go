package state

import (
	"time"

	"github.com/Lauiskk/vigil/internal/window"
)

// Snapshot is one key's state as written to the compacted changelog topic.
//
// The changelog is what makes a rebalance survivable. When a partition moves
// to another consumer, the new owner replays the changelog — keyed by entity,
// compacted, so it holds only the latest state per key — instead of replaying
// the input topic from the beginning. This is the piece Kafka Streams gives
// you for free and the reason this package exists at all.
type Snapshot struct {
	Key    Key            `json:"key"`
	Points []window.Point `json:"points"`

	// Written is stamped by the producer so a reader can tell a stale replay
	// from a current one.
	Written time.Time `json:"written"`
}

// Snapshot captures a key's current state, or reports false when the key is
// not tracked. The points are copied: the caller serialises them on another
// goroutine, and the window keeps mutating underneath.
func (s *Store) Snapshot(k Key, now time.Time) (Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[k]
	if !ok {
		return Snapshot{}, false
	}
	pts := e.w.InWindow()
	out := make([]window.Point, len(pts))
	copy(out, pts)
	return Snapshot{Key: k, Points: out, Written: now}, true
}

// Restore rebuilds a key from a snapshot, replacing whatever was there.
//
// An empty snapshot is a tombstone: compaction turns a deleted key into a
// record with no points, and restoring that must remove the key rather than
// resurrect it as an empty window that the stall rule would then sweep.
func (s *Store) Restore(snap Snapshot, g Geometry, seen time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(snap.Points) == 0 {
		delete(s.entries, snap.Key)
		return
	}
	w := window.New(g.Window, g.Grace, g.MaxPoints)
	for _, p := range snap.Points {
		w.Add(p)
	}
	s.entries[snap.Key] = &entry{w: w, lastSeen: seen}
}
