package state

import (
	"testing"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/window"
)

var (
	base = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	geom = Geometry{Window: time.Minute, Grace: 5 * time.Second, MaxPoints: 64}
	card = Key{Stream: domain.StreamPayments, ID: "card-1"}
)

func pt(id string, off time.Duration, v float64) window.Point {
	return window.Point{ID: id, At: base.Add(off), Value: v}
}

func TestAddCreatesAndAccumulates(t *testing.T) {
	s := New()
	if s.Window(card) != nil {
		t.Fatal("an untracked key should have no window")
	}
	if !s.Add(card, geom, pt("a", 0, 1), base) {
		t.Fatal("first observation was rejected")
	}
	s.Add(card, geom, pt("b", time.Second, 2), base)

	w := s.Window(card)
	if w == nil {
		t.Fatal("window missing after Add")
	}
	if got := w.Count(); got != 2 {
		t.Errorf("Count = %d, want 2", got)
	}
	if got := s.Len(); got != 1 {
		t.Errorf("Len = %d, want 1", got)
	}
}

func TestAddReportsLateRecords(t *testing.T) {
	s := New()
	s.Add(card, geom, pt("a", 0, 1), base)
	s.Add(card, geom, pt("b", 2*time.Minute, 2), base)
	// The watermark is now +2m, so +1s is far outside window+grace.
	if s.Add(card, geom, pt("c", time.Second, 3), base) {
		t.Error("a record beyond the grace period should be rejected")
	}
}

func TestDrop(t *testing.T) {
	s := New()
	s.Add(card, geom, pt("a", 0, 1), base)
	s.Drop(card)
	if s.Window(card) != nil || s.Len() != 0 {
		t.Error("Drop did not remove the key")
	}
	s.Drop(card) // dropping an absent key must not panic
}

func TestRange(t *testing.T) {
	s := New()
	for _, id := range []string{"a", "b", "c"} {
		s.Add(Key{Stream: domain.StreamPayments, ID: id}, geom, pt(id, 0, 1), base)
	}

	seen := 0
	s.Range(func(Key, *window.Sliding) bool { seen++; return true })
	if seen != 3 {
		t.Errorf("visited %d keys, want 3", seen)
	}

	visited := 0
	s.Range(func(Key, *window.Sliding) bool { visited++; return false })
	if visited != 1 {
		t.Errorf("returning false visited %d keys, want 1", visited)
	}
}

func TestEvictIdle(t *testing.T) {
	s := New()
	s.Add(Key{Stream: domain.StreamPayments, ID: "old"}, geom, pt("a", 0, 1), base)
	s.Add(Key{Stream: domain.StreamPayments, ID: "new"}, geom, pt("b", 0, 1), base.Add(time.Hour))

	if got := s.EvictIdle(base.Add(30 * time.Minute)); got != 1 {
		t.Errorf("evicted %d, want 1", got)
	}
	if s.Len() != 1 {
		t.Errorf("Len = %d, want the recent key to survive", s.Len())
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := New()
	s.Add(card, geom, pt("a", 0, 10), base)
	s.Add(card, geom, pt("b", time.Second, 20), base)

	snap, ok := s.Snapshot(card, base)
	if !ok {
		t.Fatal("Snapshot reported the key missing")
	}
	if len(snap.Points) != 2 || snap.Key != card {
		t.Fatalf("snapshot = %+v", snap)
	}

	// The snapshot must be a copy: further writes must not alter it.
	s.Add(card, geom, pt("c", 2*time.Second, 30), base)
	if len(snap.Points) != 2 {
		t.Error("snapshot aliased the window's storage")
	}

	restored := New()
	restored.Restore(snap, geom, base)
	w := restored.Window(card)
	if w == nil || w.Count() != 2 {
		t.Fatalf("restore produced %v", w)
	}
	if last, _ := w.Last(); last.Value != 20 {
		t.Errorf("restored last value = %v, want 20", last.Value)
	}
}

func TestSnapshotOfMissingKey(t *testing.T) {
	if _, ok := New().Snapshot(card, base); ok {
		t.Error("Snapshot of an untracked key should report false")
	}
}

// Compaction turns a deleted key into a record with no points. Restoring one
// must delete rather than resurrect an empty window for the sweep to find.
func TestRestoreEmptySnapshotIsATombstone(t *testing.T) {
	s := New()
	s.Add(card, geom, pt("a", 0, 1), base)
	s.Restore(Snapshot{Key: card}, geom, base)
	if s.Window(card) != nil || s.Len() != 0 {
		t.Error("an empty snapshot should have removed the key")
	}
}
