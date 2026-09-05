package stream

import (
	"sync"
	"testing"
)

func TestReadinessLifecycle(t *testing.T) {
	tests := []struct {
		name   string
		steps  func(r *Readiness)
		ready  bool
		reason string
	}{
		{
			name:   "a fresh processor owns nothing and is not ready",
			steps:  func(*Readiness) {},
			ready:  false,
			reason: "waiting for a partition assignment",
		},
		{
			name:   "assigned but not replayed is not ready",
			steps:  func(r *Readiness) { r.Assigned([]int32{0, 1}) },
			ready:  false,
			reason: "replaying the changelog",
		},
		{
			name: "assigned and replayed is ready",
			steps: func(r *Readiness) {
				r.Assigned([]int32{0, 1})
				r.Restored(false)
			},
			ready:  true,
			reason: "serving",
		},
		{
			name: "a failed replay is still ready, and says so",
			steps: func(r *Readiness) {
				r.Assigned([]int32{0})
				r.Restored(true)
			},
			ready:  true,
			reason: "serving, but the changelog replay did not finish cleanly",
		},
		{
			name: "a revoked assignment stops being ready",
			steps: func(r *Readiness) {
				r.Assigned([]int32{0, 1})
				r.Restored(false)
				r.Revoked()
			},
			ready:  false,
			reason: "waiting for a partition assignment",
		},
		{
			name: "a rebalance drops readiness until the new state is replayed",
			steps: func(r *Readiness) {
				r.Assigned([]int32{0, 1})
				r.Restored(false)
				r.Revoked()
				r.Assigned([]int32{1})
			},
			ready:  false,
			reason: "replaying the changelog",
		},
		{
			name: "a clean replay after a degraded one clears the flag",
			steps: func(r *Readiness) {
				r.Assigned([]int32{0})
				r.Restored(true)
				r.Assigned([]int32{0})
				r.Restored(false)
			},
			ready:  true,
			reason: "serving",
		},
		{
			name:   "an empty assignment is not an assignment",
			steps:  func(r *Readiness) { r.Assigned(nil); r.Restored(false) },
			ready:  false,
			reason: "waiting for a partition assignment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReadiness()
			tt.steps(r)

			if got := r.Ready(); got != tt.ready {
				t.Errorf("Ready() = %v, want %v", got, tt.ready)
			}
			state := r.State()
			if state.Ready != tt.ready {
				t.Errorf("State().Ready = %v, want %v", state.Ready, tt.ready)
			}
			if state.Reason != tt.reason {
				t.Errorf("State().Reason = %q, want %q", state.Reason, tt.reason)
			}
			if state.ForSeconds < 0 {
				t.Errorf("State().ForSeconds = %v, want >= 0", state.ForSeconds)
			}
		})
	}
}

func TestReadinessCopiesPartitions(t *testing.T) {
	// The caller owns the slice it passed and franz-go reuses its assignment
	// maps between rebalances. Holding a reference would let a later rebalance
	// silently rewrite what this reports.
	r := NewReadiness()
	parts := []int32{0, 1}
	r.Assigned(parts)
	parts[0] = 99

	if got := r.State().Partitions; got[0] != 0 {
		t.Errorf("Partitions[0] = %d, want 0 — Assigned kept the caller's slice", got[0])
	}

	reported := r.State().Partitions
	reported[0] = 77
	if got := r.State().Partitions; got[0] != 0 {
		t.Errorf("Partitions[0] = %d, want 0 — State handed out its own slice", got[0])
	}
}

func TestReadinessIsSafeUnderConcurrency(t *testing.T) {
	// The rebalance callbacks run on franz-go's goroutine while the probe is
	// served on net/http's. Run with -race; without the mutex this fails.
	r := NewReadiness()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				r.Assigned([]int32{int32(n)})
				r.Restored(j%2 == 0)
				_ = r.State()
				_ = r.Ready()
				r.Revoked()
			}
		}(i)
	}
	wg.Wait()
}
