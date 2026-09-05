package stream

import (
	"sync"
	"time"
)

// Readiness reports whether this processor is in a state where its answers can
// be trusted.
//
// "The process is running" and "the process can answer correctly" are different
// questions, and only the second one belongs behind a readiness probe. A
// processor that has just started, or that is partway through a rebalance,
// is alive and useless: it owns partitions whose window state has not been
// replayed yet, so every stateful rule it evaluates finds an empty history and
// silently reports nothing. Velocity, z-score and geovelocity all just stop
// firing, and nothing in the logs says so.
//
// Liveness answers "should this be restarted" and is served by /health.
// Readiness answers "should this be counted as working" and is served here.
// During a rolling update the second one is what makes the rollout wait for a
// restore instead of taking down the next replica while this one is still
// catching up.
type Readiness struct {
	mu         sync.RWMutex
	partitions []int32
	restored   bool
	degraded   bool
	changedAt  time.Time
}

// State is the readiness answer, shaped for a JSON body. A probe only reads
// the status code; a human reading `kubectl describe` wants the reason.
type State struct {
	Ready      bool    `json:"ready"`
	Reason     string  `json:"reason"`
	Partitions []int32 `json:"partitions"`
	// Degraded means the changelog replay did not finish cleanly. The
	// processor is still counted as ready — see Restored.
	Degraded bool `json:"degraded,omitempty"`
	// Seconds in the current state, which is how a stuck restore is spotted.
	ForSeconds float64 `json:"forSeconds"`
}

// NewReadiness returns a Readiness that is not ready: no partitions are owned
// until the group hands some over.
func NewReadiness() *Readiness {
	return &Readiness{changedAt: time.Now()}
}

// Assigned records that the group has handed these partitions over and their
// state has not been replayed yet.
func (r *Readiness) Assigned(partitions []int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partitions = append([]int32(nil), partitions...)
	r.restored = false
	r.degraded = false
	r.changedAt = time.Now()
}

// Restored records that the changelog replay has finished.
//
// `degraded` is true when the replay returned an error. The processor is still
// marked ready in that case, deliberately: it is going to consume those
// partitions whether or not this endpoint admits it, and a pod that is
// permanently unready is one Kubernetes will never finish rolling out — so the
// deploy hangs while the old replica, which has the same problem, keeps
// serving. Ready-and-degraded is visible; never-ready is a deadlock.
func (r *Readiness) Restored(degraded bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.restored = true
	r.degraded = degraded
	r.changedAt = time.Now()
}

// Revoked records that the group has taken the partitions away. A rebalance is
// in progress and this processor owns nothing until it ends.
func (r *Readiness) Revoked() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partitions = nil
	r.restored = false
	r.degraded = false
	r.changedAt = time.Now()
}

// Ready is the probe's answer.
func (r *Readiness) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ready()
}

// State is the probe's answer with the reasoning attached.
func (r *Readiness) State() State {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return State{
		Ready:      r.ready(),
		Reason:     r.reason(),
		Partitions: append([]int32(nil), r.partitions...),
		Degraded:   r.degraded,
		ForSeconds: time.Since(r.changedAt).Round(time.Millisecond).Seconds(),
	}
}

func (r *Readiness) ready() bool { return len(r.partitions) > 0 && r.restored }

func (r *Readiness) reason() string {
	switch {
	case len(r.partitions) == 0:
		return "waiting for a partition assignment"
	case !r.restored:
		return "replaying the changelog"
	case r.degraded:
		return "serving, but the changelog replay did not finish cleanly"
	default:
		return "serving"
	}
}
