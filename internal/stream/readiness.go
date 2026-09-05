package stream

import (
	"sort"
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
	mu sync.RWMutex
	// owned maps each partition this member holds to whether its state has
	// been replayed. A single "restored" flag cannot describe a cooperative
	// rebalance, where a member keeps some partitions and loses others in the
	// same round -- and franz-go's default balancer is cooperative-sticky.
	owned     map[int32]bool
	degraded  bool
	changedAt time.Time
}

// State is the readiness answer, shaped for a JSON body. A probe only reads
// the status code; a human reading `kubectl describe` wants the reason.
type State struct {
	Ready      bool    `json:"ready"`
	Reason     string  `json:"reason"`
	Partitions []int32 `json:"partitions"`
	// Degraded means a changelog replay did not finish cleanly. The processor
	// is still counted as ready -- see Restored.
	Degraded bool `json:"degraded,omitempty"`
	// Seconds in the current state, which is how a stuck restore is spotted.
	ForSeconds float64 `json:"forSeconds"`
}

// NewReadiness returns a Readiness that is not ready: no partitions are owned
// until the group hands some over.
func NewReadiness() *Readiness {
	return &Readiness{owned: map[int32]bool{}, changedAt: time.Now()}
}

// Assigned records partitions the group has just handed over, whose state has
// not been replayed yet.
//
// It adds rather than replaces. Under cooperative rebalancing the callback
// carries only the *newly* assigned partitions, so replacing the set would
// discard everything this member already holds.
func (r *Readiness) Assigned(partitions []int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range partitions {
		r.owned[p] = false
	}
	r.degraded = false
	r.changedAt = time.Now()
}

// Restored records that these partitions' changelog replay has finished.
//
// `degraded` is true when the replay returned an error. The processor is still
// marked ready in that case, deliberately: it is going to consume those
// partitions whether or not this endpoint admits it, and a pod that is
// permanently unready is one Kubernetes will never finish rolling out -- so
// the deploy hangs while the old replica, which has the same problem, keeps
// serving. Ready-and-degraded is visible; never-ready is a deadlock.
func (r *Readiness) Restored(partitions []int32, degraded bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range partitions {
		if _, held := r.owned[p]; held {
			r.owned[p] = true
		}
	}
	r.degraded = degraded
	r.changedAt = time.Now()
}

// Revoked records partitions the group has taken away.
//
// It removes only those, for the same reason Assigned adds only its own: a
// cooperative rebalance revokes a subset. Clearing everything here is what
// made a processor that still owned a partition report that it owned none --
// and cooperative-sticky never re-assigns what a member kept, so it stayed
// wrong until the next restart.
func (r *Readiness) Revoked(partitions []int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range partitions {
		delete(r.owned, p)
	}
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

	partitions := make([]int32, 0, len(r.owned))
	for p := range r.owned {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	return State{
		Ready:      r.ready(),
		Reason:     r.reason(),
		Partitions: partitions,
		Degraded:   r.degraded,
		ForSeconds: time.Since(r.changedAt).Round(time.Millisecond).Seconds(),
	}
}

// ready reports whether every partition this member owns has had its state
// replayed, and that it owns at least one.
func (r *Readiness) ready() bool {
	if len(r.owned) == 0 {
		return false
	}
	for _, restored := range r.owned {
		if !restored {
			return false
		}
	}
	return true
}

func (r *Readiness) reason() string {
	switch {
	case len(r.owned) == 0:
		return "waiting for a partition assignment"
	case !r.ready():
		return "replaying the changelog"
	case r.degraded:
		return "serving, but the changelog replay did not finish cleanly"
	default:
		return "serving"
	}
}
