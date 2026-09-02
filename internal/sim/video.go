package sim

import (
	"fmt"
	"sync"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
)

var (
	codecs      = []string{"h264", "hevc", "av1", "vp9"}
	resolutions = []string{"720p", "1080p", "1440p", "2160p"}
)

// Video simulates a transcode farm: jobs are admitted, report throughput as
// they progress, and finish. It is the stream that exercises the absence-based
// rule, because a wedged encoder announces nothing at all.
type Video struct {
	rng   *lockedRand
	ids   *ids
	sched *scheduled

	mu     sync.Mutex
	jobs   map[string]*job
	order  []string // stable iteration, so every job reports regularly
	serial int
}

type job struct {
	id           string
	codec, res   string
	progress     float64
	baseFPS      float64
	starvedUntil time.Time

	// nextReport paces a job's progress records. Real transcoders report
	// every few seconds, and that cadence is what makes a retry storm
	// detectable at all: without it, normal reporting and a storm differ only
	// in a volume the velocity rule cannot separate.
	nextReport time.Time
}

func NewVideo(seed int64) *Video {
	return &Video{
		rng:   newRand(seed),
		ids:   &ids{prefix: "vid"},
		sched: newScheduled(),
		jobs:  make(map[string]*job),
	}
}

func (v *Video) Stream() domain.Stream { return domain.StreamVideo }

func (v *Video) Faults() []Fault {
	return []Fault{FaultJobStall, FaultSLABreach, FaultRetryStorm}
}

// reportEvery is the interval at which a healthy job reports progress.
const reportEvery = 15 * time.Second

// active is how many jobs the farm keeps in flight. The floor gives the feed
// something to show at rest; the ceiling stops a surge turning into an
// unbounded job table.
func active(rate int) int {
	switch {
	case rate < 24:
		return 24
	case rate > 256:
		return 256
	}
	return rate
}

func (v *Video) admit() *job {
	v.serial++
	j := &job{
		id:      fmt.Sprintf("job-%05d", v.serial),
		codec:   pick(v.rng, codecs),
		res:     pick(v.rng, resolutions),
		baseFPS: 24 + v.rng.float()*36, // 24 to 60 fps
	}
	v.jobs[j.id] = j
	v.order = append(v.order, j.id)
	return j
}

func (v *Video) event(j *job, at time.Time, status string) domain.Event {
	fps := v.rng.normal(j.baseFPS, 3)
	if at.Before(j.starvedUntil) {
		// A starved encoder is not merely slower; it is below the floor the
		// SLA rule watches for.
		fps = v.rng.normal(7, 1.5)
	}
	return domain.Event{
		ID:     v.ids.next(),
		Stream: domain.StreamVideo,
		Key:    j.id,
		At:     at,
		Value:  round2(fps),
		Unit:   "fps",
		Labels: map[string]string{
			domain.LabelStatus: status,
			"codec":            j.codec,
			"resolution":       j.res,
			"progress":         fmt.Sprintf("%.0f", j.progress),
		},
	}
}

func (v *Video) Tick(now time.Time, rate int) []domain.Event {
	out := v.sched.due(now)

	v.mu.Lock()
	defer v.mu.Unlock()

	for len(v.jobs) < active(rate) {
		j := v.admit()
		// Stagger the first report so the whole farm does not report in
		// lockstep on the tick after start-up.
		j.nextReport = now.Add(time.Duration(v.rng.intn(int(reportEvery))))
	}

	emitted, kept := 0, v.order[:0]
	for _, id := range v.order {
		j, ok := v.jobs[id]
		if !ok {
			continue
		}
		kept = append(kept, id)

		if emitted >= rate || now.Before(j.nextReport) || v.sched.muted(id, now) {
			continue
		}
		emitted++
		// Jitter so jobs do not converge into a synchronised cohort.
		j.nextReport = now.Add(reportEvery + time.Duration(v.rng.intn(6000)-3000)*time.Millisecond)

		j.progress += 4 + v.rng.float()*8
		if j.progress >= 100 {
			out = append(out, v.event(j, now, "done"))
			delete(v.jobs, id)
			kept = kept[:len(kept)-1]
			continue
		}
		out = append(out, v.event(j, now, "running"))
	}
	v.order = kept
	return out
}

func (v *Video) Inject(f Fault, now time.Time) (string, error) {
	v.mu.Lock()
	if len(v.jobs) == 0 {
		v.admit()
	}
	id := v.order[v.rng.intn(len(v.order))]
	j := v.jobs[id]
	v.mu.Unlock()

	switch f {
	case FaultJobStall:
		// The encoder wedges: no further progress, no failure, no signal at
		// all. Only the sweep can notice.
		v.sched.mute(id, now.Add(4*time.Minute))

	case FaultSLABreach:
		v.mu.Lock()
		j.starvedUntil = now.Add(2 * time.Minute)
		v.mu.Unlock()

	case FaultRetryStorm:
		// The job thrashes: repeated admission attempts inside a few seconds.
		// Sized against the velocity limit — a job only emits about a dozen
		// progress records in its whole life, so the storm has to clear that
		// plus the limit to be the thing that trips it.
		const n = 24
		for i := 0; i < n; i++ {
			ev := v.event(j, now.Add(time.Duration(i)*6*time.Second/n), "retrying")
			ev.Labels["attempt"] = fmt.Sprintf("%d", i+1)
			v.sched.add(ev)
		}

	default:
		return "", fmt.Errorf("%w: %s does not implement %q", ErrUnknownFault, v.Stream(), f)
	}
	return id, nil
}
