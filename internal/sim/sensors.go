package sim

import (
	"fmt"
	"sync"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
)

// sensorGrid is the city mesh. A fixed pool, reported round-robin, so every
// sensor checks in on a predictable cadence — which is what makes going quiet
// meaningful rather than merely unlucky.
const sensorGrid = 64

// Sensors simulates an air-quality mesh reporting PM2.5 in µg/m³.
//
// The WHO 24-hour guideline is 15; 150 is the band where public health
// advisories go out. Baselines here sit in the ordinary urban 18–46 range so
// the threshold rule means something when it fires.
type Sensors struct {
	rng   *lockedRand
	ids   *ids
	sched *scheduled

	mu       sync.Mutex
	cursor   int
	elevated map[string]time.Time // sensors an injected fault is driving high
}

func NewSensors(seed int64) *Sensors {
	return &Sensors{
		rng:      newRand(seed),
		ids:      &ids{prefix: "sen"},
		sched:    newScheduled(),
		elevated: make(map[string]time.Time),
	}
}

func (s *Sensors) Stream() domain.Stream { return domain.StreamSensors }

func (s *Sensors) Faults() []Fault {
	return []Fault{FaultAirQuality, FaultSensorSpike, FaultSensorOffline}
}

// sensorAt derives a sensor from its index: a grid reference, a position
// around Goiânia, and a stable personal baseline.
func sensorAt(i int) (id string, lat, lon, baseline float64) {
	row := rune('A' + i/8)
	col := i%8 + 1
	id = fmt.Sprintf("grid-%c%d", row, col)
	// A ~10 km mesh over the city.
	lat = -16.6869 + float64(i/8-4)*0.02
	lon = -49.2648 + float64(i%8-4)*0.02
	baseline = 18 + float64((i*11)%28)
	return
}

func (s *Sensors) reading(i int, at time.Time, value float64) domain.Event {
	id, lat, lon, _ := sensorAt(i)
	return domain.Event{
		ID:     s.ids.next(),
		Stream: domain.StreamSensors,
		Key:    id,
		At:     at,
		Value:  round2(value),
		Unit:   "µg/m³",
		Geo:    &domain.Geo{Lat: lat, Lon: lon, Place: id},
		Labels: map[string]string{"metric": "pm25", "model": "SDS011"},
	}
}

func (s *Sensors) Tick(now time.Time, rate int) []domain.Event {
	out := s.sched.due(now)

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < rate; i++ {
		idx := s.cursor % sensorGrid
		s.cursor++

		id, _, _, baseline := sensorAt(idx)
		if s.sched.muted(id, now) {
			continue
		}
		if until, ok := s.elevated[id]; ok {
			if now.Before(until) {
				out = append(out, s.reading(idx, now, s.rng.normal(182, 12)))
				continue
			}
			delete(s.elevated, id)
		}
		out = append(out, s.reading(idx, now, s.rng.normal(baseline, baseline*0.18)))
	}
	return out
}

func (s *Sensors) Inject(f Fault, now time.Time) (string, error) {
	idx := s.rng.intn(sensorGrid)
	id, _, _, baseline := sensorAt(idx)

	switch f {
	case FaultAirQuality:
		// Hold the sensor above the advisory threshold for long enough that
		// the sustain requirement is satisfied and this is not just a spike.
		s.mu.Lock()
		s.elevated[id] = now.Add(3 * time.Minute)
		s.mu.Unlock()

	case FaultSensorSpike:
		// The z-score rule judges a sensor against its own history, so give
		// it one. Kept well inside the velocity limit.
		const priming = 18
		for i := 0; i < priming; i++ {
			s.sched.add(now.Add(time.Duration(i)*2*time.Second), s.reading(idx, now, s.rng.normal(baseline, baseline*0.18)))
		}
		s.sched.add(now.Add(priming*2*time.Second+time.Second), s.reading(idx, now, baseline*9))

	case FaultSensorOffline:
		s.sched.mute(id, now.Add(5*time.Minute))

	default:
		return "", fmt.Errorf("%w: %s does not implement %q", ErrUnknownFault, s.Stream(), f)
	}
	return id, nil
}
