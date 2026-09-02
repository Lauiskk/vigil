package sim

import "time"

// Pacer turns a target rate in events per second into a whole number of
// events for each tick, carrying the remainder forward.
//
// Without the carry, a rate of 6/s on 100ms ticks truncates to zero events
// per tick and the generator produces nothing at all — the failure mode is
// silence, which is the hardest kind to notice.
type Pacer struct{ acc float64 }

// Take returns how many events this tick owes, given the target rate.
func (p *Pacer) Take(perSecond float64, tick time.Duration) int {
	if perSecond <= 0 {
		p.acc = 0
		return 0
	}
	p.acc += perSecond * tick.Seconds()
	n := int(p.acc)
	p.acc -= float64(n)
	return n
}
