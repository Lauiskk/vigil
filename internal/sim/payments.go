package sim

import (
	"fmt"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
)

// place is a city a card can transact in.
type place struct {
	name     string
	lat, lon float64
}

// home cities, all within Brazil, so ordinary traffic never looks like travel.
var homeCities = []place{
	{"São Paulo", -23.5505, -46.6333},
	{"Rio de Janeiro", -22.9068, -43.1729},
	{"Goiânia", -16.6869, -49.2648},
	{"Brasília", -15.7975, -47.8919},
	{"Belo Horizonte", -19.9167, -43.9345},
	{"Curitiba", -25.4284, -49.2733},
	{"Salvador", -12.9777, -38.5016},
	{"Fortaleza", -3.7319, -38.5267},
	{"Porto Alegre", -30.0346, -51.2177},
	{"Recife", -8.0476, -34.8770},
}

// far cities, used only by the impossible-travel fault.
var farCities = []place{
	{"Tokyo", 35.6762, 139.6503},
	{"London", 51.5074, -0.1278},
	{"New York", 40.7128, -74.0060},
	{"Lagos", 6.5244, 3.3792},
	{"Sydney", -33.8688, 151.2093},
	{"Moscow", 55.7558, 37.6173},
}

var merchants = []string{
	"Padaria Aurora", "Posto Ipiranga", "Mercado Extra", "iFood", "Uber",
	"Farmácia Pague Menos", "Livraria Cultura", "Netflix", "Casas Bahia", "Magalu",
}

// Payments simulates card authorisations.
//
// Cards are derived from their index rather than pre-allocated, so the
// population can scale with the event rate. That matters: if the pool were
// fixed, turning the rate up would raise every card's personal velocity and
// the surge would read as thousands of simultaneous card-testing attacks
// rather than as ordinary busy traffic.
type Payments struct {
	rng   *lockedRand
	ids   *ids
	sched *scheduled

	// poolFor keeps the per-card rate roughly constant as the global rate
	// changes. Overridable in tests.
	poolFor func(rate int) int
}

func NewPayments(seed int64) *Payments {
	return &Payments{
		rng:   newRand(seed),
		ids:   &ids{prefix: "pay"},
		sched: newScheduled(),
		poolFor: func(rate int) int {
			// One authorisation per card per minute at any rate.
			if n := rate * 60; n > 500 {
				return n
			}
			return 500
		},
	}
}

func (p *Payments) Stream() domain.Stream { return domain.StreamPayments }

func (p *Payments) Faults() []Fault {
	return []Fault{FaultImpossibleTravel, FaultCardTesting, FaultAmountAnomaly}
}

// card describes one simulated card. Every attribute is a pure function of the
// index, so the same index always yields the same card without storing any.
type card struct {
	id      string
	home    place
	typical float64
}

func cardAt(i int) card {
	return card{
		id:      fmt.Sprintf("card-%05d", i),
		home:    homeCities[i%len(homeCities)],
		typical: 40 + float64((i*7)%13)*15, // R$ 40 to R$ 220
	}
}

// authorisation builds one ordinary transaction for a card.
func (p *Payments) authorisation(c card, at time.Time) domain.Event {
	// A little jitter around the city centre so the map is not a single dot.
	// Well under the geovelocity rule's 100 km floor.
	return domain.Event{
		ID:     p.ids.next(),
		Stream: domain.StreamPayments,
		Key:    c.id,
		At:     at,
		Value:  round2(p.rng.normal(c.typical, c.typical*0.25)),
		Unit:   "BRL",
		Geo: &domain.Geo{
			Lat:   c.home.lat + (p.rng.float()-0.5)*0.2,
			Lon:   c.home.lon + (p.rng.float()-0.5)*0.2,
			Place: c.home.name,
		},
		Labels: map[string]string{"merchant": pick(p.rng, merchants)},
	}
}

func (p *Payments) Tick(now time.Time, rate int) []domain.Event {
	out := p.sched.due(now)
	pool := p.poolFor(rate)
	for i := 0; i < rate; i++ {
		c := cardAt(p.rng.intn(pool))
		if p.sched.muted(c.id, now) {
			continue
		}
		out = append(out, p.authorisation(c, now))
	}
	return out
}

func (p *Payments) Inject(f Fault, now time.Time) (string, error) {
	c := cardAt(p.rng.intn(p.poolFor(1)))

	switch f {
	case FaultImpossibleTravel:
		// The card transacts at home, then on the other side of the world
		// three seconds later. Nothing about that is ambiguous.
		far := pick(p.rng, farCities)
		here := p.authorisation(c, now)
		there := p.authorisation(c, now)
		there.Geo = &domain.Geo{Lat: far.lat, Lon: far.lon, Place: far.name}
		there.Value = round2(c.typical * 12)
		p.sched.add(now, here)
		p.sched.add(now.Add(3*time.Second), there)

	case FaultCardTesting:
		// Two dozen R$ 1,00 authorisations in eight seconds: someone walking
		// a stolen card number to find out whether it is live.
		const n = 24
		for i := 0; i < n; i++ {
			ev := p.authorisation(c, now)
			ev.Value = 1
			ev.Labels["merchant"] = "—"
			p.sched.add(now.Add(time.Duration(i)*8*time.Second/n), ev)
		}

	case FaultAmountAnomaly:
		// The z-score rule needs a baseline before it can judge, so prime one
		// first. Kept under the velocity limit so this fault demonstrates the
		// rule it is named after rather than tripping a different one.
		const priming = 14
		for i := 0; i < priming; i++ {
			p.sched.add(now.Add(time.Duration(i)*2*time.Second), p.authorisation(c, now))
		}
		big := p.authorisation(c, now)
		big.Value = round2(c.typical * 40)
		big.Labels["merchant"] = "Joalheria Vivara"
		p.sched.add(now.Add(priming*2*time.Second+time.Second), big)

	default:
		return "", fmt.Errorf("%w: %s does not implement %q", ErrUnknownFault, p.Stream(), f)
	}
	return c.id, nil
}

func round2(v float64) float64 { return float64(int64(v*100+0.5)) / 100 }
