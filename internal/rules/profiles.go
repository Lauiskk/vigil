package rules

import (
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
)

// Profile is everything the processor needs to know about one stream: the
// geometry of the window its keys are tracked in, and the rules that run.
//
// Adding a fourth stream is a Profile, not a code path — which is the whole
// argument for five generic primitives over three bespoke pipelines.
type Profile struct {
	Stream domain.Stream

	// Window is how far back a key's history reaches; Grace is how long an
	// out-of-order record is still accepted after the watermark passes it.
	Window time.Duration
	Grace  time.Duration

	// MaxPoints bounds per-key memory. A key busier than this degrades to its
	// most recent observations rather than growing without limit.
	MaxPoints int

	// Cooldown suppresses a repeat of the same rule on the same key. Without
	// it a card being tested forty times raises thirty alerts about one fact.
	Cooldown time.Duration

	// SweepEvery is how often absence-based rules are evaluated.
	SweepEvery time.Duration

	Rules []Rule
}

// DefaultProfiles returns the tuned configuration for the three streams.
//
// The numbers are chosen so the live demo produces something worth watching
// within seconds rather than being production-conservative — a real deployment
// would widen these considerably, and the README says so.
func DefaultProfiles() map[domain.Stream]Profile {
	payments := domain.StreamPayments
	video := domain.StreamVideo
	sensors := domain.StreamSensors

	return map[domain.Stream]Profile{
		payments: {
			Stream:     payments,
			Window:     60 * time.Second,
			Grace:      5 * time.Second,
			MaxPoints:  256,
			Cooldown:   30 * time.Second,
			SweepEvery: 0, // no absence rule on payments: a quiet card is normal
			Rules: []Rule{
				// Twenty authorisations on one card inside a minute is the
				// classic card-testing signature. The limit sits above what
				// the z-score rule needs to build a baseline (12 samples),
				// so an unusual *amount* does not also read as unusual
				// *frequency* and raise two alerts for one fact.
				Velocity{Max: 20, Per: time.Minute, On: []domain.Stream{payments}},
				// Faster than a commercial jet, over a distance that cannot be
				// a geocoding artefact.
				GeoVelocity{MaxKmh: 1000, MinKm: 100, On: []domain.Stream{payments}},
				// Typical spends run R$ 40–220, so a deviation under R$ 200
				// is not worth waking anyone for however many sigma it is.
				ZScore{K: 3.5, MinSamples: 12, MinDelta: 200, On: []domain.Stream{payments}},
			},
		},
		video: {
			Stream:     video,
			Window:     5 * time.Minute,
			Grace:      10 * time.Second,
			MaxPoints:  256,
			Cooldown:   time.Minute,
			SweepEvery: 15 * time.Second,
			Rules: []Rule{
				// A job reports progress every 15s and finishes in about a
				// dozen records. A retry storm adds two dozen in six
				// seconds, and this limit sits in the gap between them.
				Velocity{Max: 20, Per: 5 * time.Minute, On: []domain.Stream{video}},
				Stall{After: 90 * time.Second, On: []domain.Stream{video}},
				// Value is encoder throughput in fps; sustained starvation is
				// the SLA breach, a single slow frame is not.
				Threshold{Bound: 12, Above: false, Sustained: 45 * time.Second, On: []domain.Stream{video}},
			},
		},
		sensors: {
			Stream:     sensors,
			Window:     3 * time.Minute,
			Grace:      15 * time.Second,
			MaxPoints:  256,
			Cooldown:   2 * time.Minute,
			SweepEvery: 30 * time.Second,
			Rules: []Rule{
				// PM2.5 in µg/m³. The WHO 24-hour guideline is 15; 150 is the
				// "everyone should stay indoors" band.
				Threshold{Bound: 150, Above: true, Sustained: time.Minute, On: []domain.Stream{sensors}},
				// Five sigma, not four. The mesh produces thousands of
				// readings an hour, and at four sigma ordinary noise alone
				// would raise a spike every few hours.
				ZScore{K: 5, MinSamples: 15, MinDelta: 40, On: []domain.Stream{sensors}},
				Velocity{Max: 60, Per: 3 * time.Minute, On: []domain.Stream{sensors}},
				// The mesh reports round-robin every ten seconds or so, so
				// two minutes of silence is unambiguous rather than unlucky.
				Stall{After: 2 * time.Minute, On: []domain.Stream{sensors}},
			},
		},
	}
}
