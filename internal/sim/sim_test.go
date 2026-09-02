package sim_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/engine"
	"github.com/Lauiskk/vigil/internal/rules"
	"github.com/Lauiskk/vigil/internal/sim"
)

var base = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

const (
	tick        = 100 * time.Millisecond
	ratePerTick = 1 // ten events per second
)

// harness runs a source into a real engine over virtual time.
//
// This is the test that keeps the profile numbers honest. Every threshold in
// rules.DefaultProfiles is a judgement call about what ordinary traffic looks
// like, and the only way to know a judgement is right is to generate the
// traffic and see which rules fire.
type harness struct {
	source sim.Source
	engine *engine.Engine
	now    time.Time
	alerts []domain.Alert
}

func newHarness(source sim.Source) *harness {
	h := &harness{source: source, now: base}
	h.engine = engine.New(rules.DefaultProfiles(), engine.WithClock(func() time.Time { return h.now }))
	return h
}

// run advances virtual time, feeding every generated event through the engine
// and sweeping once a second, exactly as the processor does.
func (h *harness) run(t *testing.T, d time.Duration) {
	t.Helper()
	ticks := int(d / tick)
	for i := 0; i < ticks; i++ {
		for _, ev := range h.source.Tick(h.now, ratePerTick) {
			alerts, err := h.engine.Ingest(ev)
			if err != nil {
				t.Fatalf("engine rejected a generated event: %v (%+v)", err, ev)
			}
			h.alerts = append(h.alerts, alerts...)
		}
		if i%10 == 0 {
			h.alerts = append(h.alerts, h.engine.Sweep()...)
		}
		h.now = h.now.Add(tick)
	}
}

func (h *harness) alertsFor(key, rule string) int {
	n := 0
	for _, a := range h.alerts {
		if a.Key == key && a.Rule == rule {
			n++
		}
	}
	return n
}

// Every fault is named after the rule it is meant to demonstrate. If a fault
// trips a different rule, the demo teaches the wrong thing.
func TestEachFaultTripsTheRuleItIsNamedFor(t *testing.T) {
	tests := []struct {
		name     string
		source   func() sim.Source
		fault    sim.Fault
		wantRule string
	}{
		{"impossible travel", func() sim.Source { return sim.NewPayments(1) }, sim.FaultImpossibleTravel, "geovelocity"},
		{"card testing", func() sim.Source { return sim.NewPayments(2) }, sim.FaultCardTesting, "velocity"},
		{"amount anomaly", func() sim.Source { return sim.NewPayments(3) }, sim.FaultAmountAnomaly, "zscore"},
		{"job stall", func() sim.Source { return sim.NewVideo(4) }, sim.FaultJobStall, "stall"},
		{"sla breach", func() sim.Source { return sim.NewVideo(5) }, sim.FaultSLABreach, "threshold"},
		{"retry storm", func() sim.Source { return sim.NewVideo(6) }, sim.FaultRetryStorm, "velocity"},
		{"air quality", func() sim.Source { return sim.NewSensors(7) }, sim.FaultAirQuality, "threshold"},
		{"sensor spike", func() sim.Source { return sim.NewSensors(8) }, sim.FaultSensorSpike, "zscore"},
		{"sensor offline", func() sim.Source { return sim.NewSensors(9) }, sim.FaultSensorOffline, "stall"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(tc.source())

			// Let ordinary traffic establish itself first, so faults land in
			// a running system rather than an empty one.
			h.run(t, 90*time.Second)

			key, err := h.source.Inject(tc.fault, h.now)
			if err != nil {
				t.Fatalf("Inject(%s): %v", tc.fault, err)
			}
			h.run(t, 5*time.Minute)

			if got := h.alertsFor(key, tc.wantRule); got == 0 {
				t.Errorf("injecting %s into %s raised no %q alert for key %q\nalerts seen: %s",
					tc.fault, h.source.Stream(), tc.wantRule, key, summarise(h.alerts))
			}
		})
	}
}

// The counterpart, and the more fragile property: ordinary traffic must be
// quiet. A detector that alerts on normal behaviour is worse than none.
//
// Swept across seeds rather than run once. A single seed proves only that one
// particular sequence of random draws happens to be quiet, and the thresholds
// this is guarding are exactly the kind that pass by luck.
func TestOrdinaryTrafficRaisesNoAlerts(t *testing.T) {
	sources := map[string]func(int64) sim.Source{
		"payments": func(seed int64) sim.Source { return sim.NewPayments(seed) },
		"video":    func(seed int64) sim.Source { return sim.NewVideo(seed) },
		"sensors":  func(seed int64) sim.Source { return sim.NewSensors(seed) },
	}
	for name, mk := range sources {
		t.Run(name, func(t *testing.T) {
			for seed := int64(1); seed <= 8; seed++ {
				h := newHarness(mk(seed))
				h.run(t, 10*time.Minute)
				if len(h.alerts) != 0 {
					t.Errorf("seed %d: %d false positives in ten minutes of ordinary traffic: %s",
						seed, len(h.alerts), summarise(h.alerts))
				}
			}
		})
	}
}

func TestInjectRejectsFaultsASourceDoesNotImplement(t *testing.T) {
	for _, src := range []sim.Source{sim.NewPayments(1), sim.NewVideo(1), sim.NewSensors(1)} {
		if _, err := src.Inject("not-a-fault", base); err == nil {
			t.Errorf("%s accepted an unknown fault", src.Stream())
		}
		// And every advertised fault must be accepted.
		for _, f := range src.Faults() {
			if _, err := src.Inject(f, base); err != nil {
				t.Errorf("%s advertises %q but rejected it: %v", src.Stream(), f, err)
			}
		}
	}
}

func TestGeneratedEventsAreValidAndUniquelyIdentified(t *testing.T) {
	for _, src := range []sim.Source{sim.NewPayments(21), sim.NewVideo(22), sim.NewSensors(23)} {
		t.Run(string(src.Stream()), func(t *testing.T) {
			seen := map[string]bool{}
			now := base
			for i := 0; i < 600; i++ {
				for _, ev := range src.Tick(now, 4) {
					if err := ev.Validate(); err != nil {
						t.Fatalf("generated an invalid event: %v (%+v)", err, ev)
					}
					if ev.Stream != src.Stream() {
						t.Fatalf("event carries stream %q from a %q source", ev.Stream, src.Stream())
					}
					if seen[ev.ID] {
						t.Fatalf("duplicate event id %q — the window's Preceding lookup relies on these being unique", ev.ID)
					}
					seen[ev.ID] = true
				}
				now = now.Add(tick)
			}
			if len(seen) == 0 {
				t.Error("the source produced nothing at all")
			}
		})
	}
}

func summarise(alerts []domain.Alert) string {
	if len(alerts) == 0 {
		return "(none)"
	}
	counts := map[string]int{}
	for _, a := range alerts {
		counts[string(a.Stream)+"/"+a.Rule]++
	}
	out := ""
	for k, n := range counts {
		out += "\n  " + k + " x" + strconv.Itoa(n)
	}
	return out
}
