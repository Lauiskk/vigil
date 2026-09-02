package rules

import (
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/window"
)

var base = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

var (
	saoPaulo  = &domain.Geo{Lat: -23.5505, Lon: -46.6333, Place: "São Paulo"}
	tokyo     = &domain.Geo{Lat: 35.6762, Lon: 139.6503, Place: "Tokyo"}
	rio       = &domain.Geo{Lat: -22.9068, Lon: -43.1729, Place: "Rio de Janeiro"}
	guarulhos = &domain.Geo{Lat: -23.4356, Lon: -46.4731, Place: "Guarulhos"}
)

// seq keeps generated event ids unique. Two events may legitimately share a
// timestamp — that is the impossible-travel case — so the id cannot be derived
// from the offset alone.
var seq atomic.Int64

// ev builds an event at an offset from base.
func ev(stream domain.Stream, key string, off time.Duration, val float64, geo *domain.Geo) domain.Event {
	return domain.Event{
		ID: fmt.Sprintf("%s@%s#%d", key, off, seq.Add(1)), Stream: stream, Key: key,
		At: base.Add(off), Value: val, Geo: geo,
	}
}

// feed loads events into a window and returns it plus the final event, which
// is what the rule under test is evaluating.
func feed(size, grace time.Duration, evs ...domain.Event) (*window.Sliding, domain.Event) {
	w := window.New(size, grace, 256)
	for _, e := range evs {
		w.Add(window.Point{ID: e.ID, At: e.At, Value: e.Value, Geo: e.Geo})
	}
	return w, evs[len(evs)-1]
}

// now is the detection instant used throughout; a fixed offset keeps the
// latency arithmetic in the alerts deterministic.
var now = base.Add(time.Hour)

// ── velocity ──────────────────────────────────────────────────────────────

func TestVelocity(t *testing.T) {
	rule := Velocity{Max: 10, Per: time.Minute, On: []domain.Stream{domain.StreamPayments}}

	burst := func(n int) []domain.Event {
		out := make([]domain.Event, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, ev(domain.StreamPayments, "card-1", time.Duration(i)*time.Second, 1, nil))
		}
		return out
	}

	tests := []struct {
		name    string
		count   int
		wantHit bool
		wantSev domain.Severity
	}{
		{"below the limit", 5, false, ""},
		{"exactly at the limit is not an alert", 10, false, ""},
		{"one over", 11, true, domain.SeverityInfo},
		{"well over", 13, true, domain.SeverityWarn},
		{"double the limit", 20, true, domain.SeverityCritical},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, last := feed(time.Minute, 0, burst(tc.count)...)
			got := rule.Eval(last, w, now)
			if !tc.wantHit {
				if got != nil {
					t.Fatalf("expected no alert, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected an alert, got nil")
			}
			if got.Rule != "velocity" || got.Key != "card-1" {
				t.Errorf("wrong identity: rule=%q key=%q", got.Rule, got.Key)
			}
			if got.Title != "Card testing" {
				t.Errorf("Title = %q, want the payments phrasing", got.Title)
			}
			if got.Severity != tc.wantSev {
				t.Errorf("Severity = %q, want %q", got.Severity, tc.wantSev)
			}
			if got.Evidence["count"].(int) != tc.count {
				t.Errorf("evidence count = %v, want %d", got.Evidence["count"], tc.count)
			}
			if got.DetectedAt != now {
				t.Error("DetectedAt was not taken from the injected clock")
			}
			if len(got.EventIDs) > 12 {
				t.Errorf("EventIDs = %d, should be capped at 12", len(got.EventIDs))
			}
		})
	}
}

func TestVelocityAppliesTo(t *testing.T) {
	scoped := Velocity{On: []domain.Stream{domain.StreamPayments}}
	if !scoped.AppliesTo(domain.StreamPayments) || scoped.AppliesTo(domain.StreamVideo) {
		t.Error("scoped rule ignored its stream set")
	}
	unscoped := Velocity{}
	for _, s := range domain.Streams {
		if !unscoped.AppliesTo(s) {
			t.Errorf("an empty stream set must mean every stream; %q was excluded", s)
		}
	}
}

// ── geovelocity ───────────────────────────────────────────────────────────

func TestGeoVelocity(t *testing.T) {
	rule := GeoVelocity{MaxKmh: 1000, MinKm: 100, On: []domain.Stream{domain.StreamPayments}}

	tests := []struct {
		name    string
		events  []domain.Event
		wantHit bool
	}{
		{
			"são paulo to tokyo in 41 seconds",
			[]domain.Event{
				ev(domain.StreamPayments, "c", 0, 89.90, saoPaulo),
				ev(domain.StreamPayments, "c", 41*time.Second, 2400, tokyo),
			},
			true,
		},
		{
			"são paulo to rio over five hours is a flight",
			[]domain.Event{
				ev(domain.StreamPayments, "c", 0, 50, saoPaulo),
				ev(domain.StreamPayments, "c", 5*time.Hour, 50, rio),
			},
			false,
		},
		{
			"across town is below the distance floor",
			[]domain.Event{
				ev(domain.StreamPayments, "c", 0, 50, saoPaulo),
				ev(domain.StreamPayments, "c", time.Second, 50, guarulhos),
			},
			false,
		},
		{
			"two cities at the same instant is impossible, not merely fast",
			[]domain.Event{
				ev(domain.StreamPayments, "c", 0, 50, saoPaulo),
				ev(domain.StreamPayments, "c", 0, 50, tokyo),
			},
			true,
		},
		{
			"no previous observation to compare against",
			[]domain.Event{ev(domain.StreamPayments, "c", 0, 50, saoPaulo)},
			false,
		},
		{
			"current event carries no location",
			[]domain.Event{
				ev(domain.StreamPayments, "c", 0, 50, saoPaulo),
				ev(domain.StreamPayments, "c", time.Second, 50, nil),
			},
			false,
		},
		{
			"previous event carries no location",
			[]domain.Event{
				ev(domain.StreamPayments, "c", 0, 50, nil),
				ev(domain.StreamPayments, "c", time.Second, 50, tokyo),
			},
			false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, last := feed(6*time.Hour, 0, tc.events...)
			got := rule.Eval(last, w, now)
			if tc.wantHit != (got != nil) {
				t.Fatalf("alert = %v, want %v (%+v)", got != nil, tc.wantHit, got)
			}
			if got != nil && got.Title != "Impossible travel" {
				t.Errorf("Title = %q", got.Title)
			}
		})
	}
}

// An out-of-order arrival must be compared against its own predecessor in
// event time, not against whatever happens to be newest in the window.
func TestGeoVelocityUsesTheArrivingEventsPredecessor(t *testing.T) {
	rule := GeoVelocity{MaxKmh: 1000, MinKm: 100}

	// São Paulo at +0s and +90s, both legitimate. A record for Tokyo at +45s
	// then arrives late — out of order, but inside the grace period.
	w := window.New(10*time.Minute, time.Minute, 256)
	w.Add(window.Point{ID: "a", At: base, Geo: saoPaulo})
	w.Add(window.Point{ID: "c", At: base.Add(90 * time.Second), Geo: saoPaulo})
	late := domain.Event{
		ID: "b", Stream: domain.StreamPayments, Key: "card-1",
		At: base.Add(45 * time.Second), Geo: tokyo,
	}
	w.Add(window.Point{ID: late.ID, At: late.At, Geo: late.Geo})

	got := rule.Eval(late, w, now)
	if got == nil {
		t.Fatal("expected an alert: São Paulo to Tokyo in 45s")
	}
	// Had the rule used Previous() it would have compared against the +90s
	// São Paulo record and computed a negative interval instead.
	if got.Evidence["fromPlace"] != "São Paulo" || got.Evidence["toPlace"] != "Tokyo" {
		t.Errorf("compared the wrong pair: %v -> %v", got.Evidence["fromPlace"], got.Evidence["toPlace"])
	}
	if secs := got.Evidence["seconds"].(float64); secs != 45 {
		t.Errorf("interval = %vs, want 45s against the +0s record", secs)
	}
}

// ── zscore ────────────────────────────────────────────────────────────────

// steady is a plausible spending history: mean near 90 with real variance.
var steady = []float64{85, 92, 88, 95, 90, 87, 93, 91, 89, 94, 86, 90}

func historyThen(final float64) []domain.Event {
	out := make([]domain.Event, 0, len(steady)+1)
	for i, v := range steady {
		out = append(out, ev(domain.StreamPayments, "card-9", time.Duration(i)*time.Second, v, nil))
	}
	out = append(out, ev(domain.StreamPayments, "card-9", time.Duration(len(steady))*time.Second, final, nil))
	return out
}

func TestZScore(t *testing.T) {
	rule := ZScore{K: 3.5, MinSamples: 12, On: []domain.Stream{domain.StreamPayments}}

	t.Run("a wildly larger charge is an outlier", func(t *testing.T) {
		w, last := feed(time.Hour, 0, historyThen(2400)...)
		got := rule.Eval(last, w, now)
		if got == nil {
			t.Fatal("expected an alert")
		}
		if got.Title != "Amount anomaly" {
			t.Errorf("Title = %q", got.Title)
		}
		if z := got.Evidence["z"].(float64); z <= 3.5 {
			t.Errorf("z = %v, expected it past K", z)
		}
		if n := got.Evidence["samples"].(int); n != len(steady) {
			t.Errorf("samples = %d, want %d — the current event must be excluded", n, len(steady))
		}
	})

	t.Run("an ordinary charge is not", func(t *testing.T) {
		w, last := feed(time.Hour, 0, historyThen(91)...)
		if got := rule.Eval(last, w, now); got != nil {
			t.Fatalf("expected no alert, got %+v", got)
		}
	})

	t.Run("too little history to judge", func(t *testing.T) {
		w, last := feed(time.Hour, 0,
			ev(domain.StreamPayments, "c", 0, 90, nil),
			ev(domain.StreamPayments, "c", time.Second, 5000, nil),
		)
		if got := rule.Eval(last, w, now); got != nil {
			t.Fatalf("must withhold judgement below MinSamples, got %+v", got)
		}
	})

	t.Run("a flat history has no scale to measure against", func(t *testing.T) {
		evs := make([]domain.Event, 0, 20)
		for i := 0; i < 19; i++ {
			evs = append(evs, ev(domain.StreamPayments, "c", time.Duration(i)*time.Second, 100, nil))
		}
		evs = append(evs, ev(domain.StreamPayments, "c", 19*time.Second, 9999, nil))
		w, last := feed(time.Hour, 0, evs...)
		if got := rule.Eval(last, w, now); got != nil {
			t.Fatalf("zero variance must not divide, got %+v", got)
		}
	})

	t.Run("an unusually small charge reports the direction", func(t *testing.T) {
		w, last := feed(time.Hour, 0, historyThen(-500)...)
		got := rule.Eval(last, w, now)
		if got == nil {
			t.Fatal("expected an alert")
		}
		if !contains(got.Detail, "below") {
			t.Errorf("Detail = %q, expected it to say below", got.Detail)
		}
	})
}

// ── stall ─────────────────────────────────────────────────────────────────

func TestStall(t *testing.T) {
	rule := Stall{After: 90 * time.Second, On: []domain.Stream{domain.StreamVideo}}

	t.Run("Eval never fires", func(t *testing.T) {
		w, last := feed(5*time.Minute, 0, ev(domain.StreamVideo, "job-1", 0, 30, nil))
		if got := rule.Eval(last, w, now); got != nil {
			t.Errorf("Stall must fire only from Sweep, got %+v", got)
		}
	})

	t.Run("idle past the threshold", func(t *testing.T) {
		w, _ := feed(10*time.Minute, 0, ev(domain.StreamVideo, "job-1", 0, 30, nil))
		got := rule.Sweep(domain.StreamVideo, "job-1", w, base.Add(94*time.Second))
		if got == nil {
			t.Fatal("expected an alert")
		}
		if got.Title != "Job stalled" {
			t.Errorf("Title = %q", got.Title)
		}
		if got.Key != "job-1" || got.Stream != domain.StreamVideo {
			t.Error("Sweep did not carry the stream and key it was given")
		}
	})

	t.Run("recently active", func(t *testing.T) {
		w, _ := feed(10*time.Minute, 0, ev(domain.StreamVideo, "job-1", 0, 30, nil))
		if got := rule.Sweep(domain.StreamVideo, "job-1", w, base.Add(10*time.Second)); got != nil {
			t.Errorf("expected no alert, got %+v", got)
		}
	})

	t.Run("a key with no observations", func(t *testing.T) {
		w := window.New(time.Minute, 0, 8)
		if got := rule.Sweep(domain.StreamVideo, "job-x", w, now); got != nil {
			t.Errorf("expected no alert for an empty window, got %+v", got)
		}
	})
}

// ── threshold ─────────────────────────────────────────────────────────────

func TestThreshold(t *testing.T) {
	// Encoder throughput: alert when it stays under 12 fps for 45 seconds.
	rule := Threshold{Bound: 12, Above: false, Sustained: 45 * time.Second, On: []domain.Stream{domain.StreamVideo}}

	series := func(vals ...float64) []domain.Event {
		out := make([]domain.Event, 0, len(vals))
		for i, v := range vals {
			out = append(out, ev(domain.StreamVideo, "job-2", time.Duration(i*10)*time.Second, v, nil))
		}
		return out
	}

	tests := []struct {
		name    string
		vals    []float64
		wantHit bool
	}{
		{"healthy throughput", []float64{30, 29, 31, 28}, false},
		{"a brief dip is not a breach", []float64{30, 8, 29, 31}, false},
		{"starved for 40s is still short of the sustain", []float64{30, 28, 10, 9, 8, 8, 7}, false},
		{"starved for 50s breaches", []float64{30, 28, 10, 9, 8, 8, 7, 7}, true},
		{"recovery breaks the streak", []float64{10, 9, 8, 7, 30, 9}, false},
		{"current sample above the bound is never a breach", []float64{8, 8, 8, 8, 8, 40}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, last := feed(10*time.Minute, 0, series(tc.vals...)...)
			got := rule.Eval(last, w, now)
			if tc.wantHit != (got != nil) {
				t.Fatalf("alert = %v, want %v (%+v)", got != nil, tc.wantHit, got)
			}
			if got != nil && got.Title != "SLA breach" {
				t.Errorf("Title = %q", got.Title)
			}
		})
	}

	t.Run("the above-bound direction", func(t *testing.T) {
		air := Threshold{Bound: 150, Above: true, Sustained: time.Minute, On: []domain.Stream{domain.StreamSensors}}
		evs := make([]domain.Event, 0, 9)
		for i := 0; i < 9; i++ {
			evs = append(evs, ev(domain.StreamSensors, "grid-A7", time.Duration(i*10)*time.Second, 182, nil))
		}
		w, last := feed(10*time.Minute, 0, evs...)
		got := air.Eval(last, w, now)
		if got == nil {
			t.Fatal("expected an alert")
		}
		if got.Title != "Threshold exceeded" {
			t.Errorf("Title = %q", got.Title)
		}
		if !contains(got.Detail, "above") {
			t.Errorf("Detail = %q, expected it to say above", got.Detail)
		}
	})
}

// ── helpers ───────────────────────────────────────────────────────────────

func TestCompactFloat(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{math.Inf(1), "∞"},
		{math.Inf(-1), "∞"},
		{math.NaN(), "—"},
		{1_627_000, "1.6M"},
		{18_530, "18.5K"},
		{1000, "1K"},
		{182, "182"},
		{89.9, "89.9"},
		{182.4, "182"},
		{0, "0"},
		{-2400, "-2.4K"},
		{-8.5, "-8.5"},
	}
	for _, tc := range tests {
		if got := compactFloat(tc.in); got != tc.want {
			t.Errorf("compactFloat(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCompactDuration(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{420 * time.Millisecond, "420ms"},
		{41 * time.Second, "41s"},
		{1500 * time.Millisecond, "1.5s"},
		{94 * time.Second, "1m34s"},
		{3 * time.Hour, "3h00m"},
	}
	for _, tc := range tests {
		if got := compactDuration(tc.in); got != tc.want {
			t.Errorf("compactDuration(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestTitleForFallsBackToTheRuleName(t *testing.T) {
	if got := titleFor(domain.StreamPayments, "stall"); got != "stall" {
		t.Errorf("unmapped rule = %q, want the raw name", got)
	}
	if got := titleFor(domain.Stream("unknown"), "velocity"); got != "velocity" {
		t.Errorf("unmapped stream = %q, want the raw name", got)
	}
}

func TestSeverityForHandlesAZeroThreshold(t *testing.T) {
	if got := severityFor(1, 0); got != domain.SeverityCritical {
		t.Errorf("a zero threshold cannot be graded; want critical, got %q", got)
	}
}

func TestStatsExcludingLastEdges(t *testing.T) {
	if _, _, n := statsExcludingLast(nil); n != 0 {
		t.Error("nil points should report no samples")
	}
	if _, _, n := statsExcludingLast([]window.Point{{Value: 1}}); n != 0 {
		t.Error("a lone point leaves no history once excluded")
	}
	mean, sd, n := statsExcludingLast([]window.Point{{Value: 4}, {Value: 999}})
	if n != 1 || mean != 4 || sd != 0 {
		t.Errorf("single-sample history = (mean %v, sd %v, n %d), want (4, 0, 1)", mean, sd, n)
	}
}

func TestRatePerSecond(t *testing.T) {
	if got := ratePerSecond(10, 0); got != 0 {
		t.Errorf("a zero span cannot yield a rate; got %v", got)
	}
	if got := ratePerSecond(20, 10*time.Second); got != 2 {
		t.Errorf("ratePerSecond = %v, want 2", got)
	}
}

func TestPlaceOf(t *testing.T) {
	if got := placeOf(nil); got != "?" {
		t.Errorf("placeOf(nil) = %q", got)
	}
	if got := placeOf(&domain.Geo{Lat: 1.5, Lon: -2.25}); got != "1.50,-2.25" {
		t.Errorf("unnamed point = %q, want coordinates", got)
	}
}

func TestDefaultProfilesAreCoherent(t *testing.T) {
	profiles := DefaultProfiles()
	for _, s := range domain.Streams {
		p, ok := profiles[s]
		if !ok {
			t.Fatalf("no profile for stream %q", s)
		}
		if len(p.Rules) == 0 {
			t.Errorf("%s: no rules configured", s)
		}
		if p.Window <= 0 || p.MaxPoints < 2 || p.Cooldown <= 0 {
			t.Errorf("%s: implausible window geometry %+v", s, p)
		}
		for _, r := range p.Rules {
			if !r.AppliesTo(s) {
				t.Errorf("%s: rule %q is configured for a stream it excludes", s, r.Name())
			}
			// Any absence-based rule needs a sweep interval or it never runs.
			if _, isSweeper := r.(Sweeper); isSweeper && p.SweepEvery <= 0 {
				t.Errorf("%s: rule %q needs SweepEvery > 0", s, r.Name())
			}
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}

func TestTrimZero(t *testing.T) {
	tests := []struct{ in, want string }{
		{"1.50", "1.5"},
		{"2.00", "2"},
		{"1.05", "1.05"},
		{"42", "42"}, // no decimal point: returned untouched
		{"", ""},
	}
	for _, tc := range tests {
		if got := trimZero(tc.in); got != tc.want {
			t.Errorf("trimZero(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// The absolute floor must veto an alert that clears K sigma only because the
// key's estimated variance happens to be small.
func TestZScoreMinDeltaFloor(t *testing.T) {
	tests := []struct {
		name    string
		final   float64
		floor   float64
		wantHit bool
	}{
		{"eight sigma with no floor configured", 108, 0, true},
		{"the floor vetoes a deviation large only in sigma", 108, 40, false},
		{"a genuinely large deviation still alerts", 900, 40, true},
		{"a deviation exactly at the floor is not below it", 140, 40, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, last := feed(time.Hour, 0, tightHistory(tc.final)...)
			rule := ZScore{K: 5, MinSamples: 12, MinDelta: tc.floor}
			got := rule.Eval(last, w, now)
			if tc.wantHit != (got != nil) {
				t.Fatalf("alert = %v, want %v (%+v)", got != nil, tc.wantHit, got)
			}
			if got != nil && got.Evidence["minDelta"].(float64) != tc.floor {
				t.Errorf("evidence did not record the floor: %+v", got.Evidence)
			}
		})
	}
}

// tightHistory clusters twelve readings around 100 — a standard deviation of
// roughly one — and then appends the value under test.
func tightHistory(final float64) []domain.Event {
	tight := []float64{99, 100, 101, 100, 99, 101, 100, 100, 101, 99, 100, 100}
	evs := make([]domain.Event, 0, len(tight)+1)
	for i, v := range tight {
		evs = append(evs, ev(domain.StreamSensors, "grid-A1", time.Duration(i)*time.Second, v, nil))
	}
	return append(evs, ev(domain.StreamSensors, "grid-A1", time.Duration(len(tight))*time.Second, final, nil))
}
