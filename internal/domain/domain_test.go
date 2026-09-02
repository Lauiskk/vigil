package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

var (
	saoPaulo = Geo{Lat: -23.5505, Lon: -46.6333, Place: "São Paulo"}
	tokyo    = Geo{Lat: 35.6762, Lon: 139.6503, Place: "Tokyo"}
	rio      = Geo{Lat: -22.9068, Lon: -43.1729, Place: "Rio de Janeiro"}
)

func TestGeoDistanceKm(t *testing.T) {
	tests := []struct {
		name    string
		a, b    Geo
		wantKm  float64
		tolerKm float64
	}{
		{"same point is zero", saoPaulo, saoPaulo, 0, 0.001},
		{"são paulo to rio", saoPaulo, rio, 357, 5},
		{"são paulo to tokyo", saoPaulo, tokyo, 18530, 60},
		{"symmetric", tokyo, saoPaulo, 18530, 60},
		{"antipodal does not NaN", Geo{Lat: 0, Lon: 0}, Geo{Lat: 0, Lon: 180}, 20015, 10},
		{"pole to pole", Geo{Lat: 90}, Geo{Lat: -90}, 20015, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.a.DistanceKm(tc.b)
			if math.IsNaN(got) {
				t.Fatalf("DistanceKm returned NaN")
			}
			if math.Abs(got-tc.wantKm) > tc.tolerKm {
				t.Errorf("DistanceKm = %.1f km, want %.1f ±%.1f", got, tc.wantKm, tc.tolerKm)
			}
		})
	}
}

func TestEventValidate(t *testing.T) {
	ok := Event{ID: "e1", Stream: StreamPayments, Key: "card-1", At: time.Now(), Value: 10}

	tests := []struct {
		name    string
		mutate  func(*Event)
		wantErr bool
	}{
		{"valid", func(*Event) {}, false},
		{"empty id", func(e *Event) { e.ID = "" }, true},
		{"empty key", func(e *Event) { e.Key = "" }, true},
		{"zero time", func(e *Event) { e.At = time.Time{} }, true},
		{"unknown stream", func(e *Event) { e.Stream = "telemetry" }, true},
		{"zero value is legal", func(e *Event) { e.Value = 0 }, false},
		{"negative value is legal", func(e *Event) { e.Value = -1 }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := ok
			tc.mutate(&e)
			err := e.Validate()
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				// Every rejection must be routable to the DLQ by sentinel.
				if !errors.Is(err, ErrInvalidEvent) {
					t.Errorf("error %v does not wrap ErrInvalidEvent", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestStreamValid(t *testing.T) {
	for _, s := range Streams {
		if !s.Valid() {
			t.Errorf("canonical stream %q reports invalid", s)
		}
	}
	if Stream("").Valid() || Stream("nope").Valid() {
		t.Error("unknown stream reported valid")
	}
}

func TestEventLabel(t *testing.T) {
	e := Event{Labels: map[string]string{"merchant": "Padaria"}}
	if got := e.Label("merchant"); got != "Padaria" {
		t.Errorf("Label = %q, want Padaria", got)
	}
	if got := e.Label("absent"); got != "" {
		t.Errorf("missing label = %q, want empty", got)
	}
	var zero Event // nil map must not panic
	if got := zero.Label("x"); got != "" {
		t.Errorf("label on nil map = %q, want empty", got)
	}
}

func TestSeverityRank(t *testing.T) {
	if SeverityCritical.Rank() <= SeverityWarn.Rank() || SeverityWarn.Rank() <= SeverityInfo.Rank() {
		t.Error("severity ranks are not strictly ordered")
	}
	if Severity("unset").Rank() != SeverityInfo.Rank() {
		t.Error("unknown severity should rank as info")
	}
}

func TestAlertLatency(t *testing.T) {
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	a := Alert{At: at, DetectedAt: at.Add(42 * time.Millisecond)}
	if got := a.Latency(); got != 42*time.Millisecond {
		t.Errorf("Latency = %v, want 42ms", got)
	}
}

func TestEventTerminal(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"done", true},
		{"failed", true},
		{"cancelled", true},
		{"running", false},
		{"queued", false},
		{"", false},
	}
	for _, tc := range tests {
		t.Run("status="+tc.status, func(t *testing.T) {
			e := Event{Labels: map[string]string{LabelStatus: tc.status}}
			if got := e.Terminal(); got != tc.want {
				t.Errorf("Terminal = %v, want %v", got, tc.want)
			}
		})
	}
	var noLabels Event
	if noLabels.Terminal() {
		t.Error("an event with no labels is not terminal")
	}
}
