package kafkax

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/state"
	"github.com/Lauiskk/vigil/internal/window"
)

var at = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

func TestEventRoundTrip(t *testing.T) {
	ev := domain.Event{
		ID: "pay-1", Stream: domain.StreamPayments, Key: "card-1",
		At: at, Value: 89.90, Unit: "BRL",
		Geo:    &domain.Geo{Lat: -23.55, Lon: -46.63, Place: "São Paulo"},
		Labels: map[string]string{"merchant": "Padaria Aurora"},
	}

	rec, err := EventRecord(ev)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Topic != TopicEvents {
		t.Errorf("topic = %q, want %q", rec.Topic, TopicEvents)
	}
	// Partitioning by entity is what keeps one card's observations in one
	// window store. Getting this wrong would scatter a key across partitions
	// and silently break every stateful rule.
	if string(rec.Key) != ev.Key {
		t.Errorf("record key = %q, want the entity id %q", rec.Key, ev.Key)
	}

	got, err := DecodeEvent(rec)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != ev.ID || got.Value != ev.Value || !got.At.Equal(ev.At) {
		t.Errorf("round trip lost data: %+v", got)
	}
	if got.Geo == nil || got.Geo.Place != "São Paulo" {
		t.Errorf("geo did not survive: %+v", got.Geo)
	}
	if got.Label("merchant") != "Padaria Aurora" {
		t.Errorf("labels did not survive: %+v", got.Labels)
	}
}

// Malformed input must be distinguishable as unprocessable so the consumer
// dead-letters it rather than retrying it forever.
func TestDecodeEventRejectsMalformedJSON(t *testing.T) {
	_, err := DecodeEvent(&kgo.Record{Value: []byte("{not json")})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, domain.ErrInvalidEvent) {
		t.Errorf("error %v is not routable to the DLQ", err)
	}
}

func TestAlertRoundTrip(t *testing.T) {
	a := domain.Alert{
		ID: "a1", Rule: "geovelocity", Stream: domain.StreamPayments, Key: "card-1",
		At: at, DetectedAt: at.Add(42 * time.Millisecond),
		Severity: domain.SeverityCritical, Title: "Impossible travel",
		Detail:   "São Paulo → Tokyo",
		Evidence: map[string]any{"distanceKm": 18530.0},
		EventIDs: []string{"pay-1", "pay-2"},
	}
	rec, err := AlertRecord(a)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeAlert(rec)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != a.ID || got.Severity != a.Severity || got.Latency() != 42*time.Millisecond {
		t.Errorf("round trip lost data: %+v", got)
	}
	if got.Evidence["distanceKm"] != 18530.0 {
		t.Errorf("evidence did not survive: %+v", got.Evidence)
	}
}

func TestStateRoundTripAndTombstone(t *testing.T) {
	key := state.Key{Stream: domain.StreamPayments, ID: "card-1"}
	snap := state.Snapshot{
		Key:     key,
		Points:  []window.Point{{ID: "p1", At: at, Value: 10}},
		Written: at,
	}

	rec, err := StateRecord(snap)
	if err != nil {
		t.Fatal(err)
	}
	// Compaction collapses on the key, so it must identify the entity fully.
	if string(rec.Key) != "payments/card-1" {
		t.Errorf("state key = %q", rec.Key)
	}

	got, err := DecodeState(rec)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != key || len(got.Points) != 1 || got.Points[0].Value != 10 {
		t.Errorf("round trip lost data: %+v", got)
	}

	// A tombstone must decode to an empty snapshot, which Restore treats as a
	// deletion rather than as an empty window.
	tomb, err := DecodeState(StateTombstone(key))
	if err != nil {
		t.Fatal(err)
	}
	if tomb.Key != key || len(tomb.Points) != 0 {
		t.Errorf("tombstone = %+v, want the key with no points", tomb)
	}
}

func TestDecodeStateRejectsMalformedKeys(t *testing.T) {
	if _, err := DecodeState(&kgo.Record{Key: []byte("no-separator")}); err == nil {
		t.Error("expected an error for a key with no stream separator")
	}
	if _, err := DecodeState(&kgo.Record{Key: []byte("payments/card-1"), Value: []byte("{bad")}); err == nil {
		t.Error("expected an error for a malformed snapshot body")
	}
	// An id containing a slash must survive: only the first one separates.
	got, err := DecodeState(&kgo.Record{Key: []byte("sensors/grid/A7")})
	if err != nil {
		t.Fatal(err)
	}
	if got.Key.Stream != domain.StreamSensors || got.Key.ID != "grid/A7" {
		t.Errorf("key = %+v, want the split at the first separator only", got.Key)
	}
}

func TestDeadLetterPreservesTheOriginal(t *testing.T) {
	src := &kgo.Record{
		Topic: TopicEvents, Partition: 1, Offset: 42,
		Key: []byte("card-1"), Value: []byte(`{"broken":`),
	}
	rec := DeadLetterRecord(src, errors.New("malformed JSON"), at)
	if rec.Topic != TopicDLQ {
		t.Errorf("topic = %q, want %q", rec.Topic, TopicDLQ)
	}

	var dl DeadLetter
	if err := json.Unmarshal(rec.Value, &dl); err != nil {
		t.Fatal(err)
	}
	if dl.Offset != 42 || dl.Partition != 1 {
		t.Errorf("lost the source coordinates: %+v", dl)
	}
	// The original bytes are the whole point: without them the dead letter
	// says something failed but not what.
	if dl.Payload != `{"broken":` {
		t.Errorf("payload = %q, want the original bytes", dl.Payload)
	}
	if dl.Reason != "malformed JSON" {
		t.Errorf("reason = %q", dl.Reason)
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", " a:9092 , b:9092 ,")
	t.Setenv("KAFKA_USER", "avnadmin")
	t.Setenv("KAFKA_PASSWORD", "secret")

	c := ConfigFromEnv("test")
	if len(c.Brokers) != 2 || c.Brokers[0] != "a:9092" || c.Brokers[1] != "b:9092" {
		t.Errorf("brokers = %#v, want the list trimmed and the empty entry dropped", c.Brokers)
	}
	// Credentials without an explicit TLS flag is the Aiven case; failing the
	// handshake instead would be a confusing way to learn that.
	if !c.TLS {
		t.Error("SASL credentials should imply TLS")
	}
	if _, err := c.Options(); err != nil {
		t.Errorf("Options: %v", err)
	}
}

func TestConfigRejectsAnEmptyBrokerList(t *testing.T) {
	if _, err := (Config{}).Options(); err == nil {
		t.Error("expected an error with no brokers configured")
	}
}

func TestConfigFromEnvDefaultsToLocalRedpanda(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_USER", "")
	c := ConfigFromEnv("test")
	if len(c.Brokers) != 1 || c.Brokers[0] != "localhost:19092" {
		t.Errorf("brokers = %#v, want the compose default so a fresh clone runs unconfigured", c.Brokers)
	}
	if c.TLS {
		t.Error("the local broker should not require TLS")
	}
}
