package kafkax

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/state"
)

// JSON, not Avro or protobuf, and the choice is deliberate.
//
// The dashboard reads these records through a browser, the dead-letter topic
// is meant to be inspectable with kcat by a human at three in the morning, and
// the whole pipeline moves a few thousand records a second — three orders of
// magnitude below where the encoding cost would matter. A schema registry
// would buy compatibility guarantees this project does not need and cost a
// dependency it would otherwise not have.

// EventRecord renders an event for the input topic, partitioned by entity so
// every observation for one card lands on the same partition and therefore in
// the same window store.
func EventRecord(ev domain.Event) (*kgo.Record, error) {
	body, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("kafkax: encode event %s: %w", ev.ID, err)
	}
	return &kgo.Record{
		Topic:     TopicEvents,
		Key:       []byte(ev.Key),
		Value:     body,
		Timestamp: ev.At,
	}, nil
}

// DecodeEvent parses an input record.
func DecodeEvent(r *kgo.Record) (domain.Event, error) {
	var ev domain.Event
	if err := json.Unmarshal(r.Value, &ev); err != nil {
		return domain.Event{}, fmt.Errorf("%w: malformed JSON: %s", domain.ErrInvalidEvent, err)
	}
	return ev, nil
}

// AlertRecord renders an alert, keyed by entity so a consumer reading alerts
// sees one key's history in order.
func AlertRecord(a domain.Alert) (*kgo.Record, error) {
	body, err := json.Marshal(a)
	if err != nil {
		return nil, fmt.Errorf("kafkax: encode alert %s: %w", a.ID, err)
	}
	return &kgo.Record{
		Topic:     TopicAlerts,
		Key:       []byte(a.Key),
		Value:     body,
		Timestamp: a.DetectedAt,
	}, nil
}

// DecodeAlert parses an alert record.
func DecodeAlert(r *kgo.Record) (domain.Alert, error) {
	var a domain.Alert
	if err := json.Unmarshal(r.Value, &a); err != nil {
		return domain.Alert{}, fmt.Errorf("kafkax: decode alert: %w", err)
	}
	return a, nil
}

// StateRecord renders a window-store snapshot for the compacted changelog.
//
// The key is stream and entity together, which is what compaction collapses
// on: only the most recent snapshot per key survives, so replaying the topic
// rebuilds exactly the current state.
func StateRecord(snap state.Snapshot) (*kgo.Record, error) {
	body, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("kafkax: encode snapshot %s: %w", snap.Key.ID, err)
	}
	return &kgo.Record{
		Topic:     TopicState,
		Key:       stateKey(snap.Key),
		Value:     body,
		Timestamp: snap.Written,
	}, nil
}

// StateTombstone tells compaction to forget a key entirely.
func StateTombstone(k state.Key) *kgo.Record {
	return &kgo.Record{Topic: TopicState, Key: stateKey(k), Value: nil}
}

// DecodeState parses a changelog record. A nil value is a tombstone and
// decodes to an empty snapshot, which state.Restore treats as a deletion.
func DecodeState(r *kgo.Record) (state.Snapshot, error) {
	if len(r.Value) == 0 {
		k, err := parseStateKey(r.Key)
		if err != nil {
			return state.Snapshot{}, err
		}
		return state.Snapshot{Key: k}, nil
	}
	var snap state.Snapshot
	if err := json.Unmarshal(r.Value, &snap); err != nil {
		return state.Snapshot{}, fmt.Errorf("kafkax: decode snapshot: %w", err)
	}
	return snap, nil
}

func stateKey(k state.Key) []byte { return []byte(string(k.Stream) + "/" + k.ID) }

func parseStateKey(b []byte) (state.Key, error) {
	s := string(b)
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return state.Key{Stream: domain.Stream(s[:i]), ID: s[i+1:]}, nil
		}
	}
	return state.Key{}, fmt.Errorf("kafkax: malformed state key %q", s)
}

// DeadLetter wraps a record that could not be processed, preserving the
// original bytes alongside the reason so the failure can be diagnosed without
// guessing at what arrived.
type DeadLetter struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Key       string    `json:"key"`
	Reason    string    `json:"reason"`
	At        time.Time `json:"at"`
	Payload   string    `json:"payload"`
}

// DeadLetterRecord builds the dead-letter entry for a failed record.
func DeadLetterRecord(src *kgo.Record, reason error, now time.Time) *kgo.Record {
	dl := DeadLetter{
		Topic:     src.Topic,
		Partition: src.Partition,
		Offset:    src.Offset,
		Key:       string(src.Key),
		Reason:    reason.Error(),
		At:        now,
		Payload:   string(src.Value),
	}
	body, err := json.Marshal(dl)
	if err != nil {
		// A DeadLetter contains only strings and numbers, so this cannot
		// fail — but losing the record because the wrapper would not encode
		// is the one outcome worse than the original failure.
		body = []byte(fmt.Sprintf(`{"reason":%q,"payload":"<unencodable>"}`, reason.Error()))
	}
	return &kgo.Record{Topic: TopicDLQ, Key: src.Key, Value: body}
}
