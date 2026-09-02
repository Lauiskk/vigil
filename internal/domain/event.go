// Package domain holds the vocabulary the rest of the pipeline is written in:
// the event that arrives, the alert that leaves, and the geography between
// them. Nothing here performs I/O or knows that Kafka exists — which is what
// lets the rule engine be tested without a broker.
package domain

import (
	"errors"
	"fmt"
	"time"
)

// Stream identifies which simulated source produced an event. One pipeline
// carries all three; the rules are generic and each stream simply enables a
// different subset of them.
type Stream string

const (
	StreamPayments Stream = "payments"
	StreamVideo    Stream = "video"
	StreamSensors  Stream = "sensors"
)

// Streams is the canonical order used by the dashboard's stream selector.
var Streams = []Stream{StreamPayments, StreamVideo, StreamSensors}

func (s Stream) Valid() bool {
	switch s {
	case StreamPayments, StreamVideo, StreamSensors:
		return true
	}
	return false
}

// Event is the single record type on `vigil.events`. Every stream flattens
// into it, which is what keeps the topic count inside Aiven's five-topic free
// tier and lets one rule engine serve all three domains.
//
// Value and Unit are deliberately untyped-by-domain: BRL for a payment,
// percent complete for a transcode job, µg/m³ for an air sensor. The rules
// care about the shape of the number over time, not what it measures.
type Event struct {
	ID     string    `json:"id"`
	Stream Stream    `json:"stream"`
	Key    string    `json:"key"` // partition key: card, job or sensor id
	At     time.Time `json:"at"`  // event time, assigned by the producer
	Value  float64   `json:"value"`
	Unit   string    `json:"unit"`

	// Geo is present for payments and sensors, nil for video jobs.
	Geo *Geo `json:"geo,omitempty"`

	// Labels carry stream-specific context the rules never read but the
	// dashboard renders — merchant name, codec, sensor model.
	Labels map[string]string `json:"labels,omitempty"`
}

// ErrInvalidEvent is the sentinel every malformed-record path returns. The
// consumer routes these to the dead-letter topic rather than retrying, because
// a record that fails validation will fail it again forever.
var ErrInvalidEvent = errors.New("invalid event")

// Validate rejects records that cannot be processed. It is intentionally
// strict: a silent default here would show up as a phantom alert later, and a
// pipeline that quietly invents data is worse than one that rejects it.
func (e Event) Validate() error {
	switch {
	case e.ID == "":
		return fmt.Errorf("%w: empty id", ErrInvalidEvent)
	case !e.Stream.Valid():
		return fmt.Errorf("%w: unknown stream %q", ErrInvalidEvent, e.Stream)
	case e.Key == "":
		return fmt.Errorf("%w: empty key", ErrInvalidEvent)
	case e.At.IsZero():
		return fmt.Errorf("%w: zero event time", ErrInvalidEvent)
	}
	return nil
}

// Label reads a label without the two-value comma-ok dance at every call site.
func (e Event) Label(k string) string { return e.Labels[k] }

// LabelStatus is the label a producer sets to describe an entity's lifecycle.
const LabelStatus = "status"

// Terminal reports whether this event ends its key's lifecycle.
//
// It matters for the absence-based rules: a transcode job that has finished
// legitimately stops producing events, and without this the stall rule would
// eventually fire on every job that ever succeeded.
func (e Event) Terminal() bool {
	switch e.Label(LabelStatus) {
	case "done", "failed", "cancelled":
		return true
	}
	return false
}
