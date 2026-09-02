package httpapi

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// backlog is how many recent messages a new subscriber receives on connect.
// Without it a visitor opening the dashboard sees an empty panel until the
// next alert happens to fire, which on a quiet stream can be a long wait.
const backlog = 40

// subscriberBuffer is how far one connection may fall behind before it starts
// losing messages.
const subscriberBuffer = 64

// Broker fans one stream of messages out to every connected browser.
//
// A slow subscriber is dropped from, not waited on. The alternative is
// letting one stalled browser tab apply backpressure all the way up into the
// Kafka consumer, which is a real way to turn a spectator into an outage.
type Broker struct {
	mu      sync.RWMutex
	subs    map[chan []byte]struct{}
	history [][]byte

	dropped uint64
}

func NewBroker() *Broker {
	return &Broker{subs: make(map[chan []byte]struct{})}
}

// Subscribe returns a channel of messages and a function to release it.
func (b *Broker) Subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, subscriberBuffer)

	b.mu.Lock()
	// Seed with recent history before registering, so the new subscriber
	// cannot receive a live message ahead of the backlog it precedes.
	for _, msg := range b.history {
		select {
		case ch <- msg:
		default:
		}
	}
	b.subs[ch] = struct{}{}
	b.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, ch)
			b.mu.Unlock()
			close(ch)
		})
	}
}

// Publish sends to every subscriber, skipping any that cannot keep up.
func (b *Broker) Publish(msg []byte) {
	b.mu.Lock()
	b.history = append(b.history, msg)
	if len(b.history) > backlog {
		b.history = append(b.history[:0], b.history[len(b.history)-backlog:]...)
	}
	for ch := range b.subs {
		select {
		case ch <- msg:
		default:
			b.dropped++
		}
	}
	b.mu.Unlock()
}

// Stats reports subscriber count and how many messages have been dropped for
// slowness, which is worth surfacing rather than hiding.
func (b *Broker) Stats() (subscribers int, dropped uint64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs), b.dropped
}

// Handler serves the stream as server-sent events.
//
// SSE rather than WebSocket: the traffic is one-way, it survives proxies that
// mangle upgrades, it reconnects on its own, and it is about fifteen lines of
// browser code with no library.
func (b *Broker) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			Error(w, http.StatusInternalServerError, "streaming unsupported")
			return
		}

		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		// Tell nginx and friends not to buffer, which would otherwise hold
		// events until a buffer fills and make a live feed look broken.
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		msgs, release := b.Subscribe()
		defer release()

		// Comment frames keep intermediaries from reaping an idle connection.
		keepalive := time.NewTicker(20 * time.Second)
		defer keepalive.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
					return
				}
				flusher.Flush()
			case <-keepalive.C:
				if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}
