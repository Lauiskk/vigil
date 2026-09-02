//go:build integration

// Integration tests for the consume loop, run against a real broker.
//
// Behind a build tag because they need one: `go test ./...` on a laptop with
// no Kafka should pass rather than fail for the wrong reason. CI brings up
// the compose broker and runs them with -tags integration.
package stream

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/engine"
	"github.com/Lauiskk/vigil/internal/kafkax"
	"github.com/Lauiskk/vigil/internal/rules"
)

func brokerConfig(t *testing.T, clientID string) kafkax.Config {
	t.Helper()
	if os.Getenv("KAFKA_BROKERS") == "" {
		t.Setenv("KAFKA_BROKERS", "localhost:19092")
	}
	return kafkax.ConfigFromEnv(clientID)
}

func discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// The whole path, through a real broker: produce a fraud sequence, let the
// processor consume it, and read the alert back off the alerts topic.
func TestIntegrationEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg := brokerConfig(t, "vigil-itest")
	log := discard()

	admin, err := kafkax.NewClient(cfg)
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer admin.Close()
	if err := kafkax.EnsureTopics(ctx, admin, log); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	// A group per run, so a previous run's committed offsets cannot make this
	// one silently consume nothing.
	group := fmt.Sprintf("itest-%d", time.Now().UnixNano())

	consumer, err := kafkax.NewClient(cfg,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(kafkax.TopicEvents),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	changelog, err := kafkax.NewClient(cfg, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer changelog.Close()

	// Read alerts from the end before anything is produced, so the assertion
	// cannot pass on an alert left behind by an earlier run.
	alerts, err := kafkax.NewClient(cfg,
		kgo.ConsumeTopics(kafkax.TopicAlerts),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer alerts.Close()
	alerts.ForceMetadataRefresh()

	eng := engine.New(rules.DefaultProfiles())
	proc := NewProcessor(consumer, changelog, eng, log)
	proc.FlushEvery = time.Second

	go func() {
		if err := proc.Run(ctx); err != nil {
			t.Errorf("processor: %v", err)
		}
	}()
	// Let the group settle before producing, or the records land before the
	// consumer owns the partitions and are never seen.
	time.Sleep(6 * time.Second)

	producer, err := kafkax.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	key := fmt.Sprintf("card-itest-%d", time.Now().UnixNano())
	now := time.Now()
	saoPaulo := &domain.Geo{Lat: -23.5505, Lon: -46.6333, Place: "São Paulo"}
	tokyo := &domain.Geo{Lat: 35.6762, Lon: 139.6503, Place: "Tokyo"}

	// The pair is three seconds apart in event time, with the *earlier* one
	// backdated rather than the later one pushed into the future. Stamping an
	// event ahead of the clock is the bug this project already has a
	// regression test for; reintroducing it in a fixture would make the
	// latency assertion below fail for a reason that has nothing to do with
	// the pipeline.
	for i, ev := range []domain.Event{
		{ID: key + "-1", Stream: domain.StreamPayments, Key: key, At: now.Add(-3 * time.Second), Value: 89.90, Unit: "BRL", Geo: saoPaulo},
		{ID: key + "-2", Stream: domain.StreamPayments, Key: key, At: now, Value: 2400, Unit: "BRL", Geo: tokyo},
	} {
		rec, err := kafkax.EventRecord(ev)
		if err != nil {
			t.Fatal(err)
		}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		fetches := alerts.PollRecords(ctx, 100)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			t.Fatal("consumer closed before the alert arrived")
		}
		var found *domain.Alert
		fetches.EachRecord(func(rec *kgo.Record) {
			a, err := kafkax.DecodeAlert(rec)
			if err == nil && a.Key == key && a.Rule == "geovelocity" {
				found = &a
			}
		})
		if found != nil {
			if found.Severity != domain.SeverityCritical {
				t.Errorf("severity = %q, want critical for São Paulo to Tokyo in 3s", found.Severity)
			}
			if d, ok := found.PipelineLatency(); !ok || d < 0 || d > 30*time.Second {
				t.Errorf("pipeline latency = %v (usable=%v), want a small positive duration", d, ok)
			}
			t.Logf("alert arrived: %s — %s (latency %v)", found.Title, found.Detail, found.Latency())
			return
		}
	}
	t.Fatalf("no geovelocity alert for %s within the deadline", key)
}

// A record that cannot be decoded must reach the dead-letter topic rather than
// stopping the consumer or being silently dropped.
func TestIntegrationDeadLetter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg := brokerConfig(t, "vigil-itest-dlq")
	log := discard()

	admin, err := kafkax.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := kafkax.EnsureTopics(ctx, admin, log); err != nil {
		t.Fatal(err)
	}

	group := fmt.Sprintf("itest-dlq-%d", time.Now().UnixNano())
	consumer, err := kafkax.NewClient(cfg,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(kafkax.TopicEvents),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	changelog, err := kafkax.NewClient(cfg, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer changelog.Close()

	dlq, err := kafkax.NewClient(cfg,
		kgo.ConsumeTopics(kafkax.TopicDLQ),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dlq.Close()
	dlq.ForceMetadataRefresh()

	proc := NewProcessor(consumer, changelog, engine.New(rules.DefaultProfiles()), log)
	proc.FlushEvery = time.Second
	go func() { _ = proc.Run(ctx) }()
	time.Sleep(6 * time.Second)

	producer, err := kafkax.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	marker := fmt.Sprintf("poison-%d", time.Now().UnixNano())
	poison := &kgo.Record{Topic: kafkax.TopicEvents, Key: []byte(marker), Value: []byte(`{"this is": not json`)}
	if err := producer.ProduceSync(ctx, poison).FirstErr(); err != nil {
		t.Fatal(err)
	}

	// And a well-formed record behind it: the point is that the pipeline keeps
	// going, not merely that the bad one was captured.
	good, err := kafkax.EventRecord(domain.Event{
		ID: marker + "-ok", Stream: domain.StreamPayments, Key: marker,
		At: time.Now(), Value: 10, Unit: "BRL",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, good).FirstErr(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		fetches := dlq.PollRecords(ctx, 100)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		var seen bool
		fetches.EachRecord(func(rec *kgo.Record) {
			if string(rec.Key) == marker {
				seen = true
			}
		})
		if seen {
			if got := proc.Stats.Consumed.Load(); got < 2 {
				t.Errorf("consumed %d records, want the pipeline to have carried on past the poison one", got)
			}
			return
		}
	}
	t.Fatalf("the malformed record never reached %s", kafkax.TopicDLQ)
}
