// Command processor consumes the event stream, evaluates the detection rules
// against per-key windows, and publishes alerts, dead letters and the state
// changelog.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lauiskk/vigil/internal/engine"
	"github.com/Lauiskk/vigil/internal/httpapi"
	"github.com/Lauiskk/vigil/internal/kafkax"
	"github.com/Lauiskk/vigil/internal/rules"
	"github.com/Lauiskk/vigil/internal/stream"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log); err != nil {
		log.Error("processor stopped", "err", err)
		os.Exit(1)
	}
	log.Info("processor stopped cleanly")
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg := kafkax.ConfigFromEnv("vigil-processor")
	eng := engine.New(rules.DefaultProfiles())

	// The rebalance callbacks need the processor, which needs the client the
	// callbacks are attached to. An atomic pointer breaks the cycle without a
	// data race: the group is not joined until the first poll, by which time
	// this is set.
	var proc atomic.Pointer[stream.Processor]

	cl, err := kafkax.NewClient(cfg,
		kgo.ConsumerGroup(env("KAFKA_GROUP", "vigil-processor")),
		kgo.ConsumeTopics(kafkax.TopicEvents),

		// Offsets are committed by hand, after the changelog is durable.
		// Automatic commits would acknowledge work whose state had not yet
		// been written and turn every rebalance into silent data loss.
		kgo.DisableAutoCommit(),

		// Start from the beginning on a fresh group so a first run has
		// something to show rather than waiting for new traffic.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),

		kgo.OnPartitionsAssigned(func(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
			parts := assigned[kafkax.TopicEvents]
			if len(parts) == 0 {
				return
			}
			log.Info("partitions assigned", "partitions", parts)
			// Blocking here is deliberate: evaluating rules before the window
			// store is loaded would silently miss every stateful anomaly.
			if err := stream.Restore(ctx, cfg, eng, parts, log); err != nil {
				log.Error("state restore failed; continuing with what loaded", "err", err)
			}
		}),

		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
			log.Info("partitions revoked", "partitions", revoked[kafkax.TopicEvents])
			if p := proc.Load(); p != nil {
				p.Flush(ctx)
			}
		}),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	changelog, err := kafkax.NewClient(cfg, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return err
	}
	defer changelog.Close()

	if err := waitForBroker(ctx, cl, log); err != nil {
		return err
	}

	p := stream.NewProcessor(cl, changelog, eng, log)
	proc.Store(p)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.Run(ctx); err != nil {
			log.Error("consume loop", "err", err)
		}
	}()

	err = httpapi.Serve(ctx, env("PROC_ADDR", ":8082"), httpapi.CORS(routes(p, eng)), log)
	wg.Wait()
	return err
}

func waitForBroker(ctx context.Context, cl *kgo.Client, log *slog.Logger) error {
	const attempts = 30
	var err error
	for i := 0; i < attempts; i++ {
		if err = kafkax.EnsureTopics(ctx, cl, log); err == nil {
			return nil
		}
		log.Warn("waiting for the broker", "attempt", i+1, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return err
}

func routes(p *stream.Processor, eng *engine.Engine) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		httpapi.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		es := eng.Stats()
		httpapi.JSON(w, http.StatusOK, map[string]any{
			"consumed":    p.Stats.Consumed.Load(),
			"alerts":      p.Stats.Alerts.Load(),
			"deadLetters": p.Stats.DeadLetters.Load(),
			"snapshots":   p.Stats.Snapshots.Load(),
			"commits":     p.Stats.Commits.Load(),
			"latencyMs":   p.Stats.LatencyMs.Load(),
			"processed":   es.Processed.Load(),
			"suppressed":  es.Suppressed.Load(),
			"late":        es.Late.Load(),
			"invalid":     es.Invalid.Load(),
			"keys":        eng.Store().Len(),
		})
	})
	return mux
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func logLevel() slog.Level {
	if os.Getenv("LOG_LEVEL") == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}
