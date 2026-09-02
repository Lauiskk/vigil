// Command gateway is the public face of the pipeline: it consumes the alert
// stream, fans it out to browsers over server-sent events, serves the
// dashboard, and forwards the dashboard's controls to the generator.
package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/httpapi"
	"github.com/Lauiskk/vigil/internal/kafkax"
	"github.com/Lauiskk/vigil/internal/telemetry"
)

//go:embed web
var web embed.FS

// recentAlerts is how many alerts the gateway keeps for a page load. The SSE
// broker carries its own backlog; this is for clients that poll instead.
const recentAlerts = 60

// browserEventBudget caps how many raw events a second are pushed to browsers.
// During a surge the pipeline moves thousands a second, and forwarding them
// all would achieve nothing except making the tab unresponsive — the sample is
// there to show motion, and the counters carry the real numbers.
const browserEventBudget = 15

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log); err != nil {
		log.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
	log.Info("gateway stopped cleanly")
}

type gateway struct {
	broker *httpapi.Broker
	log    *slog.Logger

	generator string
	client    *http.Client

	eventRate *telemetry.Rate
	alertRate *telemetry.Rate
	endToEnd  *telemetry.Latency
	detection *telemetry.Latency

	mu     sync.Mutex
	recent []domain.Alert

	seen          atomic.Uint64
	sampleEvery   atomic.Int64
	absenceAlerts atomic.Uint64
	clockSkewed   atomic.Uint64
	windows       atomic.Uint64
	startedAt     time.Time
}

func run(ctx context.Context, log *slog.Logger) error {
	now := time.Now()
	g := &gateway{
		broker:    httpapi.NewBroker(),
		log:       log,
		generator: env("GENERATOR_URL", "http://localhost:8081"),
		client:    &http.Client{Timeout: 5 * time.Second},
		eventRate: telemetry.NewRate(now),
		alertRate: telemetry.NewRate(now),
		endToEnd:  telemetry.NewLatency(2000),
		detection: telemetry.NewLatency(2000),
		startedAt: now,
	}
	g.sampleEvery.Store(1)

	cl, err := kafkax.NewClient(kafkax.ConfigFromEnv("vigil-gateway"),
		kgo.ConsumeTopics(kafkax.TopicAlerts, kafkax.TopicEvents, kafkax.TopicMetrics),
		// No consumer group and no committed offsets: the dashboard shows
		// what is happening now. Resuming where a previous viewer left off
		// would replay stale traffic as though it were live.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); g.consume(ctx, cl) }()
	go func() { defer wg.Done(); g.publishStats(ctx) }()

	err = httpapi.Serve(ctx, env("GATEWAY_ADDR", ":8080"), httpapi.CORS(g.routes()), log)
	wg.Wait()
	return err
}

func (g *gateway) consume(ctx context.Context, cl *kgo.Client) {
	for {
		fetches := cl.PollRecords(ctx, 2000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				g.log.Warn("fetch", "topic", topic, "partition", partition, "err", err)
			}
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			switch rec.Topic {
			case kafkax.TopicAlerts:
				g.onAlert(rec)
			case kafkax.TopicEvents:
				g.onEvent(rec)
			case kafkax.TopicMetrics:
				g.onWindow(rec)
			}
		})
	}
}

func (g *gateway) onAlert(rec *kgo.Record) {
	a, err := kafkax.DecodeAlert(rec)
	if err != nil {
		g.log.Warn("undecodable alert", "offset", rec.Offset, "err", err)
		return
	}
	g.alertRate.Add(1)

	// Two different measurements, and the distinction is the interesting one.
	// Detection latency is the engine's own work. End-to-end includes both
	// Kafka hops and is what a person actually waits for.
	//
	// Absence alerts and clock-skewed ones are excluded rather than clamped:
	// a stall alert's interval is minutes of silence by design, and a
	// negative one means the producer's clock is ahead. Both are real, and
	// neither is latency. They are counted so the exclusion is visible.
	if d, ok := a.PipelineLatency(); ok {
		g.detection.Observe(d)
		g.endToEnd.Observe(time.Since(a.At))
	} else if a.Absence {
		g.absenceAlerts.Add(1)
	} else {
		g.clockSkewed.Add(1)
		g.log.Warn("alert timestamps disagree with this host's clock",
			"rule", a.Rule, "key", a.Key, "skew", d.String())
	}

	g.mu.Lock()
	g.recent = append(g.recent, a)
	if len(g.recent) > recentAlerts {
		g.recent = append(g.recent[:0], g.recent[len(g.recent)-recentAlerts:]...)
	}
	g.mu.Unlock()

	g.publish("alert", map[string]any{"alert": a})
}

func (g *gateway) onEvent(rec *kgo.Record) {
	g.eventRate.Add(1)

	// Sample rather than forward. The counters are exact; the feed is a view.
	n := g.seen.Add(1)
	every := g.sampleEvery.Load()
	if every > 1 && n%uint64(every) != 0 {
		return
	}
	ev, err := kafkax.DecodeEvent(rec)
	if err != nil {
		return // the processor dead-letters these; the dashboard just skips them
	}
	g.publish("event", map[string]any{"event": ev})
}

// onWindow forwards a closed window from the Kafka Streams topology.
//
// The record is relayed rather than re-parsed: it is already the JSON the
// dashboard wants, and decoding it here only to encode it again would create
// a second place where the aggregate's shape is defined.
func (g *gateway) onWindow(rec *kgo.Record) {
	if len(rec.Value) == 0 {
		return
	}
	g.windows.Add(1)
	g.broker.Publish(append(append([]byte(`{"type":"window","window":`), rec.Value...), '}'))
}

func (g *gateway) publish(kind string, payload map[string]any) {
	payload["type"] = kind
	body, err := json.Marshal(payload)
	if err != nil {
		g.log.Error("encode sse payload", "type", kind, "err", err)
		return
	}
	g.broker.Publish(body)
}

// publishStats rolls the rate counters once a second and pushes a summary
// frame, which is also what re-tunes the event sampling for the next second.
func (g *gateway) publishStats(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			eventsPerSecond := g.eventRate.Roll(now)
			g.alertRate.Roll(now)

			every := int64(eventsPerSecond / browserEventBudget)
			if every < 1 {
				every = 1
			}
			g.sampleEvery.Store(every)

			g.publish("stats", map[string]any{"stats": g.snapshot()})
		}
	}
}

func (g *gateway) snapshot() map[string]any {
	eventsPerSecond, events := g.eventRate.Per()
	alertsPerSecond, alerts := g.alertRate.Per()
	subscribers, dropped := g.broker.Stats()

	return map[string]any{
		"eventsPerSecond": round1(eventsPerSecond),
		"alertsPerSecond": round1(alertsPerSecond),
		"events":          events,
		"alerts":          alerts,
		"endToEnd":        g.endToEnd.Snapshot(),
		"detection":       g.detection.Snapshot(),
		"sampleEvery":     g.sampleEvery.Load(),
		"subscribers":     subscribers,
		"droppedToSlow":   dropped,
		"absenceAlerts":   g.absenceAlerts.Load(),
		"windows":         g.windows.Load(),
		"clockSkewed":     g.clockSkewed.Load(),
		"uptimeSeconds":   int(time.Since(g.startedAt).Seconds()),
	}
}

func (g *gateway) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		httpapi.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/stream", g.broker.Handler())
	mux.HandleFunc("GET /api/stats", func(w http.ResponseWriter, _ *http.Request) {
		httpapi.JSON(w, http.StatusOK, g.snapshot())
	})
	mux.HandleFunc("GET /api/alerts", func(w http.ResponseWriter, _ *http.Request) {
		g.mu.Lock()
		out := make([]domain.Alert, len(g.recent))
		copy(out, g.recent)
		g.mu.Unlock()
		httpapi.JSON(w, http.StatusOK, map[string]any{"alerts": out})
	})

	// The generator is not exposed publicly; the gateway is the only way in,
	// which keeps the control surface in one place.
	mux.HandleFunc("POST /api/inject", g.proxy("/api/inject"))
	mux.HandleFunc("POST /api/surge", g.proxy("/api/surge"))
	mux.HandleFunc("GET /api/generator", g.proxyGet("/api/state"))

	// The dashboard is embedded in the binary so the container has no asset
	// volume and the service has nothing to mount.
	assets, err := fs.Sub(web, "web")
	if err != nil {
		// Impossible unless the embed directive and the directory disagree,
		// which would be a build-time mistake.
		panic(err)
	}
	mux.Handle("GET /", http.FileServerFS(assets))
	return mux
}

func (g *gateway) proxy(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<10))
		if err != nil {
			httpapi.Error(w, http.StatusBadRequest, "request body too large")
			return
		}
		g.forward(w, r.Context(), http.MethodPost, path, body)
	}
}

func (g *gateway) proxyGet(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		g.forward(w, r.Context(), http.MethodGet, path, nil)
	}
}

func (g *gateway) forward(w http.ResponseWriter, ctx context.Context, method, path string, body []byte) {
	req, err := http.NewRequestWithContext(ctx, method, g.generator+path, bytes.NewReader(body))
	if err != nil {
		httpapi.Error(w, http.StatusInternalServerError, "could not build the upstream request")
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(req)
	if err != nil {
		// The generator being unreachable is a normal degraded state, not a
		// crash: the dashboard keeps showing the live feed and disables the
		// controls it cannot honour.
		g.log.Warn("generator unreachable", "path", path, "err", err)
		httpapi.Error(w, http.StatusServiceUnavailable, "the generator is not reachable")
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, io.LimitReader(resp.Body, 1<<20)); err != nil {
		g.log.Debug("proxy copy", "err", err)
	}
}

func round1(v float64) float64 { return float64(int64(v*10+0.5)) / 10 }

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
