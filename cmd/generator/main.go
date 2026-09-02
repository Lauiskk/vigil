// Command generator produces the traffic the pipeline runs on and exposes the
// controls the dashboard drives: change the rate, surge, or inject a fault.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/httpapi"
	"github.com/Lauiskk/vigil/internal/kafkax"
	"github.com/Lauiskk/vigil/internal/sim"
)

// tick is the production granularity. Fine enough that a surge ramps
// smoothly, coarse enough that the loop is not the bottleneck.
const tick = 100 * time.Millisecond

// Resting rates, in events per second.
//
// Only payments surges. A city does not grow more air-quality sensors because
// card traffic spiked, and a transcode farm's output is governed by how often
// jobs report rather than by demand — modelling it otherwise would turn the
// surge button into a flood of meaningless alerts on the other two streams.
var restingRate = map[domain.Stream]float64{
	domain.StreamPayments: 6,
	domain.StreamVideo:    2,
	domain.StreamSensors:  6,
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log); err != nil {
		log.Error("generator stopped", "err", err)
		os.Exit(1)
	}
	log.Info("generator stopped cleanly")
}

func run(ctx context.Context, log *slog.Logger) error {
	cl, err := kafkax.NewClient(kafkax.ConfigFromEnv("vigil-generator"))
	if err != nil {
		return err
	}
	defer cl.Close()

	if err := ensureTopics(ctx, cl, log); err != nil {
		return err
	}

	c := newController()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.produce(ctx, cl, log)
	}()

	err = httpapi.Serve(ctx, env("GEN_ADDR", ":8081"), httpapi.CORS(c.routes()), log)
	wg.Wait()
	return err
}

// ensureTopics retries: on a cold `docker compose up` the broker is often not
// ready when the first service is, and exiting would leave compose restarting
// a container that was never actually misconfigured.
func ensureTopics(ctx context.Context, cl *kgo.Client, log *slog.Logger) error {
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

// controller owns the sources and the current rates.
type controller struct {
	mu      sync.Mutex
	sources map[domain.Stream]sim.Source
	pacers  map[domain.Stream]*sim.Pacer
	rates   map[domain.Stream]float64

	surgeRate  float64
	surgeUntil time.Time

	produced atomic.Uint64
	failed   atomic.Uint64
}

func newController() *controller {
	seed := time.Now().UnixNano()
	c := &controller{
		sources: map[domain.Stream]sim.Source{
			domain.StreamPayments: sim.NewPayments(seed),
			domain.StreamVideo:    sim.NewVideo(seed + 1),
			domain.StreamSensors:  sim.NewSensors(seed + 2),
		},
		pacers: map[domain.Stream]*sim.Pacer{},
		rates:  map[domain.Stream]float64{},
	}
	for s, r := range restingRate {
		c.rates[s] = r
		c.pacers[s] = &sim.Pacer{}
	}
	return c
}

// rateFor returns the effective rate, applying any surge in force.
func (c *controller) rateFor(s domain.Stream, now time.Time) float64 {
	if s == domain.StreamPayments && now.Before(c.surgeUntil) {
		return c.surgeRate
	}
	return c.rates[s]
}

func (c *controller) produce(ctx context.Context, cl *kgo.Client, log *slog.Logger) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush what is already batched; losing the last few hundred
			// records on shutdown would show up as a phantom stall.
			flush, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err := cl.Flush(flush); err != nil {
				log.Warn("flush on shutdown was incomplete", "err", err)
			}
			return

		case now := <-ticker.C:
			for _, ev := range c.collect(now) {
				rec, err := kafkax.EventRecord(ev)
				if err != nil {
					log.Error("encode", "err", err)
					c.failed.Add(1)
					continue
				}
				cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
					if err != nil {
						// Not fatal and not retried here: franz-go already
						// retries internally, so an error at this point means
						// the record is genuinely lost and the counter is
						// what the dashboard should show.
						if !errors.Is(err, context.Canceled) {
							c.failed.Add(1)
						}
						return
					}
					c.produced.Add(1)
				})
			}
		}
	}
}

// collect gathers this tick's events from every source.
func (c *controller) collect(now time.Time) []domain.Event {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []domain.Event
	for s, src := range c.sources {
		n := c.pacers[s].Take(c.rateFor(s, now), tick)
		out = append(out, src.Tick(now, n)...)
	}
	return out
}

func (c *controller) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		httpapi.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/state", c.handleState)
	mux.HandleFunc("POST /api/inject", c.handleInject)
	mux.HandleFunc("POST /api/surge", c.handleSurge)
	return mux
}

func (c *controller) handleState(w http.ResponseWriter, _ *http.Request) {
	now := time.Now()

	c.mu.Lock()
	rates := make(map[string]float64, len(c.sources))
	faults := make(map[string][]sim.Fault, len(c.sources))
	for s, src := range c.sources {
		rates[string(s)] = c.rateFor(s, now)
		faults[string(s)] = src.Faults()
	}
	surging := now.Before(c.surgeUntil)
	surgeLeft := 0.0
	if surging {
		surgeLeft = c.surgeUntil.Sub(now).Seconds()
	}
	c.mu.Unlock()

	httpapi.JSON(w, http.StatusOK, map[string]any{
		"rates":            rates,
		"faults":           faults,
		"surging":          surging,
		"surgeSecondsLeft": surgeLeft,
		"produced":         c.produced.Load(),
		"failed":           c.failed.Load(),
	})
}

func (c *controller) handleInject(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Stream string `json:"stream"`
		Fault  string `json:"fault"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil {
		httpapi.Error(w, http.StatusBadRequest, "malformed request body")
		return
	}

	c.mu.Lock()
	src, ok := c.sources[domain.Stream(req.Stream)]
	c.mu.Unlock()
	if !ok {
		httpapi.Error(w, http.StatusBadRequest, "unknown stream "+strconv.Quote(req.Stream))
		return
	}

	key, err := src.Inject(sim.Fault(req.Fault), time.Now())
	if err != nil {
		httpapi.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	httpapi.JSON(w, http.StatusAccepted, map[string]string{
		"stream": req.Stream, "fault": req.Fault, "key": key,
	})
}

func (c *controller) handleSurge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PerSecond float64 `json:"perSecond"`
		Seconds   float64 `json:"seconds"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil {
		httpapi.Error(w, http.StatusBadRequest, "malformed request body")
		return
	}
	if req.PerSecond <= 0 {
		req.PerSecond = 2000
	}
	if req.Seconds <= 0 {
		req.Seconds = 20
	}
	// Bounded because this endpoint is reachable by anyone who opens the
	// dashboard. The ceiling is what the free-tier broker will take, not what
	// the generator could produce.
	req.PerSecond = min(req.PerSecond, 5000)
	req.Seconds = min(req.Seconds, 60)

	c.mu.Lock()
	c.surgeRate = req.PerSecond
	c.surgeUntil = time.Now().Add(time.Duration(req.Seconds * float64(time.Second)))
	c.mu.Unlock()

	httpapi.JSON(w, http.StatusAccepted, map[string]any{
		"perSecond": req.PerSecond, "seconds": req.Seconds,
	})
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
