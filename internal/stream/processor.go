// Package stream is the Kafka-facing half of the processor: the consume loop,
// the dead-letter path, and the changelog that makes a rebalance survivable.
package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lauiskk/vigil/internal/domain"
	"github.com/Lauiskk/vigil/internal/engine"
	"github.com/Lauiskk/vigil/internal/kafkax"
	"github.com/Lauiskk/vigil/internal/state"
)

// Processor consumes events, runs them through the engine, and publishes the
// alerts, dead letters and state changelog that result.
type Processor struct {
	// cl consumes events and produces alerts and dead letters, partitioned
	// the ordinary way.
	cl *kgo.Client

	// changelog produces state snapshots on a client configured with the
	// manual partitioner, so a snapshot lands on the same partition as the
	// events it summarises. Restore reads the changelog partition matching
	// each assigned event partition, so the two must agree.
	//
	// A separate client because franz-go's partitioner is a client-wide
	// setting. The alternative — keying snapshots by entity id alone so the
	// default partitioner co-locates them — would make two streams sharing an
	// id silently overwrite each other under compaction.
	changelog *kgo.Client

	engine *engine.Engine
	log    *slog.Logger

	// FlushEvery is how often the changelog is written and offsets committed.
	//
	// The order is what makes recovery exact rather than approximate: the
	// changelog is flushed *before* offsets are committed, so a restored
	// consumer replays every input record whose effect had not yet reached
	// the changelog. Committing first would lose that window silently.
	FlushEvery time.Duration

	// SweepEvery is how often absence-based rules are evaluated.
	SweepEvery time.Duration

	mu    sync.Mutex
	dirty map[state.Key]int32 // keys awaiting a changelog write, and their partition

	Stats ProcessorStats
}

// ProcessorStats is what the metrics endpoint reports.
type ProcessorStats struct {
	Consumed    atomic.Uint64
	Alerts      atomic.Uint64
	DeadLetters atomic.Uint64
	Snapshots   atomic.Uint64
	Commits     atomic.Uint64
	LatencyMs   atomic.Uint64 // most recent end-to-end detection latency
}

func NewProcessor(cl, changelog *kgo.Client, eng *engine.Engine, log *slog.Logger) *Processor {
	return &Processor{
		cl:         cl,
		changelog:  changelog,
		engine:     eng,
		log:        log,
		FlushEvery: 2 * time.Second,
		SweepEvery: 5 * time.Second,
		dirty:      make(map[state.Key]int32),
	}
}

// Run consumes until the context is cancelled.
func (p *Processor) Run(ctx context.Context) error {
	flush := time.NewTicker(p.FlushEvery)
	defer flush.Stop()
	sweep := time.NewTicker(p.SweepEvery)
	defer sweep.Stop()
	evict := time.NewTicker(time.Minute)
	defer evict.Stop()

	for {
		select {
		case <-ctx.Done():
			// One last flush so the changelog reflects everything already
			// acknowledged upstream.
			p.flush(context.WithoutCancel(ctx))
			return nil

		case <-flush.C:
			p.flush(ctx)

		case <-sweep.C:
			p.publishAlerts(ctx, p.engine.Sweep(), -1)

		case <-evict.C:
			// Keys the pipeline has not seen in an hour are gone for good;
			// retaining them is a slow memory leak dressed as caution.
			if n := p.engine.EvictIdle(time.Hour); n > 0 {
				p.log.Debug("evicted idle keys", "count", n)
			}

		default:
			fetches := p.cl.PollRecords(ctx, 1000)
			if fetches.IsClientClosed() {
				return nil
			}
			if err := fetchErr(fetches); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				p.log.Warn("fetch", "err", err)
				continue
			}
			fetches.EachRecord(func(rec *kgo.Record) { p.handle(ctx, rec) })
		}
	}
}

// handle processes one input record.
//
// Nothing in here returns an error upward. A single bad record must not stop
// the pipeline — that is what the dead-letter topic is for, and a consumer
// that halts on malformed input is a consumer that halts.
func (p *Processor) handle(ctx context.Context, rec *kgo.Record) {
	p.Stats.Consumed.Add(1)

	ev, err := kafkax.DecodeEvent(rec)
	if err != nil {
		p.deadLetter(ctx, rec, err)
		return
	}

	alerts, err := p.engine.Ingest(ev)
	if err != nil {
		p.deadLetter(ctx, rec, err)
		return
	}

	p.mu.Lock()
	p.dirty[state.Key{Stream: ev.Stream, ID: ev.Key}] = rec.Partition
	p.mu.Unlock()

	p.publishAlerts(ctx, alerts, rec.Partition)
}

func (p *Processor) publishAlerts(ctx context.Context, alerts []domain.Alert, partition int32) {
	for _, a := range alerts {
		rec, err := kafkax.AlertRecord(a)
		if err != nil {
			p.log.Error("encode alert", "err", err, "alert", a.ID)
			continue
		}
		// Co-partitioning: an alert lands on the same partition as the events
		// that produced it, so anything reading both sees them consistently.
		// Swept alerts have no source record, so they are hashed as usual.
		if partition >= 0 {
			rec.Partition = partition
		}
		p.produce(ctx, rec, "alert")
		p.Stats.Alerts.Add(1)
		// Only real pipeline latency. An absence alert's interval is idle
		// time, and a negative one is clock skew — converting either to
		// uint64 would publish a wildly wrong gauge, and a negative one
		// would underflow into something near 2^64.
		if d, ok := a.PipelineLatency(); ok {
			p.Stats.LatencyMs.Store(uint64(d.Milliseconds()))
		}

		p.log.Info("alert",
			"rule", a.Rule, "stream", a.Stream, "key", a.Key,
			"severity", a.Severity, "detail", a.Detail,
			"latencyMs", a.Latency().Milliseconds())
	}
}

func (p *Processor) deadLetter(ctx context.Context, src *kgo.Record, reason error) {
	p.Stats.DeadLetters.Add(1)
	p.log.Warn("dead letter",
		"topic", src.Topic, "partition", src.Partition, "offset", src.Offset, "reason", reason)

	rec := kafkax.DeadLetterRecord(src, reason, time.Now())
	rec.Partition = src.Partition
	p.produce(ctx, rec, "dead letter")
}

// flush writes the changelog for every key touched since the last flush, then
// commits the offsets those changes came from. See FlushEvery for why the
// order matters.
func (p *Processor) flush(ctx context.Context) {
	p.mu.Lock()
	dirty := p.dirty
	p.dirty = make(map[state.Key]int32, len(dirty))
	p.mu.Unlock()

	now := time.Now()
	for key, partition := range dirty {
		snap, ok := p.engine.Store().Snapshot(key, now)
		var rec *kgo.Record
		if !ok {
			// The key was dropped — a job finished, or it aged out. A
			// tombstone tells compaction to forget it rather than leaving a
			// snapshot that a future restore would resurrect.
			rec = kafkax.StateTombstone(key)
		} else {
			var err error
			if rec, err = kafkax.StateRecord(snap); err != nil {
				p.log.Error("encode snapshot", "err", err, "key", key.ID)
				continue
			}
		}
		rec.Partition = partition
		p.changelog.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil && !errors.Is(err, context.Canceled) {
				p.log.Error("changelog produce failed", "key", key.ID, "err", err)
			}
		})
		p.Stats.Snapshots.Add(1)
	}

	if len(dirty) > 0 {
		// Block until the changelog is durable. Committing offsets over a
		// changelog still sitting in a producer batch would reintroduce
		// exactly the gap this ordering exists to close.
		if err := p.changelog.Flush(ctx); err != nil {
			p.log.Warn("changelog flush incomplete, holding offsets", "err", err)
			return
		}
	}

	if err := p.cl.CommitUncommittedOffsets(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			p.log.Warn("commit", "err", err)
		}
		return
	}
	p.Stats.Commits.Add(1)
}

func (p *Processor) produce(ctx context.Context, rec *kgo.Record, what string) {
	p.cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		if err != nil && !errors.Is(err, context.Canceled) {
			p.log.Error("produce failed", "what", what, "topic", rec.Topic, "err", err)
		}
	})
}

func fetchErr(f kgo.Fetches) error {
	var errs []error
	f.EachError(func(topic string, partition int32, err error) {
		errs = append(errs, fmt.Errorf("%s[%d]: %w", topic, partition, err))
	})
	return errors.Join(errs...)
}

// Flush writes the pending changelog and commits offsets. Exported so the
// partition-revoked callback can run it: handing a partition over without
// flushing would make the next owner replay work this one had already done.
func (p *Processor) Flush(ctx context.Context) { p.flush(ctx) }
