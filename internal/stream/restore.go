package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lauiskk/vigil/internal/engine"
	"github.com/Lauiskk/vigil/internal/kafkax"
)

// Restore rebuilds the window store for the given partitions by replaying the
// compacted changelog.
//
// This is the payoff for keeping a changelog at all. When a partition moves to
// another consumer, the new owner reads a topic that holds one record per live
// key rather than replaying every event ever produced — bounded work, so a
// rebalance costs seconds instead of the retention period.
//
// It runs inside the partition-assigned callback, which blocks the group on
// purpose: processing a partition before its state is loaded would evaluate
// rules against an empty history and miss precisely the anomalies that depend
// on it.
func Restore(ctx context.Context, cfg kafkax.Config, eng *engine.Engine, partitions []int32, log *slog.Logger) error {
	if len(partitions) == 0 {
		return nil
	}

	cl, err := kafkax.NewClient(cfg,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			kafkax.TopicState: offsetsAtStart(partitions),
		}),
	)
	if err != nil {
		return fmt.Errorf("restore: client: %w", err)
	}
	defer cl.Close()

	// The end offsets as of now are the finish line. Anything produced after
	// this point belongs to the consume loop, not to recovery.
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, kafkax.TopicState)
	if err != nil {
		return fmt.Errorf("restore: end offsets: %w", err)
	}

	remaining := map[int32]int64{}
	for _, part := range partitions {
		if o, ok := ends.Lookup(kafkax.TopicState, part); ok && o.Offset > 0 {
			remaining[part] = o.Offset
		}
	}
	if len(remaining) == 0 {
		log.Info("changelog is empty, nothing to restore", "partitions", partitions)
		return nil
	}

	started := time.Now()
	var restored, tombstones int

	for len(remaining) > 0 {
		fetches := cl.PollRecords(ctx, 2000)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := fetchErr(fetches); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("restore: fetch: %w", err)
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			snap, err := kafkax.DecodeState(rec)
			if err != nil {
				// A changelog record that will not decode cannot be replayed
				// and cannot be retried into working. Losing one key's
				// history degrades detection for that key; refusing to start
				// degrades it for all of them.
				log.Warn("skipping undecodable changelog record",
					"partition", rec.Partition, "offset", rec.Offset, "err", err)
				return
			}
			geom, ok := eng.Geometry(snap.Key.Stream)
			if !ok {
				log.Warn("changelog holds a stream this build has no profile for",
					"stream", snap.Key.Stream)
				return
			}
			eng.Store().Restore(snap, geom, time.Now())
			if len(snap.Points) == 0 {
				tombstones++
			} else {
				restored++
			}

			if end, ok := remaining[rec.Partition]; ok && rec.Offset >= end-1 {
				delete(remaining, rec.Partition)
			}
		})
	}

	log.Info("restored state from the changelog",
		"keys", restored, "tombstones", tombstones,
		"partitions", partitions, "took", time.Since(started).String())
	return nil
}

func offsetsAtStart(partitions []int32) map[int32]kgo.Offset {
	out := make(map[int32]kgo.Offset, len(partitions))
	for _, p := range partitions {
		out[p] = kgo.NewOffset().AtStart()
	}
	return out
}
