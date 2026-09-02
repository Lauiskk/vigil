package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// topicConfig is the per-topic broker configuration.
//
// The state topic is compacted rather than time-retained: it is a changelog
// keyed by entity, and the only record that matters for a key is the latest
// one. Compaction is what lets a rebalanced consumer rebuild the window store
// in bounded time instead of replaying the whole input topic.
var topicConfig = map[string]map[string]*string{
	TopicState: {
		"cleanup.policy": ptr("compact"),
		// Compact aggressively so a restart replays a small log. In a real
		// deployment this would be far less eager.
		"min.cleanable.dirty.ratio": ptr("0.1"),
		"segment.ms":                ptr("60000"),
	},
	TopicDLQ: {
		// Dead letters outlive everything else here: they exist to be read by
		// a human after the fact.
		"retention.ms": ptr("604800000"), // 7 days
	},
}

func ptr(s string) *string { return &s }

// EnsureTopics creates the pipeline's topics if they are missing.
//
// Missing permission is not an error. On a managed free tier the topics are
// provisioned through the provider's console and the application account has
// no create rights — the correct behaviour there is to carry on and let the
// first produce fail loudly if a topic really is absent.
func EnsureTopics(ctx context.Context, cl *kgo.Client, log *slog.Logger) error {
	adm := kadm.NewClient(cl)

	existing, err := adm.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("kafkax: list topics: %w", err)
	}

	var missing []string
	for _, t := range Topics {
		if !existing.Has(t) {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	for _, t := range missing {
		// kadm reports a per-topic failure through the returned error rather
		// than only through the response, so both have to be considered — and
		// the response error has to be checked before the response is used.
		resp, err := adm.CreateTopic(ctx, Partitions, -1, topicConfig[t], t)
		if err == nil {
			err = resp.Err
		}

		switch {
		case err == nil:
			log.Info("created topic", "topic", t, "partitions", Partitions)

		case errors.Is(err, kerr.TopicAlreadyExists):
			// Every service calls this on start-up, so on a cold compose the
			// three of them race and two lose. Losing that race is the
			// intended outcome, not a failure to report.
			log.Debug("topic already exists", "topic", t)

		case isPermission(err):
			log.Warn("cannot create topic, assuming it is managed externally", "topic", t, "err", err)

		default:
			return fmt.Errorf("kafkax: create %s: %w", t, err)
		}
	}
	return nil
}

func isPermission(err error) bool {
	return errors.Is(err, kerr.TopicAuthorizationFailed) ||
		errors.Is(err, kerr.ClusterAuthorizationFailed) ||
		errors.Is(err, kerr.SaslAuthenticationFailed)
}
