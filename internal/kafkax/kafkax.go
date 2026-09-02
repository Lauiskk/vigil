// Package kafkax is the Kafka boundary: topic names, client construction and
// the JSON codec. Everything above it — the rules, the windows, the engine —
// is written without knowing a broker exists.
package kafkax

import (
	"crypto/tls"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// The five topics, and there are exactly five on purpose.
//
// Aiven's free Kafka plan — the only managed Kafka that is free indefinitely,
// with no card and no expiry — allows five topics of two partitions each.
// That ceiling is why all three streams share one input topic rather than
// getting one apiece, and it is a design constraint rather than an oversight.
// Adding a sixth topic means changing the hosting story.
const (
	TopicEvents  = "vigil.events"  // every stream, keyed by entity
	TopicAlerts  = "vigil.alerts"  // rule hits
	TopicMetrics = "vigil.metrics" // rolling aggregates from the Java topology
	TopicState   = "vigil.state"   // compacted changelog of the window store
	TopicDLQ     = "vigil.dlq"     // records that cannot be processed
)

// Topics is every topic the pipeline uses, in creation order.
var Topics = []string{TopicEvents, TopicAlerts, TopicMetrics, TopicState, TopicDLQ}

// Partitions is the partition count for the working topics. Two, again
// because of the free-tier ceiling — and two is enough to demonstrate the
// thing worth demonstrating: kill one processor of two and watch the
// partitions reassign and the state rebuild from the changelog.
const Partitions = 2

// Config describes how to reach the cluster.
type Config struct {
	Brokers  []string
	User     string // SASL/SCRAM-SHA-512, as Aiven issues
	Password string
	TLS      bool
	ClientID string
}

// ConfigFromEnv reads the standard variables, defaulting to the local
// Redpanda in docker-compose so a fresh clone runs with no configuration.
func ConfigFromEnv(clientID string) Config {
	c := Config{
		Brokers:  splitList(env("KAFKA_BROKERS", "localhost:19092")),
		User:     os.Getenv("KAFKA_USER"),
		Password: os.Getenv("KAFKA_PASSWORD"),
		TLS:      os.Getenv("KAFKA_TLS") == "true",
		ClientID: clientID,
	}
	// Aiven always requires both; treating credentials as implying TLS avoids
	// a confusing handshake failure when only one variable gets set.
	if c.User != "" {
		c.TLS = true
	}
	return c
}

// Options renders the config as franz-go options.
func (c Config) Options(extra ...kgo.Opt) ([]kgo.Opt, error) {
	if len(c.Brokers) == 0 {
		return nil, fmt.Errorf("kafkax: no brokers configured")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		// Every producer in this pipeline is idempotent. It costs a sequence
		// number per partition and removes duplicate delivery on retry, which
		// would otherwise show up as phantom velocity alerts.
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	}
	if c.User != "" {
		auth := scram.Auth{User: c.User, Pass: c.Password}
		opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
	}
	if c.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	return append(opts, extra...), nil
}

// NewClient dials the cluster.
func NewClient(c Config, extra ...kgo.Opt) (*kgo.Client, error) {
	opts, err := c.Options(extra...)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkax: dial %v: %w", c.Brokers, err)
	}
	return cl, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
