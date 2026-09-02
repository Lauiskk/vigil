// Command forwarder bridges the alert topic to the event-driven tier.
//
// It exists because of one gap between local and real. In AWS, a self-managed
// Kafka event source mapping reads vigil.alerts and invokes the Lambda
// directly, with no bridge at all — that is what infra/terraform provisions.
// LocalStack's free tier does not implement that event source, so locally this
// process does the same job: consume the topic, invoke the function.
//
// The handler accepts both delivery shapes, so the code exercised on a laptop
// is the code that would run in AWS. Only the delivery differs.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lauiskk/vigil/internal/kafkax"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log); err != nil {
		log.Error("forwarder stopped", "err", err)
		os.Exit(1)
	}
	log.Info("forwarder stopped cleanly")
}

func run(ctx context.Context, log *slog.Logger) error {
	var opts []func(*awsconfig.LoadOptions) error
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(endpoint))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return err
	}
	fn := lambda.NewFromConfig(cfg)
	name := env("ALERTS_FUNCTION", "vigil-alerts")

	cl, err := kafkax.NewClient(kafkax.ConfigFromEnv("vigil-forwarder"),
		kgo.ConsumerGroup(env("KAFKA_GROUP", "vigil-forwarder")),
		kgo.ConsumeTopics(kafkax.TopicAlerts),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	log.Info("forwarding alerts", "function", name, "endpoint", os.Getenv("AWS_ENDPOINT_URL"))

	for {
		fetches := cl.PollRecords(ctx, 50)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Warn("fetch", "topic", t, "partition", p, "err", err)
			}
		})

		delivered := true
		fetches.EachRecord(func(rec *kgo.Record) {
			if err := invoke(ctx, fn, name, rec.Value); err != nil {
				// Offsets are not committed, so the batch is retried. The
				// handler's write is conditional on the alert id, which is
				// what makes replaying it safe rather than duplicating rows.
				log.Error("invoke failed, will retry", "offset", rec.Offset, "err", err)
				delivered = false
			}
		})

		if delivered {
			if err := cl.CommitUncommittedOffsets(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Warn("commit", "err", err)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

func invoke(ctx context.Context, fn *lambda.Client, name string, alert []byte) error {
	payload, err := json.Marshal(map[string]json.RawMessage{"alert": alert})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	out, err := fn.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: aws.String(name),
		Payload:      payload,
	})
	if err != nil {
		return err
	}
	if out.FunctionError != nil {
		return errors.New(*out.FunctionError + ": " + string(out.Payload))
	}
	return nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
