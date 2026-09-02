// Command alerts is the event-driven tier: an AWS Lambda that receives alerts
// from the pipeline, enriches them, and persists them to DynamoDB.
//
// It accepts two input shapes on purpose. In AWS, a self-managed-Kafka event
// source mapping delivers batches of Kafka records straight from the alert
// topic. Locally, against LocalStack, the forwarder invokes it directly with a
// single alert — because the Kafka event source mapping is not available in
// LocalStack's free tier. One handler, both paths, so the code that runs on a
// laptop is the code that would run in AWS.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/Lauiskk/vigil/internal/domain"
)

// retention is how long an alert is kept. DynamoDB's TTL does the deleting,
// which costs nothing, where a scheduled cleanup would cost read capacity.
const retention = 30 * 24 * time.Hour

type handler struct {
	db    *dynamodb.Client
	table string
	log   *slog.Logger
}

// record is the DynamoDB item.
//
// The partition key groups an entity's alerts together and the sort key orders
// them, so "everything that happened to this card, newest first" is one query
// rather than a scan. Getting this wrong is how a table becomes unusable at
// exactly the moment it matters.
type record struct {
	PK string `dynamodbav:"pk"` // ALERT#<stream>#<entity>
	SK string `dynamodbav:"sk"` // <detectedAt RFC3339Nano>#<alertID>

	AlertID   string         `dynamodbav:"alertId"`
	Rule      string         `dynamodbav:"rule"`
	Stream    string         `dynamodbav:"stream"`
	EntityKey string         `dynamodbav:"entityKey"`
	Severity  string         `dynamodbav:"severity"`
	Rank      int            `dynamodbav:"severityRank"`
	Title     string         `dynamodbav:"title"`
	Detail    string         `dynamodbav:"detail"`
	Evidence  map[string]any `dynamodbav:"evidence,omitempty"`
	EventIDs  []string       `dynamodbav:"eventIds,omitempty"`

	At         string `dynamodbav:"at"`
	DetectedAt string `dynamodbav:"detectedAt"`
	LatencyMs  *int64 `dynamodbav:"latencyMs,omitempty"`
	Absence    bool   `dynamodbav:"absence"`

	ExpiresAt int64 `dynamodbav:"expiresAt"` // unix seconds, the TTL attribute
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	h, err := newHandler(context.Background(), log)
	if err != nil {
		log.Error("cannot start", "err", err)
		os.Exit(1)
	}
	lambda.Start(h.handle)
}

func newHandler(ctx context.Context, log *slog.Logger) (*handler, error) {
	opts := []func(*awsconfig.LoadOptions) error{}

	// LocalStack, or anything else speaking the DynamoDB API. Absent in AWS,
	// where the SDK resolves the real endpoint on its own.
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(endpoint))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	table := os.Getenv("ALERTS_TABLE")
	if table == "" {
		table = "vigil-alerts"
	}
	return &handler{db: dynamodb.NewFromConfig(cfg), table: table, log: log}, nil
}

// input covers both delivery shapes. Exactly one of them is populated.
type input struct {
	// Records is how a self-managed-Kafka event source mapping delivers:
	// a map of "topic-partition" to a batch of base64-encoded records.
	Records map[string][]kafkaRecord `json:"records,omitempty"`

	// Alert is a direct invocation carrying one alert.
	Alert *domain.Alert `json:"alert,omitempty"`
}

type kafkaRecord struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
	Value     string `json:"value"` // base64
}

type result struct {
	Written int      `json:"written"`
	Skipped int      `json:"skipped"`
	Errors  []string `json:"errors,omitempty"`
}

func (h *handler) handle(ctx context.Context, raw json.RawMessage) (result, error) {
	var in input
	if err := json.Unmarshal(raw, &in); err != nil {
		return result{}, fmt.Errorf("unrecognised event shape: %w", err)
	}

	alerts, decodeErrs := collect(in)
	out := result{Errors: decodeErrs}

	for _, a := range alerts {
		switch err := h.put(ctx, a); {
		case err == nil:
			out.Written++
		case errors.Is(err, errAlreadyStored):
			// An event source mapping is at-least-once, so a redelivery after
			// a partial batch failure is normal rather than exceptional. The
			// conditional write makes it a no-op instead of a duplicate.
			out.Skipped++
		default:
			// Returning an error would redeliver the whole batch, including
			// the records already written. Reporting per-record and
			// succeeding is the right trade when the write is idempotent.
			h.log.Error("write failed", "alert", a.ID, "err", err)
			out.Errors = append(out.Errors, fmt.Sprintf("%s: %v", a.ID, err))
		}
	}

	h.log.Info("batch handled", "written", out.Written, "skipped", out.Skipped, "errors", len(out.Errors))
	return out, nil
}

// collect flattens either delivery shape into a list of alerts.
func collect(in input) ([]domain.Alert, []string) {
	if in.Alert != nil {
		return []domain.Alert{*in.Alert}, nil
	}

	var alerts []domain.Alert
	var problems []string
	for partition, records := range in.Records {
		for _, r := range records {
			body, err := base64.StdEncoding.DecodeString(r.Value)
			if err != nil {
				problems = append(problems, fmt.Sprintf("%s@%d: bad base64: %v", partition, r.Offset, err))
				continue
			}
			var a domain.Alert
			if err := json.Unmarshal(body, &a); err != nil {
				problems = append(problems, fmt.Sprintf("%s@%d: bad json: %v", partition, r.Offset, err))
				continue
			}
			alerts = append(alerts, a)
		}
	}
	return alerts, problems
}

var errAlreadyStored = errors.New("already stored")

func (h *handler) put(ctx context.Context, a domain.Alert) error {
	if a.ID == "" {
		return errors.New("alert has no id, so it cannot be written idempotently")
	}

	item := record{
		PK:        fmt.Sprintf("ALERT#%s#%s", a.Stream, a.Key),
		SK:        a.DetectedAt.UTC().Format(time.RFC3339Nano) + "#" + a.ID,
		AlertID:   a.ID,
		Rule:      a.Rule,
		Stream:    string(a.Stream),
		EntityKey: a.Key,
		Severity:  string(a.Severity),
		Rank:      a.Severity.Rank(),
		Title:     a.Title,
		Detail:    a.Detail,
		Evidence:  a.Evidence,
		EventIDs:  a.EventIDs,

		At:         a.At.UTC().Format(time.RFC3339Nano),
		DetectedAt: a.DetectedAt.UTC().Format(time.RFC3339Nano),
		Absence:    a.Absence,
		ExpiresAt:  a.DetectedAt.Add(retention).Unix(),
	}
	// Only a meaningful latency is stored. An absence alert's interval is idle
	// time and a negative one is clock skew; writing either as "latency" would
	// poison any query that averages the column later.
	if d, ok := a.PipelineLatency(); ok {
		ms := d.Milliseconds()
		item.LatencyMs = &ms
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal item: %w", err)
	}

	_, err = h.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(h.table),
		Item:      av,
		// The alert id is unique per detection, so this makes redelivery a
		// no-op rather than a second row.
		ConditionExpression: aws.String("attribute_not_exists(pk) AND attribute_not_exists(sk)"),
	})
	if err != nil {
		var conflict *types.ConditionalCheckFailedException
		if errors.As(err, &conflict) {
			return errAlreadyStored
		}
		return err
	}
	return nil
}
