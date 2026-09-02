package dev.luisfelipe.vigil.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Tumbling aggregation over the event stream, one window per stream name.
 *
 * <p>This is the part of the pipeline that is genuinely better as Kafka
 * Streams than as hand-written Go. A {@code KTable} backed by a windowed store,
 * with the changelog, the retention and the late-record handling supplied by
 * the library, is a great deal of machinery to reimplement for an aggregation
 * that is not on the detection path. The Go processor does the stateful
 * detection because it must — Kafka Streams is JVM-only — not because
 * hand-rolling it was preferable. See docs/decisions/0001.
 */
public final class MetricsTopology {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MetricsTopology() {}

    public static Topology build(String eventsTopic, String metricsTopic, Duration window, Duration grace) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Stats> statsSerde = statsSerde();

        builder.stream(eventsTopic, Consumed.with(Serdes.String(), Serdes.String()))
                // Re-key from entity to stream name: the detection rules care
                // about one card, these metrics care about all of payments.
                .map((key, json) -> toStreamValue(json))
                .filter((stream, value) -> stream != null && value != null)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))

                // A grace period, not zero. Records genuinely do arrive out of
                // order, and a window that closes the instant it ends reports
                // a count lower than what actually happened.
                .windowedBy(TimeWindows.ofSizeAndGrace(window, grace))
                .aggregate(
                        Stats::empty,
                        (stream, value, agg) -> agg.add(value),
                        Materialized.with(Serdes.String(), statsSerde))

                .toStream()
                .map(MetricsTopology::toRecord)
                .to(metricsTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    /** Extracts the stream name and the measured value from an event. */
    private static KeyValue<String, Double> toStreamValue(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            JsonNode stream = node.get("stream");
            JsonNode value = node.get("value");
            if (stream == null || !stream.isTextual() || value == null || !value.isNumber()) {
                return KeyValue.pair(null, null);
            }
            return KeyValue.pair(stream.asText(), value.asDouble());
        } catch (Exception e) {
            // Filtered out downstream. The Go processor owns the dead-letter
            // path; duplicating it here would put the same bad record in the
            // DLQ twice.
            return KeyValue.pair(null, null);
        }
    }

    /** Renders one closed window as a JSON record keyed by stream name. */
    private static KeyValue<String, String> toRecord(Windowed<String> key, Stats stats) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("stream", key.key());
        out.put("windowStart", key.window().startTime().toString());
        out.put("windowEnd", key.window().endTime().toString());
        out.put("count", stats.count());
        out.put("sum", round(stats.sum()));
        out.put("mean", round(stats.mean()));
        out.put("min", round(stats.safeMin()));
        out.put("max", round(stats.safeMax()));
        out.put("perSecond", round(perSecond(key, stats)));
        return KeyValue.pair(key.key(), out.toString());
    }

    private static double perSecond(Windowed<String> key, Stats stats) {
        long millis = key.window().end() - key.window().start();
        return millis <= 0 ? 0 : stats.count() * 1000.0 / millis;
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /**
     * A JSON serde for the aggregate.
     *
     * <p>Kafka Streams persists the running aggregate to a changelog between
     * updates, so it has to be serialisable — which is easy to forget until a
     * rebalance turns the state store into an exception.
     */
    static Serde<Stats> statsSerde() {
        Serializer<Stats> serializer = (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new IllegalStateException("encoding the window aggregate failed", e);
            }
        };
        Deserializer<Stats> deserializer = (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, Stats.class);
            } catch (Exception e) {
                throw new IllegalStateException("decoding the window aggregate failed", e);
            }
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
