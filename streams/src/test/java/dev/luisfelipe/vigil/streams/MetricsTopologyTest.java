package dev.luisfelipe.vigil.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Driven with TopologyTestDriver, so the whole topology — windowing, the state
 * store, the aggregation — is exercised without a broker.
 */
class MetricsTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-09-02T12:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> events;
    private TestOutputTopic<String, String> metrics;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class.getName());

        driver = new TopologyTestDriver(
                MetricsTopology.build("vigil.events", "vigil.metrics",
                        Duration.ofSeconds(10), Duration.ofSeconds(5)),
                props);
        events = driver.createInputTopic("vigil.events",
                Serdes.String().serializer(), Serdes.String().serializer());
        metrics = driver.createOutputTopic("vigil.metrics",
                Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static String event(String stream, String key, Instant at, double value) {
        return """
               {"id":"e","stream":"%s","key":"%s","at":"%s","value":%s,"unit":"BRL"}
               """.formatted(stream, key, at.toString(), value);
    }

    @Test
    void aggregatesOneWindowPerStream() {
        events.pipeInput("card-1", event("payments", "card-1", T0.plusSeconds(1), 100));
        events.pipeInput("card-2", event("payments", "card-2", T0.plusSeconds(2), 200));
        events.pipeInput("grid-A1", event("sensors", "grid-A1", T0.plusSeconds(3), 40));
        // Push the stream time past the window plus its grace so it closes.
        events.pipeInput("card-3", event("payments", "card-3", T0.plusSeconds(40), 1));

        List<JsonNode> out = metrics.readValuesToList().stream().map(MetricsTopologyTest::parse).toList();
        assertFalse(out.isEmpty(), "the window produced nothing");

        JsonNode payments = last(out, "payments", 2);
        assertEquals(2, payments.get("count").asLong());
        assertEquals(300.0, payments.get("sum").asDouble(), 0.001);
        assertEquals(150.0, payments.get("mean").asDouble(), 0.001);
        assertEquals(100.0, payments.get("min").asDouble(), 0.001);
        assertEquals(200.0, payments.get("max").asDouble(), 0.001);

        JsonNode sensors = last(out, "sensors", 1);
        assertEquals(1, sensors.get("count").asLong());
        assertEquals(40.0, sensors.get("mean").asDouble(), 0.001);
    }

    /**
     * Window membership must come from the event's own timestamp. If it came
     * from ingest time, these two records would land in the same window
     * despite describing moments a minute apart.
     */
    @Test
    void windowsOnEventTimeNotIngestTime() {
        events.pipeInput("card-1", event("payments", "card-1", T0.plusSeconds(1), 10));
        events.pipeInput("card-1", event("payments", "card-1", T0.plusSeconds(61), 20));
        events.pipeInput("card-1", event("payments", "card-1", T0.plusSeconds(120), 30));

        List<JsonNode> out = metrics.readValuesToList().stream().map(MetricsTopologyTest::parse).toList();
        assertTrue(out.size() >= 2, "expected separate windows, got " + out.size());
        for (JsonNode n : out) {
            assertEquals(1, n.get("count").asLong(),
                    "records a minute apart were folded into one window");
        }
    }

    /** A record that will not parse must be skipped, not fail the topology. */
    @Test
    void skipsMalformedRecords() {
        events.pipeInput("card-1", "{not json");
        events.pipeInput("card-1", "{\"stream\":\"payments\"}"); // no value
        events.pipeInput("card-1", event("payments", "card-1", T0.plusSeconds(1), 50));
        events.pipeInput("card-1", event("payments", "card-1", T0.plusSeconds(40), 1));

        JsonNode first = parse(metrics.readValuesToList().get(0));
        assertEquals(1, first.get("count").asLong(), "a malformed record was counted");
    }

    private static JsonNode last(List<JsonNode> all, String stream, long wantCount) {
        return all.stream()
                .filter(n -> stream.equals(n.get("stream").asText()))
                .filter(n -> n.get("count").asLong() == wantCount)
                .reduce((a, b) -> b)
                .orElseThrow(() -> new AssertionError(
                        "no %s window with count %d in %s".formatted(stream, wantCount, all)));
    }

    private static JsonNode parse(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new AssertionError("unreadable output: " + json, e);
        }
    }
}
