package dev.luisfelipe.vigil.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Takes window membership from the event's own {@code at} field rather than
 * from when the broker happened to receive it.
 *
 * <p>This is the difference between windowing and bucketing. Under a surge, or
 * during a replay, ingest time and event time diverge by seconds — and a
 * ten-second window keyed on ingest time would then be reporting on a mixture
 * of moments rather than on one.
 */
public final class EventTimeExtractor implements TimestampExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof String json) {
            try {
                JsonNode at = MAPPER.readTree(json).get("at");
                if (at != null && at.isTextual()) {
                    return Instant.parse(at.asText()).toEpochMilli();
                }
            } catch (Exception ignored) {
                // Fall through. A record whose timestamp cannot be read is not
                // worth failing the whole topology over; the fallback below
                // keeps it in roughly the right window, and the Go processor
                // dead-letters malformed records independently.
            }
        }
        long broker = record.timestamp();
        return broker >= 0 ? broker : partitionTime;
    }
}
