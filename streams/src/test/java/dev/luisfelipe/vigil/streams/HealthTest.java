package dev.luisfelipe.vigil.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;

class HealthTest {

    private HttpResponse<String> get(Health health, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + health.port() + path))
                .GET()
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void livenessAnswersWhateverTheStreamIsDoing() throws Exception {
        AtomicReference<KafkaStreams.State> state =
                new AtomicReference<>(KafkaStreams.State.ERROR);

        try (Health health = Health.start(0, state::get)) {
            // Liveness is not about the stream. If the process can answer, a
            // restart would not fix anything.
            assertEquals(200, get(health, "/health").statusCode());
        }
    }

    @Test
    void readinessIsTrueOnlyWhileRunning() throws Exception {
        AtomicReference<KafkaStreams.State> state =
                new AtomicReference<>(KafkaStreams.State.CREATED);

        try (Health health = Health.start(0, state::get)) {
            assertEquals(503, get(health, "/readyz").statusCode());

            state.set(KafkaStreams.State.REBALANCING);
            HttpResponse<String> rebalancing = get(health, "/readyz");
            assertEquals(503, rebalancing.statusCode());
            assertTrue(rebalancing.body().contains("REBALANCING"), rebalancing.body());

            state.set(KafkaStreams.State.RUNNING);
            HttpResponse<String> running = get(health, "/readyz");
            assertEquals(200, running.statusCode());
            assertTrue(running.body().contains("\"ready\":true"), running.body());

            // A client that has stopped must stop being ready, or a rollout
            // will happily replace the next replica with another dead one.
            state.set(KafkaStreams.State.NOT_RUNNING);
            assertEquals(503, get(health, "/readyz").statusCode());
        }
    }
}
