package dev.luisfelipe.vigil.streams;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaStreams;

/**
 * The smallest HTTP surface that answers the two questions an orchestrator asks.
 *
 * <p>This topology had none, which is fine under docker-compose and is not fine
 * under Kubernetes: with no probes at all, a pod counts as ready the instant the
 * container starts, and a rolling update will take down the next replica while
 * this one is still rebalancing. There is no metric to scrape either, because
 * the JVM exposes nothing on a port.
 *
 * <p>The two endpoints answer different questions, and conflating them is the
 * usual mistake. {@code /health} says the process is running, which is the only
 * thing a restart can fix. {@code /readyz} says {@link KafkaStreams} has reached
 * RUNNING — during REBALANCING the client is alive and holds no assignment, so
 * routing to it or continuing a rollout past it is premature.
 *
 * <p>Built on the JDK's own HTTP server rather than a framework: two endpoints
 * returning fixed JSON do not justify a dependency, and the base image already
 * ships {@code jdk.httpserver}.
 */
final class Health implements AutoCloseable {

    private final HttpServer server;

    private Health(HttpServer server) {
        this.server = server;
    }

    /**
     * Binds and starts serving. Pass port 0 for an ephemeral port, which is
     * what the tests do.
     */
    static Health start(int port, Supplier<KafkaStreams.State> state) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/health", exchange -> respond(exchange, 200, "{\"status\":\"ok\"}"));

        server.createContext("/readyz", exchange -> {
            KafkaStreams.State current = state.get();
            boolean ready = current == KafkaStreams.State.RUNNING;
            respond(
                    exchange,
                    ready ? 200 : 503,
                    String.format("{\"ready\":%b,\"state\":\"%s\"}", ready, current));
        });

        // The default executor runs handlers on the accept thread. These
        // handlers do no I/O and no blocking work, so a thread pool would cost
        // more than it saves.
        server.setExecutor(null);
        server.start();
        return new Health(server);
    }

    /** The bound port, which differs from the requested one when 0 was asked for. */
    int port() {
        return server.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, payload.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(payload);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
