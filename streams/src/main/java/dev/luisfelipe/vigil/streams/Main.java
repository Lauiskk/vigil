package dev.luisfelipe.vigil.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for the rolling-metrics topology. */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Topology topology = MetricsTopology.build(
                env("EVENTS_TOPIC", "vigil.events"),
                env("METRICS_TOPIC", "vigil.metrics"),
                Duration.ofSeconds(Long.parseLong(env("WINDOW_SECONDS", "10"))),
                Duration.ofSeconds(Long.parseLong(env("GRACE_SECONDS", "5"))));

        LOG.info("topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, config());
        CountDownLatch stopped = new CountDownLatch(1);

        // Started before the stream, so an orchestrator gets an honest "not
        // ready yet" during start-up rather than a connection refused it
        // cannot tell apart from a crash.
        Health health;
        try {
            health = Health.start(envInt("HEALTH_PORT", 8084), streams::state);
            LOG.info("health listening on {}", envInt("HEALTH_PORT", 8084));
        } catch (java.io.IOException e) {
            LOG.error("could not bind the health port", e);
            System.exit(1);
            return;
        }

        streams.setUncaughtExceptionHandler(throwable -> {
            LOG.error("stream thread died", throwable);
            // Replace the thread rather than killing the client. A single
            // poisoned record should cost one thread, not the whole service.
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutting down");
            // Health first: stop telling anyone this instance is ready before
            // spending ten seconds closing the stream.
            health.close();
            streams.close(Duration.ofSeconds(10));
            stopped.countDown();
        }, "vigil-streams-shutdown"));

        try {
            streams.start();
            stopped.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            LOG.error("failed to start", t);
            System.exit(1);
        }
    }

    private static Properties config() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, env("APPLICATION_ID", "vigil-streams"));
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BROKERS", "localhost:19092"));

        // Event time, not ingest time. See EventTimeExtractor.
        p.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class.getName());

        // Exactly-once is affordable here in a way it is not on the detection
        // path: this topology reads one topic and writes one topic, and the
        // transaction spans only that. The Go processor's trade-off is
        // different and is argued in docs/decisions/0001.
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // Replication for the internal topics Streams creates for itself --
        // the repartition and changelog topics behind the aggregate.
        //
        // One is correct for the single-broker dev container and wrong
        // everywhere else. On a cluster with min.insync.replicas >= 2, an
        // RF=1 internal topic cannot satisfy an EXACTLY_ONCE_V2 transaction,
        // and the topology fails at its first commit rather than at start-up
        // -- which is the worst place to find out. It has to match the
        // cluster it is pointed at, so it is read from the environment.
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, envInt("REPLICATION_FACTOR", 1));

        String user = System.getenv("KAFKA_USER");
        if (user != null && !user.isBlank()) {
            p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);
            p.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
            p.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                    user, env("KAFKA_PASSWORD", "")));
        }
        return p;
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }

    /**
     * Reads an integer from the environment, falling back rather than dying.
     *
     * <p>A malformed replication factor is a typo in a deployment manifest,
     * and refusing to start leaves the topology down until someone notices.
     * Starting on the default and saying so loudly keeps the stream running
     * while the typo is found.
     */
    static int envInt(String key, int fallback) {
        return parseIntOr(key, System.getenv(key), fallback);
    }

    /**
     * The parsing half of {@link #envInt}, split out because System.getenv
     * cannot be set from a test and an untestable branch is one that will
     * eventually be wrong.
     */
    static int parseIntOr(String key, String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            LOG.warn("{} is not a number, falling back to {}: {}", key, fallback, value);
            return fallback;
        }
    }
}
