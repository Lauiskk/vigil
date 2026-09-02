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

        streams.setUncaughtExceptionHandler(throwable -> {
            LOG.error("stream thread died", throwable);
            // Replace the thread rather than killing the client. A single
            // poisoned record should cost one thread, not the whole service.
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutting down");
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
        // The free-tier broker allows two partitions per topic and no more, so
        // asking for a higher replication factor fails topic creation.
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

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
}
