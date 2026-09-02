package dev.luisfelipe.vigil.streams;

/**
 * A rolling aggregate over one window of one stream.
 *
 * <p>Immutable, so the aggregation is a pure fold. Kafka Streams may call the
 * aggregator concurrently for different keys and will call it repeatedly for
 * the same key as late records arrive; mutating shared state here is the
 * classic way to produce results that are subtly wrong under load only.
 */
public record Stats(long count, double sum, double min, double max) {

    public static Stats empty() {
        return new Stats(0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    public Stats add(double value) {
        return new Stats(
                count + 1,
                sum + value,
                Math.min(min, value),
                Math.max(max, value));
    }

    /** Mean over the window, or zero when the window is empty. */
    public double mean() {
        return count == 0 ? 0 : sum / count;
    }

    /**
     * The bounds of an empty window are the identity values of min and max,
     * which are infinities. They serialise as {@code null} in JSON and would
     * reach the dashboard as NaN, so they are reported as zero instead.
     */
    public double safeMin() {
        return count == 0 ? 0 : min;
    }

    public double safeMax() {
        return count == 0 ? 0 : max;
    }
}
