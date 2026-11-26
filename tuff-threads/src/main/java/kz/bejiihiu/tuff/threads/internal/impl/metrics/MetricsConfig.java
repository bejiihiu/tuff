package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import java.time.Duration;
import java.util.Objects;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;

/**
 * Sampling policy of the metrics layer.
 *
 * <p>The three switches come from {@code thread-management.metrics}; the two intervals are not in the
 * settings model yet, so they live here with defaults that match the design: sample at most once per
 * second, and scan for unaccounted threads at most once per half minute. Both are read on every call,
 * which is what makes the whole section live reloadable without rebuilding the manager.
 *
 * @param enabled              whether the sampler runs at all
 * @param perPool              whether pool snapshots and saturation are published
 * @param perThread            whether the per thread list is published
 * @param interval             minimum time between two sampling passes
 * @param unregisteredInterval minimum time between two unaccounted thread scans
 */
public record MetricsConfig(
    boolean enabled,
    boolean perPool,
    boolean perThread,
    Duration interval,
    Duration unregisteredInterval
) {

    /**
     * Validates the policy.
     *
     * @throws NullPointerException     if an interval is {@code null}
     * @throws IllegalArgumentException if an interval is zero or negative
     */
    public MetricsConfig {
        Objects.requireNonNull(interval, "interval");
        Objects.requireNonNull(unregisteredInterval, "unregisteredInterval");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive, got " + interval);
        }
        if (unregisteredInterval.isZero() || unregisteredInterval.isNegative()) {
            throw new IllegalArgumentException(
                "unregisteredInterval must be positive, got " + unregisteredInterval
            );
        }
    }

    /**
     * Returns the default policy: everything published, sampled once per second.
     *
     * @return the default policy
     */
    public static MetricsConfig defaults() {
        return from(ThreadManagementSettings.Metrics.defaults());
    }

    /**
     * Builds the policy from the configuration model.
     *
     * <p>Only the switches come from the model; the intervals keep their design defaults, because the
     * settings tree has no keys for them yet and inventing a unit there would be a config change, not a
     * metrics change.
     *
     * @param metrics configured switches
     * @return the policy
     * @throws NullPointerException if {@code metrics} is {@code null}
     */
    public static MetricsConfig from(final ThreadManagementSettings.Metrics metrics) {
        Objects.requireNonNull(metrics, "metrics");
        return new MetricsConfig(metrics.enabled(), metrics.perPool(), metrics.perThread(), Duration.ofSeconds(1),
            Duration.ofSeconds(30));
    }

    /**
     * Returns the sampling interval in nanoseconds.
     *
     * @return the interval, always positive
     */
    public long intervalNanos() {
        return this.interval.toNanos();
    }

    /**
     * Returns the unaccounted thread scan interval in nanoseconds.
     *
     * @return the interval, always positive
     */
    public long unregisteredIntervalNanos() {
        return this.unregisteredInterval.toNanos();
    }
}
