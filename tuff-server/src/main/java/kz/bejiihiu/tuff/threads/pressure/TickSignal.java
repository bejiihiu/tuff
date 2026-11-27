package kz.bejiihiu.tuff.threads.pressure;

import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;

/**
 * One reading of region tick health, as the pressure controller needs it.
 *
 * <p>The source is the summary the thread sampler already publishes - never a tick report read from a
 * region thread. Only three numbers matter for the decision: how many regions there are, how many of
 * them are behind, and how slow the worst one is. {@link #maximumMspt()} is carried for the log line
 * and the recording, not for the decision.
 *
 * <p>{@link #measured()} is the honest flag: a pool with no regions (an empty server, or a pass where
 * the probe could not read them) reports {@code false}, and the controller makes no decision from it.
 * Inventing "zero lag" for "we did not look" would let a lagging server release its reserve because the
 * region walk failed that second.
 *
 * @param measured    whether a region summary was actually read
 * @param regions     regions the pool ticks
 * @param lagFraction share of regions below 18 TPS, in {@code [0, 1]}, or {@code 0} when not measured
 * @param minimumTps  slowest region's 5 s TPS, or {@code -1} when no region reported
 * @param maximumMspt worst single 5 s tick across regions in milliseconds, or {@code -1}
 */
public record TickSignal(
    boolean measured,
    int regions,
    double lagFraction,
    double minimumTps,
    double maximumMspt
) {

    /**
     * Validates a signal.
     *
     * @throws IllegalArgumentException if a count or a share is outside its range
     */
    public TickSignal {
        if (regions < 0) {
            throw new IllegalArgumentException("regions must not be negative, got " + regions);
        }
        if (!(lagFraction >= 0.0d) || lagFraction > 1.0d) {
            throw new IllegalArgumentException("lagFraction must be in [0, 1], got " + lagFraction);
        }
        if (minimumTps != -1.0d && !(minimumTps >= 0.0d)) {
            throw new IllegalArgumentException("minimumTps must be -1 or non negative, got " + minimumTps);
        }
        if (maximumMspt != -1.0d && !(maximumMspt >= 0.0d)) {
            throw new IllegalArgumentException("maximumMspt must be -1 or non negative, got " + maximumMspt);
        }
        if (!measured && (regions != 0 || lagFraction != 0.0d || minimumTps != -1.0d || maximumMspt != -1.0d)) {
            throw new IllegalArgumentException("an unmeasured signal cannot carry region numbers");
        }
    }

    /**
     * Returns the signal of a pass that could not read any region.
     *
     * @return the unmeasured signal
     */
    public static TickSignal unknown() {
        return new TickSignal(false, 0, 0.0d, -1.0d, -1.0d);
    }

    /**
     * Reads the signal out of a published resource snapshot.
     *
     * <p>Never throws: a snapshot without the region pool, or a pool without a region summary, is
     * {@link #unknown()} rather than an exception.
     *
     * @param snapshot the sampler's latest pass
     * @return the signal, never {@code null}
     * @throws NullPointerException if {@code snapshot} is {@code null}
     */
    public static TickSignal read(final ResourceSnapshot snapshot) {
        return snapshot.pool(PoolId.REGION_EXECUTION)
            .flatMap(PoolSnapshot::regionMetrics)
            .map(TickSignal::of)
            .orElseGet(TickSignal::unknown);
    }

    /**
     * Converts the pool's region summary into a signal.
     *
     * @param metrics region summary of the region pool
     * @return the measured signal
     * @throws NullPointerException if {@code metrics} is {@code null}
     */
    public static TickSignal of(final RegionPoolMetrics metrics) {
        return new TickSignal(
            true,
            metrics.regions(),
            metrics.lagFraction(),
            metrics.minimumTps(),
            metrics.maximumMspt5s()
        );
    }

    /**
     * Checks whether the regions ask for the compute pool to give up a core.
     *
     * <p>Two independent triggers: a big enough share of regions is below 18 TPS, or the slowest
     * region is below the entry TPS even if it is the only one. Both use the configured values, so
     * the test and the server cannot drift apart.
     *
     * @param enterLagFraction share of lagging regions that starts pressure
     * @param enterMinTps      slowest region TPS below which pressure starts
     * @return {@code true} when pressure should start or grow
     */
    public boolean lagging(final double enterLagFraction, final double enterMinTps) {
        if (!this.measured) {
            return false;
        }
        if (this.lagFraction > 0.0d && this.lagFraction >= enterLagFraction) {
            return true;
        }
        return this.minimumTps >= 0.0d && this.minimumTps < enterMinTps;
    }

    /**
     * Checks whether the regions are calm enough to release a core.
     *
     * <p>Calm is stricter than "not lagging": no region below 18 TPS and the slowest region at or
     * above the exit TPS. The gap between {@code enterMinTps} and {@code exitMinTps} is the
     * hysteresis band where nothing moves.
     *
     * @param exitMinTps slowest region TPS at or above which a sample is calm
     * @return {@code true} when the sample counts towards releasing the reserve
     */
    public boolean calm(final double exitMinTps) {
        return this.measured
            && this.lagFraction == 0.0d
            && this.minimumTps >= exitMinTps;
    }
}
