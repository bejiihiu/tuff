package kz.bejiihiu.tuff.threads.region;

import java.util.List;
import java.util.Objects;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;

/**
 * Folds per-region readings into the one summary a pool snapshot carries.
 *
 * <p>The arithmetic is the whole job, which is why it lives apart from the Folia read: averages are taken
 * over regions that reported, the maximum is the worst single tick any region saw, and the minimum TPS is
 * the slowest region. Regions without a report still count as regions and still contribute their entity,
 * player and chunk totals - only the timing and TPS values skip them.
 *
 * <p>There is no weighting by chunk count or player count. A region is a region: one region at 10 TPS is
 * a lagging region whether it is empty or crowded, and the point of the summary is to show that at least
 * one region is behind, not to average it away behind a busy neighbour.
 */
public final class RegionPoolMetricsAggregator {

    private RegionPoolMetricsAggregator() {
    }

    /**
     * Summarises one reading of every region.
     *
     * @param readings per-region readings, possibly empty
     * @return the summary; empty metrics for an empty list
     * @throws NullPointerException if {@code readings} is {@code null}
     */
    public static RegionPoolMetrics aggregate(final List<RegionReading> readings) {
        Objects.requireNonNull(readings, "readings");
        if (readings.isEmpty()) {
            return RegionPoolMetrics.empty();
        }

        int timed5s = 0;
        int timed15s = 0;
        int regions = 0;
        int lagging = 0;
        double sumMspt5s = 0.0d;
        double sumMspt15s = 0.0d;
        double maxMspt5s = -1.0d;
        double maxMspt15s = -1.0d;
        double minTps = -1.0d;
        long entities = 0L;
        long players = 0L;
        long chunks = 0L;

        for (final RegionReading reading : readings) {
            if (reading == null) {
                continue;
            }
            regions++;
            entities += reading.entities();
            players += reading.players();
            chunks += reading.chunks();

            if (reading.averageMspt5s() >= 0.0d) {
                sumMspt5s += reading.averageMspt5s();
                timed5s++;
            }
            if (reading.maximumMspt5s() >= 0.0d) {
                maxMspt5s = Math.max(maxMspt5s, reading.maximumMspt5s());
            }
            if (reading.averageMspt15s() >= 0.0d) {
                sumMspt15s += reading.averageMspt15s();
                timed15s++;
            }
            if (reading.maximumMspt15s() >= 0.0d) {
                maxMspt15s = Math.max(maxMspt15s, reading.maximumMspt15s());
            }
            if (reading.tps5s() >= 0.0d) {
                minTps = minTps < 0.0d ? reading.tps5s() : Math.min(minTps, reading.tps5s());
                if (reading.tps5s() < RegionPoolMetrics.LAG_TPS) {
                    lagging++;
                }
            }
        }

        return new RegionPoolMetrics(
            regions,
            timed5s == 0 ? -1.0d : sumMspt5s / timed5s,
            maxMspt5s,
            timed15s == 0 ? -1.0d : sumMspt15s / timed15s,
            maxMspt15s,
            minTps,
            lagging,
            saturated(entities),
            saturated(players),
            saturated(chunks)
        );
    }

    /**
     * Clamps a long total into the int range the summary uses.
     *
     * <p>Counts come from {@code AtomicInteger}s per region, so a sum of a few thousand regions can only
     * overflow in a fantasy scenario; clamping is still cheaper to reason about than a silent wrap.
     *
     * @param value the sum
     * @return the sum as an int
     */
    private static int saturated(final long value) {
        return (int) Math.min(Integer.MAX_VALUE, value);
    }
}
