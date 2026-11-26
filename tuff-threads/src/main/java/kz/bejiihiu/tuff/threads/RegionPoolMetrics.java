package kz.bejiihiu.tuff.threads;

/**
 * Aggregate tick health of the regions running on one pool.
 *
 * <p>There is no server wide tick metric on a regionised server: TPS and MSPT belong to a region. This
 * record is the readable summary of one moment across every region the pool ticks - how many there are,
 * how long a tick takes on average and at worst, and how many regions are behind. It is deliberately an
 * aggregate: per region numbers are {@code /tps}, and the snapshot stays cheap.
 *
 * <p>The {@code -1} rule is the same as everywhere else in this module: a value the reader could not
 * measure is {@code -1}, never a fake zero. An empty pool has no minimum TPS to report, and a region that
 * has not ticked yet contributes a reading without a report; both are {@code -1}.
 *
 * @param regions        regions the pool ticks right now, at least zero
 * @param averageMspt5s  mean tick time over the 5 s window across regions, in milliseconds, or {@code -1}
 * @param maximumMspt5s  worst single tick over the 5 s window across regions, in milliseconds, or {@code -1}
 * @param averageMspt15s mean tick time over the 15 s window across regions, in milliseconds, or {@code -1}
 * @param maximumMspt15s worst single tick over the 15 s window across regions, in milliseconds, or {@code -1}
 * @param minimumTps     lowest region TPS over the 5 s window, or {@code -1} when no region reported
 * @param laggingRegions regions below {@link #LAG_TPS}, at least zero and at most {@link #regions}
 * @param entities       entities summed over all regions
 * @param players        players summed over all regions
 * @param chunks         loaded chunks summed over all regions
 */
public record RegionPoolMetrics(
    int regions,
    double averageMspt5s,
    double maximumMspt5s,
    double averageMspt15s,
    double maximumMspt15s,
    double minimumTps,
    int laggingRegions,
    int entities,
    int players,
    int chunks
) {

    /**
     * TPS below which a region is counted as lagging.
     *
     * <p>This is a proxy, not a deadline miss: Folia does not publish the starting overshoot of a tick
     * without a patch, so "the region is late" is expressed as "the region is visibly under 20 TPS". A
     * region at exactly this value is not late.
     */
    public static final double LAG_TPS = 18.0d;

    /**
     * Validates a region summary.
     *
     * @throws IllegalArgumentException if a count is negative, the lag count exceeds the region count or
     *                                  a measurement is below the {@code -1} sentinel
     */
    public RegionPoolMetrics {
        if (regions < 0) {
            throw new IllegalArgumentException("regions must not be negative, got " + regions);
        }
        if (laggingRegions < 0 || laggingRegions > regions) {
            throw new IllegalArgumentException(
                "laggingRegions must be in [0, " + regions + "], got " + laggingRegions
            );
        }
        if (entities < 0 || players < 0 || chunks < 0) {
            throw new IllegalArgumentException("region totals must not be negative");
        }
        checkMeasured(averageMspt5s, "averageMspt5s");
        checkMeasured(maximumMspt5s, "maximumMspt5s");
        checkMeasured(averageMspt15s, "averageMspt15s");
        checkMeasured(maximumMspt15s, "maximumMspt15s");
        checkMeasured(minimumTps, "minimumTps");
    }

    private static void checkMeasured(final double value, final String name) {
        if (value != -1.0d && !(value >= 0.0d)) {
            throw new IllegalArgumentException(name + " must be -1 or non negative, got " + value);
        }
    }

    /**
     * Returns a summary of a pool with no regions, where every measurement is unknown.
     *
     * @return the empty summary
     */
    public static RegionPoolMetrics empty() {
        return new RegionPoolMetrics(0, -1.0d, -1.0d, -1.0d, -1.0d, -1.0d, 0, 0, 0, 0);
    }

    /**
     * Returns the share of regions below {@link #LAG_TPS}.
     *
     * <p>Zero for a pool with no regions: there is nothing late in an empty pool, and a division by zero
     * would print as a misleading spike.
     *
     * @return the share in {@code [0, 1]}
     */
    public double lagFraction() {
        return this.regions == 0 ? 0.0d : (double) this.laggingRegions / (double) this.regions;
    }
}
