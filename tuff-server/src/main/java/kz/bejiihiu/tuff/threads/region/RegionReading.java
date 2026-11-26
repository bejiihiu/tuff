package kz.bejiihiu.tuff.threads.region;

/**
 * One region's tick reading, as the region scheduler reported it on this pass.
 *
 * <p>A region that has just been created, split or reactivated has fresh tick windows and nothing to
 * report yet. Such a reading keeps {@code -1} for every measured value and still counts as a region:
 * "there is a region here, it has not ticked long enough to be judged" is a different statement from
 * "there is no region".
 *
 * @param tps5s           TPS over the last 5 s, or {@code -1} when the window is empty
 * @param tps15s          TPS over the last 15 s, or {@code -1} when the window is empty
 * @param averageMspt5s   mean tick time over the last 5 s, in milliseconds, or {@code -1}
 * @param maximumMspt5s   worst tick over the last 5 s, in milliseconds, or {@code -1}
 * @param averageMspt15s  mean tick time over the last 15 s, in milliseconds, or {@code -1}
 * @param maximumMspt15s  worst tick over the last 15 s, in milliseconds, or {@code -1}
 * @param entities        entities in the region, at least zero
 * @param players         players in the region, at least zero
 * @param chunks          loaded chunks in the region, at least zero
 */
public record RegionReading(
    double tps5s,
    double tps15s,
    double averageMspt5s,
    double maximumMspt5s,
    double averageMspt15s,
    double maximumMspt15s,
    int entities,
    int players,
    int chunks
) {

    /**
     * Validates one region reading.
     *
     * @throws IllegalArgumentException if a count is negative or a measurement is below the {@code -1}
     *                                  sentinel
     */
    public RegionReading {
        checkMeasured(tps5s, "tps5s");
        checkMeasured(tps15s, "tps15s");
        checkMeasured(averageMspt5s, "averageMspt5s");
        checkMeasured(maximumMspt5s, "maximumMspt5s");
        checkMeasured(averageMspt15s, "averageMspt15s");
        checkMeasured(maximumMspt15s, "maximumMspt15s");
        if (entities < 0 || players < 0 || chunks < 0) {
            throw new IllegalArgumentException("region counts must not be negative");
        }
    }

    private static void checkMeasured(final double value, final String name) {
        if (value != -1.0d && !(value >= 0.0d)) {
            throw new IllegalArgumentException(name + " must be -1 or non negative, got " + value);
        }
    }

    /**
     * Checks whether this region had enough ticks to be judged.
     *
     * @return {@code true} when the 5 s TPS is a real measurement
     */
    public boolean measured() {
        return this.tps5s >= 0.0d;
    }
}
