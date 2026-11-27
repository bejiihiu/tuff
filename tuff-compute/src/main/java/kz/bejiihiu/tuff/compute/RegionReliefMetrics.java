package kz.bejiihiu.tuff.compute;

/**
 * Counters and gauges of the {@code region-relief} path.
 *
 * <p>An immutable snapshot. The counters are monotonic since the scheduler was created and the
 * gauges describe the moment the snapshot was taken, exactly like {@link SchedulerMetrics}.
 *
 * <p>What the counters mean together:
 * <ul>
 *     <li>every accepted {@link RegionRelief#submit} call adds one to {@link #requests()};</li>
 *     <li>a request that was handed to the pool adds one to {@link #accepted()};</li>
 *     <li>an accepted request whose result reached the region thread and passed the epoch check adds
 *     one to {@link #completed()} and its compute time to {@link #estimatedSavedNanos()};</li>
 *     <li>an accepted result dropped because the epoch moved on adds one to {@link #stale()};</li>
 *     <li>a result dropped because the region stopped, or because the schedule ran it off the owner
 *     thread, adds one to {@link #dropped()};</li>
 *     <li>work that ran inline instead of on the pool - because relief is switched off, the pool is
 *     disabled, the queue is full, the per-region in-flight cap is reached or the queued request
 *     expired - adds one to {@link #inlineFallbacks()}; {@link #expired()} is the subset that was
 *     accepted first and expired afterwards;</li>
 *     <li>a compute body that threw adds one to {@link #failed()}.</li>
 * </ul>
 *
 * <p>{@link #estimatedSavedNanos()} is an estimate, not a measurement: it sums the wall time the
 * compute bodies of applied results spent off the region thread. The applicator still runs on the
 * region, so the real saving is the difference between that time and the apply time.
 *
 * @param enabled               whether relief offloads at all
 * @param consumerRegistered    whether the {@code region-relief} consumer is registered
 * @param inFlight              accepted requests of all regions that have not been applied, dropped
 *                              or failed yet
 * @param maxInFlightPerRegion  configured in-flight cap of one region
 * @param reservedCores         cores tick pressure may not take away from the pool for relief
 * @param requests              submissions that reached the relief service
 * @param accepted              submissions handed to the compute pool
 * @param completed             results applied on the owning region thread
 * @param stale                 results dropped because their epoch was no longer current
 * @param inlineFallbacks       requests executed inline on the region thread
 * @param expired               accepted requests that expired in the queue and ran inline afterwards
 * @param failed                compute bodies that threw
 * @param dropped               results dropped because the region was gone or the apply was not on
 *                              the owner thread
 * @param estimatedSavedNanos   estimated region time saved by the applied results
 */
public record RegionReliefMetrics(
    boolean enabled,
    boolean consumerRegistered,
    int inFlight,
    int maxInFlightPerRegion,
    int reservedCores,
    long requests,
    long accepted,
    long completed,
    long stale,
    long inlineFallbacks,
    long expired,
    long failed,
    long dropped,
    long estimatedSavedNanos
) {

    /**
     * Validates the snapshot.
     *
     * @throws IllegalArgumentException if a gauge or counter is negative
     */
    public RegionReliefMetrics {
        if (inFlight < 0 || maxInFlightPerRegion < 0 || reservedCores < 0) {
            throw new IllegalArgumentException("relief gauges must not be negative");
        }
        if (requests < 0 || accepted < 0 || completed < 0 || stale < 0 || inlineFallbacks < 0
            || expired < 0 || failed < 0 || dropped < 0 || estimatedSavedNanos < 0) {
            throw new IllegalArgumentException("relief counters must not be negative");
        }
    }

    /**
     * Returns an all zero snapshot of a scheduler without relief support.
     *
     * @return the empty snapshot
     */
    public static RegionReliefMetrics empty() {
        return new RegionReliefMetrics(false, false, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }
}
