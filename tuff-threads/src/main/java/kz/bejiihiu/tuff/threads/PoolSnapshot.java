package kz.bejiihiu.tuff.threads;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Point in time observation of one pool.
 *
 * <p>Counters are per pool and monotonic since the pool was registered, unless the metrics
 * implementation documents a rolling window. Queue values use {@code -1} for "this pool does not expose
 * it": region and chunk pools have no readable queue depth without touching their internals, and
 * inventing a zero there would look like an empty queue instead of an unknown one.
 *
 * <p>{@link #activeThreads()} and {@link #idleThreads()} may also be {@code -1} when the probe cannot
 * classify states; the metrics layer then derives them from {@link #aliveThreads()} and the thread
 * snapshots. They never sum to {@link #aliveThreads()} in every case, because blocked threads are
 * neither active nor idle.
 *
 * <p>The task counters follow the same rule as every other "unknown" in this module: {@code -1} means
 * "this layer has no source for it", never a measured zero. {@code submittedTasks} and friends are
 * filled only when the pool owner exposes them through a probe; the metrics layer does not own the
 * queues, so until then they are {@code -1} and an operator must not read them as "nothing was
 * rejected".
 *
 * @param spec            registered description of the pool
 * @param aliveThreads    threads that currently exist, at least zero
 * @param coreThreads     threads the pool keeps alive regardless of load, at least zero
 * @param activeThreads   threads currently runnable, or {@code -1} when unknown
 * @param idleThreads     threads currently waiting for work, or {@code -1} when unknown
 * @param queuedTasks     tasks waiting in the pool queue, or {@code -1} when unknown
 * @param queueCapacity   maximum queue depth, or {@code -1} when unknown or unbounded
 * @param submittedTasks  tasks accepted since registration, or {@code -1} when this layer has no counter
 * @param completedTasks  tasks completed since registration, or {@code -1} when this layer has no counter
 * @param rejectedTasks   tasks refused since registration, or {@code -1} when this layer has no counter
 * @param cpuLoad         share of the usable cores consumed by this pool over the last sampling
 *                        window, from zero to one, or {@code -1} when unavailable
 * @param saturation      saturation summary
 * @param degradedReasons reasons the pool cannot be fully managed; empty when it can
 * @param regionMetrics   tick health of the regions this pool runs, or empty when it runs none or the
 *                        reader has no source for it
 */
public record PoolSnapshot(
    PoolSpec spec,
    int aliveThreads,
    int coreThreads,
    int activeThreads,
    int idleThreads,
    long queuedTasks,
    long queueCapacity,
    long submittedTasks,
    long completedTasks,
    long rejectedTasks,
    double cpuLoad,
    SaturationReport saturation,
    List<DegradedReason> degradedReasons,
    Optional<RegionPoolMetrics> regionMetrics
) {

    /**
     * Validates and freezes a pool snapshot.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if a count or a share is outside its range
     */
    public PoolSnapshot {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(saturation, "saturation");
        Objects.requireNonNull(degradedReasons, "degradedReasons");
        Objects.requireNonNull(regionMetrics, "regionMetrics");
        if (aliveThreads < 0) {
            throw new IllegalArgumentException("aliveThreads must not be negative, got " + aliveThreads);
        }
        if (coreThreads < 0) {
            throw new IllegalArgumentException("coreThreads must not be negative, got " + coreThreads);
        }
        if (activeThreads < -1) {
            throw new IllegalArgumentException("activeThreads must be -1 or non negative, got " + activeThreads);
        }
        if (idleThreads < -1) {
            throw new IllegalArgumentException("idleThreads must be -1 or non negative, got " + idleThreads);
        }
        if (queuedTasks < -1L) {
            throw new IllegalArgumentException("queuedTasks must be -1 or non negative, got " + queuedTasks);
        }
        if (queueCapacity < -1L) {
            throw new IllegalArgumentException("queueCapacity must be -1 or non negative, got " + queueCapacity);
        }
        if (submittedTasks < -1L || completedTasks < -1L || rejectedTasks < -1L) {
            throw new IllegalArgumentException("task counters must be -1 (unknown) or non negative");
        }
        if (cpuLoad != -1.0d && (!(cpuLoad >= 0.0d) || cpuLoad > 1.0d)) {
            throw new IllegalArgumentException("cpuLoad must be -1 or in [0, 1], got " + cpuLoad);
        }
        if (!saturation.pool().equals(spec.id())) {
            throw new IllegalArgumentException(
                "saturation is for pool " + saturation.pool() + ", expected " + spec.id()
            );
        }
        degradedReasons = List.copyOf(degradedReasons);
    }

    /**
     * Creates a pool snapshot without region metrics.
     *
     * <p>Kept for call sites that describe a pool by hand - tests, fixtures, pools that run no regions.
     * Region data is additive: a snapshot that has none is not incomplete, it just has nothing to say
     * about ticks.
     *
     * @param spec            registered description of the pool
     * @param aliveThreads    threads that currently exist, at least zero
     * @param coreThreads     threads the pool keeps alive regardless of load, at least zero
     * @param activeThreads   threads currently runnable, or {@code -1} when unknown
     * @param idleThreads     threads currently waiting for work, or {@code -1} when unknown
     * @param queuedTasks     tasks waiting in the pool queue, or {@code -1} when unknown
     * @param queueCapacity   maximum queue depth, or {@code -1} when unknown or unbounded
     * @param submittedTasks  tasks accepted since registration, or {@code -1} when this layer has no counter
     * @param completedTasks  tasks completed since registration, or {@code -1} when this layer has no counter
     * @param rejectedTasks   tasks refused since registration, or {@code -1} when this layer has no counter
     * @param cpuLoad         share of the usable cores consumed by this pool, or {@code -1} when unavailable
     * @param saturation      saturation summary
     * @param degradedReasons reasons the pool cannot be fully managed; empty when it can
     */
    public PoolSnapshot(
        final PoolSpec spec,
        final int aliveThreads,
        final int coreThreads,
        final int activeThreads,
        final int idleThreads,
        final long queuedTasks,
        final long queueCapacity,
        final long submittedTasks,
        final long completedTasks,
        final long rejectedTasks,
        final double cpuLoad,
        final SaturationReport saturation,
        final List<DegradedReason> degradedReasons
    ) {
        this(
            spec,
            aliveThreads,
            coreThreads,
            activeThreads,
            idleThreads,
            queuedTasks,
            queueCapacity,
            submittedTasks,
            completedTasks,
            rejectedTasks,
            cpuLoad,
            saturation,
            degradedReasons,
            Optional.empty()
        );
    }

    /**
     * Returns the pool id.
     *
     * @return the id of {@link #spec()}
     */
    public PoolId id() {
        return this.spec.id();
    }

    /**
     * Returns the pool kind.
     *
     * @return the kind of {@link #spec()}
     */
    public PoolKind kind() {
        return this.spec.kind();
    }

    /**
     * Returns the integration level.
     *
     * @return the coverage of {@link #spec()}
     */
    public CoverageLevel coverage() {
        return this.spec.coverage();
    }

    /**
     * Checks whether the pool is fully manageable right now.
     *
     * @return {@code true} if at least one degraded reason was reported
     */
    public boolean degraded() {
        return !this.degradedReasons.isEmpty();
    }
}
