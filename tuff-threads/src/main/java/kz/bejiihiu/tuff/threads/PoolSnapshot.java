package kz.bejiihiu.tuff.threads;

import java.util.List;
import java.util.Objects;

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
 * @param spec            registered description of the pool
 * @param aliveThreads    threads that currently exist, at least zero
 * @param coreThreads     threads the pool keeps alive regardless of load, at least zero
 * @param activeThreads   threads currently runnable, or {@code -1} when unknown
 * @param idleThreads     threads currently waiting for work, or {@code -1} when unknown
 * @param queuedTasks     tasks waiting in the pool queue, or {@code -1} when unknown
 * @param queueCapacity   maximum queue depth, or {@code -1} when unknown or unbounded
 * @param submittedTasks  tasks accepted since registration
 * @param completedTasks  tasks completed since registration
 * @param rejectedTasks   tasks refused since registration
 * @param cpuLoad         share of the usable cores consumed by this pool over the last sampling
 *                        window, from zero to one, or {@code -1} when unavailable
 * @param saturation      saturation summary
 * @param degradedReasons reasons the pool cannot be fully managed; empty when it can
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
    List<DegradedReason> degradedReasons
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
        if (submittedTasks < 0L || completedTasks < 0L || rejectedTasks < 0L) {
            throw new IllegalArgumentException("task counters must not be negative");
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
