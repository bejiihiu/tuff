package kz.bejiihiu.tuff.threads;

import java.util.List;
import java.util.Optional;

/**
 * Read only view of the thread layer's collected metrics.
 *
 * <p>Metrics are sampled by the manager, not by the caller: a snapshot is a consistent copy taken at a
 * known moment, and repeated reads do not trigger a new scan. The view answers the questions an
 * operator asks during an incident - how many threads exist, which pool owns them, what is saturated,
 * what was created and destroyed recently - without requiring a profiler or a thread dump.
 *
 * <p>When collection is disabled {@link #enabled()} is {@code false} and the snapshot carries no
 * measurement: it still lists the registered pools and the threads the registry accounts for, with
 * {@code -1} for everything that would have been measured, an empty per thread list and
 * {@link DegradedReason#METRICS_UNAVAILABLE} at the top level. Callers must check the flag before
 * treating any number as a measurement.
 *
 * <p>Threading: all methods are thread safe and non blocking. Snapshot construction is bounded and does
 * not take pool locks, so it may be called from a command handler; it still should not be called from a
 * region tick thread in a loop, because it copies per thread data.
 */
public interface ResourceMetrics {

    /**
     * Checks whether metrics collection is enabled.
     *
     * @return {@code true} when the manager is allowed to sample and publish metrics
     */
    boolean enabled();

    /**
     * Takes a consistent snapshot of the whole thread layer.
     *
     * <p>The returned instance describes one moment: pool and thread entries agree with each other even
     * while the manager keeps changing. Unregistered threads are not part of it; use
     * {@link #unregisteredThreads()} for that report.
     *
     * @return the snapshot, never {@code null}
     */
    ResourceSnapshot snapshot();

    /**
     * Looks up the latest observation of one pool.
     *
     * @param pool pool id
     * @return the pool snapshot, or an empty optional if the pool is not registered
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    Optional<PoolSnapshot> pool(PoolId pool);

    /**
     * Returns the latest saturation of one pool.
     *
     * @param pool pool id
     * @return the report, or {@link SaturationReport#unknown(PoolId)} when the pool has no probe
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    SaturationReport saturation(PoolId pool);

    /**
     * Returns the last captured threads of one pool.
     *
     * <p>The list is a copy from the most recent sample, not a live view. It is empty when per thread
     * collection is disabled or the pool has no live threads.
     *
     * @param pool pool id
     * @return an immutable list of thread snapshots, possibly empty
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    List<ThreadSnapshot> threads(PoolId pool);

    /**
     * Returns threads that exist in the JVM but are not accounted to any pool.
     *
     * <p>This is the leak and bypass report: JDK, logger, JFR and third party threads are expected
     * here and are filtered by the implementation's allow list, while a server owned thread that was
     * created outside the factory is a finding. The list is produced by an explicit scan, so it is
     * empty when the scan is disabled or has not run yet.
     *
     * @return an immutable list of unaccounted threads, possibly empty
     */
    List<ThreadSnapshot> unregisteredThreads();

    /**
     * Returns the number of threads created per second over the last sampling window.
     *
     * @return a non negative rate
     */
    double creationRatePerSecond();

    /**
     * Returns the number of threads terminated per second over the last sampling window.
     *
     * @return a non negative rate
     */
    double terminationRatePerSecond();

    /**
     * Returns the number of live registered threads.
     *
     * @return the total, zero when nothing is registered or collection is disabled
     */
    int totalThreads();

    /**
     * Returns the number of registered threads that are currently runnable.
     *
     * @return the active count
     */
    int activeThreads();

    /**
     * Returns the number of registered threads that are currently waiting for work.
     *
     * @return the idle count
     */
    int idleThreads();
}
