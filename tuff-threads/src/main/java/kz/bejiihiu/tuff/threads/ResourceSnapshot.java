package kz.bejiihiu.tuff.threads;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Consistent point in time snapshot of the whole thread layer.
 *
 * <p>All values in one instance describe the same moment: reading two accessors of the same snapshot
 * never mixes two sampling passes. The snapshot is immutable and safe to hand to a command, a JFR
 * event or a log writer.
 *
 * <p>Sizes are totals of <b>registered</b> threads. Unregistered threads are a separate report: they
 * are not part of {@link #threads()} and not part of the totals, because the manager cannot attribute
 * them to a pool. A non empty unregistered list is a finding, not a metric.
 *
 * @param capturedAt               moment the snapshot was taken
 * @param usableCores              cores available to managed pools after the reserve, at least one
 * @param reservedCores            cores kept out of the managed budget, at least zero
 * @param totalThreads             live registered threads
 * @param activeThreads            registered threads that are runnable
 * @param idleThreads              registered threads waiting for work
 * @param peakThreads              highest number of live registered threads since start
 * @param creationRatePerSecond    threads created per second over the last sampling window
 * @param terminationRatePerSecond threads terminated per second over the last sampling window
 * @param processCpuLoad           share of the machine consumed by the whole JVM over the last sampling
 *                                 window, from zero to one, or {@code -1} when unavailable
 * @param pools                    per pool snapshots, in id order
 * @param threads                  per thread snapshots, may be empty when per thread collection is
 *                                 disabled
 * @param degradedReasons          union of the reasons reported by all pools
 */
public record ResourceSnapshot(
    Instant capturedAt,
    int usableCores,
    int reservedCores,
    int totalThreads,
    int activeThreads,
    int idleThreads,
    int peakThreads,
    double creationRatePerSecond,
    double terminationRatePerSecond,
    double processCpuLoad,
    List<PoolSnapshot> pools,
    List<ThreadSnapshot> threads,
    Set<DegradedReason> degradedReasons
) {

    /**
     * Validates and freezes a resource snapshot.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if a count or a share is outside its range
     */
    public ResourceSnapshot {
        Objects.requireNonNull(capturedAt, "capturedAt");
        Objects.requireNonNull(pools, "pools");
        Objects.requireNonNull(threads, "threads");
        Objects.requireNonNull(degradedReasons, "degradedReasons");
        if (usableCores < 1) {
            throw new IllegalArgumentException("usableCores must be at least 1, got " + usableCores);
        }
        if (reservedCores < 0) {
            throw new IllegalArgumentException("reservedCores must not be negative, got " + reservedCores);
        }
        if (totalThreads < 0 || activeThreads < 0 || idleThreads < 0 || peakThreads < 0) {
            throw new IllegalArgumentException("thread counts must not be negative");
        }
        if (peakThreads < totalThreads) {
            throw new IllegalArgumentException(
                "peakThreads must not be below totalThreads, got " + peakThreads + " < " + totalThreads
            );
        }
        if (!(creationRatePerSecond >= 0.0d) || !(terminationRatePerSecond >= 0.0d)) {
            throw new IllegalArgumentException("thread rates must not be negative");
        }
        if (processCpuLoad != -1.0d && (!(processCpuLoad >= 0.0d) || processCpuLoad > 1.0d)) {
            throw new IllegalArgumentException("processCpuLoad must be -1 or in [0, 1], got " + processCpuLoad);
        }
        pools = List.copyOf(pools);
        threads = List.copyOf(threads);
        degradedReasons = Set.copyOf(degradedReasons);
    }

    /**
     * Looks a pool up in the snapshot.
     *
     * @param id pool id
     * @return the pool snapshot, or an empty optional if no pool with that id is registered
     * @throws NullPointerException if {@code id} is {@code null}
     */
    public Optional<PoolSnapshot> pool(final PoolId id) {
        for (final PoolSnapshot pool : this.pools) {
            if (pool.id().equals(id)) {
                return Optional.of(pool);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns all pools of one kind.
     *
     * @param kind kind to filter by
     * @return an immutable list of matching snapshots, in id order
     * @throws NullPointerException if {@code kind} is {@code null}
     */
    public List<PoolSnapshot> pools(final PoolKind kind) {
        return this.pools.stream().filter(pool -> pool.kind() == kind).toList();
    }

    /**
     * Checks whether any pool reported a degraded reason.
     *
     * @return {@code true} if {@link #degradedReasons()} is not empty
     */
    public boolean degraded() {
        return !this.degradedReasons.isEmpty();
    }

    /**
     * Looks a thread up by JVM id.
     *
     * @param threadId JVM thread id
     * @return the thread snapshot, or an empty optional if it was not captured
     */
    public Optional<ThreadSnapshot> thread(final long threadId) {
        for (final ThreadSnapshot thread : this.threads) {
            if (thread.threadId() == threadId) {
                return Optional.of(thread);
            }
        }
        return Optional.empty();
    }
}
