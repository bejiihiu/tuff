package kz.bejiihiu.tuff.threads.internal.spi;

import java.util.List;
import java.util.Optional;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;

/**
 * One reading of a pool's current state, produced by its probe.
 *
 * <p>This is what a pool owner knows cheaply: how many threads it has, whether it can classify them,
 * what its queue looks like and which threads belong to it. Values that the owner cannot cheaply
 * expose are {@code -1}, which the metrics layer renders as "unknown" instead of zero.
 *
 * <p>The sampler calls this about once per sampling interval, from its own thread, never from a region
 * tick thread. An implementation must therefore be non blocking and allocation bounded: reading
 * counters and copying a thread array is fine, scanning all regions or taking a pool wide lock is not.
 */
public interface PoolObservation {

    /**
     * Returns the number of threads that currently exist in the pool.
     *
     * @return live threads, at least zero
     */
    int aliveThreads();

    /**
     * Returns the number of threads the pool keeps alive regardless of load.
     *
     * @return core threads, at least zero
     */
    int coreThreads();

    /**
     * Returns the number of threads that are runnable right now.
     *
     * @return active threads, or {@code -1} when the pool cannot classify them cheaply
     */
    int activeThreads();

    /**
     * Returns the number of threads that are waiting for work right now.
     *
     * @return idle threads, or {@code -1} when the pool cannot classify them cheaply
     */
    int idleThreads();

    /**
     * Returns the number of tasks waiting in the pool queue.
     *
     * @return queued tasks, or {@code -1} when the queue depth is not exposed
     */
    long queuedTasks();

    /**
     * Returns the maximum queue depth.
     *
     * @return the capacity, or {@code -1} when it is unknown or unbounded
     */
    long queueCapacity();

    /**
     * Returns the threads that belong to the pool.
     *
     * <p>A snapshot copy, not a live collection. The order is not specified; callers that need a stable
     * order sort by thread id.
     *
     * @return an immutable list of live threads, possibly empty
     */
    List<Thread> threads();

    /**
     * Returns the tick health of the regions this pool runs.
     *
     * <p>Only a pool that executes region ticks can answer this; every other pool keeps the default and
     * reports nothing. Absent means "this probe has no region view" or "it could not be read this pass",
     * never "zero regions".
     *
     * @return the region summary, or empty when the pool runs no regions
     */
    default Optional<RegionPoolMetrics> regionMetrics() {
        return Optional.empty();
    }
}
