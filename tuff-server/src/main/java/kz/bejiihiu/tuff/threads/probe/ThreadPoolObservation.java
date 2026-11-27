package kz.bejiihiu.tuff.threads.probe;

import java.util.ArrayList;
import java.util.List;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;

/**
 * One generic reading of a pool that is not Folia's region scheduler.
 *
 * <p>The region pool has its own record because it also carries tick health. Everything else reports
 * the same six cheap numbers: alive, core, active, idle, queued and capacity, plus the threads the
 * probe could list. {@code -1} means "not measured" for every value whose interface allows it, so an
 * unavailable pool does not look like an empty one.
 *
 * @param aliveThreads  live threads, at least zero
 * @param coreThreads   threads kept regardless of load, at least zero
 * @param activeThreads runnable threads, or {@code -1} when the source could not answer
 * @param idleThreads   waiting threads, or {@code -1} when the source could not answer
 * @param queuedTasks   queued tasks, or {@code -1} when the pool does not expose a depth
 * @param queueCapacity queue capacity, or {@code -1} when it is unknown or unbounded
 * @param threads       the live threads the probe knows
 */
record ThreadPoolObservation(
    int aliveThreads,
    int coreThreads,
    int activeThreads,
    int idleThreads,
    long queuedTasks,
    long queueCapacity,
    List<Thread> threads
) implements PoolObservation {

    ThreadPoolObservation {
        threads = List.copyOf(threads);
    }

    /**
     * Classifies a thread list by state.
     *
     * <p>Same approximation as the region probe: runnable is active, waiting and timed waiting are idle,
     * everything else (blocked, new, terminated) is neither. It is the only cheap signal there is.
     *
     * @param threads  live threads
     * @param core     core threads, or {@code -1} when the owner has no floor
     * @param queued   queued tasks, or {@code -1}
     * @param capacity queue capacity, or {@code -1}
     * @return the reading
     */
    static ThreadPoolObservation fromThreads(
        final List<Thread> threads,
        final int core,
        final long queued,
        final long capacity
    ) {
        int active = 0;
        int idle = 0;
        for (final Thread thread : threads) {
            final Thread.State state = thread.getState();
            if (state == Thread.State.RUNNABLE) {
                active++;
            } else if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                idle++;
            }
        }
        return new ThreadPoolObservation(
            threads.size(),
            Math.max(0, core),
            active,
            idle,
            queued,
            capacity,
            threads
        );
    }

    /**
     * Builds a reading from a pool's own counters and the threads found by name.
     *
     * <p>Alive is the larger of the two counts: a thread that is dying can still be listed after the
     * pool stopped counting it, and reporting the smaller number would make it look unaccounted.
     *
     * @param counters pool counters
     * @param threads  threads matched by name
     * @return the reading
     */
    static ThreadPoolObservation fromCounters(final PoolCounters counters, final List<Thread> threads) {
        final int alive = Math.max(Math.max(0, counters.alive()), threads.size());
        final int active = Math.max(0, Math.min(counters.active(), alive));
        return new ThreadPoolObservation(
            alive,
            Math.max(0, counters.core()),
            active,
            Math.max(0, alive - active),
            counters.queued(),
            -1L,
            threads
        );
    }

    /**
     * Returns the reading of a source that could not be read at all.
     *
     * @return zero threads and unknown activity
     */
    static ThreadPoolObservation unavailable() {
        return new ThreadPoolObservation(0, 0, -1, -1, -1L, -1L, List.of());
    }

    /**
     * Drops nulls from a raw thread array, which a fake or a dying pool can contain.
     *
     * @param raw raw threads
     * @return a clean list
     */
    static List<Thread> nonNull(final Thread[] raw) {
        final List<Thread> threads = new ArrayList<>(raw.length);
        for (final Thread thread : raw) {
            if (thread != null) {
                threads.add(thread);
            }
        }
        return threads;
    }
}
