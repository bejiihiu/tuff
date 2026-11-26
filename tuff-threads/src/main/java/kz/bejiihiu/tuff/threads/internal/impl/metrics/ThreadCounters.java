package kz.bejiihiu.tuff.threads.internal.impl.metrics;

/**
 * The narrow slice of JVM counters the sampler needs.
 *
 * <p>It exists so the sampler can be tested without a real runtime and so the "this JVM cannot measure"
 * case is a value, not a guess: a fake returns {@code -1} and the snapshots say unknown, exactly like a
 * JVM without CPU accounting does. Production reads {@code ThreadMXBean} and the platform OS bean.
 *
 * <p>Everything here is a cheap read: no allocation, no lock, no stack dump. Calls come from the sampler
 * only, once per thread per sampling window.
 */
public interface ThreadCounters {

    /**
     * Checks whether the runtime can report per thread CPU time.
     *
     * @return {@code true} if {@link #cpuTimeNanos(long)} may return a real value
     */
    boolean cpuTimeSupported();

    /**
     * Returns the total CPU time a thread consumed since it started.
     *
     * @param threadId JVM thread id
     * @return nanoseconds, or {@code -1} when the id is unknown or CPU time is unsupported
     */
    long cpuTimeNanos(long threadId);

    /**
     * Returns how many threads the JVM started since it came up.
     *
     * @return the monotonic started count
     */
    long startedTotal();

    /**
     * Returns how many threads are alive right now.
     *
     * @return the live count
     */
    long liveThreads();

    /**
     * Returns the CPU load of the whole process over the platform's averaging window.
     *
     * @return a share between zero and one, or {@code -1} when the platform cannot estimate it
     */
    double processCpuLoad();
}
