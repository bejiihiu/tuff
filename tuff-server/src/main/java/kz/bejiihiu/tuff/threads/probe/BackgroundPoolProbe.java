package kz.bejiihiu.tuff.threads.probe;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import net.minecraft.util.Util;
import org.jspecify.annotations.Nullable;

/**
 * Observe-only probe of {@code Util.backgroundExecutor}.
 *
 * <p>The executor is Paper's {@code Worker-Main-N} fork-join pool. A {@code ForkJoinPool} exposes its
 * size, active count, parallelism and queued task count for free, so the probe asks it directly and
 * only falls back to names when the configuration made the executor synchronous.
 *
 * <p>Nothing is submitted, parked, resized or shut down. The fork-join pool is allowed to grow above its
 * parallelism while workers block, which is why {@code alive} can be larger than {@code core}; both
 * numbers are printed as they are instead of pretending the pool is fixed.
 */
public final class BackgroundPoolProbe implements PoolProbe {

    /**
     * Name format of the background workers, e.g. {@code Worker-Main-1}.
     */
    static final String THREAD_PREFIX = "Worker-Main-";

    private final PoolCountersSource counters;
    private final LiveThreadSource threads;

    /**
     * Creates the probe bound to the live server.
     */
    public BackgroundPoolProbe() {
        this(BackgroundPoolProbe::serverCounters, LiveThreadSource.jvm());
    }

    /**
     * Creates a probe with explicit sources, for tests.
     *
     * @param counters counter source
     * @param threads  thread source
     * @throws NullPointerException if a source is {@code null}
     */
    BackgroundPoolProbe(final PoolCountersSource counters, final LiveThreadSource threads) {
        this.counters = Objects.requireNonNull(counters, "counters");
        this.threads = Objects.requireNonNull(threads, "threads");
    }

    @Override
    public PoolId pool() {
        return PoolId.BACKGROUND;
    }

    /**
     * Takes one reading of the background pool.
     *
     * <p>Never throws: a source that is gone reads as unavailable.
     *
     * @return the reading, never {@code null}
     */
    @Override
    public PoolObservation observe() {
        final @Nullable List<Thread> matched = this.matchingThreads();
        final @Nullable PoolCounters measured = this.countersSafely();
        if (measured != null) {
            return ThreadPoolObservation.fromCounters(measured, matched == null ? List.of() : matched);
        }
        if (matched == null) {
            return ThreadPoolObservation.unavailable();
        }
        return ThreadPoolObservation.fromThreads(matched, 0, -1L, -1L);
    }

    private @Nullable List<Thread> matchingThreads() {
        try {
            final List<Thread> matched = new ArrayList<>();
            for (final Thread thread : this.threads.threads()) {
                if (thread != null && thread.getName() != null && thread.getName().startsWith(THREAD_PREFIX)) {
                    matched.add(thread);
                }
            }
            return matched;
        } catch (RuntimeException | LinkageError _) {
            return null;
        }
    }

    private @Nullable PoolCounters countersSafely() {
        try {
            return this.counters.counters();
        } catch (RuntimeException | LinkageError _) {
            return null;
        }
    }

    private static @Nullable PoolCounters serverCounters() {
        final ExecutorService executor = Util.backgroundExecutor().service();
        if (executor instanceof ForkJoinPool pool) {
            return new PoolCounters(
                pool.getPoolSize(),
                pool.getParallelism(),
                pool.getActiveThreadCount(),
                pool.getQueuedTaskCount()
            );
        }
        return null;
    }
}
