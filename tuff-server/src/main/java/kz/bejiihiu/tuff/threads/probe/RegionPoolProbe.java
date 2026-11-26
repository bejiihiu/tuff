package kz.bejiihiu.tuff.threads.probe;

import java.util.List;
import java.util.Objects;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;

/**
 * Observe-only probe of Folia's region tick pool.
 *
 * <p>The pool keeps its scheduler, its thread class and its lifecycle: this probe only reports what the
 * server already knows. The counts are the region scheduler's total thread count, the thread list comes
 * from the region {@code ThreadGroup}, and busy / waiting are derived from {@link Thread#getState()} - an
 * approximation by design, because EDF parks an idle deadline thread and the state is the only cheap
 * signal available without a patch.
 *
 * <p>Queue depth is {@code -1} ("unknown") rather than zero on purpose: the prioritised queues have no
 * public getter, and an invented zero would look like an empty queue instead of an unmeasured one. CPU
 * per thread is also not part of this reading - the metrics layer takes it from {@code ThreadMXBean} for
 * every thread the probe lists, so there is exactly one CPU source.
 */
public final class RegionPoolProbe implements PoolProbe {

    private final RegionThreadScan scan;

    /**
     * Creates the probe bound to the live region scheduler.
     */
    public RegionPoolProbe() {
        this(new RegionThreadScan());
    }

    /**
     * Creates a probe with an explicit scan, for tests.
     *
     * @param scan thread source
     * @throws NullPointerException if {@code scan} is {@code null}
     */
    RegionPoolProbe(final RegionThreadScan scan) {
        this.scan = Objects.requireNonNull(scan, "scan");
    }

    /**
     * Returns the pool this probe observes.
     *
     * @return {@link PoolId#REGION_EXECUTION}
     */
    @Override
    public PoolId pool() {
        return PoolId.REGION_EXECUTION;
    }

    /**
     * Takes one reading of the region pool.
     *
     * <p>Never throws: a scheduler that is gone, a group that cannot be enumerated or a dying thread all
     * degrade to zeros and an empty list.
     *
     * @return the reading, never {@code null}
     */
    @Override
    public PoolObservation observe() {
        try {
            final List<Thread> threads = this.scan.threads();
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
            final int alive = Math.max(this.scan.aliveThreads(), threads.size());
            return new RegionObservation(alive, active, idle, threads);
        } catch (RuntimeException | LinkageError _) {
            return RegionObservation.empty();
        }
    }

    /**
     * One reading of the region pool.
     *
     * @param aliveThreads  live region threads
     * @param activeThreads runnable threads
     * @param idleThreads   waiting or timed waiting threads
     * @param threads       the live threads
     */
    private record RegionObservation(
        int aliveThreads,
        int activeThreads,
        int idleThreads,
        List<Thread> threads
    ) implements PoolObservation {

        static RegionObservation empty() {
            return new RegionObservation(0, 0, 0, List.of());
        }

        RegionObservation {
            threads = List.copyOf(threads);
        }

        /**
         * Region threads are not retired while the scheduler runs, but an observe-only pool does not get
         * to claim a guaranteed floor; the live count is the honest number and the spec stays at zero.
         */
        @Override
        public int coreThreads() {
            return 0;
        }

        @Override
        public long queuedTasks() {
            return -1L;
        }

        @Override
        public long queueCapacity() {
            return -1L;
        }
    }
}
