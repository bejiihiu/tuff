package kz.bejiihiu.tuff.threads.probe;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import kz.bejiihiu.tuff.threads.region.FoliaRegionTickProvider;
import kz.bejiihiu.tuff.threads.region.RegionPoolMetricsAggregator;
import kz.bejiihiu.tuff.threads.region.RegionReading;
import kz.bejiihiu.tuff.threads.region.RegionTickProvider;

/**
 * Observe-only probe of Folia's region tick pool.
 *
 * <p>The pool keeps its scheduler, its thread class and its lifecycle: this probe only reports what the
 * server already knows. The counts are the region scheduler's total thread count, the thread list comes
 * from the region {@code ThreadGroup}, and busy / waiting are derived from {@link Thread#getState()} - an
 * approximation by design, because EDF parks an idle deadline thread and the state is the only cheap
 * signal available without a patch.
 *
 * <p>Tick health is a second, independent read: {@link RegionTickProvider} hands over one reading per
 * region and {@link RegionPoolMetricsAggregator} folds them into the summary the snapshot carries. The two
 * reads fail apart - a broken thread scan still reports regions, and a broken region read still reports
 * threads - because "the pool has four threads" and "the regions are healthy" are separate answers.
 *
 * <p>Queue depth is {@code -1} ("unknown") rather than zero on purpose: the prioritised queues have no
 * public getter, and an invented zero would look like an empty queue instead of an unmeasured one. CPU
 * per thread is also not part of this reading - the metrics layer takes it from {@code ThreadMXBean} for
 * every thread the probe lists, so there is exactly one CPU source.
 */
public final class RegionPoolProbe implements PoolProbe {

    private final RegionThreadScan scan;
    private final RegionTickProvider regions;

    /**
     * Creates the probe bound to the live region scheduler.
     */
    public RegionPoolProbe() {
        this(new RegionThreadScan(), new FoliaRegionTickProvider());
    }

    /**
     * Creates a probe with an explicit scan and no region tick source, for tests.
     *
     * @param scan thread source
     * @throws NullPointerException if {@code scan} is {@code null}
     */
    RegionPoolProbe(final RegionThreadScan scan) {
        this(scan, List::of);
    }

    /**
     * Creates a probe with explicit sources, for tests.
     *
     * @param scan    thread source
     * @param regions region tick source
     * @throws NullPointerException if a source is {@code null}
     */
    RegionPoolProbe(final RegionThreadScan scan, final RegionTickProvider regions) {
        this.scan = Objects.requireNonNull(scan, "scan");
        this.regions = Objects.requireNonNull(regions, "regions");
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
     * <p>Never throws: a scheduler that is gone, a group that cannot be enumerated, a dying thread or an
     * unreadable tick window all degrade to zeros, an empty list or an absent summary.
     *
     * @return the reading, never {@code null}
     */
    @Override
    public PoolObservation observe() {
        final Optional<RegionPoolMetrics> metrics = this.regionMetrics();
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
            return new RegionObservation(alive, active, idle, threads, metrics);
        } catch (RuntimeException | LinkageError _) {
            return RegionObservation.empty(metrics);
        }
    }

    private Optional<RegionPoolMetrics> regionMetrics() {
        try {
            final List<RegionReading> readings = this.regions.read();
            // no regions is "nothing to say", not a zero reading: an empty summary line under the pool
            // would look like a measured pool of zero regions
            return readings.isEmpty()
                ? Optional.empty()
                : Optional.of(RegionPoolMetricsAggregator.aggregate(readings));
        } catch (RuntimeException | LinkageError _) {
            return Optional.empty();
        }
    }

    /**
     * One reading of the region pool.
     *
     * @param aliveThreads  live region threads
     * @param activeThreads runnable threads
     * @param idleThreads   waiting or timed waiting threads
     * @param threads       the live threads
     * @param regionMetrics tick health of the regions, or empty when it could not be read
     */
    private record RegionObservation(
        int aliveThreads,
        int activeThreads,
        int idleThreads,
        List<Thread> threads,
        Optional<RegionPoolMetrics> regionMetrics
    ) implements PoolObservation {

        static RegionObservation empty(final Optional<RegionPoolMetrics> regionMetrics) {
            return new RegionObservation(0, 0, 0, List.of(), regionMetrics);
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
