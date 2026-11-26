package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import org.jspecify.annotations.Nullable;

/**
 * Observe-only probe of the compute engine's pools.
 *
 * <p>The engine owns the queue, the busy flags and the lane limits, so this probe only translates its
 * {@link SchedulerMetrics} snapshot into a {@link PoolObservation}. Nothing is counted twice: busy and
 * idle come from the engine's worker bookkeeping (a worker holding a permit and polling the queue is
 * idle there and {@code RUNNABLE} here), queue depth comes from the queue, and created/terminated
 * rates come from the thread layer's own registry.
 *
 * <p>The thread list is a name scan, not a registry read: at L4 the layer already knows the workers,
 * but at L1 it does not, and a snapshot without threads is exactly the case where an operator cannot
 * see CPU per worker. The engine's metrics sampler ({@link ComputeSettings#METRICS_THREAD_PREFIX}) is
 * part of the platform pool too, so it is matched as well - otherwise it belongs to no pool and shows
 * up as an unknown thread even though the engine owns it. Virtual threads are skipped - they do not
 * show up in a thread dump and the lane reports its own live count.
 *
 * <p>Never throws: a stopped engine, a null snapshot or a dying thread all degrade to zeros.
 */
final class ComputePoolProbe implements PoolProbe {

    private final PoolId pool;
    private final Supplier<@Nullable SchedulerMetrics> metrics;
    private final String prefix;
    private final int coreThreads;

    /**
     * Creates the probe.
     *
     * @param pool        pool this probe observes
     * @param metrics     source of the engine snapshot, or {@code null} while no engine exists
     * @param prefix      worker name prefix, used for the thread list
     * @param coreThreads workers the pool keeps alive regardless of load
     * @throws NullPointerException if a required reference is {@code null}
     */
    ComputePoolProbe(
        final PoolId pool,
        final Supplier<@Nullable SchedulerMetrics> metrics,
        final String prefix,
        final int coreThreads
    ) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.coreThreads = coreThreads;
    }

    @Override
    public PoolId pool() {
        return this.pool;
    }

    @Override
    public PoolObservation observe() {
        final @Nullable SchedulerMetrics snapshot;
        try {
            snapshot = this.metrics.get();
        } catch (RuntimeException | LinkageError _) {
            return Reading.empty();
        }
        if (snapshot == null) {
            return Reading.empty();
        }
        try {
            return this.pool.equals(PoolId.COMPUTE_VIRTUAL) ? virtual(snapshot) : this.platform(snapshot);
        } catch (RuntimeException | LinkageError _) {
            return Reading.empty();
        }
    }

    private PoolObservation platform(final SchedulerMetrics snapshot) {
        return new Reading(
            snapshot.totalWorkers(),
            this.coreThreads,
            snapshot.activeWorkers(),
            snapshot.idleWorkers(),
            snapshot.queuedTasks(),
            snapshot.queueCapacity(),
            scan(this.prefix)
        );
    }

    private static PoolObservation virtual(final SchedulerMetrics snapshot) {
        final SchedulerMetrics.VirtualLane lane = snapshot.lanes().virtual();
        if (!lane.enabled()) {
            return Reading.empty();
        }
        // the pump is the one platform thread the lane always has; the running virtual threads come
        // from the lane itself. Idle stays unknown: the lane exposes running tasks, not pump state
        return new Reading(lane.running() + 1, 1, lane.running(), -1, lane.queued(), -1L, List.of());
    }

    /**
     * Collects the live platform threads of the compute pool.
     *
     * <p>A thread belongs to the platform pool when it is named with the worker prefix or when it is
     * the engine's metrics sampler. The virtual lane's pump and the completion threads also match the
     * prefix but belong to {@link PoolId#COMPUTE_VIRTUAL}, so they are filtered out here instead of
     * showing up under the platform pool.
     *
     * @param prefix worker name prefix
     * @return the matching threads, possibly empty
     */
    static List<Thread> scan(final String prefix) {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        while (group.getParent() != null) {
            group = group.getParent();
        }
        Thread[] buffer = new Thread[group.activeCount() + 16];
        int count = group.enumerate(buffer, true);
        while (count == buffer.length) {
            buffer = new Thread[buffer.length * 2];
            count = group.enumerate(buffer, true);
        }
        final List<Thread> found = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            final Thread thread = buffer[index];
            if (thread == null || thread.isVirtual()) {
                continue;
            }
            if (belongsToPlatformPool(thread.getName(), prefix)) {
                found.add(thread);
            }
        }
        return found;
    }

    private static boolean belongsToPlatformPool(final String name, final String prefix) {
        if (name.startsWith(prefix + "-virtual") || name.startsWith(prefix + "-completion")) {
            // lane threads match the worker prefix but are accounted under the virtual pool
            return false;
        }
        return name.startsWith(prefix) || name.startsWith(ComputeSettings.METRICS_THREAD_PREFIX);
    }

    /**
     * One reading of a compute pool.
     *
     * @param aliveThreads  live threads
     * @param coreThreads   threads the pool keeps alive
     * @param activeThreads busy threads, or {@code -1} when unknown
     * @param idleThreads   idle threads, or {@code -1} when unknown
     * @param queuedTasks   queued tasks, or {@code -1} when unknown
     * @param queueCapacity queue limit, or {@code -1} when unknown
     * @param threads       live threads
     */
    private record Reading(
        int aliveThreads,
        int coreThreads,
        int activeThreads,
        int idleThreads,
        long queuedTasks,
        long queueCapacity,
        List<Thread> threads
    ) implements PoolObservation {

        Reading {
            threads = List.copyOf(threads);
        }

        static Reading empty() {
            return new Reading(0, 0, 0, 0, -1L, -1L, List.of());
        }
    }
}
