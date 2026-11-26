package kz.bejiihiu.tuff.threads.probe;

import ca.spottedleaf.moonrise.common.util.TickThread;
import io.papermc.paper.threadedregions.TickRegionScheduler;
import io.papermc.paper.threadedregions.TickRegions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Reads the live threads of Folia's region tick pool.
 *
 * <p>There is no public list of region threads: the pool is a leafpile scheduler, the thread class is
 * private and the only stable handles are the scheduler's own total count and the dedicated
 * {@code ThreadGroup}. The scan therefore walks to the root group, finds the region group by name and
 * keeps the threads that are also {@link TickThread}s - a name is not proof, the class is.
 *
 * <p>Everything is read lazily, on the calling thread, and nothing is cached: the sample interval belongs
 * to the metrics layer. Both reads are guarded, because a died scheduler or an absent server class must
 * report zero instead of taking a probe call down.
 */
public final class RegionThreadScan {

    /**
     * Name Folia gives the group of region tick threads.
     */
    static final String GROUP_NAME = "Folia Region Scheduler ThreadGroup";

    private final Supplier<List<Thread>> threads;
    private final IntSupplier schedulerThreads;

    /**
     * Creates the scan bound to the live server classes.
     */
    public RegionThreadScan() {
        this(RegionThreadScan::scanGroup, RegionThreadScan::schedulerThreads);
    }

    /**
     * Creates a scan with explicit sources, for tests.
     *
     * @param threads          thread list source
     * @param schedulerThreads live thread count source
     * @throws NullPointerException if a source is {@code null}
     */
    RegionThreadScan(final Supplier<List<Thread>> threads, final IntSupplier schedulerThreads) {
        this.threads = Objects.requireNonNull(threads, "threads");
        this.schedulerThreads = Objects.requireNonNull(schedulerThreads, "schedulerThreads");
    }

    /**
     * Returns the thread count the region scheduler reports.
     *
     * <p>Zero before the scheduler exists (early startup) and after it halted. This is the authoritative
     * alive number; the group scan is the fallback for the thread list.
     *
     * @return the reported count, never negative
     */
    public int aliveThreads() {
        try {
            return Math.max(0, this.schedulerThreads.getAsInt());
        } catch (RuntimeException | LinkageError _) {
            return 0;
        }
    }

    /**
     * Returns the live region tick threads.
     *
     * @return an immutable list, possibly empty
     */
    public List<Thread> threads() {
        try {
            return List.copyOf(this.threads.get());
        } catch (RuntimeException | LinkageError _) {
            return List.of();
        }
    }

    private static int schedulerThreads() {
        final TickRegionScheduler scheduler = TickRegions.getScheduler();
        return scheduler == null ? 0 : scheduler.getTotalThreadCount();
    }

    private static List<Thread> scanGroup() {
        final ThreadGroup group = findRegionGroup();
        if (group == null) {
            return List.of();
        }
        Thread[] buffer = new Thread[group.activeCount() + 16];
        int count = group.enumerate(buffer, false);
        while (count == buffer.length) {
            buffer = new Thread[buffer.length * 2];
            count = group.enumerate(buffer, false);
        }
        final List<Thread> threads = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            if (buffer[index] instanceof TickThread) {
                threads.add(buffer[index]);
            }
        }
        return threads;
    }

    private static ThreadGroup findRegionGroup() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        while (group.getParent() != null) {
            group = group.getParent();
        }
        return find(group, GROUP_NAME);
    }

    private static ThreadGroup find(final ThreadGroup group, final String name) {
        if (name.equals(group.getName())) {
            return group;
        }
        ThreadGroup[] children = new ThreadGroup[group.activeGroupCount() + 16];
        int count = group.enumerate(children, false);
        while (count == children.length) {
            children = new ThreadGroup[children.length * 2];
            count = group.enumerate(children, false);
        }
        for (int index = 0; index < count; index++) {
            final ThreadGroup found = find(children[index], name);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
