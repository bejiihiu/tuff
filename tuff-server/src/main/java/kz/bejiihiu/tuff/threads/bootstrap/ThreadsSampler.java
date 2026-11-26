package kz.bejiihiu.tuff.threads.bootstrap;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.internal.runtime.ThreadManagerImpl;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * Background refresher of every number the thread layer shows.
 *
 * <p>Folia runs console commands on the global region tick thread, so a command cannot read the region
 * scheduler or walk the JVM thread table itself: the regioniser lock and a thread enumeration are
 * exactly what a tick thread must not do, and the guards that used to protect it made the region lines
 * and the unknown count unreachable from the console. This class moves both reads to one daemon
 * thread that does the work on a fixed clock and publishes an immutable sample; the command only ever
 * reads that sample.
 *
 * <p>One thread is enough and it is accounted like every other Tuff thread: the {@code service} pool
 * is raised to {@link CoverageLevel#L2} and the thread is built by the {@code ThreadFactoryProvider},
 * so it appears in the snapshot and is never counted as unknown. Its name is
 * {@value #THREAD_NAME} (the provider appends a counter); platform threads only, no virtual lane.
 *
 * <p>Order inside one pass matters: the metrics pass first (it runs the probes, so the region list is
 * walked from this thread), then the unaccounted thread scan, then both are published together. The
 * unknown count is computed against the same snapshot, which is what stops a thread the region probe
 * listed from being reported as unknown.
 *
 * <p>Shutdown is a flag and an interrupt, never a join: this path runs in the window where the
 * watchdog is still armed and a blocked thread looks like a hung server. The thread is a daemon, so a
 * late pass cannot keep the JVM alive; a pass that races the stop reads the closed sampler and keeps
 * the last values.
 */
final class ThreadsSampler {

    /**
     * Base name of the refresher thread.
     */
    static final String THREAD_NAME = "Tuff Threads Sampler";

    private static final Logger LOGGER = LogManager.getLogger("Tuff/Threads");

    /**
     * How often the sample is refreshed. One second matches the metrics policy and the region read
     * throttle, so a command never sees a sample older than the policy allows.
     */
    private static final long INTERVAL_MILLIS = 1_000L;

    /**
     * The refresher sleeps almost all the time and only touches atomics; one core is the smallest cap
     * the ledger can express, and it claims no reserve.
     */
    private static final ResourceBudget BUDGET = ResourceBudget.fixed(1, 0);

    private static final Thread.UncaughtExceptionHandler FAILURE =
        (thread, failure) -> LOGGER.error("Tuff thread sampler {} died", thread.getName(), failure);

    private final ThreadManagerRuntime runtime;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<Sample> sample = new AtomicReference<>();
    private volatile @Nullable Thread thread;

    private ThreadsSampler(final ThreadManagerRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * Registers the refresher pool, creates its daemon thread and takes the first pass.
     *
     * <p>The first pass runs on the calling thread on purpose: it costs tens of microseconds and it
     * means the first {@code /tuff threads} already has data instead of an empty cache. Called from
     * the server startup hook, which is not a region tick thread.
     *
     * @param runtime running thread manager
     * @return the started refresher
     * @throws NullPointerException  if {@code runtime} is {@code null}
     * @throws IllegalStateException if the manager refuses the pool or the thread
     */
    static ThreadsSampler start(final ThreadManagerRuntime runtime) {
        final UnifiedThreadManager manager = runtime.manager();
        manager.pools().register(serviceSpec());
        final ThreadsSampler sampler = new ThreadsSampler(runtime);
        final Thread thread = manager.threadFactory().newThread(
            ThreadSpec.of(PoolId.SERVICE, THREAD_NAME, FAILURE).withDaemon(true),
            sampler::loop
        );
        sampler.thread = thread;
        // warm the cache before anybody can ask for it, so "no sample yet" only ever means "the
        // sampler could not start", not "the first command came too early"
        sampler.refresh();
        thread.start();
        return sampler;
    }

    /**
     * Returns the last published sample.
     *
     * @return the sample, or {@code null} before the first pass finished
     */
    @Nullable Sample sample() {
        return this.sample.get();
    }

    /**
     * Asks the refresher to stop.
     *
     * <p>No join, no sleep, no interrupt storm: the loop checks the flag and the interrupt ends the
     * current sleep, so the thread exits within one interval at worst. Safe to call twice.
     */
    void stop() {
        this.running.set(false);
        final Thread current = this.thread;
        if (current != null) {
            current.interrupt();
        }
    }

    /**
     * Checks whether the loop is still allowed to run, for diagnostics.
     *
     * @return {@code true} until {@link #stop()}
     */
    boolean isRunning() {
        return this.running.get();
    }

    private void loop() {
        while (this.running.get()) {
            try {
                Thread.sleep(INTERVAL_MILLIS);
            } catch (InterruptedException _) {
                // stop() interrupts the sleep; a stray interrupt also ends the loop instead of
                // spinning on a thread that is going away anyway
                return;
            }
            if (!this.running.get()) {
                return;
            }
            this.refresh();
        }
    }

    /**
     * Takes one pass and publishes it.
     *
     * <p>Never throws: a probe, a registry or a thread table that fails while the server is starting
     * or stopping must leave the previous sample in place, not kill the refresher.
     */
    private void refresh() {
        try {
            final UnifiedThreadManager manager = this.runtime.manager();
            final ResourceSnapshot snapshot = manager.snapshot();
            final int unknown;
            if (manager.metrics().enabled()) {
                // forced, not the throttled accessor: the refresher is the only scan owner now, and
                // the unknown list should move on the same one second clock as the pools
                final List<ThreadSnapshot> unregistered = manager instanceof ThreadManagerImpl impl
                    ? impl.scanUnregistered()
                    : manager.metrics().unregisteredThreads();
                unknown = countUnknown(unregistered, snapshot);
            } else {
                // the structural snapshot is not a measurement; saying zero unknown would be a lie
                unknown = -1;
            }
            this.sample.set(new Sample(snapshot, unknown, System.nanoTime()));
        } catch (RuntimeException | LinkageError ex) {
            LOGGER.warn("Could not refresh the Tuff thread sample", ex);
        }
    }

    /**
     * Counts threads that are neither registered nor listed by a pool probe.
     *
     * <p>The module's registry only knows threads the manager created, so a thread an observe-only
     * probe lists (Folia's region pool, the compute engine) is still "unregistered" from the
     * registry's point of view. The command must not call those unknown: the probe did account for
     * them, just not through the factory.
     *
     * @param unregistered the raw unaccounted thread report
     * @param snapshot     the snapshot whose per thread list contains probe observations
     * @return how many reported threads no pool accounted for
     */
    static int countUnknown(final List<ThreadSnapshot> unregistered, final ResourceSnapshot snapshot) {
        if (unregistered.isEmpty()) {
            return 0;
        }
        final Set<Long> observed = new HashSet<>(snapshot.threads().size() * 2);
        for (final ThreadSnapshot thread : snapshot.threads()) {
            observed.add(thread.threadId());
        }
        int count = 0;
        for (final ThreadSnapshot thread : unregistered) {
            if (!observed.contains(thread.threadId())) {
                count++;
            }
        }
        return count;
    }

    /**
     * The service pool at L2, so the provider is allowed to create the refresher thread and the
     * snapshot can show it.
     *
     * @return the pool description
     */
    private static PoolSpec serviceSpec() {
        return new PoolSpec(
            PoolId.SERVICE,
            PoolKind.SERVICE,
            "tuff",
            "short lived server services and the thread metrics sampler",
            1,
            -1,
            CoverageLevel.L2,
            BUDGET
        );
    }

    /**
     * One published pass: what was measured, how many threads nobody accounted for and when.
     *
     * @param snapshot       the measured resource snapshot
     * @param unknownThreads threads no pool accounted for, or -1 when metrics are disabled
     * @param takenAtNanos   monotonic timestamp of the pass
     */
    record Sample(ResourceSnapshot snapshot, int unknownThreads, long takenAtNanos) {

        /**
         * Returns how old this sample is at the moment of the call.
         *
         * @return age in milliseconds, never negative
         */
        long ageMillis() {
            return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.takenAtNanos));
        }
    }
}
