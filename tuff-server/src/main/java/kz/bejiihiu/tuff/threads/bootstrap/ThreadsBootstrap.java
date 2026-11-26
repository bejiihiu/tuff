package kz.bejiihiu.tuff.threads.bootstrap;

import ca.spottedleaf.moonrise.common.util.TickThread;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import kz.bejiihiu.tuff.compute.config.ConfigProblem;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.config.ThreadsConfig;
import kz.bejiihiu.tuff.threads.internal.runtime.DefaultThreadManagerFactory;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import kz.bejiihiu.tuff.threads.probe.RegionPoolProbe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * Owns the unified thread layer for the lifetime of one server process.
 *
 * <p>Two hooks drive it, both already exist in the server for the compute engine, so the thread layer
 * needs no patch of its own:
 * <ol>
 *     <li>{@link #start()} runs first, at the top of {@code ComputeBootstrap.start()}. The runtime is
 *     built from {@code thread-management} and started; starting it registers the canonical pools at
 *     their configured coverage. The region pool is then attached to a probe, because the manager cannot
 *     see threads it did not create.</li>
 *     <li>{@link #stop()} runs last, at the bottom of {@code ComputeBootstrap.stop()}. The compute engine
 *     stops first: its workers get their grace period, and only then does the accounting layer go away.
 *     The runtime's shutdown does not join, sleep or interrupt anything, so it is safe inside the
 *     shutdown window.</li>
 * </ol>
 *
 * <p>With {@code thread-management.enabled: false} no runtime is created at all: no pool is registered,
 * no probe runs and no thread exists. That is the honest kill switch, not a paused manager.
 *
 * <p>Every hook is wrapped so a bad configuration or a failed runtime is logged instead of thrown. The
 * server must start and stop normally whatever this subsystem does.
 *
 * <p>The lifecycle is an instance so it can be driven more than once in a process, which is what the
 * startup and shutdown tests do; {@link #start()}, {@link #stop()} and {@link #status()} act on the one
 * instance bound to the server.
 */
public final class ThreadsBootstrap {

    private static final Logger LOGGER = LogManager.getLogger("Tuff/Threads");

    /**
     * Bound on the wait for the stopped state.
     *
     * <p>{@code shutdown} is synchronous, so the wait returns at once; the bound only exists so a future
     * asynchronous step cannot turn this hook into a hidden sleep during shutdown.
     */
    private static final Duration AWAIT_CAP = Duration.ofSeconds(1L);

    private static final ThreadsBootstrap SERVER = new ThreadsBootstrap();

    private final Object lock = new Object();
    private volatile ThreadsStatus status = ThreadsStatus.notInitialised();
    private volatile @Nullable ThreadManagerRuntime runtime;
    private boolean started;

    /**
     * Creates a lifecycle with no runtime. The server uses the static facade, tests use this directly.
     */
    ThreadsBootstrap() {
    }

    /**
     * Reads the configuration and starts the thread layer.
     *
     * <p>Called once per process from the existing compute startup hook. A second call is a no-op.
     */
    public static void start() {
        SERVER.startWith(ThreadsConfig.serverWide());
    }

    /**
     * Stops the thread layer during server shutdown.
     *
     * <p>Called at the end of the existing compute stop hook, after the compute engine is down. The
     * manager owns no executor and no thread of its own, so this is bookkeeping only: detach the probes,
     * close the sampler, refuse further registration. Nothing here waits for game work.
     */
    public static void stop() {
        SERVER.shutdown();
    }

    /**
     * Returns what the bootstrap currently knows, including a fresh snapshot when the layer runs.
     *
     * <p>The snapshot is taken here rather than in the command so the report stays a pure function. The
     * call is cheap when the sampling interval has not elapsed and never blocks on a running pass.
     *
     * @return an immutable status, never {@code null}
     */
    public static ThreadsStatus status() {
        return SERVER.currentStatus();
    }

    /**
     * Starts the layer from a specific configuration.
     *
     * @param config configuration to read
     */
    void startWith(final ThreadsConfig config) {
        synchronized (this.lock) {
            if (this.started) {
                return;
            }
            this.started = true;
            ThreadManagerRuntime created = null;
            try {
                final ThreadsConfig.ThreadsLoadResult result = config.load();
                if (result.missingFile()) {
                    // the thread layer starts before the compute engine, which is what writes the template
                    LOGGER.info("Thread config: {} does not exist yet; using the observe-only defaults",
                        config.file());
                } else {
                    for (final ConfigProblem problem : result.problems()) {
                        LOGGER.warn("Thread config: {}", problem);
                    }
                }
                final ThreadManagementSettings settings = result.settings();
                if (!settings.enabled()) {
                    this.status = ThreadsStatus.disabled(settings);
                    LOGGER.info("Tuff thread management is disabled in {} (thread-management.enabled: false); "
                        + "no pool is registered and no thread is created", config.file());
                    return;
                }
                created = new DefaultThreadManagerFactory().create(settings);
                created.start();
                // the probe is attached after start, because start is what registers the pools; attaching
                // earlier would be rejected by the probe registry with "pool is not registered"
                created.attachProbe(new RegionPoolProbe());
                this.runtime = created;
                this.status = ThreadsStatus.running(settings);
                logStarted(created.manager());
            } catch (final Throwable ex) {
                this.runtime = null;
                this.status = ThreadsStatus.failed();
                if (created != null) {
                    shutdownQuietly(created);
                }
                LOGGER.error("The Tuff thread layer failed to start; the server runs without it", ex);
            }
        }
    }

    /**
     * Stops the layer and forgets the runtime.
     */
    void shutdown() {
        synchronized (this.lock) {
            final ThreadManagerRuntime current = this.runtime;
            final ThreadsStatus previous = this.status;
            this.runtime = null;
            this.status = ThreadsStatus.notInitialised();
            if (current == null) {
                // one line even when there is nothing to stop, otherwise an operator cannot tell the hook
                // ran at all (the disabled start already logged its own reason)
                LOGGER.info("Tuff thread management is {}; nothing to stop",
                    previous.state().name().toLowerCase(Locale.ROOT).replace('_', ' '));
                return;
            }
            try {
                current.shutdown(Duration.ZERO);
                if (!current.awaitTermination(AWAIT_CAP)) {
                    LOGGER.warn("The Tuff thread layer did not report a stopped state within {} ms",
                        AWAIT_CAP.toMillis());
                }
                LOGGER.info("Tuff thread management stopped");
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while waiting for the Tuff thread layer to stop");
            } catch (final Throwable ex) {
                LOGGER.error("Failed to stop the Tuff thread layer", ex);
            }
        }
    }

    /**
     * Returns the current status, taking a snapshot when the runtime exists.
     *
     * @return an immutable status, never {@code null}
     */
    ThreadsStatus currentStatus() {
        final ThreadManagerRuntime current = this.runtime;
        final ThreadsStatus base = this.status;
        if (current == null || !base.isRunning()) {
            return base;
        }
        final UnifiedThreadManager manager = current.manager();
        try {
            final ResourceSnapshot snapshot = manager.snapshot();
            // the unaccounted report takes one JVM wide stack dump, which must never happen on a
            // region tick thread; a player running the command gets the pool numbers without it
            final boolean measured = manager.metrics().enabled() && !TickThread.isTickThread();
            final int unknown = measured
                ? countUnknown(manager.metrics().unregisteredThreads(), snapshot)
                : -1;
            return base.withSnapshot(snapshot, unknown);
        } catch (final RuntimeException ex) {
            LOGGER.warn("Could not read the Tuff thread snapshot", ex);
            return base;
        }
    }

    /**
     * Counts threads that are neither registered nor listed by a pool probe.
     *
     * <p>The module's registry only knows threads the manager created, so a thread that an observe-only
     * probe lists (Folia's region pool) is still "unregistered" from the registry's point of view. The
     * command must not call those unknown: the probe did account for them, just not through the factory.
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
     * Checks whether the region pool is registered on the running runtime, used by diagnostics and the
     * startup test.
     *
     * @return {@code true} when the runtime exists and the region pool has a spec
     */
    boolean regionPoolRegistered() {
        final ThreadManagerRuntime current = this.runtime;
        return current != null && current.manager().pools().contains(PoolId.REGION_EXECUTION);
    }

    private static void shutdownQuietly(final ThreadManagerRuntime runtime) {
        try {
            runtime.shutdown(Duration.ZERO);
            runtime.awaitTermination(AWAIT_CAP);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (final Throwable ex) {
            LOGGER.warn("Could not stop the partially started Tuff thread layer", ex);
        }
    }

    private static void logStarted(final UnifiedThreadManager manager) {
        final ThreadManagementSettings settings = manager.settings();
        LOGGER.info("Tuff thread management enabled: {} pool(s) registered, {}",
            manager.pools().ids().size(),
            settings.anyManagementEnabled() ? "some pools are managed" : "all pools are observed only");
        if (settings.anyManagementEnabled()) {
            LOGGER.info("Tuff thread management: managed capacity is configured; the manager only creates threads for "
                + "pools whose owner hands them over, so nothing changes for Folia's region scheduler");
        } else {
            LOGGER.info("Tuff thread management: region execution is observed at L1 (scheduler {}, threads {}); "
                + "thread-management.* changes take effect after a restart",
                settings.regionExecution().scheduler(),
                settings.regionExecution().threads() > 0
                    ? Integer.toString(settings.regionExecution().threads())
                    : "auto");
        }
    }
}
