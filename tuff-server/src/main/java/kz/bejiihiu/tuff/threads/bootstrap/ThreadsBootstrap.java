package kz.bejiihiu.tuff.threads.bootstrap;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.bootstrap.ComputeUtmiAdapter;
import kz.bejiihiu.tuff.compute.config.ConfigProblem;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.bridge.RegionThreads;
import kz.bejiihiu.tuff.threads.config.ThreadsConfig;
import kz.bejiihiu.tuff.threads.internal.runtime.DefaultThreadManagerFactory;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import kz.bejiihiu.tuff.threads.kairos.KairosGate;
import kz.bejiihiu.tuff.threads.kairos.KairosTuning;
import kz.bejiihiu.tuff.threads.pressure.TickPressureMonitor;
import kz.bejiihiu.tuff.threads.pressure.TickPressureSnapshot;
import kz.bejiihiu.tuff.threads.probe.BackgroundPoolProbe;
import kz.bejiihiu.tuff.threads.probe.ChatPoolProbe;
import kz.bejiihiu.tuff.threads.probe.ChunkPoolProbe;
import kz.bejiihiu.tuff.threads.probe.NetworkPoolProbe;
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
 *     their configured coverage. The region bridge is installed right after, before Folia constructs
 *     its region scheduler, so a managed region pool can wrap tick bodies while everything else in that
 *     scheduler stays Folia's. The region pool is then attached to a probe, because the manager cannot
 *     see threads it did not create, and the background refresher takes over every heavy read.</li>
 *     <li>{@link #stop()} runs last, at the bottom of {@code ComputeBootstrap.stop()}. The compute engine
 *     stops first, then the refresher is signalled and only then does the accounting layer go away.
 *     Nothing joins, sleeps or interrupts a blocking call, so the hooks are safe inside the shutdown
 *     window.</li>
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
 * instance bound to the server. {@link #shutdown()} leaves the instance reusable: a later start builds
 * a fresh runtime, so a stopped layer never turns a restart into a silent no-op.
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
    private volatile @Nullable ThreadsSampler sampler;
    private volatile @Nullable TickPressureMonitor pressure;
    private volatile @Nullable ComputeUtmiAdapter computeAdapter;
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
     * refresher is signalled first, then the runtime closes its probes and sampler. Nothing here waits
     * for game work or joins a thread: this path runs inside the shutdown window.
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
     * <p>Starting a layer that is already up - running, disabled or failed - is a no-op, so the
     * startup hook can be called more than once without building a second runtime. After
     * {@link #shutdown()} the instance is reusable: the next call builds a fresh runtime from the
     * file as it is on disk then.
     *
     * @param config configuration to read
     */
    void startWith(final ThreadsConfig config) {
        synchronized (this.lock) {
            if (this.started) {
                // legitimate when the hook runs twice, but an operator debugging a restart should be
                // able to tell "already started" apart from "never called"
                LOGGER.debug("Tuff thread management is already up ({}); ignoring the start request",
                    this.status.state());
                return;
            }
            this.started = true;
            // a fresh start never inherits a bridge from a previous lifecycle in the same process
            RegionThreads.clear();
            ThreadManagerRuntime created = null;
            try {
                final ThreadsConfig.ThreadsLoadResult result = config.load();
                // the region scheduler object already exists by the time this file is read, so the
                // explicit Kairos opt-in and the fund limits have to travel from here to start()
                final ThreadManagementSettings.RegionExecution region = result.settings().regionExecution();
                final ThreadManagementSettings.RegionExecution.Kairos kairos = region.kairos();
                KairosGate.arm(
                    region.kairosUnstable(),
                    new KairosTuning(
                        kairos.elastic(),
                        kairos.maxThreads(),
                        kairos.keepAliveSeconds() * 1000,
                        kairos.spawnRatePerSecond(),
                        1_000L
                    )
                );
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
                // before Folia builds its region scheduler, so the factory sees the L2 spec and the
                // bridge can hand the tick bodies to the provider; a no-op when managed is off
                RegionThreads.install(created.manager());
                // the probe is attached after start, because start is what registers the pools; attaching
                // earlier would be rejected by the probe registry with "pool is not registered"
                created.attachProbe(new RegionPoolProbe());
                attachObservedPools(created);
                // every heavy read (region walk, thread table, probes) happens on this thread from now
                // on: the console command is itself on a region tick thread in Folia and must not do it
                final TickPressureMonitor createdPressure = pressureMonitor(settings, created);
                final ThreadsSampler createdSampler = ThreadsSampler.start(created, createdPressure);
                this.runtime = created;
                this.sampler = createdSampler;
                this.pressure = createdPressure;
                this.status = ThreadsStatus.running(settings);
                logStarted(created.manager());
            } catch (final Throwable ex) {
                this.runtime = null;
                RegionThreads.clear();
                final ThreadsSampler partial = this.sampler;
                this.sampler = null;
                if (partial != null) {
                    partial.stop();
                }
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
     *
     * <p>Leaves the instance ready for another start: the flag that makes a repeated start a no-op is
     * cleared here, together with the runtime and the status. Without that reset a started-then-stopped
     * bootstrap would quietly ignore every later start.
     */
    void shutdown() {
        synchronized (this.lock) {
            final ThreadManagerRuntime current = this.runtime;
            final ThreadsSampler currentSampler = this.sampler;
            final TickPressureMonitor currentPressure = this.pressure;
            final ThreadsStatus previous = this.status;
            this.runtime = null;
            this.sampler = null;
            this.pressure = null;
            this.computeAdapter = null;
            this.started = false;
            this.status = ThreadsStatus.notInitialised();
            // before the runtime stops: no new region body may be wrapped while the manager is going down
            RegionThreads.clear();
            KairosGate.disarm();
            // flag + interrupt, no join: this runs in the window where the watchdog is still armed and
            // a blocking wait would look like a hung server. The thread is a daemon and exits on its own
            if (currentSampler != null) {
                currentSampler.stop();
            }
            if (currentPressure != null) {
                currentPressure.stop();
            }
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
     * Returns the compute adapter of the running thread layer, building it on first use.
     *
     * <p>The compute bootstrap asks this before it creates the engine: the adapter registers the
     * compute pools at their configured coverage and is what the engine then uses as its thread and
     * budget port. A thread layer that is disabled, failed or already stopped answers {@code null},
     * which is the signal for "keep the engine's own threads and budget".
     *
     * <p>Building the adapter is wrapped, because a broken registration must not take the engine down:
     * the answer is then {@code null} and the engine runs exactly as it did before.
     *
     * @param settings compute configuration the engine is about to run with
     * @return the adapter, or {@code null} when the thread layer cannot serve the engine
     * @throws NullPointerException if {@code settings} is {@code null}
     */
    public static @Nullable ComputeUtmiAdapter computeAdapter(final ComputeSettings settings) {
        return SERVER.adapterFor(settings);
    }

    /**
     * Returns the current status from the cached sample.
     *
     * <p>This is the only read the command path does, and it is safe on any thread: Folia runs console
     * commands on the global region tick thread, so snapshotting here would put a region walk and a
     * thread enumeration inside a tick. The refresher owns those reads; this method only copies the
     * last published sample and stamps its age.
     *
     * @return an immutable status, never {@code null}
     */
    ThreadsStatus currentStatus() {
        final ThreadsStatus base = this.status;
        if (!base.isRunning()) {
            return base;
        }
        final TickPressureSnapshot pressure = this.pressure == null ? null : this.pressure.snapshot();
        final ThreadsSampler current = this.sampler;
        final ThreadsSampler.@Nullable Sample sample = current == null ? null : current.sample();
        if (sample == null) {
            return base.withPressure(pressure);
        }
        return base.withSnapshot(sample.snapshot(), sample.unknownThreads(), sample.ageMillis())
            .withPressure(pressure);
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

    /**
     * Builds (or returns) the compute adapter of the running runtime.
     *
     * @param settings compute configuration the engine is about to run with
     * @return the adapter, or {@code null} when there is no running runtime or it refused the pools
     * @throws NullPointerException if {@code settings} is {@code null}
     */
    @Nullable ComputeUtmiAdapter adapterFor(final ComputeSettings settings) {
        Objects.requireNonNull(settings, "settings");
        synchronized (this.lock) {
            final @Nullable ComputeUtmiAdapter existing = this.computeAdapter;
            if (existing != null) {
                return existing;
            }
            final ThreadManagerRuntime current = this.runtime;
            if (current == null) {
                return null;
            }
            try {
                final ComputeUtmiAdapter created = ComputeUtmiAdapter.attach(current, settings, this.pressure);
                this.computeAdapter = created;
                final TickPressureMonitor monitor = this.pressure;
                if (monitor != null && created.isManaged()) {
                    // only a managed pool can apply a reserve; an observed one keeps logging and reporting
                    monitor.bind(created.pressureSink());
                }
                return created;
            } catch (final Throwable ex) {
                LOGGER.error("Tuff compute cannot use the thread layer; the engine keeps its own threads and budget", ex);
                return null;
            }
        }
    }

    /**
     * Attaches a probe to every remaining observed pool.
     *
     * <p>Each probe is independent and optional: one that cannot be attached leaves its pool registered
     * and visible, just without numbers, while the rest of the layer keeps working. The region probe
     * stays on the startup path because tick health is the one reading the report must not lose.
     *
     * @param runtime running thread manager
     */
    private static void attachObservedPools(final ThreadManagerRuntime runtime) {
        attach(runtime, new NetworkPoolProbe());
        attach(runtime, ChunkPoolProbe.worker());
        attach(runtime, ChunkPoolProbe.io());
        attach(runtime, new ChatPoolProbe());
        attach(runtime, new BackgroundPoolProbe());
    }

    private static void attach(final ThreadManagerRuntime runtime, final PoolProbe probe) {
        try {
            runtime.attachProbe(probe);
        } catch (RuntimeException | LinkageError ex) {
            LOGGER.warn("Could not attach the {} pool probe; the pool stays registered without one", probe.pool(), ex);
        }
    }

    /**
     * Builds the tick pressure monitor when the policy is on.
     *
     * <p>One line at startup states the whole policy, so an operator does not have to reverse the
     * thresholds from the file to understand what the pool will do. The monitor is {@code null} when
     * the feature is off, which is also what makes {@code /tuff threads} print "disabled".
     *
     * @param settings effective thread settings
     * @param runtime  running thread manager
     * @return the monitor, or {@code null} when the policy is off
     */
    private static @Nullable TickPressureMonitor pressureMonitor(
        final ThreadManagementSettings settings,
        final ThreadManagerRuntime runtime
    ) {
        final ThreadManagementSettings.AsyncCompute.TickPressure policy = settings.tickPressure();
        if (!policy.enabled()) {
            return null;
        }
        LOGGER.info("Tuff compute pressure enabled: a region below {} TPS (or {}% of regions lagging) yields up to "
                + "{} core(s) in steps of {}, released after {} calm sample(s) at {} TPS, cooldown {} s",
            policy.enterMinTps(), Math.round(policy.enterLagFraction() * 100.0d), policy.maxReserveCores(),
            policy.stepCores(), policy.exitWindows(), policy.exitMinTps(), policy.cooldownSeconds());
        return new TickPressureMonitor(policy, () -> runtime.manager().resources().usableCores());
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
        if (settings.regionExecution().managed()) {
            LOGGER.info("Tuff thread management: region execution is at L2; Folia still creates and schedules its "
                + "tick threads, the manager only accounts their bodies");
        } else if (settings.anyManagementEnabled()) {
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
