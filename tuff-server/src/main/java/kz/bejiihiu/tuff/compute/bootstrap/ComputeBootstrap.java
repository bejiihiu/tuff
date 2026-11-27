package kz.bejiihiu.tuff.compute.bootstrap;

import ca.spottedleaf.moonrise.common.util.MoonriseCommon;
import io.papermc.paper.threadedregions.TickRegionScheduler;
import io.papermc.paper.threadedregions.TickRegions;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import kz.bejiihiu.tuff.api.compute.TuffCompute;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.RegionRelief;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import kz.bejiihiu.tuff.compute.config.ComputeThreadBudget;
import kz.bejiihiu.tuff.compute.config.ConfigDiff;
import kz.bejiihiu.tuff.compute.config.ConfigLoadResult;
import kz.bejiihiu.tuff.compute.config.TuffComputeConfig;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeBudgetPort;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsBootstrap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * Owns the compute engine for the lifetime of one server process.
 *
 * <p>Three hooks drive it, all of them called from the server itself:
 * <ol>
 *     <li>{@link #start()} runs at the end of the {@code CraftServer} constructor, after the command
 *     map, the plugin managers and the Bukkit configuration exist and before any plugin is enabled.
 *     It reads {@code config/tuff.yml}, creates the scheduler with a conservative processor budget,
 *     starts it and publishes it through {@link TuffCompute}.</li>
 *     <li>{@link #applyRegionBudget()} runs right after the region scheduler started. Only then do the
 *     region, chunk and IO thread counts exist, so only then can the pool be sized against what the
 *     rest of the server actually owns.</li>
 *     <li>{@link #stop()} runs during {@code MinecraftServer#stopServer()}, after plugins are
 *     disabled and their async tasks finished, but before the world save starts.</li>
 * </ol>
 *
 * <p>The engine is deliberately not created when {@code async-compute.enabled} is {@code false}: no
 * threads are started, nothing is published to plugins and {@link TuffCompute#scheduler()} stays
 * empty, which is exactly what the plugin API documents as "disabled". The command still answers and
 * explains how to enable it.
 *
 * <p>Every hook is wrapped so that a bad configuration or a failing pool is logged instead of thrown:
 * the server must start and stop normally no matter what this subsystem does.
 */
public final class ComputeBootstrap {

    private static final Logger LOGGER = LogManager.getLogger("Tuff/Compute");

    private static final Object LOCK = new Object();

    /**
     * Hard ceiling of the wait after {@code shutdown(grace)}.
     *
     * <p>{@code /restart} gives the server roughly ten seconds before the process is killed and the
     * world save runs right after this hook, so the compute stop has to leave room for both. The
     * grace period already waits for the drain; the extra second covers the bookkeeping after it.
     */
    private static final Duration SHUTDOWN_AWAIT_CAP = Duration.ofSeconds(5L);

    private static volatile ComputeStatus status = ComputeStatus.notInitialised();
    private static volatile @Nullable TuffComputeConfig config;
    private static volatile @Nullable ComputeUtmiAdapter computeAdapter;
    private static volatile List<String> lastAppliedConsumers = List.of();
    private static volatile List<String> lastDeferredConsumers = List.of();

    private ComputeBootstrap() {
    }

    /**
     * Reads the configuration, starts the engine and publishes it to plugins.
     *
     * <p>Called once per process from the {@code CraftServer} constructor. A second call is a no-op:
     * the engine is not rebuilt by {@code /reload}.
     */
    public static void start() {
        synchronized (LOCK) {
            if (config != null) {
                return;
            }
            // the thread layer goes first: it only registers pools and attaches probes, so the compute
            // engine already sees a registered world when it starts and asks for a budget
            try {
                ThreadsBootstrap.start();
            } catch (final Throwable ex) {
                LOGGER.error("Could not start the Tuff thread layer; the compute engine starts without it", ex);
            }
            final TuffComputeConfig loaded = TuffComputeConfig.serverWide();
            config = loaded;
            try {
                loaded.setApplyTarget(ComputeBootstrap::applyLive);
                final ConfigLoadResult result = loaded.load();
                final ComputeSettings settings = result.settings();
                if (!settings.enabled()) {
                    status = ComputeStatus.disabled();
                    LOGGER.info("Tuff compute is disabled in {} (async-compute.enabled: false)", loaded.file());
                    return;
                }
                startEngine(settings);
            } catch (final Throwable ex) {
                status = ComputeStatus.failed();
                LOGGER.error("Could not initialise the Tuff compute engine; the server runs without it", ex);
            }
        }
    }

    /**
     * Sizes the pool against the threads the rest of the server owns.
     *
     * <p>Called from the server tick loop right after {@code RegionizedServer.init()}, because before
     * that point the region scheduler is not running and its thread count is not final. A no-op when
     * the engine is not running, when the configured pool size is fixed, or when the thread counts
     * cannot be read.
     *
     * <p>When the thread layer manages the pool it is the only budget source: the number comes from
     * {@link ComputeUtmiAdapter#processors()} and {@code ComputeThreadBudget} is not called at all, so
     * the reserve is subtracted once, in the controller.
     */
    public static void applyRegionBudget() {
        final @Nullable AsyncComputeScheduler scheduler = status.engine().orElse(null);
        if (scheduler == null) {
            return;
        }
        final @Nullable ComputeUtmiAdapter adapter = computeAdapter;
        final @Nullable ComputeBudgetPort managed = adapter != null && adapter.isManaged() ? adapter : null;
        final int cores = Runtime.getRuntime().availableProcessors();
        if (!scheduler.settings().workers().automaticThreadCount()) {
            LOGGER.info("Tuff compute: fixed worker count {}, the server processor budget is not applied",
                scheduler.settings().workers().threads());
            return;
        }
        int resolved;
        try {
            final @Nullable TickRegionScheduler regionScheduler = TickRegions.getScheduler();
            final int regionThreads = regionScheduler == null ? 0 : regionScheduler.getTotalThreadCount();
            final int chunkThreads = MoonriseCommon.WORKER_POOL.getAliveThreads().length;
            final int ioThreads = MoonriseCommon.IO_POOL.getAliveThreads().length;
            resolved = liveBudget(managed, cores, regionThreads, chunkThreads, ioThreads);
            if (managed != null) {
                LOGGER.info("Tuff compute: processor budget {} of {} (source {}, ledger {})",
                    resolved, cores, managed.source(), adapter.budgetLedger());
            } else {
                LOGGER.info("Tuff compute: processor budget {} of {} ({} region, {} chunk, {} IO threads, source legacy)",
                    resolved, cores, regionThreads, chunkThreads, ioThreads);
            }
        } catch (final Throwable ex) {
            // the engine is waiting for its first budget: falling back to the startup number is still
            // better than leaving the pool without a single worker
            LOGGER.warn("Could not read the server thread counts; the compute pool is created with the startup budget", ex);
            resolved = startupBudget(managed, cores);
        }
        final int budget = resolved;
        try {
            scheduler.applyProcessorBudget(budget);
        } catch (final Throwable ex) {
            LOGGER.warn("Could not apply the processor budget to the compute engine", ex);
        }
    }

    /**
     * Resolves the budget the live pool should adopt by asking exactly one source.
     *
     * <p>The managed port already returns a number net of region, chunk, IO and the reserve. The
     * legacy formula subtracts those itself. Reading both - or subtracting the live counts from the
     * port's answer - is the double subtraction that collapsed the pool to one worker, so this method
     * is the only place the choice is made.
     *
     * @param managed     thread layer budget port, or {@code null} when the engine owns its budget
     * @param cores       processors reported by the JVM
     * @param region      live region tick threads
     * @param chunk       live chunk worker threads
     * @param io          live chunk IO threads
     * @return the processor budget, always at least {@code 1}
     */
    static int liveBudget(
        final @Nullable ComputeBudgetPort managed,
        final int cores,
        final int region,
        final int chunk,
        final int io
    ) {
        if (managed != null) {
            return managed.processors();
        }
        return ComputeThreadBudget.resolve(cores, region, chunk, io);
    }

    /**
     * Resolves the budget the engine starts with.
     *
     * <p>Same single source rule as {@link #liveBudget}: with a managed port the legacy formula is not
     * evaluated at all, not even as a fallback.
     *
     * @param managed thread layer budget port, or {@code null} when the engine owns its budget
     * @param cores   processors reported by the JVM
     * @return the processor budget, always at least {@code 1}
     */
    static int startupBudget(final @Nullable ComputeBudgetPort managed, final int cores) {
        return managed == null ? ComputeThreadBudget.resolve(cores) : managed.processors();
    }

    /**
     * Stops the engine while the server shuts down.
     *
     * <p>Called after plugins are disabled and their async tasks finished, before the world save
     * starts. Running tasks get the configured grace period, then the pool is given a bounded wait
     * (see {@link #shutdownTimeout(Duration)}) to report {@code TERMINATED}; nothing here throws, so
     * a stuck task cannot break the shutdown.
     */
    public static void stop() {
        ComputeJfr.shutdown();
        TuffCompute.uninstall();

        try {
            stopEngine();
        } finally {
            // reverse of startup: compute is down, only then does the accounting layer go away. it owns
            // no executor, so this cannot outlive the shutdown budget
            try {
                ThreadsBootstrap.stop();
            } catch (final Throwable ex) {
                LOGGER.error("Could not stop the Tuff thread layer", ex);
            }
        }
    }

    private static void stopEngine() {
        final ComputeStatus previous = status;
        final @Nullable AsyncComputeScheduler scheduler = previous.engine().orElse(null);
        status = ComputeStatus.notInitialised();
        computeAdapter = null;
        final TuffComputeConfig loaded = config;
        if (loaded != null) {
            loaded.setApplyTarget(null);
        }
        if (scheduler == null) {
            // one line even when there is nothing to stop, otherwise an operator cannot tell the
            // hook ran at all (the disabled start already logged its own reason)
            LOGGER.info("Tuff compute is {}; nothing to stop",
                previous.state().name().toLowerCase(Locale.ROOT).replace('_', ' '));
            return;
        }

        final Duration grace = scheduler.settings().shutdownGracePeriod();
        final Duration timeout = shutdownTimeout(grace);
        final long startNanos = System.nanoTime();
        try {
            scheduler.shutdown(grace);
            final boolean terminated = scheduler.awaitTermination(timeout);
            final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
            if (terminated) {
                LOGGER.info("Tuff compute stopped in {} ms", elapsedMillis);
            } else {
                LOGGER.error("The Tuff compute engine did not terminate within {} ms; its workers are daemon threads, "
                    + "so the process exit finishes the job", timeout.toMillis());
                dumpWorkers(scheduler);
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for the Tuff compute engine to stop");
        } catch (final Throwable ex) {
            LOGGER.error("Failed to stop the Tuff compute engine", ex);
        }
    }

    /**
     * Works out how long the shutdown may wait for {@code TERMINATED}.
     *
     * <p>The grace period plus one second for the steps that follow it, capped at five seconds: a
     * {@code /restart} gives the whole shutdown about ten seconds and the world save still has to
     * run, so this hook must not eat the budget even when the file asks for a long grace.
     *
     * @param grace configured shutdown grace period
     * @return the wait bound, never longer than five seconds
     */
    static Duration shutdownTimeout(final Duration grace) {
        final Duration wanted = grace.plusSeconds(1L);
        return wanted.compareTo(SHUTDOWN_AWAIT_CAP) > 0 ? SHUTDOWN_AWAIT_CAP : wanted;
    }

    /**
     * Returns what the bootstrap currently knows about the engine.
     *
     * @return an immutable status snapshot, never {@code null}
     */
    public static ComputeStatus status() {
        return status;
    }

    /**
     * Returns the region relief service of the running engine.
     *
     * <p>The empty result means the engine is not running or failed to start; relief itself being
     * switched off does not hide the service, because submissions still fall back to running inline.
     * This is the entry point a region subsystem uses when it decides to shed work.
     *
     * @return the relief service, or an empty optional when no engine is available
     */
    public static Optional<RegionRelief> regionRelief() {
        return status.engine().flatMap(AsyncComputeScheduler::regionRelief);
    }

    /**
     * Re-reads {@code config/tuff.yml} and applies what a live pool can adopt.
     *
     * <p>Must run on the global tick thread, like any other configuration mutation. The answer is
     * {@code null} when the engine is not running, so the caller can tell "nothing to reload" apart
     * from "reloaded and nothing changed".
     *
     * @return what the reload read and applied, or {@code null} if no engine is running
     */
    public static @Nullable ReloadReport reload() {
        final @Nullable TuffComputeConfig loaded = config;
        if (loaded == null || !status.isRunning()) {
            return null;
        }
        lastAppliedConsumers = List.of();
        lastDeferredConsumers = List.of();
        final ConfigLoadResult result = loaded.reload();
        final List<String> applied = lastAppliedConsumers;
        final List<String> deferredConsumers = lastDeferredConsumers;
        return new ReloadReport(
            result,
            applied,
            deferredConsumers,
            ConsumerReload.deferredPaths(result.diff(), applied, deferredConsumers)
        );
    }

    private static void startEngine(final ComputeSettings settings) {
        // the adapter registers the compute pools before the engine spawns its first worker; when the
        // thread layer is off (or refuses), it is null and nothing about the engine changes
        final @Nullable ComputeUtmiAdapter adapter = ThreadsBootstrap.computeAdapter(settings);
        computeAdapter = adapter;
        final @Nullable ComputeBudgetPort managed = adapter != null && adapter.isManaged() ? adapter : null;
        final int budget = startupBudget(managed, Runtime.getRuntime().availableProcessors());
        final ComputeSettings.Workers workers = settings.workers();
        final int resolved = workers.resolveThreadCount(budget);
        final String source = managed == null ? "legacy" : managed.source();
        final RegionReliefSettings relief = config == null
            ? RegionReliefSettings.defaults()
            : config.reliefSettings();
        // the budget is provisional until the region scheduler reports its threads; spawning now and
        // shrinking a moment later would report a peak the server never needed
        final boolean deferWorkers = workers.automaticThreadCount();
        @Nullable AsyncComputeScheduler scheduler = null;
        try {
            scheduler = ComputeSchedulers.create(
                settings,
                ComputeClock.system(),
                ComputeBudgetPort.fixed(budget, source),
                adapter == null ? ComputeThreadFactoryPort.legacy() : adapter,
                relief,
                deferWorkers
            );
            scheduler.start();
            if (adapter != null) {
                adapter.bind(scheduler);
            }
            final LimitedConsumerScheduler guarded = LimitedConsumerScheduler.wrap(scheduler);
            TuffCompute.install(guarded);
            status = ComputeStatus.running(guarded);
            final String requested = workers.automaticThreadCount() ? "auto" : Integer.toString(workers.threads());
            final String sizing = deferWorkers
                ? resolved + " worker(s) once the region budget is known (startup estimate)"
                : resolved + " worker(s)";
            LOGGER.info("Tuff compute enabled: {} (requested {}, processor budget {}, source {}), queue capacity {}, scheduling {}",
                sizing, requested, budget, source, settings.queue().capacity(), settings.scheduling().strategy());
            if (deferWorkers) {
                LOGGER.info("Tuff compute: the worker pool is created with the first region processor budget, so its size "
                    + "matches what the server actually owns; submissions queue until then");
            }
            if (settings.consumers().allowDynamicRegistration()) {
                LOGGER.info("Tuff compute: consumer ids registered on demand are capped at {} (configured ids are not counted)",
                    LimitedConsumerScheduler.MAX_DYNAMIC_CONSUMERS);
            }
            if (budgetClampsRequestedThreads(workers, budget)) {
                LOGGER.warn("Tuff compute: {} worker(s) were requested, but the processor budget of {} and max-threads ({}) "
                        + "leave room for {}; the pool runs with {} worker(s). /tuff shows the requested and the live count.",
                    workers.threads(), budget, workers.maxThreads() == -1 ? "auto" : workers.maxThreads(), resolved, resolved);
            }
        } catch (final Throwable ex) {
            status = ComputeStatus.failed();
            LOGGER.error("The Tuff compute engine failed to start; the server runs without it", ex);
            if (scheduler != null) {
                shutdownQuietly(scheduler);
            }
        }
    }

    /**
     * Checks whether the configured {@code workers.threads} is lowered by the pool ceiling.
     *
     * <p>The ceiling comes from {@code max-threads} when it is set, otherwise from the processor
     * budget. This is the predicate behind the startup warning; it never touches the engine.
     *
     * @param workers configured workers section
     * @param budget  processors the pool may use
     * @return {@code true} when the effective count is lower than the requested one
     */
    static boolean budgetClampsRequestedThreads(final ComputeSettings.Workers workers, final int budget) {
        return workers.threads() > 0 && workers.resolveThreadCount(budget) < workers.threads();
    }

    /**
     * Applies one configuration snapshot to a running engine.
     *
     * <p>Only consumer policies can change on a live pool: {@code AsyncComputeScheduler} keeps its
     * settings as an immutable snapshot, so worker sizing, queue capacity, scheduling, metrics, the
     * adaptive controller and the shutdown grace period are baked into the engine created at startup.
     * Those keys are reported as "after a restart" instead of being silently ignored. Even among
     * consumers, only the ones whose configured policy really changed are touched, and a policy a
     * plugin passed to {@code registerConsumer} stays unless the
     * file has an explicit override for that id. Called with the configuration lock held, so it must
     * stay cheap.
     *
     * @param previous the settings the engine was running with
     * @param updated  the newly published settings
     * @param diff     what changed between them
     */
    private static void applyLive(final ComputeSettings previous, final ComputeSettings updated, final ConfigDiff diff) {
        lastAppliedConsumers = List.of();
        lastDeferredConsumers = List.of();
        final @Nullable AsyncComputeScheduler scheduler = status.engine().orElse(null);
        if (scheduler == null) {
            return;
        }
        final ConsumerReload.Outcome outcome = ConsumerReload.apply(scheduler, previous, updated);
        lastAppliedConsumers = outcome.applied();
        lastDeferredConsumers = outcome.deferred();
        if (!lastAppliedConsumers.isEmpty()) {
            LOGGER.info("Tuff compute: applied new consumer policies to {}", String.join(", ", lastAppliedConsumers));
        }
        if (!lastDeferredConsumers.isEmpty()) {
            LOGGER.info("Tuff compute: kept the new consumer policies for ids that are not registered yet: {}",
                String.join(", ", lastDeferredConsumers));
        }
        LOGGER.debug("Tuff compute reload changed {}", diff.summary());
    }

    private static void shutdownQuietly(final AsyncComputeScheduler scheduler) {
        try {
            scheduler.shutdown(Duration.ofSeconds(1L));
            scheduler.awaitTermination(Duration.ofSeconds(3L));
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (final Throwable ex) {
            LOGGER.warn("Could not stop the partially started compute engine", ex);
        }
    }

    private static void dumpWorkers(final AsyncComputeScheduler scheduler) {
        final String prefix = scheduler.settings().workers().threadNamePrefix();
        for (final Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            if (!entry.getKey().getName().startsWith(prefix)) {
                continue;
            }
            final StringBuilder builder = new StringBuilder("Compute worker still alive: ").append(entry.getKey().getName());
            for (final StackTraceElement element : entry.getValue()) {
                builder.append("\n\tat ").append(element);
            }
            LOGGER.error(builder.toString());
        }
    }
}
