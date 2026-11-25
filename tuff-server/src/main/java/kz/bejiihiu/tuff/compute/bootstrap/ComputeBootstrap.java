package kz.bejiihiu.tuff.compute.bootstrap;

import ca.spottedleaf.moonrise.common.util.MoonriseCommon;
import io.papermc.paper.threadedregions.TickRegionScheduler;
import io.papermc.paper.threadedregions.TickRegions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import kz.bejiihiu.tuff.api.compute.TuffCompute;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.config.ComputeThreadBudget;
import kz.bejiihiu.tuff.compute.config.ConfigDiff;
import kz.bejiihiu.tuff.compute.config.ConfigLoadResult;
import kz.bejiihiu.tuff.compute.config.TuffComputeConfig;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
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
    private static final String CONSUMER_PREFIX = "async-compute.consumers.";

    private static final Object LOCK = new Object();

    private static volatile ComputeStatus status = ComputeStatus.notInitialised();
    private static volatile @Nullable TuffComputeConfig config;
    private static volatile List<String> lastAppliedConsumers = List.of();

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
     */
    public static void applyRegionBudget() {
        final @Nullable AsyncComputeScheduler scheduler = status.engine().orElse(null);
        if (scheduler == null) {
            return;
        }
        final int cores = Runtime.getRuntime().availableProcessors();
        final int regionThreads;
        final int chunkThreads;
        final int ioThreads;
        try {
            final @Nullable TickRegionScheduler regionScheduler = TickRegions.getScheduler();
            regionThreads = regionScheduler == null ? 0 : regionScheduler.getTotalThreadCount();
            chunkThreads = MoonriseCommon.WORKER_POOL.getAliveThreads().length;
            ioThreads = MoonriseCommon.IO_POOL.getAliveThreads().length;
        } catch (final Throwable ex) {
            LOGGER.warn("Could not read the server thread counts; the compute pool keeps its startup size", ex);
            return;
        }
        final int budget = ComputeThreadBudget.resolve(cores, regionThreads, chunkThreads, ioThreads);
        if (!scheduler.settings().workers().automaticThreadCount()) {
            LOGGER.info("Tuff compute: fixed worker count {}, the server processor budget of {} is not applied",
                scheduler.settings().workers().threads(), budget);
            return;
        }
        LOGGER.info("Tuff compute: processor budget {} of {} ({} region, {} chunk, {} IO threads)",
            budget, cores, regionThreads, chunkThreads, ioThreads);
        try {
            scheduler.applyProcessorBudget(budget);
        } catch (final Throwable ex) {
            LOGGER.warn("Could not apply the processor budget to the compute engine", ex);
        }
    }

    /**
     * Stops the engine while the server shuts down.
     *
     * <p>Called after plugins are disabled and their async tasks finished, before the world save
     * starts. Running tasks get the configured grace period, then the pool is given five more seconds
     * to report {@code TERMINATED}; nothing here throws, so a stuck task cannot break the shutdown.
     */
    public static void stop() {
        ComputeJfr.shutdown();
        TuffCompute.uninstall();

        final @Nullable AsyncComputeScheduler scheduler = status.engine().orElse(null);
        status = ComputeStatus.notInitialised();
        final TuffComputeConfig loaded = config;
        if (loaded != null) {
            loaded.setApplyTarget(null);
        }
        if (scheduler == null) {
            return;
        }

        final Duration grace = scheduler.settings().shutdownGracePeriod();
        final Duration timeout = grace.plusSeconds(5L);
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
     * Returns what the bootstrap currently knows about the engine.
     *
     * @return an immutable status snapshot, never {@code null}
     */
    public static ComputeStatus status() {
        return status;
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
        final ConfigLoadResult result = loaded.reload();
        final List<String> restartRequired = result.diff().restartRequiredPaths();
        final List<String> deferred = new ArrayList<>();
        for (final String path : result.diff().changedPaths()) {
            if (restartRequired.contains(path) || !path.startsWith(CONSUMER_PREFIX)) {
                deferred.add(path);
            }
        }
        return new ReloadReport(result, lastAppliedConsumers, deferred);
    }

    private static void startEngine(final ComputeSettings settings) {
        final int budget = ComputeThreadBudget.resolve(Runtime.getRuntime().availableProcessors());
        @Nullable AsyncComputeScheduler scheduler = null;
        try {
            scheduler = ComputeSchedulers.create(settings, ComputeClock.system(), budget);
            scheduler.start();
            TuffCompute.install(scheduler);
            status = ComputeStatus.running(scheduler);
            LOGGER.info("Tuff compute enabled: {} worker(s) (processor budget {}), queue capacity {}, scheduling {}",
                settings.workers().resolveThreadCount(budget), budget, settings.queue().capacity(), settings.scheduling().strategy());
        } catch (final Throwable ex) {
            status = ComputeStatus.failed();
            LOGGER.error("The Tuff compute engine failed to start; the server runs without it", ex);
            if (scheduler != null) {
                shutdownQuietly(scheduler);
            }
        }
    }

    /**
     * Applies one configuration snapshot to a running engine.
     *
     * <p>Only consumer policies can change on a live pool: {@code AsyncComputeScheduler} keeps its
     * settings as an immutable snapshot, so worker sizing, queue capacity and the metrics switches are
     * baked into the engine created at startup. Those keys are reported as "after a restart" instead
     * of being silently ignored. Called with the configuration lock held, so it must stay cheap.
     *
     * @param previous the settings the engine was running with
     * @param updated  the newly published settings
     * @param diff     what changed between them
     */
    private static void applyLive(final ComputeSettings previous, final ComputeSettings updated, final ConfigDiff diff) {
        lastAppliedConsumers = List.of();
        final @Nullable AsyncComputeScheduler scheduler = status.engine().orElse(null);
        if (scheduler == null) {
            return;
        }
        final List<String> applied = new ArrayList<>();
        for (final String consumerId : scheduler.consumerIds()) {
            final ConsumerPolicy before = previous.consumers().policyFor(consumerId);
            final ConsumerPolicy after = updated.consumers().policyFor(consumerId);
            if (before.equals(after)) {
                continue;
            }
            try {
                scheduler.updateConsumerPolicy(consumerId, after);
                applied.add(consumerId);
            } catch (IllegalArgumentException _) {
                // the consumer was unregistered between listing and updating; nothing to apply
            }
        }
        applied.sort(Comparator.naturalOrder());
        lastAppliedConsumers = List.copyOf(applied);
        if (!applied.isEmpty()) {
            LOGGER.info("Tuff compute: applied new consumer policies to {}", String.join(", ", applied));
        }
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
