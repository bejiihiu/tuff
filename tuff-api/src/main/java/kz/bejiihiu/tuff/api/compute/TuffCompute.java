package kz.bejiihiu.tuff.api.compute;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import org.jspecify.annotations.Nullable;

/**
 * Process wide access point to the Tuff compute engine.
 *
 * <p>The server bootstrap owns the engine: it creates the scheduler from {@code config/tuff.yml},
 * starts it, and then publishes it here with {@link #install(AsyncComputeScheduler)} before the
 * regionised server starts ticking. On shutdown it stops the engine and calls {@link #uninstall()}.
 * Plugins never install or uninstall the engine; they only read it.
 *
 * <p>Typical plugin usage:
 *
 * <pre>{@code
 * final AsyncComputeScheduler compute = TuffCompute.scheduler().orElse(null);
 * if (compute == null) {
 *     getLogger().warning("Tuff compute is disabled; using the synchronous fallback");
 *     return;
 * }
 * final CompletableFuture<Pathfinder.PathResult> route = compute.submit(ComputePriority.NORMAL, () -> plan(snapshot));
 * ComputeHandoff.toEntity(this, mob, route, mob.getPathfinder()::moveTo);
 * }</pre>
 *
 * <p>All methods here are thread safe. The accessor is intentionally independent of the Bukkit
 * services system: the engine exists before plugins are enabled, may be disabled entirely by
 * configuration, and is not owned by any plugin.
 */
public final class TuffCompute {

    private static final AtomicReference<AsyncComputeScheduler> INSTALLED = new AtomicReference<>();

    private TuffCompute() {
    }

    /**
     * Returns the installed compute scheduler, if the engine is available.
     *
     * <p>An empty result means the engine is disabled in the server configuration, failed to start,
     * or the server has not installed it yet. This method never throws, so it is the right choice
     * for code that has a fallback.
     *
     * @return the installed scheduler, or an empty optional when the engine is unavailable
     */
    public static Optional<AsyncComputeScheduler> scheduler() {
        return Optional.ofNullable(INSTALLED.get());
    }

    /**
     * Returns the installed compute scheduler or fails loudly.
     *
     * <p>Use this when the plugin cannot work without the engine. The thrown exception explains how
     * to diagnose the situation; it is meant to fail plugin enable with an actionable message
     * instead of producing a {@link NullPointerException} later.
     *
     * @return the installed scheduler, never {@code null}
     * @throws IllegalStateException if the engine is disabled, failed to start, or is not installed
     *     yet
     */
    public static AsyncComputeScheduler require() {
        final @Nullable AsyncComputeScheduler scheduler = INSTALLED.get();
        if (scheduler == null) {
            throw new IllegalStateException(
                "The Tuff compute engine is not available. It is either disabled in config/tuff.yml, "
                    + "failed to start, or has not been installed yet. Check the server log, or use "
                    + "TuffCompute.scheduler() if your plugin can work without it."
            );
        }
        return scheduler;
    }

    /**
     * Checks whether a scheduler is currently installed.
     *
     * <p>This is a cheap alternative to {@code scheduler().isPresent()} for hot paths. It says
     * nothing about the scheduler being {@linkplain AsyncComputeScheduler#isRunning() running}; a
     * scheduler that is shutting down may still be installed for a short moment.
     *
     * @return {@code true} if {@link #scheduler()} would return a value
     */
    public static boolean isAvailable() {
        return INSTALLED.get() != null;
    }

    /**
     * Publishes the running engine for plugins.
     *
     * <p>Intended for the server bootstrap only, and called exactly once during startup. The method
     * is idempotent for the same instance: installing the scheduler twice is a no-op. Installing a
     * different instance while another one is installed is a programming error and is rejected, so
     * two subsystems can never silently publish two engines.
     *
     * <p>An already terminated scheduler is rejected as well; publishing a dead engine would make
     * every later submission fail with {@code ComputeShutdownException}.
     *
     * @param scheduler the started scheduler to publish
     * @throws NullPointerException     if {@code scheduler} is {@code null}
     * @throws IllegalArgumentException if {@code scheduler} is already terminated
     * @throws IllegalStateException    if a different scheduler is already installed
     */
    public static void install(final AsyncComputeScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        if (scheduler.state() == AsyncComputeScheduler.SchedulerState.TERMINATED) {
            throw new IllegalArgumentException("Cannot install a terminated compute scheduler");
        }
        for (;;) {
            final @Nullable AsyncComputeScheduler current = INSTALLED.get();
            if (current == scheduler) {
                return;
            }
            if (current != null) {
                throw new IllegalStateException(
                    "A different compute scheduler is already installed: " + current
                        + ". Uninstall it before installing " + scheduler + "."
                );
            }
            if (INSTALLED.compareAndSet(null, scheduler)) {
                return;
            }
        }
    }

    /**
     * Removes the installed scheduler and returns it.
     *
     * <p>Intended for the server bootstrap only, after the engine has been shut down. The method is
     * idempotent: calling it when nothing is installed returns an empty optional. After this call
     * {@link #scheduler()} is empty and {@link #require()} throws.
     *
     * @return the scheduler that was installed, or an empty optional if there was none
     */
    public static Optional<AsyncComputeScheduler> uninstall() {
        return Optional.ofNullable(INSTALLED.getAndSet(null));
    }

    /**
     * Creates an {@link java.util.concurrent.Executor} backed by the installed engine.
     *
     * <p>This is a convenience shortcut for {@code new ComputeExecutor(require(), consumerId)}. The
     * consumer id must be one the server configuration knows, or dynamic consumer registration must
     * be enabled; otherwise submissions are rejected and the executor applies its rejection policy.
     *
     * @param consumerId consumer the executor submits for
     * @return an executor that submits to the installed engine
     * @throws IllegalStateException    if the engine is unavailable
     * @throws NullPointerException     if {@code consumerId} is {@code null}
     * @throws IllegalArgumentException if {@code consumerId} is blank
     */
    public static ComputeExecutor executor(final String consumerId) {
        return new ComputeExecutor(require(), consumerId);
    }
}
