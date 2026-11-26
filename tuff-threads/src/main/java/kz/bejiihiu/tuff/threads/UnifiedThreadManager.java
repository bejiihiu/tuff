package kz.bejiihiu.tuff.threads;

import java.util.Optional;
import java.util.Set;

/**
 * Read only facade over the unified thread layer.
 *
 * <p>This is what commands, diagnostics and subsystem owners hold: one object that answers "what pools
 * exist, how are they loaded, what budget is left, which threads belong to whom". It exposes the
 * registry, the resource controller, the thread factory and the metrics view, all of which are
 * themselves contracts with their own documentation.
 *
 * <p>The manager does not schedule game work. It does not choose regions, does not move tasks between
 * pools, does not enforce region ownership and does not replace Folia's scheduler or the compute
 * engine's queue. Folia keeps owning ticks, order and ownership; the compute engine keeps owning task
 * priority and fairness; UTMI owns the threads, the CPU budget and the accounting underneath both.
 *
 * <p>Lifecycle: an instance is created in {@link State#NEW}, started by the server bootstrap and
 * stopped near the end of shutdown. The lifecycle calls themselves are internal, because a plugin must
 * never be able to start or stop the server's thread layer. While running, the manager may report
 * {@link State#DEGRADED}: everything still works, but one or more pools cannot be managed as configured
 * and {@link #degradedReasons()} says why. After {@link State#STOPPED} every mutating call fails and
 * every read returns the last known values.
 *
 * <p>Threading: all methods are thread safe and non blocking. {@link #snapshot()} copies per pool and
 * per thread data and is meant for commands and periodic reporting, not for hot paths.
 */
public interface UnifiedThreadManager {

    /**
     * Returns the configuration the manager was created with.
     *
     * @return the effective settings, immutable
     */
    ThreadManagementSettings settings();

    /**
     * Returns the pool registry.
     *
     * @return the registry, never {@code null}
     */
    ExecutionPoolRegistry pools();

    /**
     * Returns the resource controller.
     *
     * @return the controller, never {@code null}
     */
    ThreadResourceController resources();

    /**
     * Returns the thread factory provider.
     *
     * @return the factory provider, never {@code null}
     */
    ThreadFactoryProvider threadFactory();

    /**
     * Returns the metrics view.
     *
     * @return the metrics view, never {@code null}
     */
    ResourceMetrics metrics();

    /**
     * Returns the current lifecycle state.
     *
     * @return the state, never {@code null}
     */
    State state();

    /**
     * Checks whether the manager is running and accepts registration, factories and leases.
     *
     * @return {@code true} for {@link State#RUNNING} and {@link State#DEGRADED}
     */
    default boolean isRunning() {
        return this.state() == State.RUNNING || this.state() == State.DEGRADED;
    }

    /**
     * Checks whether the manager is running but cannot honour its configuration fully.
     *
     * @return {@code true} if the state is {@link State#DEGRADED}
     */
    default boolean degraded() {
        return this.state() == State.DEGRADED;
    }

    /**
     * Takes a consistent snapshot of the whole thread layer.
     *
     * <p>Equivalent to {@link ResourceMetrics#snapshot()}.
     *
     * @return the snapshot, never {@code null}
     */
    ResourceSnapshot snapshot();

    /**
     * Looks up the pool a live thread is accounted to.
     *
     * <p>Equivalent to {@link ThreadFactoryProvider#poolOf(Thread)}; kept here because most callers
     * already hold the manager and should not have to reach for the factory just to answer an ownership
     * question.
     *
     * @param thread thread to look up
     * @return the pool id, or an empty optional if the thread is not accounted
     * @throws NullPointerException if {@code thread} is {@code null}
     */
    Optional<PoolId> poolOf(Thread thread);

    /**
     * Returns every reason any pool reported for not being fully manageable.
     *
     * @return an immutable set of reasons, empty when everything is managed as configured
     */
    Set<DegradedReason> degradedReasons();

    /**
     * Lifecycle state of the manager.
     */
    enum State {

        /**
         * Created but not started. Nothing is registered, no thread is created and every mutating call
         * fails.
         */
        NEW,

        /**
         * Running with the configured coverage. Registration, factories and leases work.
         */
        RUNNING,

        /**
         * Running, but at least one pool cannot be managed as configured. Everything still works;
         * {@link UnifiedThreadManager#degradedReasons()} explains what gave up and why.
         */
        DEGRADED,

        /**
         * Shutting down. New registrations, factories and leases are refused; existing threads finish
         * their current work and snapshots keep updating until the last pool is gone.
         */
        SHUTTING_DOWN,

        /**
         * Stopped for good. Reads return the last known values; mutations fail.
         */
        STOPPED
    }
}
