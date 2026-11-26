package kz.bejiihiu.tuff.threads.internal.spi;

import java.time.Duration;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;

/**
 * Lifecycle seam of the thread manager.
 *
 * <p>Public callers only ever see {@link UnifiedThreadManager}; creation, start and shutdown are
 * restricted to the server bootstrap, which is what this interface is for. Keeping it internal is not
 * cosmetic: a plugin or a subsystem that could start or stop the layer would be able to take thread
 * accounting away from the rest of the server.
 *
 * <p>Start order: construct everything (registry, controller, metrics, probes), then call
 * {@link #start()}. Threads are created during start, not in the constructor, so a manager that fails to
 * start leaves no threads behind. Shutdown order: stop accepting work, give running tasks a grace
 * period, stop probes and samplers, join threads, then let the JVM continue its own shutdown. The
 * runtime never installs a JVM shutdown hook; there is exactly one shutdown path and it belongs to the
 * server.
 *
 * <p>All methods are thread safe. {@link #start()} and {@link #shutdown(Duration)} are idempotent;
 * {@link #awaitTermination(Duration)} is the only blocking one and must not be called from a region tick
 * thread.
 */
public interface ThreadManagerRuntime {

    /**
     * Returns the public facade backed by this runtime.
     *
     * @return the manager, never {@code null}
     */
    UnifiedThreadManager manager();

    /**
     * Starts the manager.
     *
     * <p>Registers nothing on its own, creates the pools' threads according to the configuration and
     * starts the metrics sampler. Calling it twice has no effect; calling it after shutdown fails.
     *
     * @throws IllegalStateException if the runtime already stopped
     */
    void start();

    /**
     * Starts a graceful shutdown.
     *
     * <p>New registrations, factories and leases are refused. Running work is not interrupted: the
     * manager waits at most {@code gracePeriod} for leases to be released and threads to finish, then
     * moves to STOPPED anyway and logs what was still alive. Idempotent. Does not block.
     *
     * @param gracePeriod how long to wait for the last leases before forcing the stop, not negative
     * @throws IllegalArgumentException if {@code gracePeriod} is negative
     */
    void shutdown(Duration gracePeriod);

    /**
     * Waits for the runtime to reach the stopped state.
     *
     * @param timeout maximum time to wait, not negative
     * @return {@code true} if the runtime stopped within the timeout
     * @throws InterruptedException     if the calling thread was interrupted while waiting
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    boolean awaitTermination(Duration timeout) throws InterruptedException;
}
