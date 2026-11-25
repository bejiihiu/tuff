package kz.bejiihiu.tuff.compute.internal.worker;

import java.time.Duration;

/**
 * Pool of reusable worker threads.
 *
 * <p>Implementations are created by the engine with a {@link WorkerLoop}, a
 * {@link kz.bejiihiu.tuff.compute.internal.util.ComputeClock}, a thread factory and the sizing from
 * {@link kz.bejiihiu.tuff.compute.ComputeSettings.Workers}. Construction details are the
 * implementation's business; this interface is what the engine and the lifecycle require.
 *
 * <p>Guarantees the engine relies on:
 * <ul>
 *     <li>a worker that throws out of {@code executeNext} does not kill the pool, the loop is
 *     re-entered or the worker exits after reporting the cause;</li>
 *     <li>no worker thread is ever interrupted by the pool, cancellation is cooperative;</li>
 *     <li>{@link #shutdown()} stops claiming new tasks but lets the current one finish;</li>
 *     <li>{@link #shutdownNow()} also stops claiming immediately and never waits for the queue to
 *     drain;</li>
 *     <li>counters are cheap enough to be read from a metrics sampler every second.</li>
 * </ul>
 */
public interface WorkerPool {

    /**
     * Creates at least the minimum number of workers and lets the pool start claiming work.
     *
     * <p>Idempotent: starting a started pool does nothing.
     */
    void start();

    /**
     * Starts a graceful shutdown.
     *
     * <p>Workers stop claiming new tasks after the current one finishes and exit once the queue
     * offers nothing more. Queued tasks are not cancelled by the pool; the engine decides what
     * happens to them.
     */
    void shutdown();

    /**
     * Starts an immediate shutdown.
     *
     * <p>Workers stop claiming new tasks right away. A task that is already running is allowed to
     * finish, because interrupting a compute task would break the world state the task is reading.
     */
    void shutdownNow();

    /**
     * Waits for all worker threads to exit.
     *
     * @param timeout maximum time to wait, must not be negative
     * @return {@code true} if the pool terminated within the timeout
     * @throws InterruptedException if the calling thread was interrupted while waiting
     */
    boolean awaitTermination(Duration timeout) throws InterruptedException;

    /**
     * Checks whether a shutdown was requested.
     *
     * @return {@code true} after {@link #shutdown()} or {@link #shutdownNow()}
     */
    boolean isShutdown();

    /**
     * Checks whether every worker thread has exited.
     *
     * @return {@code true} if the pool is fully stopped
     */
    boolean isTerminated();

    /**
     * Returns the number of live worker threads.
     *
     * @return live workers, idle or busy
     */
    int workerCount();

    /**
     * Returns the number of workers currently inside a task.
     *
     * @return busy workers
     */
    int activeWorkers();

    /**
     * Returns the number of workers currently waiting for a task.
     *
     * @return idle workers
     */
    int idleWorkers();

    /**
     * Returns the highest number of workers that ever existed at the same time.
     *
     * @return the peak worker count since the pool started
     */
    int peakWorkers();

    /**
     * Returns the pool wide concurrency limit.
     *
     * @return the maximum number of workers allowed to execute a task at the same time
     */
    int concurrencyLimit();

    /**
     * Changes the pool wide concurrency limit.
     *
     * <p>Lowering the limit never interrupts a running task; it only stops idle workers from claiming
     * until the number of busy workers is below the limit again. Raising it may wake parked workers
     * or create new ones up to the configured maximum.
     *
     * @param permits new limit, at least {@code 1} and at most {@link #workerCount()} once the pool
     *                has grown; values above the current worker count are clamped
     */
    void setConcurrencyLimit(int permits);
}
