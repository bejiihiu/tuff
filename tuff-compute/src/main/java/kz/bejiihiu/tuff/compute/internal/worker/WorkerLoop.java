package kz.bejiihiu.tuff.compute.internal.worker;

import org.jspecify.annotations.Nullable;

/**
 * What a worker thread does on every iteration.
 *
 * <p>Implemented by the engine, called by the pool. The contract is intentionally tiny: the pool
 * knows nothing about tasks, queues or futures, it just keeps calling {@link #executeNext(int)} until
 * it returns {@code false} or the pool is shutting down.
 */
public interface WorkerLoop {

    /**
     * Claims and executes one task, or waits briefly for one to appear.
     *
     * <p>Called in a tight loop by a worker thread. The implementation owns the whole cycle:
     * dispatching a task while respecting the concurrency limit, executing it, completing its
     * handle, releasing its consumer quota and recording metrics. It must never let an exception
     * escape: a task failure is a normal outcome and is delivered to the caller's future, not thrown
     * out of this method.
     *
     * <p>The call may block for a short, bounded time (for example the configured
     * {@link kz.bejiihiu.tuff.compute.ComputeSettings.Scheduling#idleWait()}) while there is nothing
     * to run. It must not block indefinitely, otherwise shutdown cannot complete.
     *
     * @param workerIndex zero based index of the calling worker, used for work stealing and for
     *                    per worker counters
     * @return {@code true} if the worker should keep running, {@code false} if it should exit
     */
    boolean executeNext(int workerIndex);

    /**
     * Notifies the loop that a worker thread came up.
     *
     * <p>Called on the worker thread itself before the first {@link #executeNext(int)}. Implementations
     * use it for per worker bookkeeping; the default does nothing.
     *
     * @param workerIndex zero based index of the worker
     */
    default void workerStarted(final int workerIndex) {
    }

    /**
     * Notifies the loop that a worker thread is about to exit.
     *
     * <p>Called on the worker thread itself after the last {@link #executeNext(int)}, including when
     * the worker exits because of an unrecoverable error. The default does nothing.
     *
     * @param workerIndex zero based index of the worker
     * @param cause       throwable that caused the exit, or {@code null} for a normal exit
     */
    default void workerStopped(final int workerIndex, final @Nullable Throwable cause) {
    }
}
