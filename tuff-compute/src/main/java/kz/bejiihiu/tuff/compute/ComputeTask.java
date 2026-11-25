package kz.bejiihiu.tuff.compute;

/**
 * A unit of CPU bound work executed by the scheduler on one of its workers.
 *
 * <p>Implementations are called exactly once per successful submission (never retried
 * automatically) on a worker thread that belongs to the scheduler. They must be self contained:
 * input is captured by the caller, and mutable world state must not be touched because the worker
 * owns no region and no entity.
 *
 * <p>Any {@link Throwable} thrown by {@link #execute()} is caught by the scheduler. The worker is
 * returned to the pool, the failure is counted in the metrics and the caller's future completes
 * exceptionally with that throwable. Throwing is never a way to stop the pool.
 *
 * <p>Cancellation is cooperative: the task body should check the handle it was submitted with, or
 * simply let the execution finish, because workers are never interrupted.
 *
 * @param <T> type of the produced value
 */
@FunctionalInterface
public interface ComputeTask<T> {

    /**
     * Performs the computation.
     *
     * <p>A task must produce a value; returning {@code null} is treated as a bug in the task and
     * fails the submission with a {@link NullPointerException} instead of silently handing a
     * {@code null} to the caller. Use {@link java.util.Optional} when absence is meaningful.
     * Checked exceptions are allowed and are delivered to the caller as the scheduler documents,
     * without retrying the task.
     *
     * @return the computed value
     * @throws Exception if the computation failed
     */
    T execute() throws Exception;
}
