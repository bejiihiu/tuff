package kz.bejiihiu.tuff.compute.internal.adaptive;

import java.time.Duration;

/**
 * Decides how many tasks the pool may execute at the same time.
 *
 * <p>The controller is fed one sample per finished task (queue wait, execution time, success) and one
 * queue state sample per dispatch cycle. Over a configured window it compares queue wait against
 * execution time: rising wait with flat execution means the pool is saturated, so more concurrency
 * helps; rising execution time with flat wait means the machine is already thrashing, so less
 * concurrency helps. That is the whole theory, and every implementation must be honest about it: it
 * may only make small steps, and it must never claim to know more than its samples say.
 *
 * <p>The controller does not touch the pool. It exposes {@link #currentLimit()}, and the engine calls
 * {@link #evaluate()} on a timer and pushes the result into the worker pool and the quota controller.
 * Keeping the two apart makes the controller testable with a fake clock and a synthetic sample stream.
 */
public interface AdaptiveConcurrencyController {

    /**
     * Checks whether the controller is allowed to move the limit.
     *
     * @return {@code true} if adaptive concurrency is enabled in the configuration
     */
    boolean enabled();

    /**
     * Starts the controller, for example by scheduling its evaluation timer.
     *
     * <p>Idempotent.
     */
    void start();

    /**
     * Stops the controller and releases its timer.
     *
     * <p>Idempotent. The current limit stays as it is.
     */
    void stop();

    /**
     * Returns the limit currently in force.
     *
     * @return the number of tasks the pool may execute concurrently, at least {@code 1}
     */
    int currentLimit();

    /**
     * Returns the limit the controller would pick right now.
     *
     * <p>Equal to {@link #currentLimit()} until an {@link #evaluate()} call moves it; exposed so the
     * engine can log the decision before applying it.
     *
     * @return the desired limit, within {@code [minLimit, maxLimit]}
     */
    int desiredLimit();

    /**
     * Returns the lowest limit the controller may pick.
     *
     * @return the configured minimum, at least {@code 1}
     */
    int minLimit();

    /**
     * Returns the highest limit the controller may pick.
     *
     * @return the configured maximum, or the pool size when the configuration says automatic
     */
    int maxLimit();

    /**
     * Records one finished task.
     *
     * <p>Samples are aggregated in a fixed size window. Samples for tasks that were cancelled after
     * starting must not be recorded, because their execution time is meaningless.
     *
     * @param queueWaitNanos how long the task waited before it started
     * @param executionNanos how long the task body ran
     * @param failed         whether the task failed, which usually means the estimate is unreliable
     */
    void recordSample(long queueWaitNanos, long executionNanos, boolean failed);

    /**
     * Records the queue depth observed by the dispatcher.
     *
     * @param queuedTasks current queue depth
     * @param capacity    configured queue capacity
     */
    void onQueueState(int queuedTasks, int capacity);

    /**
     * Recomputes the desired limit from the samples collected since the previous evaluation.
     *
     * <p>Does nothing and returns the current limit when the controller is disabled, when the sample
     * window is not full yet, or when fewer than the configured minimum number of samples arrived.
     * When it does move, the change is at most one additive increase step or one multiplicative
     * decrease, and the result is clamped into {@code [minLimit, maxLimit]}.
     *
     * @return the limit that should now be applied to the pool
     */
    int evaluate();

    /**
     * Returns a snapshot of the controller state.
     *
     * @return the snapshot
     */
    AdaptiveSnapshot snapshot();

    /**
     * Returns the configured evaluation window.
     *
     * @return the window over which samples are aggregated
     */
    Duration sampleWindow();
}
