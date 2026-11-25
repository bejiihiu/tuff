package kz.bejiihiu.tuff.compute;

import java.time.Duration;
import java.time.Instant;

/**
 * Point in time statistics of a single consumer.
 *
 * <p>Like {@link SchedulerMetrics} this is an immutable snapshot. A consumer that was unregistered
 * keeps its last snapshot for diagnostics until the scheduler is reset, after which it disappears
 * from {@link SchedulerMetrics#consumers()}.
 */
public interface ConsumerMetrics {

    /**
     * Returns the moment this snapshot was taken.
     *
     * @return the capture instant
     */
    Instant capturedAt();

    /**
     * Returns the id of the consumer that owns these numbers.
     *
     * @return the consumer id
     */
    String consumerId();

    /**
     * Checks whether the consumer is currently enabled.
     *
     * @return {@code true} if the consumer may submit work
     */
    boolean enabled();

    /**
     * Returns how many tasks of this consumer are currently executing.
     *
     * @return active tasks of this consumer
     */
    int activeTasks();

    /**
     * Returns how many tasks of this consumer are currently queued.
     *
     * @return queued tasks of this consumer
     */
    int queuedTasks();

    /**
     * Returns the effective concurrency limit of this consumer.
     *
     * @return the number of tasks of this consumer that may run at the same time
     */
    int concurrencyLimit();

    /**
     * Returns the configured per consumer queue bound.
     *
     * @return the bound, or {@code -1} when only the global queue capacity applies
     */
    int maxQueuedTasks();

    /**
     * Returns the total number of accepted submissions of this consumer.
     *
     * @return accepted submissions
     */
    long submittedTasks();

    /**
     * Returns the number of tasks of this consumer that finished successfully.
     *
     * @return successfully completed tasks
     */
    long completedTasks();

    /**
     * Returns the number of tasks of this consumer that finished with a throwable.
     *
     * @return failed tasks
     */
    long failedTasks();

    /**
     * Returns the number of submissions of this consumer refused by the scheduler.
     *
     * @return rejected submissions
     */
    long rejectedTasks();

    /**
     * Returns the number of tasks of this consumer cancelled after acceptance.
     *
     * @return cancelled tasks
     */
    long cancelledTasks();

    /**
     * Returns the number of tasks of this consumer cancelled because they waited too long.
     *
     * @return expired tasks, a subset of {@link #cancelledTasks()}
     */
    long expiredTasks();

    /**
     * Returns the average time tasks of this consumer spent waiting in the queue.
     *
     * @return average queue wait, {@link Duration#ZERO} when no task has started yet
     */
    Duration averageQueueWait();

    /**
     * Returns the longest time a task of this consumer spent waiting in the queue.
     *
     * @return the longest queue wait, {@link Duration#ZERO} when no task has started yet
     */
    Duration longestQueueWait();

    /**
     * Returns the average execution time of this consumer's finished tasks.
     *
     * @return average execution time, {@link Duration#ZERO} when no task has finished yet
     */
    Duration averageExecutionTime();

    /**
     * Returns the longest execution time of this consumer's finished tasks.
     *
     * @return the longest execution time, {@link Duration#ZERO} when no task has finished yet
     */
    Duration longestExecutionTime();

    /**
     * Returns the observed throughput of this consumer.
     *
     * @return finished tasks per second over the scheduler's throughput window
     */
    double throughputPerSecond();
}
