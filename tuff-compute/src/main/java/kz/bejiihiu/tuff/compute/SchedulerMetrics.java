package kz.bejiihiu.tuff.compute;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Point in time statistics of the whole scheduler.
 *
 * <p>An instance is an immutable snapshot taken when {@link AsyncComputeScheduler#metrics()} was
 * called. Counters are monotonic since the last {@code reset} of the scheduler, gauges describe the
 * moment the snapshot was taken, and durations are averages over the lifetime of the scheduler
 * unless the scheduler is configured with a rolling window.
 *
 * <p>Snapshot values are internally consistent with each other: reading two accessors of the same
 * instance never observes two different moments.
 */
public interface SchedulerMetrics {

    /**
     * Returns the moment this snapshot was taken.
     *
     * @return the capture instant
     */
    Instant capturedAt();

    /**
     * Checks whether metrics collection is enabled in the scheduler configuration.
     *
     * <p>When collection is disabled the snapshot still exists, but counters stay at zero and gauges
     * may be stale.
     *
     * @return {@code true} if metrics are collected
     */
    boolean enabled();

    /**
     * Returns the total number of accepted submissions.
     *
     * @return accepted submissions, including the ones that were later cancelled
     */
    long submittedTasks();

    /**
     * Returns the number of tasks that finished successfully.
     *
     * @return successfully completed tasks
     */
    long completedTasks();

    /**
     * Returns the number of tasks that finished with a throwable.
     *
     * @return failed tasks
     */
    long failedTasks();

    /**
     * Returns the number of submissions refused by the scheduler.
     *
     * @return rejected submissions, these were never queued
     */
    long rejectedTasks();

    /**
     * Returns the number of tasks cancelled after acceptance.
     *
     * @return cancelled tasks, whether they were queued or already running
     */
    long cancelledTasks();

    /**
     * Returns the number of queued tasks that expired before a worker picked them up.
     *
     * @return tasks cancelled because they waited longer than their consumer policy allows
     */
    long expiredTasks();

    /**
     * Returns the number of queued tasks currently waiting.
     *
     * @return current queue depth across all consumers
     */
    int queuedTasks();

    /**
     * Returns the configured global queue capacity.
     *
     * @return maximum queue depth, never below {@code 1}
     */
    int queueCapacity();

    /**
     * Returns the number of worker threads that currently exist.
     *
     * @return live workers, idle or not
     */
    int totalWorkers();

    /**
     * Returns the number of workers currently executing a task.
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
     * @return peak worker count since the scheduler started
     */
    int peakWorkers();

    /**
     * Returns the current adaptive concurrency limit.
     *
     * <p>The limit caps how many tasks the pool claims at once and may be lower than
     * {@link #totalWorkers()} while adaptive concurrency is throttling the pool.
     *
     * @return maximum number of concurrently executing tasks
     */
    int concurrencyLimit();

    /**
     * Returns the observed throughput of the scheduler.
     *
     * @return finished tasks per second over the configured throughput window
     */
    double throughputPerSecond();

    /**
     * Returns the estimated CPU utilisation of the worker pool.
     *
     * @return a value between {@code 0.0} and {@code 1.0}, or {@link Double#NaN} when the estimate is
     *     unavailable or disabled
     */
    double cpuUtilization();

    /**
     * Returns the average time tasks spent waiting in the queue.
     *
     * @return average queue wait, {@link Duration#ZERO} when no task has started yet
     */
    Duration averageQueueWait();

    /**
     * Returns the longest time a task spent waiting in the queue.
     *
     * @return the longest observed queue wait, {@link Duration#ZERO} when no task has started yet
     */
    Duration longestQueueWait();

    /**
     * Returns the average execution time of finished tasks.
     *
     * @return average execution time, {@link Duration#ZERO} when no task has finished yet
     */
    Duration averageExecutionTime();

    /**
     * Returns the longest execution time of a finished task.
     *
     * @return the longest observed execution time, {@link Duration#ZERO} when no task has finished yet
     */
    Duration longestExecutionTime();

    /**
     * Returns a convenience value combining {@link #completedTasks()} and {@link #failedTasks()}.
     *
     * @return the number of tasks that reached a result, successful or not
     */
    default long executedTasks() {
        return this.completedTasks() + this.failedTasks();
    }

    /**
     * Returns per consumer snapshots.
     *
     * @return an immutable map of consumer id to consumer metrics, including disabled consumers
     */
    Map<String, ConsumerMetrics> consumers();

    /**
     * Returns the snapshot of one consumer.
     *
     * @param consumerId consumer to look up
     * @return the consumer metrics, or an empty optional if no consumer with that id exists
     */
    Optional<ConsumerMetrics> consumer(String consumerId);
}
