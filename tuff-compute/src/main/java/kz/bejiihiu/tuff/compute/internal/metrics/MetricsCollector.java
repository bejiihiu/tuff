package kz.bejiihiu.tuff.compute.internal.metrics;

import java.util.Map;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;

/**
 * Receives lifecycle events from the engine and the worker pool, and produces metric snapshots.
 *
 * <p>Every method here is called from the hot path, so implementations must be cheap, lock free or
 * nearly lock free, and must never block, allocate per event beyond what is strictly needed, or
 * throw. A broken metric must never fail a task.
 *
 * <p>Event order per task is: {@link #onSubmitted(QueueEntry)} once, then exactly one of
 * {@link #onStarted(QueueEntry, long)} followed by exactly one of
 * {@link #onCompleted(QueueEntry, long)}, {@link #onFailed(QueueEntry, long, Throwable)} or
 * {@link #onCancelled(QueueEntry, ComputeCancelledException.Reason)} for tasks that ran, or just
 * {@link #onCancelled(QueueEntry, ComputeCancelledException.Reason)} for tasks cancelled while
 * queued. Rejected submissions never produce a {@code QueueEntry}, they are reported through
 * {@link #onRejected(String, ComputeRejectedException.Reason)}.
 */
public interface MetricsCollector {

    /**
     * Checks whether collection is enabled.
     *
     * @return {@code true} if counters and gauges are maintained
     */
    boolean enabled();

    /**
     * Records an accepted submission.
     *
     * @param entry the queued entry
     */
    void onSubmitted(QueueEntry entry);

    /**
     * Records a refused submission.
     *
     * @param consumerId consumer the submission was made for
     * @param reason     why it was refused
     */
    void onRejected(String consumerId, ComputeRejectedException.Reason reason);

    /**
     * Records that a task started executing.
     *
     * @param entry          the executed entry
     * @param queueWaitNanos how long it waited in the queue
     */
    void onStarted(QueueEntry entry, long queueWaitNanos);

    /**
     * Records that a task finished successfully.
     *
     * @param entry          the executed entry
     * @param executionNanos how long the task body ran
     */
    void onCompleted(QueueEntry entry, long executionNanos);

    /**
     * Records that a task finished with a throwable.
     *
     * @param entry          the executed entry
     * @param executionNanos how long the task body ran before failing
     * @param failure        throwable thrown by the task
     */
    void onFailed(QueueEntry entry, long executionNanos, Throwable failure);

    /**
     * Records that a task was cancelled, queued or running.
     *
     * @param entry  the cancelled entry
     * @param reason why it was cancelled
     */
    void onCancelled(QueueEntry entry, ComputeCancelledException.Reason reason);

    /**
     * Records a queue depth sample.
     *
     * @param queuedTasks current depth
     * @param capacity    configured capacity
     */
    void onQueueState(int queuedTasks, int capacity);

    /**
     * Records a worker pool sample.
     *
     * @param totalWorkers     live workers
     * @param activeWorkers    busy workers
     * @param idleWorkers      idle workers
     * @param concurrencyLimit current pool wide limit
     */
    void onWorkerState(int totalWorkers, int activeWorkers, int idleWorkers, int concurrencyLimit);

    /**
     * Records a newly registered consumer.
     *
     * @param consumer         the registered consumer
     * @param concurrencyLimit its resolved concurrency limit
     */
    void onConsumerRegistered(ComputeConsumer consumer, int concurrencyLimit);

    /**
     * Records that a consumer policy changed.
     *
     * @param consumer         the updated consumer
     * @param concurrencyLimit its newly resolved concurrency limit
     */
    void onConsumerPolicyUpdated(ComputeConsumer consumer, int concurrencyLimit);

    /**
     * Records that a consumer is gone.
     *
     * <p>Its last snapshot is kept for diagnostics until {@link #reset()}.
     *
     * @param consumerId id of the removed consumer
     */
    void onConsumerUnregistered(String consumerId);

    /**
     * Returns the scheduler wide snapshot.
     *
     * @return an immutable snapshot taken now
     */
    SchedulerMetrics schedulerMetrics();

    /**
     * Returns snapshots of all consumers, including unregistered ones.
     *
     * @return immutable map of consumer id to snapshot
     */
    Map<String, ConsumerMetrics> consumerMetrics();

    /**
     * Returns the snapshot of one consumer.
     *
     * @param consumerId consumer to look up
     * @return the snapshot, or {@code null} if that consumer was never registered
     */
    ConsumerMetrics consumerMetrics(String consumerId);

    /**
     * Drops every counter, gauge and consumer snapshot.
     *
     * <p>Called when the scheduler is reset or restarted.
     */
    void reset();

    /**
     * Releases any resources held by the collector, for example the periodic JFR sampler.
     */
    void close();
}
