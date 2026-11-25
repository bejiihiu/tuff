package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;

/**
 * Immutable snapshot of one consumer, produced by {@link ComputeMetricsCollector}.
 *
 * <p>Unregistered consumers keep their last snapshot, with {@link #enabled()} reporting {@code false},
 * until the collector is reset. The reason breakdowns and the latency distributions are beyond the
 * public contract and exist for diagnostics.
 *
 * @param capturedAt                moment the snapshot was taken
 * @param consumerId                consumer id
 * @param enabled                   whether the consumer may still submit work
 * @param activeTasks               tasks of this consumer running right now
 * @param queuedTasks               tasks of this consumer waiting in the queue
 * @param concurrencyLimit          effective concurrency limit
 * @param maxQueuedTasks            per consumer queue bound, or {@code -1} for the global one only
 * @param submittedTasks            accepted submissions
 * @param completedTasks            successfully completed tasks
 * @param failedTasks               tasks that finished with a throwable
 * @param rejectedTasks             refused submissions
 * @param cancelledTasks            tasks cancelled after acceptance
 * @param expiredTasks              tasks cancelled because their queue wait expired
 * @param evictedTasks              tasks evicted by the drop-oldest overflow policy
 * @param averageQueueWait          mean queue wait
 * @param longestQueueWait          longest queue wait
 * @param averageExecutionTime      mean execution time
 * @param longestExecutionTime      longest execution time
 * @param throughputPerSecond       finished tasks per second across the throughput window
 * @param recentThroughputPerSecond rate of the most recent sample interval
 * @param rejectedByReason          rejections per refusal reason
 * @param cancelledByReason         cancellations per reason
 * @param queueWaitStats            full queue wait distribution
 * @param executionStats            full execution time distribution
 */
public record ConsumerMetricsSnapshot(
    Instant capturedAt,
    String consumerId,
    boolean enabled,
    int activeTasks,
    int queuedTasks,
    int concurrencyLimit,
    int maxQueuedTasks,
    long submittedTasks,
    long completedTasks,
    long failedTasks,
    long rejectedTasks,
    long cancelledTasks,
    long expiredTasks,
    long evictedTasks,
    Duration averageQueueWait,
    Duration longestQueueWait,
    Duration averageExecutionTime,
    Duration longestExecutionTime,
    double throughputPerSecond,
    double recentThroughputPerSecond,
    Map<ComputeRejectedException.Reason, Long> rejectedByReason,
    Map<ComputeCancelledException.Reason, Long> cancelledByReason,
    LatencyStats queueWaitStats,
    LatencyStats executionStats
) implements ConsumerMetrics {

    /**
     * Freezes the maps and validates the required values.
     */
    public ConsumerMetricsSnapshot {
        Objects.requireNonNull(capturedAt, "capturedAt");
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(averageQueueWait, "averageQueueWait");
        Objects.requireNonNull(longestQueueWait, "longestQueueWait");
        Objects.requireNonNull(averageExecutionTime, "averageExecutionTime");
        Objects.requireNonNull(longestExecutionTime, "longestExecutionTime");
        Objects.requireNonNull(queueWaitStats, "queueWaitStats");
        Objects.requireNonNull(executionStats, "executionStats");
        rejectedByReason = Map.copyOf(rejectedByReason);
        cancelledByReason = Map.copyOf(cancelledByReason);
    }
}
