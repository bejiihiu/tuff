package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.SchedulerMetrics.LaneMetrics;

/**
 * Immutable scheduler snapshot produced by {@link ComputeMetricsCollector}.
 *
 * <p>Besides the values the public {@link SchedulerMetrics} contract exposes, this record keeps the
 * diagnostic breakdowns the contract has no place for: rejections and cancellations per reason,
 * evictions, queue depth per priority and the full latency histograms. The maps are copied on
 * construction, so a caller can never mutate the collector's view afterwards.
 *
 * @param capturedAt                moment the snapshot was taken
 * @param enabled                   whether collection is enabled
 * @param submittedTasks            accepted submissions
 * @param completedTasks            successfully completed tasks
 * @param failedTasks               tasks that finished with a throwable
 * @param rejectedTasks             refused submissions
 * @param cancelledTasks            tasks cancelled after acceptance
 * @param expiredTasks              queued tasks cancelled because their wait expired
 * @param evictedTasks              queued tasks evicted by the drop-oldest overflow policy
 * @param queuedTasks               queue depth at the capture moment
 * @param queueCapacity             configured queue capacity
 * @param totalWorkers              live workers
 * @param activeWorkers             busy workers
 * @param idleWorkers               idle workers
 * @param peakWorkers               highest worker count seen so far
 * @param concurrencyLimit          current pool wide concurrency limit
 * @param throughputPerSecond       finished tasks per second across the throughput window
 * @param recentThroughputPerSecond rate of the most recent sample interval
 * @param cpuUtilization            process CPU load, or {@link Double#NaN} when unavailable
 * @param averageQueueWait          mean queue wait
 * @param longestQueueWait          longest queue wait
 * @param averageExecutionTime      mean execution time
 * @param longestExecutionTime      longest execution time
 * @param consumers                 per consumer snapshots, including unregistered consumers
 * @param rejectedByReason          rejections per refusal reason
 * @param cancelledByReason         cancellations per reason
 * @param queuedByPriority          queue depth per submitted priority
 * @param queueWaitStats            full queue wait distribution
 * @param executionStats            full execution time distribution
 * @param lanes                     execution lane snapshot
 * @param runningTasks              tasks executing at the capture moment
 * @param recentTasks               recently finished tasks, slowest first
 */
public record SchedulerMetricsSnapshot(
    Instant capturedAt,
    boolean enabled,
    long submittedTasks,
    long completedTasks,
    long failedTasks,
    long rejectedTasks,
    long cancelledTasks,
    long expiredTasks,
    long evictedTasks,
    int queuedTasks,
    int queueCapacity,
    int totalWorkers,
    int activeWorkers,
    int idleWorkers,
    int peakWorkers,
    int concurrencyLimit,
    double throughputPerSecond,
    double recentThroughputPerSecond,
    double cpuUtilization,
    Duration averageQueueWait,
    Duration longestQueueWait,
    Duration averageExecutionTime,
    Duration longestExecutionTime,
    Map<String, ConsumerMetrics> consumers,
    Map<ComputeRejectedException.Reason, Long> rejectedByReason,
    Map<ComputeCancelledException.Reason, Long> cancelledByReason,
    Map<ComputePriority, Integer> queuedByPriority,
    LatencyStats queueWaitStats,
    LatencyStats executionStats,
    LaneMetrics lanes,
    List<RunningTaskInfo> runningTasks,
    List<CompletedTaskInfo> recentTasks
) implements SchedulerMetrics {

    /**
     * Freezes every map so later collector activity cannot leak into this snapshot.
     */
    public SchedulerMetricsSnapshot {
        Objects.requireNonNull(capturedAt, "capturedAt");
        Objects.requireNonNull(averageQueueWait, "averageQueueWait");
        Objects.requireNonNull(longestQueueWait, "longestQueueWait");
        Objects.requireNonNull(averageExecutionTime, "averageExecutionTime");
        Objects.requireNonNull(longestExecutionTime, "longestExecutionTime");
        Objects.requireNonNull(queueWaitStats, "queueWaitStats");
        Objects.requireNonNull(executionStats, "executionStats");
        Objects.requireNonNull(lanes, "lanes");
        Objects.requireNonNull(runningTasks, "runningTasks");
        Objects.requireNonNull(recentTasks, "recentTasks");
        consumers = Map.copyOf(consumers);
        rejectedByReason = Map.copyOf(rejectedByReason);
        cancelledByReason = Map.copyOf(cancelledByReason);
        queuedByPriority = Map.copyOf(queuedByPriority);
        runningTasks = List.copyOf(runningTasks);
        recentTasks = List.copyOf(recentTasks);
    }

    @Override
    public Optional<ConsumerMetrics> consumer(final String consumerId) {
        return Optional.ofNullable(this.consumers.get(consumerId));
    }

    @Override
    public List<CompletedTaskInfo> slowestRecentTasks(final int limit) {
        if (limit <= 0) {
            return List.of();
        }
        return this.recentTasks.size() <= limit
            ? this.recentTasks
            : this.recentTasks.subList(0, limit);
    }
}
