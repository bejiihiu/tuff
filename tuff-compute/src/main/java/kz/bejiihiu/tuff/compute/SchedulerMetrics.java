package kz.bejiihiu.tuff.compute;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
     * Returns the number of queued tasks evicted to make room for newer work.
     *
     * <p>Eviction is a subset of {@link #cancelledTasks()}: the task is cancelled with
     * {@link ComputeCancelledException.Reason#EVICTED}. Implementations that do not track evictions
     * separately return zero, which keeps older snapshots source compatible.
     *
     * @return queued tasks dropped by the {@link OverflowPolicy#DROP_OLDEST} policy
     */
    default long evictedTasks() {
        return 0L;
    }

    /**
     * Returns cancellations grouped by the reason they happened.
     *
     * <p>The counts sum to {@link #cancelledTasks()} when every cancellation is accounted for;
     * reasons that never happened may be present with a zero count. Implementations without a
     * breakdown return an empty map.
     *
     * @return an immutable map of cancellation reason to count, never {@code null}
     */
    default Map<ComputeCancelledException.Reason, Long> cancelledByReason() {
        return Map.of();
    }

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
     * Returns the region relief snapshot.
     *
     * <p>Implementations without a relief path return an empty optional, which is also what a
     * scheduler without one would report. The snapshot is a point in time view; its counters are
     * monotonic like the rest of this interface.
     *
     * @return the relief snapshot, or an empty optional when relief is not part of this scheduler
     */
    default Optional<RegionReliefMetrics> regionRelief() {
        return Optional.empty();
    }

    /**
     * Returns the snapshot of one consumer.
     *
     * @param consumerId consumer to look up
     * @return the consumer metrics, or an empty optional if no consumer with that id exists
     */
    Optional<ConsumerMetrics> consumer(String consumerId);

    /**
     * Returns the tasks that were executing when the snapshot was taken.
     *
     * <p>The list is materialised on demand, so it describes the capture moment and not a live
     * view: {@link RunningTaskInfo#elapsedNanos()} is measured against that moment, a task that
     * finishes right after the snapshot still appears here, and a task that started a moment
     * before may not. Implementations that do not track live tasks return an empty list.
     *
     * @return the running tasks, never {@code null}
     */
    default List<RunningTaskInfo> runningTasks() {
        return List.of();
    }

    /**
     * Returns the slowest tasks that finished recently.
     *
     * <p>Implementations keep a bounded ring of the last finished tasks and sort it by execution
     * time descending. Tasks cancelled before they ever started are not part of the ring, because
     * they have no execution time to compare. Implementations without such a ring return an empty
     * list.
     *
     * @param limit maximum number of entries to return, values below one yield an empty list
     * @return at most {@code limit} recently finished tasks, never {@code null}
     */
    default List<CompletedTaskInfo> slowestRecentTasks(final int limit) {
        return List.of();
    }

    /**
     * One task that was executing when the snapshot was taken.
     *
     * @param taskId       unique task id
     * @param consumerId   consumer the task belongs to
     * @param taskType     declared category of the task
     * @param priority     submitted priority
     * @param lane         execution lane, {@code platform} or {@code virtual}
     * @param label        optional diagnostic label of the submission
     * @param startedAt    wall clock instant at which the body started
     * @param elapsedNanos how long the body had been running when the snapshot was taken
     */
    record RunningTaskInfo(
        long taskId,
        String consumerId,
        ComputeTaskType taskType,
        ComputePriority priority,
        String lane,
        Optional<String> label,
        Instant startedAt,
        long elapsedNanos
    ) {

        /**
         * Validates the entry.
         *
         * @param taskId       unique task id
         * @param consumerId   consumer the task belongs to
         * @param taskType     declared category of the task
         * @param priority     submitted priority
         * @param lane         execution lane
         * @param label        optional diagnostic label
         * @param startedAt    wall clock instant at which the body started
         * @param elapsedNanos how long the body had been running when the snapshot was taken
         * @throws NullPointerException if a reference is {@code null}
         */
        public RunningTaskInfo {
            Objects.requireNonNull(consumerId, "consumerId");
            Objects.requireNonNull(taskType, "taskType");
            Objects.requireNonNull(priority, "priority");
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(label, "label");
            Objects.requireNonNull(startedAt, "startedAt");
        }
    }

    /**
     * One task from the recent completion ring.
     *
     * @param taskId         unique task id
     * @param consumerId     consumer the task belonged to
     * @param taskType       declared category of the task
     * @param priority       submitted priority
     * @param lane           execution lane the body ran on
     * @param label          optional diagnostic label of the submission
     * @param executionNanos how long the body ran
     * @param queueWaitNanos how long the task waited in the queue before it started
     * @param state          terminal state, one of completed, failed or cancelled after start
     */
    record CompletedTaskInfo(
        long taskId,
        String consumerId,
        ComputeTaskType taskType,
        ComputePriority priority,
        String lane,
        Optional<String> label,
        long executionNanos,
        long queueWaitNanos,
        ComputeTaskHandle.State state
    ) {

        /**
         * Validates the entry.
         *
         * @param taskId         unique task id
         * @param consumerId     consumer the task belonged to
         * @param taskType       declared category of the task
         * @param priority       submitted priority
         * @param lane           execution lane the body ran on
         * @param label          optional diagnostic label
         * @param executionNanos how long the body ran
         * @param queueWaitNanos how long the task waited before it started
         * @param state          terminal state
         * @throws NullPointerException if a reference is {@code null}
         */
        public CompletedTaskInfo {
            Objects.requireNonNull(consumerId, "consumerId");
            Objects.requireNonNull(taskType, "taskType");
            Objects.requireNonNull(priority, "priority");
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(label, "label");
            Objects.requireNonNull(state, "state");
        }
    }

    /**
     * Returns the execution lane snapshot.
     *
     * <p>The platform lane has no dedicated numbers here: its counters are the scheduler wide
     * counters of this snapshot. This accessor adds the virtual lane, which runs tasks that are not
     * platform workers and therefore are not visible in {@link #activeWorkers()} or
     * {@link #totalWorkers()}.
     *
     * @return the lane snapshot, never {@code null}
     */
    default LaneMetrics lanes() {
        return LaneMetrics.disabled();
    }

    /**
     * Virtual lane counters and gauges.
     *
     * <p>{@code submitted} counts tasks that were <b>dispatched</b> to the lane, so the invariant
     * {@code submitted == completed + failed + cancelled + running} holds at every snapshot.
     * Tasks cancelled while still queued never entered a lane and are only visible in the scheduler
     * wide counters. {@code spilled} is the subset of {@code submitted} that was NORMAL work spilled
     * from the platform lane.
     */
    interface VirtualLane {

        /**
         * Checks whether the virtual lane exists.
         *
         * @return {@code true} when the lane was configured and started
         */
        boolean enabled();

        /**
         * Returns the number of virtual tasks executing right now.
         *
         * @return in flight virtual tasks
         */
        int running();

        /**
         * Returns the hard concurrency limit of the lane.
         *
         * @return {@code maxConcurrent}, or {@code 0} when the lane is disabled
         */
        int limit();

        /**
         * Returns how much work is waiting for the lane.
         *
         * <p>This is a gauge, not part of the lane invariant: it counts LOW entries plus NORMAL
         * entries that are currently spill eligible. NORMAL work that the platform lane may still
         * take is not included.
         *
         * @return queued entries the lane may claim
         */
        int queued();

        /**
         * Returns how many tasks were dispatched to the lane.
         *
         * @return started virtual tasks since the scheduler started
         */
        long submitted();

        /**
         * Returns how many virtual tasks finished successfully.
         *
         * @return completed virtual tasks
         */
        long completed();

        /**
         * Returns how many virtual tasks failed.
         *
         * @return failed virtual tasks
         */
        long failed();

        /**
         * Returns how many virtual tasks were cancelled after they started.
         *
         * @return cancelled virtual tasks
         */
        long cancelled();

        /**
         * Returns how many NORMAL tasks were spilled from the platform lane.
         *
         * @return spilled tasks, a subset of {@link #submitted()}
         */
        long spilled();
    }

    /**
     * Immutable {@link VirtualLane} snapshot.
     *
     * @param enabled   whether the lane exists
     * @param running   in flight virtual tasks
     * @param limit     hard concurrency limit
     * @param queued    work waiting for the lane
     * @param submitted tasks dispatched to the lane
     * @param completed tasks completed on the lane
     * @param failed    tasks failed on the lane
     * @param cancelled started tasks cancelled on the lane
     * @param spilled   NORMAL tasks spilled from the platform lane
     */
    record VirtualLaneMetrics(
        boolean enabled,
        int running,
        int limit,
        int queued,
        long submitted,
        long completed,
        long failed,
        long cancelled,
        long spilled
    ) implements VirtualLane {

        /**
         * Returns an all zero snapshot of a lane that does not exist.
         *
         * @return the disabled snapshot
         */
        public static VirtualLaneMetrics disabled() {
            return new VirtualLaneMetrics(false, 0, 0, 0, 0L, 0L, 0L, 0L, 0L);
        }
    }

    /**
     * Execution lane snapshot.
     */
    interface LaneMetrics {

        /**
         * Returns the virtual lane snapshot.
         *
         * @return the virtual lane
         */
        VirtualLane virtual();

        /**
         * Returns a snapshot with a disabled virtual lane.
         *
         * @return the disabled snapshot
         */
        static LaneMetrics disabled() {
            return new LaneMetricsSnapshot(VirtualLaneMetrics.disabled());
        }
    }

    /**
     * Immutable {@link LaneMetrics} snapshot.
     *
     * @param virtual virtual lane snapshot
     */
    record LaneMetricsSnapshot(VirtualLane virtual) implements LaneMetrics {

        /**
         * Validates the snapshot.
         *
         * @param virtual virtual lane snapshot
         * @throws NullPointerException if {@code virtual} is {@code null}
         */
        public LaneMetricsSnapshot {
            Objects.requireNonNull(virtual, "virtual");
        }
    }
}
