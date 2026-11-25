package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;

/**
 * Shared fixtures for the queue tests: a manually driven clock and entry factories.
 */
final class TestSupport {

    private static final long TASK_RESULT = 8L;

    private TestSupport() {
    }

    /**
     * Creates scheduling options with a one second aging interval.
     *
     * @param strategy    dispatch strategy
     * @param aging       whether aging is enabled
     * @param boost       maximum aging boost in levels
     * @param stealing    whether work stealing is enabled
     * @return the options
     */
    static ComputeSettings.Scheduling scheduling(
        final ComputeSettings.Scheduling.Strategy strategy,
        final boolean aging,
        final int boost,
        final boolean stealing
    ) {
        return new ComputeSettings.Scheduling(
            strategy,
            stealing,
            aging,
            Duration.ofSeconds(1),
            boost,
            Duration.ofMillis(50)
        );
    }

    /**
     * Creates a consumer policy with explicit queue bound, weight and overflow behaviour.
     *
     * @param maxQueuedTasks per consumer bound, or {@code -1}
     * @param weight         fair share weight
     * @param overflow       overflow policy
     * @return the policy
     */
    static ConsumerPolicy policy(final int maxQueuedTasks, final double weight, final OverflowPolicy overflow) {
        return new ConsumerPolicy(
            true,
            ConsumerPolicy.ConcurrencyLimit.automatic(),
            maxQueuedTasks,
            weight,
            overflow,
            Duration.ZERO
        );
    }

    /**
     * Creates an entry with a real handle and a task that returns a constant.
     *
     * @param taskId           unique task id
     * @param consumerId       consumer of the task
     * @param priority         submitted priority
     * @param sequence         submission sequence
     * @param submittedAtNanos submission timestamp
     * @return the entry
     */
    static QueueEntry entry(
        final long taskId,
        final String consumerId,
        final ComputePriority priority,
        final long sequence,
        final long submittedAtNanos
    ) {
        return new QueueEntry(
            taskId,
            consumerId,
            ComputeTaskType.GENERAL,
            priority,
            sequence,
            submittedAtNanos,
            () -> TASK_RESULT,
            new TestHandle(taskId, consumerId, priority, submittedAtNanos)
        );
    }

    /**
     * Clock moved by hand, so aging and oldest-wait assertions are exact.
     */
    static final class TestClock implements ComputeClock {

        private long nanos;

        @Override
        public long nanoTime() {
            return this.nanos;
        }

        @Override
        public Instant instant() {
            return Instant.EPOCH.plusNanos(this.nanos);
        }

        /**
         * Moves the clock forward.
         *
         * @param duration amount to add
         */
        void advance(final Duration duration) {
            this.nanos += Objects.requireNonNull(duration, "duration").toNanos();
        }
    }

    /**
     * Minimal handle implementation. The queue never touches handles, so this only has to be a
     * faithful value holder.
     */
    private static final class TestHandle implements ComputeTaskHandle<Object> {

        private final long taskId;
        private final String consumerId;
        private final ComputePriority priority;
        private final long submittedAtNanos;
        private final CompletableFuture<Object> future = new CompletableFuture<>();

        TestHandle(
            final long taskId,
            final String consumerId,
            final ComputePriority priority,
            final long submittedAtNanos
        ) {
            this.taskId = taskId;
            this.consumerId = consumerId;
            this.priority = priority;
            this.submittedAtNanos = submittedAtNanos;
        }

        @Override
        public long taskId() {
            return this.taskId;
        }

        @Override
        public String consumerId() {
            return this.consumerId;
        }

        @Override
        public ComputeTaskType taskType() {
            return ComputeTaskType.GENERAL;
        }

        @Override
        public ComputePriority priority() {
            return this.priority;
        }

        @Override
        public Instant submittedAt() {
            return Instant.EPOCH.plusNanos(this.submittedAtNanos);
        }

        @Override
        public CompletableFuture<Object> future() {
            return this.future;
        }

        @Override
        public State state() {
            return State.QUEUED;
        }

        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public boolean isCancellationRequested() {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public long queueWaitNanos() {
            return -1L;
        }

        @Override
        public long executionNanos() {
            return -1L;
        }

        @Override
        public Throwable failure() {
            return null;
        }
    }
}
