package kz.bejiihiu.tuff.compute.internal.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.jspecify.annotations.Nullable;

/**
 * Shared fixtures for the metrics tests: a controllable clock, queue entries with a harmless task and
 * a settings helper that only overrides the metrics section.
 */
final class MetricsFixtures {

    private static final ComputeTask<Object> TASK = () -> "result";
    private static final ComputeTaskHandle<Object> HANDLE = new TestHandle();

    private MetricsFixtures() {
    }

    static QueueEntry entry(final long taskId, final String consumerId, final ComputePriority priority) {
        return new QueueEntry(taskId, consumerId, ComputeTaskType.GENERAL, priority, taskId, 0L, TASK, HANDLE);
    }

    static ComputeConsumer consumer(final String id) {
        return ComputeConsumer.of(id, ConsumerPolicy.defaults());
    }

    static ComputeConsumer disabledConsumer(final String id) {
        final ConsumerPolicy defaults = ConsumerPolicy.defaults();
        return ComputeConsumer.of(
            id,
            new ConsumerPolicy(
                false,
                defaults.concurrencyLimit(),
                defaults.maxQueuedTasks(),
                defaults.fairShareWeight(),
                defaults.overflowPolicy(),
                defaults.maxQueueWait()
            )
        );
    }

    static ComputeSettings settings(
        final boolean metricsEnabled,
        final boolean jfrEnabled,
        final Duration throughputWindow,
        final boolean sampleCpu,
        final Duration sampleInterval
    ) {
        final ComputeSettings defaults = ComputeSettings.defaults();
        return new ComputeSettings(
            defaults.enabled(),
            defaults.workers(),
            defaults.queue(),
            defaults.scheduling(),
            defaults.consumers(),
            new ComputeSettings.Metrics(metricsEnabled, jfrEnabled, throughputWindow, sampleCpu, sampleInterval),
            defaults.adaptive(),
            defaults.shutdownGracePeriod()
        );
    }

    static ComputeSettings defaults() {
        return ComputeSettings.defaults();
    }

    /**
     * Clock driven by tests; the monotonic and wall clock readings advance together.
     */
    static final class FakeClock implements ComputeClock {

        private long nanos;
        private Instant instant = Instant.parse("2026-01-01T00:00:00Z");

        @Override
        public long nanoTime() {
            return this.nanos;
        }

        @Override
        public Instant instant() {
            return this.instant;
        }

        void advance(final Duration duration) {
            this.nanos += duration.toNanos();
            this.instant = this.instant.plus(duration);
        }
    }

    /**
     * Handle double for entries that are never cancelled in these tests.
     */
    static final class TestHandle implements ComputeTaskHandle<Object> {

        @Override
        public long taskId() {
            return 1L;
        }

        @Override
        public String consumerId() {
            return "test";
        }

        @Override
        public ComputeTaskType taskType() {
            return ComputeTaskType.GENERAL;
        }

        @Override
        public ComputePriority priority() {
            return ComputePriority.NORMAL;
        }

        @Override
        public Instant submittedAt() {
            return Instant.EPOCH;
        }

        @Override
        public CompletableFuture<Object> future() {
            return CompletableFuture.completedFuture("result");
        }

        @Override
        public State state() {
            return State.COMPLETED;
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
            return true;
        }

        @Override
        public long queueWaitNanos() {
            return 0L;
        }

        @Override
        public long executionNanos() {
            return 0L;
        }

        @Override
        public @Nullable Throwable failure() {
            return null;
        }
    }
}
