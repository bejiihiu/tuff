package kz.bejiihiu.tuff.compute.internal.metrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.ComputeMetricsCollector;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.FlightJfrRecorder;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.SchedulerMetricsSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComputeMetricsCollectorTest {

    private final List<ComputeMetricsCollector> collectors = new ArrayList<>();

    @AfterEach
    void closeCollectors() {
        for (final ComputeMetricsCollector collector : this.collectors) {
            collector.close();
        }
        this.collectors.clear();
    }

    @Test
    void countsLifecycleEventsAndLatencies() {
        final MetricsFixtures.FakeClock clock = new MetricsFixtures.FakeClock();
        final ComputeMetricsCollector collector = this.collector(true, false, clock);
        collector.onConsumerRegistered(MetricsFixtures.consumer("chunk"), 4);

        for (long taskId = 1L; taskId <= 3L; taskId++) {
            collector.onSubmitted(MetricsFixtures.entry(taskId, "chunk", ComputePriority.NORMAL));
        }
        collector.onStarted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 1_000L);
        collector.onCompleted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 5_000L);
        collector.onStarted(MetricsFixtures.entry(2L, "chunk", ComputePriority.NORMAL), 2_000L);
        collector.onFailed(MetricsFixtures.entry(2L, "chunk", ComputePriority.NORMAL), 7_000L, new IllegalStateException("boom"));
        collector.onStarted(MetricsFixtures.entry(3L, "chunk", ComputePriority.NORMAL), 3_000L);
        collector.onCancelled(MetricsFixtures.entry(3L, "chunk", ComputePriority.NORMAL), ComputeCancelledException.Reason.CALLER);

        final SchedulerMetricsSnapshot snapshot = (SchedulerMetricsSnapshot) collector.schedulerMetrics();

        assertEquals(3L, snapshot.submittedTasks());
        assertEquals(1L, snapshot.completedTasks());
        assertEquals(1L, snapshot.failedTasks());
        assertEquals(1L, snapshot.cancelledTasks());
        assertEquals(2L, snapshot.executedTasks());
        assertEquals(0L, snapshot.rejectedTasks());
        assertEquals(0L, snapshot.expiredTasks());
        assertEquals(0, snapshot.queuedTasks());
        assertEquals(Duration.ofNanos(2_000L), snapshot.averageQueueWait());
        assertEquals(Duration.ofNanos(3_000L), snapshot.longestQueueWait());
        assertEquals(Duration.ofNanos(6_000L), snapshot.averageExecutionTime());
        assertEquals(Duration.ofNanos(7_000L), snapshot.longestExecutionTime());
        assertEquals(1L, snapshot.cancelledByReason().get(ComputeCancelledException.Reason.CALLER));
        assertEquals(3L, snapshot.queueWaitStats().count());
        assertEquals(2L, snapshot.executionStats().count());

        final ConsumerMetrics consumer = snapshot.consumer("chunk").orElseThrow();
        assertEquals(3L, consumer.submittedTasks());
        assertEquals(1L, consumer.completedTasks());
        assertEquals(1L, consumer.failedTasks());
        assertEquals(1L, consumer.cancelledTasks());
        assertEquals(0, consumer.activeTasks());
        assertEquals(0, consumer.queuedTasks());
        assertEquals(4, consumer.concurrencyLimit());
        assertEquals(-1, consumer.maxQueuedTasks());
        assertTrue(consumer.enabled());
    }

    @Test
    void separatesExpiredAndEvictedCancellations() {
        final ComputeMetricsCollector collector = this.collector(true, false, new MetricsFixtures.FakeClock());
        collector.onConsumerRegistered(MetricsFixtures.consumer("path"), 2);
        collector.onSubmitted(MetricsFixtures.entry(1L, "path", ComputePriority.HIGH));
        collector.onSubmitted(MetricsFixtures.entry(2L, "path", ComputePriority.LOW));

        collector.onCancelled(MetricsFixtures.entry(1L, "path", ComputePriority.HIGH), ComputeCancelledException.Reason.QUEUE_EXPIRED);
        collector.onCancelled(MetricsFixtures.entry(2L, "path", ComputePriority.LOW), ComputeCancelledException.Reason.EVICTED);

        final SchedulerMetricsSnapshot snapshot = (SchedulerMetricsSnapshot) collector.schedulerMetrics();

        assertEquals(2L, snapshot.cancelledTasks());
        assertEquals(1L, snapshot.expiredTasks());
        assertEquals(1L, snapshot.evictedTasks());
        assertEquals(1L, snapshot.cancelledByReason().get(ComputeCancelledException.Reason.QUEUE_EXPIRED));
        assertEquals(1L, snapshot.cancelledByReason().get(ComputeCancelledException.Reason.EVICTED));
        assertEquals(0, snapshot.queuedTasks());
        assertEquals(0, snapshot.queuedByPriority().get(ComputePriority.HIGH));
    }

    @Test
    void breaksRejectionsDownByReasonAndConsumer() {
        final ComputeMetricsCollector collector = this.collector(true, false, new MetricsFixtures.FakeClock());
        collector.onConsumerRegistered(MetricsFixtures.consumer("ai"), 2);

        collector.onRejected("ai", ComputeRejectedException.Reason.QUEUE_FULL);
        collector.onRejected("ai", ComputeRejectedException.Reason.QUEUE_FULL);
        collector.onRejected("ai", ComputeRejectedException.Reason.CONSUMER_QUEUE_FULL);
        collector.onRejected("ghost", ComputeRejectedException.Reason.CONSUMER_UNKNOWN);

        final SchedulerMetricsSnapshot snapshot = (SchedulerMetricsSnapshot) collector.schedulerMetrics();

        assertEquals(4L, snapshot.rejectedTasks());
        assertEquals(2L, snapshot.rejectedByReason().get(ComputeRejectedException.Reason.QUEUE_FULL));
        assertEquals(1L, snapshot.rejectedByReason().get(ComputeRejectedException.Reason.CONSUMER_QUEUE_FULL));
        assertEquals(1L, snapshot.rejectedByReason().get(ComputeRejectedException.Reason.CONSUMER_UNKNOWN));
        assertEquals(3L, snapshot.consumer("ai").orElseThrow().rejectedTasks());
        assertTrue(snapshot.consumer("ghost").isEmpty());
        assertEquals(0L, snapshot.submittedTasks());
    }

    @Test
    void keepsPerConsumerCountersIsolated() {
        final ComputeMetricsCollector collector = this.collector(true, false, new MetricsFixtures.FakeClock());
        collector.onConsumerRegistered(MetricsFixtures.consumer("chunk"), 4);
        collector.onConsumerRegistered(MetricsFixtures.consumer("path"), 4);

        collector.onSubmitted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL));
        collector.onStarted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 10L);
        collector.onCompleted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 20L);
        collector.onSubmitted(MetricsFixtures.entry(2L, "path", ComputePriority.HIGH));
        collector.onStarted(MetricsFixtures.entry(2L, "path", ComputePriority.HIGH), 30L);

        final ConsumerMetrics chunk = collector.consumerMetrics("chunk");
        final ConsumerMetrics path = collector.consumerMetrics("path");

        assertNotNull(chunk);
        assertNotNull(path);
        assertEquals(1L, chunk.submittedTasks());
        assertEquals(1L, chunk.completedTasks());
        assertEquals(0, chunk.activeTasks());
        assertEquals(1L, path.submittedTasks());
        assertEquals(0L, path.completedTasks());
        assertEquals(1, path.activeTasks());
        assertEquals(0, path.queuedTasks());
        assertEquals(2L, collector.schedulerMetrics().submittedTasks());
    }

    @Test
    void consumerLifecycleUpdatesEnablementAndKeepsLastSnapshot() {
        final ComputeMetricsCollector collector = this.collector(true, false, new MetricsFixtures.FakeClock());
        final ComputeConsumer enabled = MetricsFixtures.consumer("ai");
        collector.onConsumerRegistered(enabled, 3);
        assertTrue(collector.consumerMetrics("ai").enabled());

        collector.onConsumerPolicyUpdated(MetricsFixtures.disabledConsumer("ai"), 3);

        assertFalse(collector.consumerMetrics("ai").enabled());

        collector.onConsumerUnregistered("ai");

        final SchedulerMetrics afterRemoval = collector.schedulerMetrics();
        assertTrue(afterRemoval.consumer("ai").isPresent());
        assertFalse(afterRemoval.consumer("ai").orElseThrow().enabled());
        assertNull(collector.consumerMetrics("missing"));
    }

    @Test
    void snapshotsAreImmutableAndDoNotFollowLaterEvents() {
        final ComputeMetricsCollector collector = this.collector(true, false, new MetricsFixtures.FakeClock());
        collector.onConsumerRegistered(MetricsFixtures.consumer("chunk"), 1);
        collector.onSubmitted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL));
        collector.onStarted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 5L);

        final SchedulerMetricsSnapshot snapshot = (SchedulerMetricsSnapshot) collector.schedulerMetrics();
        collector.onCompleted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 5L);

        assertEquals(1L, snapshot.submittedTasks());
        assertEquals(0L, snapshot.completedTasks());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.consumers().clear());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.queuedByPriority().clear());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.rejectedByReason().clear());
    }

    @Test
    void disabledCollectorStaysEmptyAndAcceptsHooks() {
        final ComputeMetricsCollector collector = this.collector(false, false, new MetricsFixtures.FakeClock());

        collector.onConsumerRegistered(MetricsFixtures.consumer("chunk"), 1);
        collector.onSubmitted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL));
        collector.onStarted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 1L);
        collector.onCompleted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 1L);
        collector.onRejected("chunk", ComputeRejectedException.Reason.QUEUE_FULL);
        collector.sampleNow();

        final SchedulerMetrics snapshot = collector.schedulerMetrics();

        assertFalse(collector.enabled());
        assertFalse(snapshot.enabled());
        assertEquals(0L, snapshot.submittedTasks());
        assertEquals(0L, snapshot.completedTasks());
        assertEquals(0L, snapshot.rejectedTasks());
        assertTrue(snapshot.consumers().isEmpty());
        assertTrue(collector.consumerMetrics().isEmpty());
        assertTrue(Double.isNaN(snapshot.cpuUtilization()));
    }

    @Test
    void queueAndWorkerGaugesFollowSamples() {
        final ComputeMetricsCollector collector = this.collector(true, false, new MetricsFixtures.FakeClock());

        collector.onQueueState(7, 100);
        collector.onWorkerState(4, 3, 1, 2);

        SchedulerMetrics snapshot = collector.schedulerMetrics();
        assertEquals(7, snapshot.queuedTasks());
        assertEquals(100, snapshot.queueCapacity());
        assertEquals(4, snapshot.totalWorkers());
        assertEquals(3, snapshot.activeWorkers());
        assertEquals(1, snapshot.idleWorkers());
        assertEquals(4, snapshot.peakWorkers());
        assertEquals(2, snapshot.concurrencyLimit());

        collector.onWorkerState(2, 1, 1, 1);
        snapshot = collector.schedulerMetrics();
        assertEquals(4, snapshot.peakWorkers());
        assertEquals(2, snapshot.totalWorkers());

        collector.reset();
        snapshot = collector.schedulerMetrics();
        assertEquals(0, snapshot.queuedTasks());
        assertEquals(0, snapshot.totalWorkers());
        assertEquals(0, snapshot.peakWorkers());
    }

    @Test
    void throughputIsSampledFromCompletionCounters() {
        final MetricsFixtures.FakeClock clock = new MetricsFixtures.FakeClock();
        final ComputeMetricsCollector collector = this.collector(true, false, clock);
        collector.onConsumerRegistered(MetricsFixtures.consumer("chunk"), 2);

        long taskId = 1L;
        for (int i = 0; i < 100; i++) {
            collector.onSubmitted(MetricsFixtures.entry(taskId, "chunk", ComputePriority.NORMAL));
            collector.onStarted(MetricsFixtures.entry(taskId, "chunk", ComputePriority.NORMAL), 1L);
            collector.onCompleted(MetricsFixtures.entry(taskId, "chunk", ComputePriority.NORMAL), 1L);
            taskId++;
        }
        collector.sampleNow();

        clock.advance(Duration.ofSeconds(1));
        for (int i = 0; i < 100; i++) {
            collector.onSubmitted(MetricsFixtures.entry(taskId, "chunk", ComputePriority.NORMAL));
            collector.onStarted(MetricsFixtures.entry(taskId, "chunk", ComputePriority.NORMAL), 1L);
            collector.onCompleted(MetricsFixtures.entry(taskId, "chunk", ComputePriority.NORMAL), 1L);
            taskId++;
        }
        collector.sampleNow();

        final SchedulerMetrics snapshot = collector.schedulerMetrics();
        assertEquals(100.0d, snapshot.throughputPerSecond(), 1.0e-6d);
        assertEquals(100.0d, collector.consumerMetrics("chunk").throughputPerSecond(), 1.0e-6d);

        clock.advance(Duration.ofSeconds(1));
        collector.sampleNow();

        assertEquals(0.0d, collector.schedulerMetrics().throughputPerSecond(), 1.0e-6d);
    }

    @Test
    void cpuUtilizationIsNaNWhenDisabledAndPlausibleWhenEnabled() {
        final ComputeMetricsCollector disabled = this.collector(true, false, new MetricsFixtures.FakeClock());
        assertTrue(Double.isNaN(disabled.schedulerMetrics().cpuUtilization()));

        final ComputeMetricsCollector enabled = this.newCollector(
            MetricsFixtures.settings(true, false, Duration.ofSeconds(10), true, Duration.ofHours(1)),
            new MetricsFixtures.FakeClock()
        );
        enabled.sampleNow();
        final double load = enabled.schedulerMetrics().cpuUtilization();
        assertTrue(Double.isNaN(load) || (load >= 0.0d && load <= 1.0d));
    }

    @Test
    @Timeout(30)
    void concurrentHooksKeepEveryCounterExact() throws InterruptedException {
        final ComputeMetricsCollector collector = this.collector(true, false, new MetricsFixtures.FakeClock());
        collector.onConsumerRegistered(MetricsFixtures.consumer("a"), 8);
        collector.onConsumerRegistered(MetricsFixtures.consumer("b"), 8);
        final int threads = 8;
        final int perThread = 500;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            final int worker = i;
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    final String consumerId = worker % 2 == 0 ? "a" : "b";
                    for (int iteration = 0; iteration < perThread; iteration++) {
                        final long taskId = (long) worker * perThread + iteration;
                        collector.onSubmitted(MetricsFixtures.entry(taskId, consumerId, ComputePriority.NORMAL));
                        collector.onStarted(MetricsFixtures.entry(taskId, consumerId, ComputePriority.NORMAL), 100L);
                        collector.onCompleted(MetricsFixtures.entry(taskId, consumerId, ComputePriority.NORMAL), 200L);
                    }
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "metrics-test-" + i);
            thread.start();
        }
        start.countDown();
        assertTrue(done.await(20, TimeUnit.SECONDS));

        final SchedulerMetrics snapshot = collector.schedulerMetrics();
        final long expected = (long) threads * perThread;
        assertEquals(expected, snapshot.submittedTasks());
        assertEquals(expected, snapshot.completedTasks());
        assertEquals(0L, snapshot.failedTasks());
        assertEquals(0L, snapshot.queuedTasks());
        assertEquals(expected, snapshot.consumer("a").orElseThrow().completedTasks() + snapshot.consumer("b").orElseThrow().completedTasks());
        assertEquals(0, snapshot.consumer("a").orElseThrow().activeTasks());
        assertEquals(0, snapshot.consumer("b").orElseThrow().activeTasks());
    }

    @Test
    void resetIsRefusedWhileATaskIsQueuedOrRunning() {
        final ComputeMetricsCollector collector = this.collector(true, false, new MetricsFixtures.FakeClock());
        collector.onConsumerRegistered(MetricsFixtures.consumer("chunk"), 4);
        collector.onSubmitted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL));
        collector.onStarted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 5L);

        assertThrows(IllegalStateException.class, collector::reset, "a live task must not be wiped by reset");
        assertEquals(1, collector.consumerMetrics("chunk").activeTasks());

        collector.onCompleted(MetricsFixtures.entry(1L, "chunk", ComputePriority.NORMAL), 5L);
        collector.reset();
        assertEquals(0L, collector.schedulerMetrics().submittedTasks());
        assertTrue(collector.consumerMetrics().isEmpty());
    }

    @Test
    void withdrawnSubmissionLeavesNoTrace() {
        final ComputeMetricsCollector collector = this.collector(true, false, new MetricsFixtures.FakeClock());
        collector.onConsumerRegistered(MetricsFixtures.consumer("chunk"), 4);
        final var entry = MetricsFixtures.entry(1L, "chunk", ComputePriority.HIGH);
        collector.onSubmitted(entry);
        collector.onSubmissionWithdrawn(entry);

        final SchedulerMetrics snapshot = collector.schedulerMetrics();
        assertEquals(0L, snapshot.submittedTasks());
        assertEquals(0, ((SchedulerMetricsSnapshot) snapshot).queuedByPriority().get(ComputePriority.HIGH));
        assertEquals(0, snapshot.consumer("chunk").orElseThrow().queuedTasks());
        assertEquals(0L, snapshot.consumer("chunk").orElseThrow().submittedTasks());
    }

    private ComputeMetricsCollector collector(final boolean metrics, final boolean jfr, final MetricsFixtures.FakeClock clock) {
        return this.newCollector(
            MetricsFixtures.settings(metrics, jfr, Duration.ofSeconds(1), false, Duration.ofHours(1)),
            clock
        );
    }

    private ComputeMetricsCollector newCollector(final ComputeSettings settings, final MetricsFixtures.FakeClock clock) {
        final ComputeMetricsCollector collector = new ComputeMetricsCollector(settings, clock, new FlightJfrRecorder());
        this.collectors.add(collector);
        return collector;
    }
}
