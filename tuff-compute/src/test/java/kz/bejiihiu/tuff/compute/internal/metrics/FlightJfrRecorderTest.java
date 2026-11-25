package kz.bejiihiu.tuff.compute.internal.metrics;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.internal.metrics.event.AgingActivatedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.QueueDepthSampleEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskCancelledEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskCompletedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskFailedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskQueueExpiredEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskRejectedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskStartedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskSubmittedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.WorkerPoolResizedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.WorkerPoolStateEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.ComputeMetricsCollector;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.FlightJfrRecorder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlightJfrRecorderTest {

    @TempDir
    Path tempDir;

    @Test
    void recordsEveryEventWithItsFields() throws IOException {
        try (Recording recording = new Recording()) {
            recording.enable(TaskSubmittedEvent.class);
            recording.enable(TaskStartedEvent.class);
            recording.enable(TaskCompletedEvent.class);
            recording.enable(TaskFailedEvent.class);
            recording.enable(TaskCancelledEvent.class);
            recording.enable(TaskQueueExpiredEvent.class);
            recording.enable(TaskRejectedEvent.class);
            recording.enable(QueueDepthSampleEvent.class);
            recording.enable(WorkerPoolStateEvent.class);
            recording.enable(WorkerPoolResizedEvent.class);
            recording.enable(AgingActivatedEvent.class);
            recording.start();

            final FlightJfrRecorder recorder = new FlightJfrRecorder();
            assertTrue(recorder.enabled());

            recorder.taskQueued(42L, "chunk", ComputeTaskType.CHUNK_GENERATION, ComputePriority.HIGH, 7);
            recorder.taskStarted(42L, "chunk", ComputeTaskType.CHUNK_GENERATION, ComputePriority.HIGH, 1_500L, 3);
            recorder.taskFinished(42L, "chunk", ComputeTaskType.CHUNK_GENERATION, ComputePriority.HIGH, 2_500L, null);
            recorder.taskFinished(
                43L,
                "chunk",
                ComputeTaskType.CHUNK_GENERATION,
                ComputePriority.NORMAL,
                3_500L,
                new IllegalStateException("boom")
            );
            recorder.taskCancelled(44L, "path", ComputeCancelledException.Reason.CALLER);
            recorder.taskCancelled(45L, "path", ComputeCancelledException.Reason.QUEUE_EXPIRED);
            recorder.taskRejected("ai", ComputeRejectedException.Reason.QUEUE_FULL);
            recorder.queueState(12, 100);
            recorder.workerState(4, 3, 1, 4);
            recorder.workerState(6, 5, 1, 6);
            recorder.agingActivated("path", ComputePriority.LOW, ComputePriority.NORMAL, 900L);

            recording.stop();

            final List<RecordedEvent> events = this.readEvents(recording, "all-events.jfr");
            final RecordedEvent submitted = single(events, "tuff.compute.TaskSubmitted");
            assertEquals(42L, submitted.getLong("taskId"));
            assertEquals("chunk", submitted.getString("consumerId"));
            assertEquals("CHUNK_GENERATION", submitted.getString("taskType"));
            assertEquals("HIGH", submitted.getString("priority"));
            assertEquals(7, submitted.getInt("queueDepth"));

            final RecordedEvent started = single(events, "tuff.compute.TaskStarted");
            assertEquals(1_500L, started.getLong("queueWaitNanos"));
            assertEquals(3, started.getInt("activeTasks"));

            final RecordedEvent completed = single(events, "tuff.compute.TaskCompleted");
            assertEquals(2_500L, completed.getLong("executionNanos"));

            final RecordedEvent failed = single(events, "tuff.compute.TaskFailed");
            assertEquals("java.lang.IllegalStateException", failed.getString("failureType"));
            assertEquals("boom", failed.getString("failureMessage"));

            final RecordedEvent cancelled = single(events, "tuff.compute.TaskCancelled");
            assertEquals("CALLER", cancelled.getString("reason"));

            final RecordedEvent expired = single(events, "tuff.compute.TaskQueueExpired");
            assertEquals(45L, expired.getLong("taskId"));

            final RecordedEvent rejected = single(events, "tuff.compute.TaskRejected");
            assertEquals("ai", rejected.getString("consumerId"));
            assertEquals("QUEUE_FULL", rejected.getString("reason"));

            final RecordedEvent queueSample = single(events, "tuff.compute.QueueDepthSample");
            assertEquals(12, queueSample.getInt("queuedTasks"));
            assertEquals(100, queueSample.getInt("capacity"));
            assertEquals(0.12d, queueSample.getDouble("fillRatio"), 1.0e-9d);

            final RecordedEvent workerSample = first(events, "tuff.compute.WorkerPoolState");
            assertEquals(4, workerSample.getInt("totalWorkers"));
            assertEquals(3, workerSample.getInt("activeWorkers"));
            assertEquals(1, workerSample.getInt("idleWorkers"));
            assertEquals(4, workerSample.getInt("concurrencyLimit"));

            final RecordedEvent resized = single(events, "tuff.compute.WorkerPoolResized");
            assertEquals(6, resized.getInt("totalWorkers"));
            assertEquals(6, resized.getInt("concurrencyLimit"));

            final RecordedEvent aging = single(events, "tuff.compute.AgingActivated");
            assertEquals("path", aging.getString("consumerId"));
            assertEquals("LOW", aging.getString("fromPriority"));
            assertEquals("NORMAL", aging.getString("toPriority"));
            assertEquals(900L, aging.getLong("waitedNanos"));
        }
    }

    @Test
    void collectorEmitsRejectionEventsThroughTheRecorder() throws IOException {
        try (Recording recording = new Recording()) {
            recording.enable(TaskRejectedEvent.class);
            recording.start();
            final ComputeMetricsCollector collector = new ComputeMetricsCollector(
                MetricsFixtures.settings(true, true, Duration.ofSeconds(10), false, Duration.ofHours(1)),
                new MetricsFixtures.FakeClock(),
                new FlightJfrRecorder()
            );
            try {
                collector.onRejected("chunk", ComputeRejectedException.Reason.CONSUMER_DISABLED);
            } finally {
                collector.close();
            }
            recording.stop();

            final RecordedEvent rejected = single(
                this.readEvents(recording, "rejection.jfr"),
                "tuff.compute.TaskRejected"
            );
            assertEquals("chunk", rejected.getString("consumerId"));
            assertEquals("CONSUMER_DISABLED", rejected.getString("reason"));
        }
    }

    @Test
    void disabledRecorderIsSilentAndNeverThrows() {
        final FlightJfrRecorder recorder = new FlightJfrRecorder();
        assertFalse(recorder.enabled());
        assertDoesNotThrow(() -> {
            recorder.taskQueued(1L, "chunk", ComputeTaskType.GENERAL, ComputePriority.NORMAL, 0);
            recorder.taskStarted(1L, "chunk", ComputeTaskType.GENERAL, ComputePriority.NORMAL, 0L, 0);
            recorder.taskFinished(1L, "chunk", ComputeTaskType.GENERAL, ComputePriority.NORMAL, 0L, null);
            recorder.taskFinished(1L, "chunk", ComputeTaskType.GENERAL, ComputePriority.NORMAL, 0L, new RuntimeException());
            recorder.taskCancelled(1L, "chunk", ComputeCancelledException.Reason.CALLER);
            recorder.taskRejected("chunk", ComputeRejectedException.Reason.QUEUE_FULL);
            recorder.queueState(0, 0);
            recorder.workerState(0, 0, 0, 0);
            recorder.agingActivated("chunk", ComputePriority.LOW, ComputePriority.NORMAL, 0L);
        });
    }

    private List<RecordedEvent> readEvents(final Recording recording, final String fileName) throws IOException {
        final Path destination = this.tempDir.resolve(fileName);
        recording.dump(destination);
        final List<RecordedEvent> events = new ArrayList<>();
        try (RecordingFile file = new RecordingFile(destination)) {
            while (file.hasMoreEvents()) {
                events.add(file.readEvent());
            }
        }
        return events;
    }

    private static RecordedEvent single(final List<RecordedEvent> events, final String name) {
        final RecordedEvent event = first(events, name);
        final long count = events.stream().filter(candidate -> name.equals(candidate.getEventType().getName())).count();
        assertEquals(1L, count, name);
        return event;
    }

    private static RecordedEvent first(final List<RecordedEvent> events, final String name) {
        return events.stream().filter(candidate -> name.equals(candidate.getEventType().getName())).findFirst().orElseThrow();
    }
}
