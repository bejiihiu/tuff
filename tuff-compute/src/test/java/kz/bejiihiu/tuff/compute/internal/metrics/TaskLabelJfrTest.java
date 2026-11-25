package kz.bejiihiu.tuff.compute.internal.metrics;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskCancelledEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskCompletedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskFailedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskQueueExpiredEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskStartedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskSubmittedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.VirtualTaskEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.FlightJfrRecorder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The diagnostic label must survive into every per-task flight recorder event.
 */
class TaskLabelJfrTest {

    @TempDir
    Path tempDir;

    @Test
    void labelIsRecordedOnEveryTaskEventAndEmptyLabelsStayEmpty() throws IOException {
        try (Recording recording = new Recording()) {
            recording.enable(TaskSubmittedEvent.class);
            recording.enable(TaskStartedEvent.class);
            recording.enable(TaskCompletedEvent.class);
            recording.enable(TaskFailedEvent.class);
            recording.enable(TaskCancelledEvent.class);
            recording.enable(TaskQueueExpiredEvent.class);
            recording.enable(VirtualTaskEvent.class);
            recording.start();

            final FlightJfrRecorder recorder = new FlightJfrRecorder();
            recorder.taskQueued(1L, "render", ComputeTaskType.GENERAL, ComputePriority.LOW, 0, "render map #12");
            recorder.taskStarted(1L, "render", ComputeTaskType.GENERAL, ComputePriority.LOW, 10L, 1, "render map #12");
            recorder.taskFinished(1L, "render", ComputeTaskType.GENERAL, ComputePriority.LOW, 20L, null, "render map #12");
            recorder.taskFinished(
                2L,
                "render",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                30L,
                new IllegalStateException("boom"),
                "failing job"
            );
            recorder.taskCancelled(3L, "render", ComputeCancelledException.Reason.CALLER, "cancelled job");
            recorder.taskCancelled(4L, "render", ComputeCancelledException.Reason.QUEUE_EXPIRED, "expired job");
            recorder.taskQueued(5L, "render", ComputeTaskType.GENERAL, ComputePriority.NORMAL, 0, "");
            recorder.virtualTask(6L, "render", ComputePriority.LOW, 12L, false, 1, 8, "render map #12");

            recording.stop();

            final List<RecordedEvent> events = this.readEvents(recording, "labels.jfr");
            assertEquals("render map #12", single(events, "tuff.compute.TaskSubmitted").getString("taskLabel"));
            assertEquals("render map #12", single(events, "tuff.compute.TaskStarted").getString("taskLabel"));
            assertEquals("render map #12", single(events, "tuff.compute.TaskCompleted").getString("taskLabel"));
            assertEquals("failing job", single(events, "tuff.compute.TaskFailed").getString("taskLabel"));
            assertEquals("cancelled job", single(events, "tuff.compute.TaskCancelled").getString("taskLabel"));
            assertEquals("expired job", single(events, "tuff.compute.TaskQueueExpired").getString("taskLabel"));
            assertEquals("render map #12", single(events, "tuff.compute.VirtualTask").getString("taskLabel"));

            final List<RecordedEvent> submitted = events.stream()
                .filter(event -> "tuff.compute.TaskSubmitted".equals(event.getEventType().getName()))
                .toList();
            assertEquals(2, submitted.size());
            final RecordedEvent withoutLabel = submitted.stream()
                .filter(event -> event.getLong("taskId") == 5L)
                .findFirst()
                .orElseThrow();
            assertNotNull(withoutLabel.getString("taskLabel"));
            assertEquals("", withoutLabel.getString("taskLabel"));
        }
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
        return events.stream()
            .filter(event -> name.equals(event.getEventType().getName()))
            .findFirst()
            .orElseThrow();
    }
}
