package kz.bejiihiu.tuff.compute.internal.lanes;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskSubmittedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.VirtualTaskEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.SettingsBuilder;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.absolute;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The label is diagnostic only: it must reach the handle and the flight recorder, be truncated, and
 * never influence ordering or quotas.
 */
@Timeout(90)
class LaneLabelTest {

    @TempDir
    Path tempDir;

    @Test
    void labelsAreExposedAndDoNotChangeOrderingOrQuotas() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("label-order").fixedWorkers(1).virtualLane(true, true, 1, millis(50L), 64));
        compute.registerConsumer(ComputeConsumer.of("labelled", absolute(1)));
        final String[] labels = {"zzz-first", "aaa-second", ""};
        final CountDownLatch[] releases = new CountDownLatch[labels.length];
        for (int i = 0; i < labels.length; i++) {
            releases[i] = new CountDownLatch(1);
        }
        final BlockingQueue<Integer> started = new LinkedBlockingQueue<>();
        final List<ComputeTaskHandle<String>> handles = new ArrayList<>();
        try {
            for (int i = 0; i < labels.length; i++) {
                final int index = i;
                handles.add(compute.enqueue(new ComputeRequest<>(
                    "labelled",
                    ComputeTaskType.GENERAL,
                    ComputePriority.LOW,
                    () -> {
                        started.add(index);
                        releases[index].await();
                        return "value-" + index;
                    },
                    labels[i]
                )));
            }
            assertEquals("zzz-first", handles.get(0).label().orElseThrow());
            assertEquals("aaa-second", handles.get(1).label().orElseThrow());
            assertTrue(handles.get(2).label().isEmpty());
            assertTrue(handles.get(0).toString().contains("label=\"zzz-first\""), handles.get(0).toString());
            assertFalse(handles.get(2).toString().contains("label="), handles.get(2).toString());

            // the queue must keep submission order even though the labels sort the other way
            for (int expected = 0; expected < labels.length; expected++) {
                final Integer index = started.poll(10L, TimeUnit.SECONDS);
                assertNotNull(index);
                assertEquals(expected, index.intValue(), "a label changed the dispatch order");
                releases[expected].countDown();
            }
            for (final ComputeTaskHandle<String> handle : handles) {
                assertNotNull(handle.future().get(10L, TimeUnit.SECONDS));
            }

            final String longLabel = "x".repeat(100);
            final ComputeTaskHandle<String> truncated = compute.enqueue(new ComputeRequest<>(
                "labelled",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> "long",
                longLabel
            ));
            assertEquals(ComputeRequest.MAX_LABEL_LENGTH, truncated.label().orElseThrow().length());
            assertEquals(longLabel.substring(0, ComputeRequest.MAX_LABEL_LENGTH), truncated.label().orElseThrow());
            assertEquals("long", truncated.future().get(10L, TimeUnit.SECONDS));
            assertTrue(truncated.toString().contains("label=\"" + "x".repeat(64) + "\""), truncated.toString());
        } finally {
            for (final CountDownLatch release : releases) {
                release.countDown();
            }
            compute.shutdown();
        }
    }

    @Test
    void distinctLabelsDoNotBuyExtraQuota() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("label-quota").fixedWorkers(2).virtualLane(true, true, 2, millis(50L), 64));
        compute.registerConsumer(ComputeConsumer.of("labelled", absolute(1)));
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch secondStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        try {
            final List<CompletableFuture<String>> futures = new ArrayList<>();
            futures.add(compute.submit("labelled", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                firstStarted.countDown();
                release.await();
                active.decrementAndGet();
                return "first";
            }));
            assertTrue(firstStarted.await(10L, TimeUnit.SECONDS));
            futures.add(compute.submit("labelled", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                secondStarted.countDown();
                release.await();
                active.decrementAndGet();
                return "second";
            }));
            // distinct labels must not be read as distinct consumers: the quota is one
            assertFalse(secondStarted.await(300L, TimeUnit.MILLISECONDS));
            assertEquals(1, compute.metrics().lanes().virtual().running());

            release.countDown();
            for (final CompletableFuture<String> future : futures) {
                assertNotNull(future.get(10L, TimeUnit.SECONDS));
            }
            assertEquals(1, peak.get(), "labels bypassed the consumer quota");
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void blankAndLongLabelsDoNotBreakJfrEvents() throws Exception {
        final SettingsBuilder settings = SettingsBuilder.of("label-jfr")
            .fixedWorkers(1)
            .virtualLane(true, true, 2, millis(50L), 64)
            .metrics(new ComputeSettings.Metrics(true, true, Duration.ofSeconds(10L), false, millis(10L)));
        try (Recording recording = new Recording()) {
            recording.enable(TaskSubmittedEvent.class);
            recording.enable(VirtualTaskEvent.class);
            recording.start();

            final AsyncComputeScheduler compute = start(settings);
            final ComputeTaskHandle<String> blank;
            final ComputeTaskHandle<String> longLabel;
            try {
                blank = compute.enqueue(new ComputeRequest<>(
                    "low",
                    ComputeTaskType.GENERAL,
                    ComputePriority.LOW,
                    () -> "blank",
                    "   "
                ));
                longLabel = compute.enqueue(new ComputeRequest<>(
                    "low",
                    ComputeTaskType.GENERAL,
                    ComputePriority.LOW,
                    () -> "long",
                    "L".repeat(100)
                ));
                assertEquals("blank", blank.future().get(10L, TimeUnit.SECONDS));
                assertEquals("long", longLabel.future().get(10L, TimeUnit.SECONDS));
            } finally {
                compute.shutdown();
            }
            recording.stop();

            final List<RecordedEvent> events = this.readEvents(recording, "lanes-labels.jfr");
            final List<RecordedEvent> submitted = events.stream()
                .filter(event -> "tuff.compute.TaskSubmitted".equals(event.getEventType().getName()))
                .toList();
            final List<RecordedEvent> virtual = events.stream()
                .filter(event -> "tuff.compute.VirtualTask".equals(event.getEventType().getName()))
                .toList();
            assertEquals(2, submitted.size());
            assertEquals(2, virtual.size());

            assertEquals("", byTaskId(submitted, blank.taskId()).getString("taskLabel"));
            assertEquals("L".repeat(64), byTaskId(submitted, longLabel.taskId()).getString("taskLabel"));
            assertEquals("", byTaskId(virtual, blank.taskId()).getString("taskLabel"));
            assertEquals("L".repeat(64), byTaskId(virtual, longLabel.taskId()).getString("taskLabel"));
        }
    }

    private static RecordedEvent byTaskId(final List<RecordedEvent> events, final long taskId) {
        final RecordedEvent event = events.stream()
            .filter(candidate -> candidate.getLong("taskId") == taskId)
            .findFirst()
            .orElseThrow(() -> new AssertionError("no JFR event for task " + taskId));
        assertNotNull(event.getString("taskLabel"));
        return event;
    }

    private List<RecordedEvent> readEvents(final Recording recording, final String name) throws IOException {
        final Path destination = this.tempDir.resolve(name);
        recording.dump(destination);
        final List<RecordedEvent> events = new ArrayList<>();
        try (RecordingFile file = new RecordingFile(destination)) {
            while (file.hasMoreEvents()) {
                events.add(file.readEvent());
            }
        }
        return events;
    }
}
