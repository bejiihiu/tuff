package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The diagnostic task label: normalization, handle exposure, toString and independence from
 * scheduling inputs.
 */
@Timeout(60)
class ComputeRequestLabelTest {

    private static final ComputeTask<String> TASK = () -> "value";

    @Test
    void legacyConstructorProducesNoLabel() {
        final ComputeRequest<String> request = new ComputeRequest<>(
            "consumer",
            ComputeTaskType.GENERAL,
            ComputePriority.NORMAL,
            TASK
        );
        assertEquals("", request.label());
        assertTrue(request.label().isEmpty());
    }

    @Test
    void labelIsTrimmedAndTruncatedAndBlankBecomesEmpty() {
        assertEquals("render map 12", ComputeRequest.normalizeLabel("  render map 12  "));
        assertEquals("", ComputeRequest.normalizeLabel("   "));
        assertEquals("", ComputeRequest.normalizeLabel(null));
        final String longLabel = "x".repeat(200);
        final String normalized = ComputeRequest.normalizeLabel(longLabel);
        assertEquals(ComputeRequest.MAX_LABEL_LENGTH, normalized.length());
        assertTrue(longLabel.startsWith(normalized));
    }

    @Test
    void factoriesCarryTheLabel() {
        assertEquals("warm cache", ComputeRequest.of("warm cache", TASK).label());
        assertEquals("urgent", ComputeRequest.of(ComputePriority.HIGH, " urgent ", TASK).label());
        assertEquals("", ComputeRequest.of(ComputePriority.HIGH, "   ", TASK).label());
    }

    @Test
    void labelDoesNotChangeSchedulingFields() {
        final ComputeRequest<String> plain = new ComputeRequest<>("c", ComputeTaskType.GENERAL, ComputePriority.LOW, TASK);
        final ComputeRequest<String> labelled =
            new ComputeRequest<>("c", ComputeTaskType.GENERAL, ComputePriority.LOW, TASK, "human name");
        assertEquals(plain.consumerId(), labelled.consumerId());
        assertEquals(plain.taskType(), labelled.taskType());
        assertEquals(plain.priority(), labelled.priority());
        assertSame(plain.task(), labelled.task());
    }

    @Test
    void handleExposesTheLabelAndToStringRendersIt() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).virtualLane(true, true, 2, Duration.ofMillis(50L), 64).build(),
            new EngineTestSupport.FakeClock()
        );
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(new ComputeRequest<>(
                "render",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    started.countDown();
                    release.await();
                    return "frame";
                },
                "render map \"12\""
            ));
            assertEquals("render map \"12\"", handle.label().orElseThrow());
            assertTrue(started.await(10L, TimeUnit.SECONDS));

            final String running = handle.toString();
            assertTrue(running.contains("label=\"render map \\\"12\\\"\""), running);
            assertTrue(running.contains("state=RUNNING"), running);
            assertTrue(running.contains("lane=virtual"), running);
            assertFalse(running.contains("exec="), running);
            assertTrue(running.startsWith("tuff-task#" + handle.taskId() + " ["), running);

            release.countDown();
            assertEquals("frame", handle.future().get(10L, TimeUnit.SECONDS));
            final String completed = handle.toString();
            assertTrue(completed.contains("state=COMPLETED"), completed);
            assertTrue(completed.contains("wait="), completed);
            assertTrue(completed.contains("exec="), completed);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void queuedHandleToStringHasNoExecutionTime() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).virtualLane(true, true, 1, Duration.ofMillis(50L), 64).build(),
            new EngineTestSupport.FakeClock()
        );
        final CountDownLatch first = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                first.countDown();
                release.await();
                return "blocker";
            });
            assertTrue(first.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> queued = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> "queued",
                "waiting work"
            ));
            final String text = queued.toString();
            assertTrue(text.contains("state=QUEUED"), text);
            assertFalse(text.contains("exec="), text);
            assertFalse(text.contains("lane="), "a queued task has no lane yet: " + text);
            assertTrue(text.contains("label=\"waiting work\""), text);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void emptyLabelIsNotRendered() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(new ComputeRequest<>(
                "plain",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> "value",
                "   "
            ));
            assertTrue(handle.label().isEmpty());
            final CompletableFuture<?> future = handle.future();
            future.get(10L, TimeUnit.SECONDS);
            assertFalse(handle.toString().contains("label="), handle.toString());
        } finally {
            compute.shutdown();
        }
    }
}
