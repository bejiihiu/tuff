package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task failures must reach the caller and must never stop a worker.
 */
@Timeout(60)
class EngineFailureTest {

    @Test
    void runtimeExceptionFailsTheFutureAndTheWorkerSurvives() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final IllegalStateException boom = new IllegalStateException("boom");
            final CompletableFuture<String> failed = compute.submit(() -> {
                throw boom;
            });
            final CompletionException wrapped = assertThrows(CompletionException.class, failed::join);
            assertEquals(boom, wrapped.getCause());

            final CompletableFuture<String> next = compute.submit(() -> "still alive");
            assertEquals("still alive", next.get(10L, TimeUnit.SECONDS));
            assertEquals(1L, compute.metrics().failedTasks());
            assertEquals(1L, compute.metrics().completedTasks());
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void errorFailsTheFutureAndTheWorkerSurvives() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<String> failed = compute.submit(() -> {
                throw new AssertionError("task level error");
            });
            final CompletionException wrapped = assertThrows(CompletionException.class, failed::join);
            assertInstanceOf(AssertionError.class, wrapped.getCause());
            assertEquals("ok", compute.submit(() -> "ok").get(10L, TimeUnit.SECONDS));
            assertEquals(1L, compute.metrics().failedTasks());
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void nullResultIsTreatedAsATaskBug() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<Object> failed = compute.submit(() -> null);
            final CompletionException wrapped = assertThrows(CompletionException.class, failed::join);
            assertInstanceOf(NullPointerException.class, wrapped.getCause());
            assertEquals(1L, compute.metrics().failedTasks());
            assertEquals("fine", compute.submit(() -> "fine").get(10L, TimeUnit.SECONDS));
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void checkedExceptionIsDeliveredUnchanged() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(
                ComputeRequest.of(() -> {
                    throw new IOException("io");
                })
            );
            final CompletionException wrapped = assertThrows(CompletionException.class, handle.future()::join);
            assertInstanceOf(IOException.class, wrapped.getCause());
            assertEquals(ComputeTaskHandle.State.FAILED, handle.state());
            assertInstanceOf(IOException.class, handle.failure());
            assertTrue(handle.isDone());
        } finally {
            compute.shutdown();
        }
    }
}
