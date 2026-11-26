package kz.bejiihiu.tuff.api.compute;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Entity;
import org.bukkit.plugin.IllegalPluginAccessException;
import org.bukkit.plugin.Plugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ComputeHandoffTest {

    private final FakeHandoffBackend backend = new FakeHandoffBackend();
    private final Plugin plugin = HandoffSubjects.plugin();
    private final World world = HandoffSubjects.world();
    private final Entity entity = HandoffSubjects.entity();

    @BeforeEach
    void installBackend() {
        ComputeHandoff.useBackend(this.backend);
    }

    @AfterEach
    void restoreBackend() {
        ComputeHandoff.resetBackend();
    }

    @Test
    void regionDeliversTheResultExactlyOnce() {
        final AtomicReference<String> applied = new AtomicReference<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toRegion(
            this.plugin,
            this.world,
            7,
            -2,
            CompletableFuture.completedFuture("route"),
            applied::set,
            failures::add
        );

        assertEquals(1, this.backend.attempts(), "the delivery must be scheduled exactly once");
        assertNull(applied.get(), "the completing thread must not apply the result");
        assertTrue(failures.isEmpty());

        this.backend.runActions();

        assertEquals("route", applied.get());
        assertEquals(1, this.backend.attempts());
        assertTrue(failures.isEmpty());
    }

    @Test
    void globalDeliversTheResultExactlyOnce() {
        final AtomicReference<String> applied = new AtomicReference<>();

        ComputeHandoff.toGlobal(this.plugin, CompletableFuture.completedFuture("weather"), applied::set);

        assertEquals(1, this.backend.attempts());
        assertNull(applied.get());
        assertEquals(FakeHandoffBackend.Target.GLOBAL, this.backend.onlyCall().target());

        this.backend.runActions();

        assertEquals("weather", applied.get());
        assertEquals(1, this.backend.attempts());
    }

    @Test
    void entityDeliversTheResultExactlyOnce() {
        final AtomicReference<String> applied = new AtomicReference<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toEntity(
            this.plugin,
            this.entity,
            CompletableFuture.completedFuture("path"),
            applied::set,
            failures::add
        );

        assertEquals(1, this.backend.attempts());
        assertNull(applied.get());
        assertTrue(failures.isEmpty());
        final FakeHandoffBackend.Call call = this.backend.onlyCall();
        assertNotNull(call);
        assertEquals(FakeHandoffBackend.Target.ENTITY, call.target());
        assertSame(this.entity, call.entity());
        assertNotNull(call.retired());

        this.backend.runActions();

        assertEquals("path", applied.get());
        assertEquals(1, this.backend.attempts());
    }

    @Test
    void locationOverloadCapturesTheTargetChunkBeforeTheFutureCompletes() {
        final CompletableFuture<String> pending = new CompletableFuture<>();
        final Location location = new Location(this.world, 130, 64, -17);

        ComputeHandoff.toRegion(this.plugin, location, pending, ignored -> {
        });

        assertTrue(this.backend.calls().isEmpty(), "nothing may be scheduled before the future completes");
        location.setX(4096);
        location.setZ(4096);
        pending.complete("value");

        final FakeHandoffBackend.Call call = this.backend.onlyCall();
        assertNotNull(call);
        assertSame(this.world, call.world());
        assertEquals(8, call.chunkX(), "chunk x must come from the location at call time");
        assertEquals(-2, call.chunkZ(), "chunk z must come from the location at call time");
    }

    @Test
    void completionThreadOnlySchedulesAndNeverApplies() throws InterruptedException {
        final CompletableFuture<String> future = new CompletableFuture<>();
        final AtomicReference<String> applied = new AtomicReference<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        ComputeHandoff.toRegion(this.plugin, this.world, 0, 0, future, applied::set, failures::add);

        final Thread worker = new Thread(() -> future.complete("value"), "handoff-compute-worker");
        worker.start();
        worker.join(5_000);

        assertFalse(worker.isAlive(), "the fake worker must finish");
        assertNull(applied.get(), "the worker must not touch the world");
        assertTrue(failures.isEmpty());
        final FakeHandoffBackend.Call call = this.backend.onlyCall();
        assertNotNull(call);
        assertSame(worker, call.schedulingThread(), "the worker may only ask the backend to schedule");

        this.backend.runActions();

        assertEquals("value", applied.get());
    }

    @Test
    void retiredEntityIsReportedAndNothingIsApplied() {
        this.backend.retireEntities();
        final AtomicReference<String> applied = new AtomicReference<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toEntity(
            this.plugin,
            this.entity,
            CompletableFuture.completedFuture("path"),
            applied::set,
            failures::add
        );

        assertNull(applied.get());
        assertEquals(1, failures.size(), "the failure handler must run instead of waiting forever");
        final HandoffException failure = assertInstanceOf(HandoffException.class, failures.getFirst());
        assertEquals(HandoffException.Reason.ENTITY_RETIRED, failure.reason());
        assertEquals(1, this.backend.attempts());
    }

    @Test
    void retiredEntityStillReportsTheComputeFailure() {
        this.backend.retireEntities();
        final IllegalStateException boom = new IllegalStateException("compute failed");
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toEntity(
            this.plugin,
            this.entity,
            CompletableFuture.failedFuture(boom),
            ignored -> fail("a failed compute must not be applied"),
            failures::add
        );

        assertEquals(1, failures.size());
        assertSame(boom, failures.getFirst());
    }

    @Test
    void disabledPluginIsReportedForEveryTarget() {
        final IllegalPluginAccessException disabled = new IllegalPluginAccessException("plugin disabled");
        this.backend.failWith(disabled);
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toRegion(
            this.plugin, this.world, 0, 0, CompletableFuture.completedFuture("r"), ignored -> fail("must not apply"), failures::add
        );
        ComputeHandoff.toEntity(
            this.plugin, this.entity, CompletableFuture.completedFuture("e"), ignored -> fail("must not apply"), failures::add
        );
        ComputeHandoff.toGlobal(
            this.plugin, CompletableFuture.completedFuture("g"), ignored -> fail("must not apply"), failures::add
        );

        assertEquals(3, this.backend.attempts());
        assertTrue(this.backend.calls().isEmpty());
        assertEquals(3, failures.size());
        for (final Throwable reported : failures) {
            final HandoffException failure = assertInstanceOf(HandoffException.class, reported);
            assertEquals(HandoffException.Reason.PLUGIN_DISABLED, failure.reason());
            assertSame(disabled, failure.getCause());
        }
    }

    @Test
    void aFailedComputeDeliversTheOriginalThrowableOnTheTarget() {
        final IllegalStateException boom = new IllegalStateException("boom");
        final AtomicReference<String> applied = new AtomicReference<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toRegion(
            this.plugin,
            this.world,
            0,
            0,
            CompletableFuture.failedFuture(boom),
            applied::set,
            failures::add
        );

        assertEquals(1, this.backend.attempts());
        assertTrue(failures.isEmpty(), "the failure handler must run on the target, not on the worker");
        assertNull(applied.get());

        this.backend.runActions();

        assertEquals(1, failures.size());
        assertSame(boom, failures.getFirst());
        assertNull(applied.get());
    }

    @Test
    void wrappedFailuresAreUnwrappedBeforeDelivery() {
        final IllegalStateException wrappedInCompletion = new IllegalStateException("completion");
        final IllegalStateException wrappedInExecution = new IllegalStateException("execution");
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toGlobal(
            this.plugin, CompletableFuture.failedFuture(new CompletionException(wrappedInCompletion)), ignored -> {
            }, failures::add
        );
        ComputeHandoff.toEntity(
            this.plugin,
            this.entity,
            CompletableFuture.failedFuture(new ExecutionException(wrappedInExecution)),
            ignored -> {
            },
            failures::add
        );
        this.backend.runActions();

        assertEquals(2, failures.size());
        assertSame(wrappedInCompletion, failures.get(0));
        assertSame(wrappedInExecution, failures.get(1));
    }

    @Test
    void cancelledFuturesAreSkippedSilently() {
        final CompletableFuture<String> cancelled = new CompletableFuture<>();
        cancelled.cancel(true);
        final CompletableFuture<String> engineCancelled = new CompletableFuture<>();
        engineCancelled.completeExceptionally(
            new ComputeCancelledException(7L, ComputeCancelledException.Reason.CALLER, "cancelled")
        );
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toRegion(
            this.plugin, this.world, 0, 0, cancelled, ignored -> fail("no result expected"), failures::add
        );
        ComputeHandoff.toGlobal(this.plugin, engineCancelled, ignored -> fail("no result expected"), failures::add);

        assertTrue(failures.isEmpty(), "cancellation is a normal outcome, not a handoff failure");
        assertEquals(0, this.backend.attempts());
    }

    @Test
    void cancellationFromAnotherThreadSchedulesNothing() throws InterruptedException {
        final CompletableFuture<String> future = new CompletableFuture<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        ComputeHandoff.toGlobal(this.plugin, future, ignored -> fail("no result expected"), failures::add);

        final Thread worker = new Thread(() -> future.cancel(false), "handoff-canceller");
        worker.start();
        worker.join(5_000);

        assertFalse(worker.isAlive());
        assertTrue(failures.isEmpty());
        assertEquals(0, this.backend.attempts());
    }

    @Test
    void nullResultIsReportedAsAFailure() {
        final CompletableFuture<String> future = new CompletableFuture<>();
        future.complete(null);
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toGlobal(this.plugin, future, ignored -> fail("a null result must not be applied"), failures::add);
        this.backend.runActions();

        assertEquals(1, failures.size());
        final NullPointerException failure = assertInstanceOf(NullPointerException.class, failures.getFirst());
        assertEquals("Compute result must not be null", failure.getMessage());
    }

    @Test
    void scheduleFailureIsReportedAsScheduleFailed() {
        final IllegalStateException shutdown = new IllegalStateException("server is stopping");
        this.backend.failWith(shutdown);
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ComputeHandoff.toGlobal(this.plugin, CompletableFuture.completedFuture("g"), ignored -> {
        }, failures::add);

        assertEquals(1, failures.size());
        final HandoffException failure = assertInstanceOf(HandoffException.class, failures.getFirst());
        assertEquals(HandoffException.Reason.SCHEDULE_FAILED, failure.reason());
        assertSame(shutdown, failure.getCause());
    }

    @Test
    void handleOverloadsBehaveLikeTheFutureOverloads() {
        final TestScheduler scheduler = new TestScheduler();
        final IllegalStateException boom = new IllegalStateException("task failed");
        final List<String> applied = new CopyOnWriteArrayList<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        final Location location = new Location(this.world, 0, 64, 0);
        final ComputeTaskHandle<String> completedRegion = scheduler.enqueue(ComputeRequest.of("handoff", () -> "region"));
        final ComputeTaskHandle<String> completedEntity = scheduler.enqueue(ComputeRequest.of("handoff", () -> "entity"));
        final ComputeTaskHandle<String> completedGlobal = scheduler.enqueue(ComputeRequest.of("handoff", () -> "global"));
        final ComputeTaskHandle<String> failedRegion = scheduler.enqueue(ComputeRequest.of("handoff", () -> {
            throw boom;
        }));
        final ComputeTaskHandle<String> failedEntity = scheduler.enqueue(ComputeRequest.of("handoff", () -> {
            throw boom;
        }));
        final ComputeTaskHandle<String> failedGlobal = scheduler.enqueue(ComputeRequest.of("handoff", () -> {
            throw boom;
        }));

        ComputeHandoff.toRegion(this.plugin, location, completedRegion, applied::add);
        ComputeHandoff.toEntity(this.plugin, this.entity, completedEntity, applied::add);
        ComputeHandoff.toGlobal(this.plugin, completedGlobal, applied::add);
        ComputeHandoff.toRegion(this.plugin, location, failedRegion, ignored -> fail("must not apply"), failures::add);
        ComputeHandoff.toEntity(this.plugin, this.entity, failedEntity, ignored -> fail("must not apply"), failures::add);
        ComputeHandoff.toGlobal(this.plugin, failedGlobal, ignored -> fail("must not apply"), failures::add);

        assertEquals(6, this.backend.attempts());
        this.backend.runActions();

        assertEquals(List.of("region", "entity", "global"), List.copyOf(applied));
        assertEquals(3, failures.size());
        for (final Throwable failure : failures) {
            assertSame(boom, failure);
        }
    }

    @Test
    void nullArgumentsAreRejected() {
        final Location location = new Location(this.world, 0, 64, 0);
        final CompletionStage<String> result = CompletableFuture.completedFuture("value");
        final Consumer<String> apply = ignored -> {
        };

        assertThrows(NullPointerException.class, () -> ComputeHandoff.toRegion(null, location, result, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toRegion(this.plugin, (Location) null, result, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toRegion(this.plugin, null, 0, 0, result, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toRegion(this.plugin, this.world, 0, 0, (CompletionStage<String>) null, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toRegion(this.plugin, this.world, 0, 0, result, null));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toRegion(this.plugin, this.world, 0, 0, result, apply, null));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toEntity(null, this.entity, result, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toEntity(this.plugin, null, result, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toEntity(this.plugin, this.entity, result, null));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toEntity(this.plugin, this.entity, result, apply, null));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toGlobal(null, result, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toGlobal(this.plugin, (CompletionStage<String>) null, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toGlobal(this.plugin, result, null));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toGlobal(this.plugin, result, apply, null));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toRegion(this.plugin, location, (ComputeTaskHandle<String>) null, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toEntity(this.plugin, this.entity, (ComputeTaskHandle<String>) null, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.toGlobal(this.plugin, (ComputeTaskHandle<String>) null, apply));
        assertThrows(NullPointerException.class, () -> ComputeHandoff.useBackend(null));
    }

    @Test
    void locationWithoutWorldIsRejected() {
        final Location orphan = new Location(null, 0, 64, 0);
        final CompletionStage<String> result = CompletableFuture.completedFuture("value");

        final NullPointerException failure = assertThrows(
            NullPointerException.class,
            () -> ComputeHandoff.toRegion(this.plugin, orphan, result, ignored -> {
            })
        );
        assertEquals("location must have a world", failure.getMessage());
    }

    @Test
    void theDefaultFailureHandlerLogsThroughThePluginLogger() {
        final List<LogRecord> records = new CopyOnWriteArrayList<>();
        final Logger logger = HandoffSubjects.capturingLogger(records);
        final Plugin loggingPlugin = HandoffSubjects.plugin(logger);
        this.backend.failWith(new IllegalPluginAccessException("plugin disabled"));

        ComputeHandoff.toEntity(loggingPlugin, this.entity, CompletableFuture.completedFuture("e"), ignored -> {
        });

        assertEquals(1, records.size());
        final LogRecord record = records.getFirst();
        assertEquals(Level.SEVERE, record.getLevel());
        final HandoffException logged = assertInstanceOf(HandoffException.class, record.getThrown());
        assertEquals(HandoffException.Reason.PLUGIN_DISABLED, logged.reason());
    }
}
