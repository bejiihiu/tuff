package kz.bejiihiu.tuff.api.compute;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComputeExecutorTest {

    private static final Consumer<Throwable> NO_FAILURE_HANDLER = throwable -> {
        throw new AssertionError("unexpected failure", throwable);
    };

    @Test
    void defaultRejectionPolicyRunsOnTheCallingThread() {
        final TestScheduler scheduler = new TestScheduler();
        scheduler.failWith(TestScheduler.FailureMode.REJECT);
        final ComputeExecutor executor = new ComputeExecutor(scheduler, "unit-test");
        final Thread caller = Thread.currentThread();
        final AtomicReference<Thread> ranOn = new AtomicReference<>();

        executor.execute(() -> ranOn.set(Thread.currentThread()));

        assertSame(caller, ranOn.get());
        assertTrue(scheduler.submissions().isEmpty(), "a rejected submission must not be retried on the engine");
    }

    @Test
    void dropPolicyDropsTheCommand() {
        final TestScheduler scheduler = new TestScheduler();
        scheduler.failWith(TestScheduler.FailureMode.REJECT);
        final ComputeExecutor executor = new ComputeExecutor(
            scheduler,
            "unit-test",
            ComputePriority.NORMAL,
            ComputeExecutor.RejectionPolicy.DROP
        );
        final AtomicInteger runs = new AtomicInteger();

        executor.execute(runs::incrementAndGet);

        assertEquals(0, runs.get());
        assertTrue(scheduler.submissions().isEmpty());
    }

    @Test
    void callerRunsPropagatesCommandFailures() {
        final TestScheduler scheduler = new TestScheduler();
        scheduler.failWith(TestScheduler.FailureMode.REJECT);
        final ComputeExecutor executor = new ComputeExecutor(scheduler, "unit-test");

        assertThrows(IllegalStateException.class, () -> executor.execute(() -> {
            throw new IllegalStateException("boom");
        }));
    }

    @Test
    void submissionsCarryConsumerPriorityAndTaskType() {
        final TestScheduler scheduler = new TestScheduler();
        final ComputeExecutor executor = new ComputeExecutor(
            scheduler,
            "map-colour-cache",
            ComputePriority.LOW,
            ComputeTaskType.CUSTOM,
            ComputeExecutor.RejectionPolicy.CALLER_RUNS,
            NO_FAILURE_HANDLER
        );
        final AtomicInteger runs = new AtomicInteger();

        executor.execute(runs::incrementAndGet);

        assertEquals(1, runs.get());
        assertEquals(1, scheduler.submissions().size());
        final TestScheduler.Submission submission = scheduler.submissions().getFirst();
        assertEquals("map-colour-cache", submission.consumerId());
        assertEquals(ComputePriority.LOW, submission.priority());
        assertEquals(ComputeTaskType.CUSTOM, submission.taskType());
    }

    @Test
    void successfulCommandsDoNotReachTheFailureHandler() {
        final TestScheduler scheduler = new TestScheduler();
        final AtomicInteger failures = new AtomicInteger();
        final ComputeExecutor executor = new ComputeExecutor(
            scheduler,
            "unit-test",
            ComputePriority.NORMAL,
            ComputeTaskType.GENERAL,
            ComputeExecutor.RejectionPolicy.CALLER_RUNS,
            throwable -> failures.incrementAndGet()
        );

        executor.execute(() -> {
        });

        assertEquals(0, failures.get());
    }

    @Test
    void commandFailuresReachTheFailureHandler() {
        final TestScheduler scheduler = new TestScheduler();
        final AtomicReference<Throwable> reported = new AtomicReference<>();
        final ComputeExecutor executor = new ComputeExecutor(
            scheduler,
            "unit-test",
            ComputePriority.NORMAL,
            ComputeTaskType.GENERAL,
            ComputeExecutor.RejectionPolicy.CALLER_RUNS,
            reported::set
        );
        final IllegalStateException failure = new IllegalStateException("task failed");

        executor.execute(() -> {
            throw failure;
        });

        assertSame(failure, reported.get());
    }

    @Test
    void engineShutdownDropsWithoutThrowing() {
        final TestScheduler scheduler = new TestScheduler();
        scheduler.failWith(TestScheduler.FailureMode.SHUTDOWN);
        final ComputeExecutor executor = new ComputeExecutor(scheduler, "unit-test");
        final AtomicInteger runs = new AtomicInteger();

        executor.execute(runs::incrementAndGet);

        assertEquals(0, runs.get());
        assertTrue(scheduler.submissions().isEmpty());
    }

    @Test
    void terminatedEngineDropsWithoutThrowing() {
        final TestScheduler scheduler = new TestScheduler();
        scheduler.shutdown();
        final ComputeExecutor executor = new ComputeExecutor(scheduler, "unit-test");
        final AtomicInteger runs = new AtomicInteger();

        executor.execute(runs::incrementAndGet);

        assertEquals(0, runs.get());
    }

    @Test
    void shutdownDetachesWithoutStoppingTheEngine() {
        final TestScheduler scheduler = new TestScheduler();
        final ComputeExecutor executor = new ComputeExecutor(scheduler, "unit-test");

        executor.shutdown();

        assertTrue(executor.isShutdown());
        assertEquals(AsyncComputeScheduler.SchedulerState.RUNNING, scheduler.state());
        assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> {
        }));
        executor.shutdown();
        assertTrue(executor.isShutdown());
    }

    @Test
    void rejectsNullCommand() {
        final ComputeExecutor executor = new ComputeExecutor(new TestScheduler(), "unit-test");

        assertThrows(NullPointerException.class, () -> executor.execute(null));
    }

    @Test
    void validatesConstructorArguments() {
        final TestScheduler scheduler = new TestScheduler();

        assertThrows(NullPointerException.class, () -> new ComputeExecutor(null, "unit-test"));
        assertThrows(NullPointerException.class, () -> new ComputeExecutor(scheduler, null));
        assertThrows(IllegalArgumentException.class, () -> new ComputeExecutor(scheduler, " "));
        assertThrows(NullPointerException.class, () -> new ComputeExecutor(
            scheduler, "unit-test", null, ComputeExecutor.RejectionPolicy.DROP
        ));
        assertThrows(NullPointerException.class, () -> new ComputeExecutor(
            scheduler, "unit-test", ComputePriority.NORMAL, null
        ));
        assertThrows(NullPointerException.class, () -> new ComputeExecutor(
            scheduler, "unit-test", ComputePriority.NORMAL, ComputeTaskType.GENERAL,
            ComputeExecutor.RejectionPolicy.DROP, null
        ));
        assertFalse(new ComputeExecutor(scheduler, "unit-test").isShutdown());
    }
}
