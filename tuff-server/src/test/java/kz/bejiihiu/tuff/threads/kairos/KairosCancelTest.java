package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cancelling a tick that already holds a permit, which is the one cancel path that has to move the
 * permit to another tick.
 *
 * <p>The failure this guards against is quiet: the replacement was owned by the runner and running,
 * but it was never linked back into the awaiting set, so the queue counters under-counted permits, a
 * later cancel of that tick answered "no", and when the runner reached the deadline the tick start threw
 * on the runner thread. A thrown exception there is outside Folia's handler, so the fund would lose a
 * thread instead of stopping the server loudly.
 */
@Timeout(120)
class KairosCancelTest {

    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    private RecordingThreadFactory threads;
    private @Nullable KairosScheduler scheduler;

    @BeforeEach
    void newFactory() {
        this.threads = new RecordingThreadFactory();
    }

    @AfterEach
    void stopEverything() {
        if (this.scheduler != null) {
            this.scheduler.halt();
            assertTrue(this.scheduler.join(10_000L), "halt must stop every thread of the fund");
        }
        KairosGate.disarm();
    }

    @Test
    void cancellingAnAwaitingTickKeepsItsReplacementLinked() throws InterruptedException {
        final KairosScheduler fixed = this.fund();
        final long now = System.nanoTime();
        final FakeTick awaiting = this.tick("awaiting", now + millis(2_000L), new CountDownLatch(1));
        final FakeTick queued = this.tick("queued", now + millis(2_500L), new CountDownLatch(1));
        fixed.schedule(awaiting);
        fixed.schedule(queued);
        fixed.start();
        this.awaitPermit(fixed);

        assertTrue(fixed.cancel(awaiting), "a tick that only waits for its deadline is cancellable");

        // the replacement holds the permit now; it must be visible in the awaiting count, otherwise the
        // snapshot silently loses one permit and the next tick start throws
        final KairosSnapshot afterCancel = fixed.snapshot();
        assertEquals(0, afterCancel.queuedTicks());
        assertEquals(1, afterCancel.awaitingTicks(), "the replacement must be linked into awaiting");
        assertEquals(
            1,
            afterCancel.queuedTicks() + afterCancel.awaitingTicks() + afterCancel.executingTicks(),
            "queued + awaiting + executing must match the scheduled set"
        );

        assertTrue(fixed.cancel(queued), "the replacement is a scheduled tick and must be cancellable");
        this.awaitIdle(fixed);
        assertEquals(0, queued.runs(), "a cancelled tick must never run");
        assertEquals(0, awaiting.runs());
    }

    @Test
    void aReplacementReachesItsDeadlineWithoutThrowing() throws InterruptedException {
        final KairosScheduler fixed = this.fund();
        final long now = System.nanoTime();
        // the first tick owns the permit and is due first, so the second one waits in the queue
        final FakeTick awaiting = this.tick("awaiting", now + millis(50L), new CountDownLatch(1));
        final FakeTick replacement = this.tick("replacement", now + millis(1_200L), new CountDownLatch(1));
        fixed.schedule(awaiting);
        fixed.schedule(replacement);
        fixed.start();
        this.awaitPermit(fixed);

        assertTrue(fixed.cancel(awaiting));
        assertTrue(replacement.entered().await(5L, TimeUnit.SECONDS), "the replacement must reach its deadline");
        this.awaitRuns(replacement, 1);
        Thread.sleep(100L);

        assertEquals(1, replacement.runs(), "exactly once");
        assertEquals(1, fixed.getAliveThreads().length, "the runner must survive its tick start");
        final KairosSnapshot idle = fixed.snapshot();
        assertEquals(0, idle.queuedTicks() + idle.awaitingTicks() + idle.executingTicks());
    }

    private KairosScheduler fund() {
        KairosGate.arm(true, new KairosTuning(false, -1, 5_000, 10, 20L));
        return this.scheduler = new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline());
    }

    private FakeTick tick(final String label, final long deadline, final CountDownLatch done) {
        return new FakeTick(label, deadline, this.order, done);
    }

    private void awaitPermit(final KairosScheduler fixed) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (fixed.snapshot().awaitingTicks() != 1) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("the first tick never took the permit: " + fixed.snapshot());
            }
            Thread.sleep(1L);
        }
    }

    private void awaitIdle(final KairosScheduler fixed) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (fixed.snapshot().awaitingTicks() != 0) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("the fund never went idle: " + fixed.snapshot());
            }
            Thread.sleep(1L);
        }
    }

    private void awaitRuns(final FakeTick tick, final int expected) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (tick.runs() < expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("expected " + expected + " run(s), saw " + tick.runs());
            }
            Thread.sleep(1L);
        }
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
