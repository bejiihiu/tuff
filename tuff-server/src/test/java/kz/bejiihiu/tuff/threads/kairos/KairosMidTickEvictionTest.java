package kz.bejiihiu.tuff.threads.kairos;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The mid-tick pass against a permit that the queue would like to move.
 *
 * <p>A runner that is draining a region inside {@code runTasks} holds that region's ticking mark. The
 * scheduler must not take the permit away in that window - neither by the EDF eviction nor by the
 * replacement a cancel performs - because the tick that got the permit would eventually call
 * {@code runTick} on a region that is still marked, and the handle answers that with an exception that
 * ends the server.
 */
@Timeout(120)
class KairosMidTickEvictionTest {

    private RecordingThreadFactory threads;
    private @Nullable KairosScheduler scheduler;
    private @Nullable MarkingTick draining;

    @BeforeEach
    void newFactory() {
        this.threads = new RecordingThreadFactory();
    }

    @AfterEach
    void stopEverything() {
        final MarkingTick pass = this.draining;
        if (pass != null) {
            // never leave a runner blocked inside the fake pass: halt cannot interrupt it
            pass.releasePass();
        }
        if (this.scheduler != null) {
            this.scheduler.halt();
            assertTrue(this.scheduler.join(10_000L), "halt must stop every thread of the fund");
        }
        KairosGate.disarm();
    }

    @Test
    void anEarlierTickNeverTakesThePermitOfARunnerInsideRunTasks() throws InterruptedException {
        final KairosScheduler fund = this.fund(2);
        final long now = System.nanoTime();
        final MarkingTick pass = this.draining =
            new MarkingTick("draining", now + millis(1_500L));
        final MarkingTick other = new MarkingTick("other", now + millis(300L)).withoutTasks();
        final MarkingTick urgent = new MarkingTick("urgent", now + millis(100L)).withoutTasks();
        fund.schedule(pass);
        fund.schedule(other);
        fund.start();

        assertTrue(pass.awaitPassEntered(5_000L), "the waiting runner must start its mid-tick pass");
        assertTrue(pass.isInsidePass());
        assertTrue(pass.isMarked(), "the pass holds the region's ticking mark");

        // every permit is taken, and the new tick is earlier than the last awaiting one: this is exactly
        // the eviction EDF performs, and the one that must not move a permit out of the pass
        fund.schedule(urgent);
        assertTrue(other.awaitBody(5_000L), "the other tick must still run");

        // wait past the deadline the displaced tick would have been refused at
        final long watchUntil = now + millis(1_650L);
        while (System.nanoTime() - watchUntil < 0L) {
            LockSupport.parkNanos(millis(2L));
        }
        try {
            assertEquals(
                0,
                pass.refusedBodies(),
                "a replacement reached a region that was still inside runTasks"
            );
            assertEquals(0, pass.bodies(), "the draining tick must still be the tick that owns the permit");
            assertEquals(1, urgent.bodies(), "the urgent tick runs exactly once, from the free permit");
        } finally {
            pass.releasePass();
        }

        assertTrue(pass.awaitBody(5_000L), "the tick must run once its pass ends");
        awaitScheduled(fund, 0);
        assertEquals(1, pass.bodies(), "exactly once");
        assertEquals(1, urgent.bodies(), "exactly once");
        assertEquals(1, other.bodies(), "exactly once");
        assertFalse(pass.isMarked(), "the region must be unmarked after the pass");
        assertEquals(2, fund.liveThreads(), "the pass must not cost a runner");
    }

    @Test
    void cancellingDuringRunTasksParksTheRunnerInsteadOfReplacingThePermit() throws InterruptedException {
        final KairosScheduler fund = this.fund(1);
        final long now = System.nanoTime();
        final MarkingTick pass = this.draining =
            new MarkingTick("draining", now + millis(1_500L));
        fund.schedule(pass);
        fund.start();
        assertTrue(pass.awaitPassEntered(5_000L), "the runner must be inside its mid-tick pass");

        // arrives while the only permit is held by a pass whose region is marked ticking
        final MarkingTick replacement = new MarkingTick("replacement", System.nanoTime() + millis(100L)).withoutTasks();
        fund.schedule(replacement);
        assertEquals(1, fund.snapshot().queuedTicks(), "an earlier tick must wait, not evict the pass");

        assertTrue(fund.cancel(pass), "the awaiting tick is cancellable even while its pass runs");

        // the permit must stay with nobody until the pass returns: handing it over here would let the
        // runner tick a different region before its own runTasks call has returned
        final KairosSnapshot during = fund.snapshot();
        assertEquals(0, during.awaitingTicks(), "no permit may be given to a runner inside runTasks");
        assertEquals(1, during.queuedTicks(), "the replacement waits in the queue");
        assertEquals(1, during.scheduledTicks(), "the cancelled tick must leave the scheduler's map");
        try {
            assertEquals(0, replacement.bodies(), "the replacement must not run before the pass ends");
        } finally {
            pass.releasePass();
        }

        assertTrue(replacement.awaitBody(5_000L), "the replacement must run once the pass ended");
        awaitScheduled(fund, 0);
        assertEquals(0, pass.bodies(), "a cancelled tick must never run");
        assertEquals(1, replacement.bodies(), "exactly once");
        assertFalse(pass.isMarked(), "the region must be unmarked after the pass");
        assertEquals(1, fund.liveThreads());
    }

    private KairosScheduler fund(final int size) {
        KairosGate.arm(true, new KairosTuning(false, -1, 5_000, 10, 20L));
        KairosGate.kernel(KairosKernel.defaults());
        KairosGate.idleWork(new KairosIdleWork(true, true, false, 100, 10_000, 100));
        return this.scheduler = new KairosScheduler(size, this.threads, tick -> ((MarkingTick) tick).deadline());
    }

    private static void awaitScheduled(final KairosScheduler fund, final int expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (fund.scheduledTicks() != expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("expected " + expected + " scheduled ticks, got " + fund.scheduledTicks());
            }
            Thread.onSpinWait();
        }
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
