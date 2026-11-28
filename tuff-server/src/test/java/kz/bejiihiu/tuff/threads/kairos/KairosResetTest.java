package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resetting the scheduler's A/B counters without restarting the fund.
 *
 * <p>A measurement series starts when somebody says so. Dropping and recreating the scheduler for
 * that would lose every live thread and every permit, so the reset is a write over the counters only.
 * These tests prove the counters really go to zero, that the fund keeps its shape, that the next tick
 * starts a fresh window, and that the call is safe from inside a tick body.
 */
@Timeout(120)
class KairosResetTest {

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
    void resetZeroesTheCountersAndLeavesTheFundStanding() throws InterruptedException {
        KairosGate.arm(true, new KairosTuning(true, 2, 5_000, 50, 20L),
            new KairosServices(floor -> 2, KairosSupervisorSource.plain()));
        final KairosScheduler elastic = this.scheduler = new KairosScheduler(
            1, this.threads, tick -> ((FakeTick) tick).deadline(), KairosTickControl.none()
        );
        final CountDownLatch done = new CountDownLatch(2);
        final long now = System.nanoTime();
        elastic.schedule(new FakeTick("first", now, this.order, done).runFor(millis(50L)));
        elastic.schedule(new FakeTick("second", now, this.order, done));
        elastic.start();
        this.awaitLive(2, 10_000L);
        assertTrue(done.await(10L, TimeUnit.SECONDS), "both ticks must run before the reset");

        final KairosSnapshot before = elastic.snapshot();
        assertTrue(before.executedTicks() >= 2L, "two bodies ran, got " + before.executedTicks());
        assertTrue(before.spawnCount() >= 1L, "the fund grew once, got " + before.spawnCount());
        assertTrue(before.leaseWait().grants() >= 2L, "two permits were measured");
        assertFalse(before.leaseWait().recent().isEmpty(), "the ring must hold the measured waits");

        final int floor = elastic.minThreads();
        final int ceiling = elastic.maxThreads();
        final int live = elastic.liveThreads();
        assertTrue(KairosScheduler.resetActiveCounters(), "the running fund must accept a reset");

        final KairosSnapshot after = elastic.snapshot();
        assertEquals(0L, after.spawnCount(), "spawns must start from zero");
        assertEquals(0L, after.retireCount(), "retires must start from zero");
        assertEquals(0L, after.executedTicks(), "executed ticks must start from zero");
        assertEquals(0L, after.affinityHits(), "affinity hits must start from zero");
        assertEquals(0L, after.affinityMisses(), "affinity misses must start from zero");
        assertEquals(0L, after.stolenTicks(), "steals must start from zero");
        assertEquals(0L, after.catchUpBursts(), "catch-up bursts must start from zero");
        assertEquals(0L, after.catchUpTicks(), "catch-up ticks must start from zero");
        assertEquals(0L, after.overdueWarns(), "overdue warnings must start from zero");
        assertEquals(0L, after.suppressedOverdueWarns(), "suppressed warnings must start from zero");
        assertEquals(0L, after.leaseWait().grants(), "measured grants must start from zero");
        assertEquals(0L, after.leaseWait().queuedMaxMillis(), "worst queued wait must start from zero");
        assertEquals(0L, after.leaseWait().parkedMaxMillis(), "worst parked wait must start from zero");
        assertTrue(after.leaseWait().recent().isEmpty(), "the sample ring must be cleared");

        assertEquals(floor, after.minThreads(), "the floor is not a counter");
        assertEquals(ceiling, after.maxThreads(), "the ceiling is not a counter");
        assertEquals(live, after.liveThreads(), "the live threads are not counters");
        assertEquals(elastic.elastic(), after.elastic(), "elasticity is not a counter");

        // the fund still schedules: the window is new, the machine is the same
        final CountDownLatch next = new CountDownLatch(1);
        elastic.schedule(new FakeTick("after reset", System.nanoTime(), this.order, next));
        assertTrue(next.await(10L, TimeUnit.SECONDS), "the fund must keep ticking after a reset");
        assertTrue(elastic.snapshot().executedTicks() >= 1L, "the new window counts its own ticks");
    }

    @Test
    void resetFromATickBodyDoesNotDeadlockTheFund() throws InterruptedException {
        final KairosScheduler fixed = this.fixedFund();
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean applied = new AtomicBoolean();
        final FakeTick body = new FakeTick("body", System.nanoTime(), this.order, done) {
            @Override
            public boolean runTick() {
                applied.set(KairosScheduler.resetActiveCounters());
                return super.runTick();
            }
        };
        fixed.schedule(body);
        fixed.start();

        assertTrue(done.await(10L, TimeUnit.SECONDS), "a tick body must be able to reset the counters");
        assertTrue(applied.get(), "resetActiveCounters must find the running fund");
        assertEquals(1, fixed.minThreads());
        assertEquals(1, fixed.liveThreads());
    }

    @Test
    void aResetWithoutARunningFundIsRefused() {
        Assumptions.assumeTrue(
            KairosScheduler.activeSnapshot() == null,
            "another test leaked a running fund, so the refusal cannot be observed"
        );
        assertFalse(KairosScheduler.resetActiveCounters(), "nothing to reset without a running fund");
    }

    private KairosScheduler fixedFund() {
        KairosGate.arm(true, new KairosTuning(false, 1, 5_000, 10, 20L));
        KairosGate.kernel(new KairosKernel(1, 0, 0, 0, 0));
        return this.scheduler = new KairosScheduler(
            1, this.threads, tick -> ((FakeTick) tick).deadline(), KairosTickControl.none()
        );
    }

    private void awaitLive(final int expected, final long timeoutMillis) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (this.scheduler.liveThreads() != expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("expected " + expected + " live threads, got "
                    + this.scheduler.liveThreads());
            }
            Thread.onSpinWait();
        }
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
