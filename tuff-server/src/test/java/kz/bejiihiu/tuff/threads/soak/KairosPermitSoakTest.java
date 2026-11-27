package kz.bejiihiu.tuff.threads.soak;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.threads.kairos.KairosGate;
import kz.bejiihiu.tuff.threads.kairos.KairosTuning;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Permit accounting and exactly-once execution under churn.
 *
 * <p>The invariant is the plan's K3: every tick that is still scheduled is either queued, awaiting a
 * deadline on a runner, or executing, and the sum of those three is exactly the number of ticks the
 * scheduler owes. Every tick reschedules a known number of times, so a double execution or a lost
 * execution changes a count that the test can read.
 *
 * <p>The churn storm is fully concurrent: four threads schedule and cancel their own far-dated ticks
 * at the same time. A cancel that frees a permit hands the replacement through the scheduler's grant
 * transition, which is exercised here on purpose.
 */
@Tag("soak")
@Timeout(150)
class KairosPermitSoakTest {

    private KairosRig rig;

    @AfterEach
    void stop() {
        if (this.rig != null) {
            this.rig.close();
            this.rig = null;
        }
        KairosGate.disarm();
    }

    @Test
    void permitAccountingIsExactAcrossScheduleAndCancel() throws InterruptedException {
        this.rig = new KairosRig(1, KairosTuning.fixed(), SoakFixtures.FaultyThreadFactory.normal(), null);
        this.rig.start();
        final long far = System.nanoTime() + TimeUnit.HOURS.toNanos(1L);
        final CountDownLatch never = new CountDownLatch(1);
        for (int index = 0; index < 5; ++index) {
            this.rig.tick("held-" + index, far, 0, 0L, 0L, never, null);
        }
        assertEquals(5, this.rig.permitsInUse(), "five scheduled ticks must be five held or waited permits");

        final List<SoakFixtures.SoakTick> ticks = this.rig.ticks();
        for (int index = 1; index < 5; ++index) {
            assertTrue(this.rig.scheduler().cancel(ticks.get(index)), "a queued tick must be cancellable: " + index);
        }
        assertEquals(1, this.rig.permitsInUse(), "cancelling four queued ticks must release exactly four permits");
        assertTrue(this.rig.scheduler().cancel(ticks.getFirst()), "the last, awaiting tick must be cancellable");
        assertEquals(0, this.rig.permitsInUse(), "cancelling everything must leave no permit behind");
        for (final SoakFixtures.SoakTick tick : ticks) {
            assertEquals(0, tick.runs(), "a tick cancelled before its deadline must not execute: " + tick);
        }
    }

    @Test
    void aConcurrentScheduleCancelStormLeavesNoPermitBehind() throws InterruptedException {
        final int permits = 4;
        this.rig = new KairosRig(permits, KairosTuning.fixed(), SoakFixtures.FaultyThreadFactory.normal(), null);
        this.rig.start();
        final long far = System.nanoTime() + TimeUnit.HOURS.toNanos(1L);
        final CountDownLatch never = new CountDownLatch(1);
        final AtomicInteger cancelled = new AtomicInteger();
        final AtomicInteger failedCancel = new AtomicInteger();
        final int perThread = 400;
        final int stormThreads = 4;
        final List<Thread> storm = new ArrayList<>();
        for (int thread = 0; thread < stormThreads; ++thread) {
            final int id = thread;
            storm.add(Thread.ofPlatform().name("churn-" + id).start(() -> {
                for (int round = 0; round < perThread; ++round) {
                    final SoakFixtures.SoakTick tick = this.rig.tick(
                        "churn-" + id + '-' + round, far, 0, 0L, 0L, never, null
                    );
                    if (this.rig.scheduler().cancel(tick)) {
                        cancelled.incrementAndGet();
                    } else {
                        failedCancel.incrementAndGet();
                    }
                }
            }));
        }
        for (final Thread thread : storm) {
            thread.join(30_000L);
            assertTrue(!thread.isAlive(), "the churn thread must finish");
        }
        assertEquals(
            stormThreads * perThread,
            cancelled.get(),
            "every churn tick was waiting and must be cancelled; failed cancel calls: " + failedCancel.get()
        );
        assertEquals(0, this.rig.permitsInUse(), "the storm must not leak a permit");
        for (final SoakFixtures.SoakTick tick : this.rig.ticks()) {
            assertEquals(0, tick.runs(), "a cancelled churn tick must never execute: " + tick);
        }

        final CountDownLatch done = new CountDownLatch(6 * 3);
        for (int index = 0; index < 6; ++index) {
            this.rig.tick("load-" + index, System.nanoTime(), 2, millis(1L), millis(1L), done, null);
        }
        assertTrue(done.await(20L, TimeUnit.SECONDS), "the load ticks must finish after the storm");
        for (final SoakFixtures.SoakTick tick : this.rig.ticks()) {
            if (tick.label().startsWith("load-")) {
                assertEquals(3, tick.runs(), "a tick with two reschedules runs exactly three times: " + tick);
            }
        }
        this.rig.awaitQuiescent(15_000L);
        assertEquals(0, this.rig.permitsInUse(), "the storm must not leak a permit");
        assertEquals(permits, this.rig.scheduler().liveThreads(), "a fixed fund keeps its size");
    }

    @Test
    void reschedulingTicksRunExactlyOncePerGeneration() throws InterruptedException {
        this.rig = new KairosRig(
            2, new KairosTuning(true, 4, 1_000, 100, 10L), SoakFixtures.FaultyThreadFactory.normal(), null
        );
        this.rig.start();
        final int ticks = 20;
        final int reschedules = 5;
        final CountDownLatch done = new CountDownLatch(ticks * (reschedules + 1));
        for (int index = 0; index < ticks; ++index) {
            this.rig.tick("chain-" + index, System.nanoTime(), reschedules, millis(1L), millis(1L), done, null);
        }
        assertTrue(done.await(30L, TimeUnit.SECONDS), "every generation of every tick must run");
        int total = 0;
        for (final SoakFixtures.SoakTick tick : this.rig.ticks()) {
            assertEquals(reschedules + 1, tick.runs(), "exactly one execution per generation: " + tick);
            total += tick.runs();
        }
        assertEquals(ticks * (reschedules + 1), total, "no execution may be duplicated or lost");
        this.rig.awaitQuiescent(15_000L);
        assertEquals(0, this.rig.permitsInUse(), "a finished chain must leave no permit behind");
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
