package kz.bejiihiu.tuff.threads.kairos;

import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bounded catch-up: a late region runs real ticks instead of writing off the whole backlog.
 *
 * <p>The clock is frozen, so "the region is ten periods behind" and "the burst spent its budget" are
 * exact numbers rather than wall-clock races. The tick is a faithful model of the region handle: it
 * keeps the period counter, applies the write-off limit and reschedules with the same clamp.
 *
 * <p>The default cap gets its own test because it has to stay byte for byte the stock behaviour: one
 * body, whole backlog written off, and the handle never even asked.
 */
@Timeout(120)
class KairosCatchUpTest {

    private static final long PERIOD = TimeUnit.MILLISECONDS.toNanos(1_000L);

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
    void theDefaultCapRunsOneBodyAndWritesOffTheWholeBacklog() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fixed = this.scheduler(KairosKernel.defaults(), clock);
        final CatchUpTick tick = new CatchUpTick("late", PERIOD, 10L, clock, null, 0L).stopAfter(1);
        fixed.schedule(tick);
        fixed.start();

        assertTrue(tick.awaitBodies(1, 5_000L), "the late tick must run");
        assertEquals(1, tick.executed(), "the stock behaviour is one body per acquisition");
        assertEquals(10, tick.skippedPeriods(), "ten whole periods are written off in that body");
        assertEquals(0, tick.limitCalls(), "catch-up off must not touch the handle at all");
        assertEquals(0L, fixed.snapshot().catchUpTicks());
        assertEquals(0L, fixed.snapshot().catchUpBursts());
    }

    @Test
    void theCapRunsThatManyBodiesAndThenSettlesTheRest() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fixed = this.scheduler(new KairosKernel(3, 60_000, 0, 3, 0), clock);
        final CatchUpTick tick = new CatchUpTick("late", PERIOD, 10L, clock, null, 0L);
        fixed.schedule(tick);
        fixed.start();

        assertTrue(tick.awaitBodies(3, 5_000L), "the burst must run three bodies");
        Thread.sleep(100L);
        assertEquals(3, tick.executed(), "the cap is the burst size, not a suggestion");
        assertEquals(
            8,
            tick.skippedPeriods(),
            "two bodies recovered a period each and the last one wrote off the remaining eight"
        );

        final KairosSnapshot snapshot = fixed.snapshot();
        assertEquals(1L, snapshot.catchUpBursts());
        assertEquals(2L, snapshot.catchUpTicks(), "the bodies beyond the first are the recovered ticks");
        assertTrue(tick.limitCalls() >= 3, "every body of the burst asked the handle for its limit");
    }

    @Test
    void theBudgetEndsTheBurstBeforeTheCapDoes() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fixed = this.scheduler(new KairosKernel(8, 100, 0, 3, 0), clock);
        // every body costs the region 60 ms of clock, so the 100 ms budget is spent after the second
        final CatchUpTick tick = new CatchUpTick("slow", PERIOD, 20L, clock, clock, TimeUnit.MILLISECONDS.toNanos(60L));
        fixed.schedule(tick);
        fixed.start();

        assertTrue(tick.awaitBodies(3, 5_000L), "the burst must stop at the budget, not at the cap");
        Thread.sleep(100L);
        assertEquals(3, tick.executed(), "two catch-up bodies plus the body that settles the rest");
        final KairosSnapshot snapshot = fixed.snapshot();
        assertEquals(1L, snapshot.catchUpBursts());
        assertEquals(2L, snapshot.catchUpTicks());
        assertTrue(snapshot.catchUpTicks() < 8L, "the wall clock ended the burst, not the cap");
    }

    @Test
    void aCancelledRegionNeverGetsACatchUpBody() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fixed = this.scheduler(new KairosKernel(5, 5_000, 0, 3, 0), clock);
        final CatchUpTick tick = new CatchUpTick("killed", PERIOD, 10L, clock, null, 0L).cancel();
        fixed.schedule(tick);
        fixed.start();

        assertTrue(tick.awaitBodies(1, 5_000L), "the fund still asks the tick once");
        Thread.sleep(50L);
        assertEquals(0, tick.executed(), "a cancelled region must not tick");
        assertEquals(0, tick.limitCalls(), "and no burst may be started for it");
        assertEquals(0L, fixed.snapshot().catchUpTicks());
        assertEquals(0, fixed.snapshot().queuedTicks(), "it is released instead of being rescheduled");
    }

    @Test
    void aRegionThatCatchesUpStopsTheBurstEarly() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fixed = this.scheduler(new KairosKernel(6, 5_000, 0, 3, 0), clock);
        // one whole period of lateness: a single catch-up body is enough
        final CatchUpTick tick = new CatchUpTick("almost", PERIOD, 1L, clock, null, 0L).stopAfter(2);
        fixed.schedule(tick);
        fixed.start();

        assertTrue(tick.awaitBodies(2, 5_000L), "the tick runs twice");
        assertEquals(0, tick.skippedPeriods(), "a recovered period is not written off");
        assertFalse(tick.lastLimit() > 0L, "the handle is handed back in the stock mode");
    }

    private KairosScheduler scheduler(final KairosKernel kernel, final MutableNanoClock clock) {
        KairosGate.arm(true, new KairosTuning(false, -1, 5_000, 10, 20L));
        KairosGate.kernel(kernel);
        return this.scheduler = new KairosScheduler(
            1,
            this.threads,
            tick -> ((CatchUpTick) tick).scheduledStart(),
            new KairosTickControl(
                tick -> ((CatchUpTick) tick).missedPeriods(),
                (tick, periods) -> ((CatchUpTick) tick).limitWriteOff(periods)
            ),
            clock
        );
    }
}
