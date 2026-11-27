package kz.bejiihiu.tuff.threads.kairos;

import ca.spottedleaf.concurrentutil.scheduler.SchedulableTick;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import kz.bejiihiu.tuff.threads.idle.IdleWorkSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Idle work: the mid-tick task drain and the idle-safe job drain, their bounds and their guard.
 *
 * <p>The deadline and the slice are wall clock, so the tests assert ranges wide enough for a loaded CI
 * machine and narrow enough to separate "stopped at the slice" from "ran to the deadline". No test
 * waits for a fixed period to pass: every wait is a condition with a budget, and the counters are read
 * at the moment the tick body starts, which is the only moment that proves nothing ran before it.
 *
 * <p>Every case starts a real fund with recorded daemon threads, and the setup halts and joins it, so
 * a failure leaves no thread behind.
 */
@Timeout(120)
class KairosIdleWorkTest {

    private static final long MILLIS = 1_000_000L;

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
    void offByDefaultIsANoOp() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fund = this.scheduler(KairosIdleWork.off(), null, clock);
        final IdleTick tick = this.tick("region", System.nanoTime() + 150L * MILLIS);
        fund.schedule(tick);
        fund.start();

        assertTrue(tick.entered().await(5L, TimeUnit.SECONDS), "the tick must run");
        assertEquals(0, tick.hasTaskCalls(), "with the path off the handle is not even asked");
        assertEquals(0, tick.passCalls(), "and no intermediate task is drained");
        final KairosSnapshot.IdleWork idle = fund.snapshot().idleWork();
        assertFalse(idle.enabled());
        assertEquals(0L, idle.midTickPasses());
        assertEquals(0L, idle.jobsTaken());
        assertEquals(KairosSnapshot.IdleWork.Disabled.NONE, idle.disabled());
        assertTrue(
            KairosReport.overview("KAIROS", 1, fund.snapshot()).stream()
                .anyMatch(line -> line.contains("idle work: off")),
            "the report must say the path is off"
        );
    }

    @Test
    void theMidTickPassDrainsTheRegionOnceWhileItWaits() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fund = this.scheduler(midTickOnly(100), null, clock);
        final IdleTick tick = this.tick("region", System.nanoTime() + 250L * MILLIS).withOneTaskPerPass();
        fund.schedule(tick);
        fund.start();

        assertTrue(tick.entered().await(5L, TimeUnit.SECONDS), "the tick must run");
        assertEquals(1, tick.hasTaskCalls(), "the handle is asked whether work waits");
        assertEquals(1, tick.passCalls(), "one pass per wait, never a second one");
        assertTrue(tick.taskUnits() >= 1, "the queued task ran before the tick");
        final KairosSnapshot.IdleWork idle = fund.snapshot().idleWork();
        assertEquals(1L, idle.midTickPasses());
        assertEquals(0L, idle.preemptions());
        assertTrue(
            KairosReport.overview("KAIROS", 1, fund.snapshot()).stream()
                .anyMatch(line -> line.contains("idle work: on (mid-tick on")),
            "the report must name the source"
        );
    }

    @Test
    void aDueTickPreemptsThePass() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fund = this.scheduler(new KairosIdleWork(true, true, false, 100, 5, 100), null, clock);
        final long deadline = System.nanoTime() + 60L * MILLIS;
        final IdleTick tick = this.tick("region", deadline);
        fund.schedule(tick);
        fund.start();

        assertTrue(tick.entered().await(5L, TimeUnit.SECONDS), "the tick must run");
        final long bodyAt = System.nanoTime();
        assertTrue(
            tick.passNanos() >= 40L * MILLIS,
            "the pass was cut by the deadline, not by its 100 ms slice: " + tick.passNanos() + " ns"
        );
        assertTrue(
            tick.passNanos() <= 400L * MILLIS,
            "the pass must stop at the deadline: " + tick.passNanos() + " ns"
        );
        assertTrue(
            bodyAt - deadline <= 100L * MILLIS,
            "the body started " + (bodyAt - deadline) + " ns after its deadline"
        );
        final KairosSnapshot.IdleWork idle = fund.snapshot().idleWork();
        assertTrue(idle.preemptions() >= 1L, "a due tick that cut a pass must be counted");
        assertEquals(1L, idle.midTickPasses());
    }

    @Test
    void thePassIsBoundedByTheSlice() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fund = this.scheduler(new KairosIdleWork(true, true, false, 10, 5, 100), null, clock);
        final IdleTick tick = this.tick("region", System.nanoTime() + 700L * MILLIS);
        fund.schedule(tick);
        fund.start();

        assertTrue(tick.entered().await(5L, TimeUnit.SECONDS), "the tick must run");
        final long pass = tick.passNanos();
        assertTrue(pass >= 8L * MILLIS, "the pass must use its slice, took " + pass + " ns");
        assertTrue(pass <= 400L * MILLIS, "the pass must stop near the 10 ms slice, took " + pass + " ns");
        assertTrue(tick.taskUnits() > 10, "the pass drained many tasks inside the slice");
    }

    @Test
    void aCancelledRegionIsNeverDrained() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fund = this.schedulerWithCancel(
            midTickOnly(100),
            null,
            clock,
            tick -> ((IdleTick) tick).isCancelled()
        );
        final IdleTick tick = this.tick("dropped", System.nanoTime() + 150L * MILLIS).cancel();
        fund.schedule(tick);
        fund.start();

        assertTrue(tick.entered().await(5L, TimeUnit.SECONDS), "the fund still reaches the tick");
        assertEquals(0, tick.hasTaskCalls(), "a dropped region is not asked about tasks");
        assertEquals(0, tick.passCalls(), "and its tasks are never started");
        assertEquals(0L, fund.snapshot().idleWork().midTickPasses());
    }

    @Test
    void anIdleJobStopsAtTheSliceBoundary() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final FakeIdleSource source = new FakeIdleSource();
        final KairosScheduler fund = this.scheduler(new KairosIdleWork(true, false, true, 20, 5, 100), source, clock);
        fund.start();

        assertTrue(source.awaitFirstRun(5_000L), "the parked runner must take the job");
        final long jobDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
        while (source.lastRunNanos() == 0L) {
            if (System.nanoTime() - jobDeadline >= 0L) {
                throw new AssertionError("the idle job never finished");
            }
            Thread.onSpinWait();
        }
        assertTrue(
            source.lastRunNanos() >= 15L * MILLIS,
            "the job must use its slice, took " + source.lastRunNanos() + " ns"
        );
        assertTrue(
            source.lastRunNanos() <= 400L * MILLIS,
            "the job must stop near the 20 ms slice, took " + source.lastRunNanos() + " ns"
        );
        assertTrue(source.units() > 10, "the job ran many units inside the slice");

        final IdleTick tick = this.tick("region", System.nanoTime() + 150L * MILLIS);
        fund.schedule(tick);
        assertTrue(tick.entered().await(5L, TimeUnit.SECONDS), "a tick must still run after idle work");
        final KairosSnapshot.IdleWork idle = fund.snapshot().idleWork();
        assertTrue(idle.jobsTaken() >= 1L, "an idle-safe job must have run");
        assertTrue(idle.shareOfIdlePercent() > 0.0d, "the slice must show up in the share of idle");
    }

    @Test
    void theShareBudgetStopsFurtherJobs() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final FakeIdleSource source = new FakeIdleSource();
        // one percent of a one second window is ten milliseconds: the first fifty millisecond job fits,
        // and nothing else fits until the window rolls over
        final KairosScheduler fund = this.scheduler(new KairosIdleWork(true, false, true, 50, 5, 1), source, clock);
        fund.start();

        assertTrue(source.awaitFirstRun(5_000L), "the first job fits the window");
        final long jobEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
        while (source.lastRunNanos() == 0L) {
            if (System.nanoTime() - jobEnd >= 0L) {
                throw new AssertionError("the first job never finished");
            }
            Thread.onSpinWait();
        }
        // the runner goes back to its idle loop every 20 ms; a whole second would be many chances to
        // poll, so a quarter of a second is enough to prove the budget, not the clock, is the reason
        final long observe = System.nanoTime() + 250L * MILLIS;
        while (System.nanoTime() - observe < 0L) {
            LockSupport.parkNanos(MILLIS);
        }
        assertEquals(1, source.polls(), "the spent share budget must stop polling");
        assertEquals(1, source.runs(), "and no second job may start");
        assertEquals(1L, fund.snapshot().idleWork().jobsTaken());
    }

    @Test
    void aJobThatIgnoresItsSliceDisablesThePath() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final FakeIdleSource source = new FakeIdleSource().ignoringTheSliceFor(40L * MILLIS);
        final KairosScheduler fund = this.scheduler(new KairosIdleWork(true, false, true, 1, 5, 100), source, clock);
        fund.start();

        assertTrue(source.awaitFirstRun(5_000L), "the bad job must start");
        final IdleTick late = this.tick("late", System.nanoTime() - MILLIS);
        fund.schedule(late);
        assertTrue(late.entered().await(5L, TimeUnit.SECONDS), "the late tick must still run");

        final KairosSnapshot.IdleWork idle = fund.snapshot().idleWork();
        assertTrue(
            idle.addedTickLagMaxMillis() >= 5L,
            "the lateness must be measured: " + idle.addedTickLagMaxMillis() + " ms"
        );
        assertEquals(KairosSnapshot.IdleWork.Disabled.LAG_BUDGET, idle.disabled());
        assertEquals(1L, idle.jobsTaken(), "the guard must stop the next job");

        final int pollsBefore = source.polls();
        final IdleTick next = this.tick("next", System.nanoTime() + 150L * MILLIS);
        fund.schedule(next);
        assertTrue(next.entered().await(5L, TimeUnit.SECONDS), "the fund keeps ticking");
        assertEquals(pollsBefore, source.polls(), "a disarmed path must not poll the source");
    }

    @Test
    void thePathRearmsAfterTheStabilityWindow() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final FakeIdleSource source = new FakeIdleSource().ignoringTheSliceFor(40L * MILLIS);
        final KairosScheduler fund = this.scheduler(new KairosIdleWork(true, false, true, 1, 5, 100), source, clock);
        fund.start();
        assertTrue(source.awaitFirstRun(5_000L), "the bad job must start");
        final IdleTick late = this.tick("late", System.nanoTime() - MILLIS);
        fund.schedule(late);
        assertTrue(late.entered().await(5L, TimeUnit.SECONDS), "the late tick must run");
        assertEquals(KairosSnapshot.IdleWork.Disabled.LAG_BUDGET, fund.snapshot().idleWork().disabled());

        clock.advance(TimeUnit.SECONDS.toNanos(6L));
        final int pollsBefore = source.polls();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
        while (source.polls() == pollsBefore) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("the idle path did not re-arm after the stability window");
            }
            Thread.onSpinWait();
        }
        assertEquals(KairosSnapshot.IdleWork.Disabled.NONE, fund.snapshot().idleWork().disabled());
    }

    @Test
    void aSourceIsIgnoredWhenTheJobFlagIsOff() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final FakeIdleSource source = new FakeIdleSource();
        final KairosScheduler fund = this.scheduler(new KairosIdleWork(true, false, false, 5, 5, 100), source, clock);
        fund.start();
        final IdleTick tick = this.tick("region", System.nanoTime() + 150L * MILLIS);
        fund.schedule(tick);
        assertTrue(tick.entered().await(5L, TimeUnit.SECONDS), "the tick must run");

        assertEquals(0, source.polls(), "a source that was not enabled is not polled");
        assertFalse(fund.snapshot().idleWork().sourcePresent(), "and the snapshot must say there is no source");
    }

    @Test
    void theUtmiSourceStaysInertWithoutAWiredSource() throws InterruptedException {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fund = this.scheduler(new KairosIdleWork(true, true, true, 5, 5, 100), null, clock);
        final IdleTick tick = this.tick("region", System.nanoTime() + 150L * MILLIS).withOneTaskPerPass();
        fund.schedule(tick);
        fund.start();

        assertTrue(tick.entered().await(5L, TimeUnit.SECONDS), "the tick must run");
        assertEquals(1, tick.passCalls(), "mid-tick does not need a source");
        final KairosSnapshot.IdleWork idle = fund.snapshot().idleWork();
        assertEquals(0L, idle.jobsTaken());
        assertFalse(idle.sourcePresent());
        assertTrue(idle.utmiJobs(), "the flag was on; only the source is missing");
    }

    private KairosScheduler scheduler(
        final KairosIdleWork idle,
        final @Nullable IdleWorkSource source,
        final MutableNanoClock clock
    ) {
        return this.schedulerWithCancel(idle, source, clock, tick -> false);
    }

    private KairosScheduler schedulerWithCancel(
        final KairosIdleWork idle,
        final @Nullable IdleWorkSource source,
        final MutableNanoClock clock,
        final Predicate<SchedulableTick> cancelled
    ) {
        KairosGate.arm(
            true,
            new KairosTuning(false, -1, 5_000, 10, 20L),
            new KairosServices(null, KairosSupervisorSource.plain(), source)
        );
        KairosGate.kernel(KairosKernel.defaults());
        KairosGate.idleWork(idle);
        return this.scheduler = new KairosScheduler(
            1,
            this.threads,
            tick -> ((FakeTick) tick).deadline(),
            new KairosTickControl(tick -> 0L, (tick, periods) -> {
                // this test has no catch-up model; the seam exists so the control is not the none port
            }, cancelled),
            clock
        );
    }

    private static KairosIdleWork midTickOnly(final int sharePercent) {
        return new KairosIdleWork(true, true, false, 5, 5, sharePercent);
    }

    private IdleTick tick(final String label, final long deadline) {
        final List<String> order = Collections.synchronizedList(new ArrayList<>());
        return new IdleTick(label, deadline, order, new CountDownLatch(1));
    }
}
