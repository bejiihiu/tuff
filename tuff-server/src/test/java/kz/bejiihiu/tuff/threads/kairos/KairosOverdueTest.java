package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.threads.kairos.KairosSnapshot.Degraded;
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
 * R5 behaviour: a tick that never gets a permit must be visible, without turning the log into a flood.
 *
 * <p>The detector is driven by a clock the test moves, so "the deadline passed 200 ms ago" and "the ten
 * second quiet window is over" cost no wall time. Ticks themselves still sleep in real time, because
 * that is what keeps a permit busy.
 *
 * <p>The threshold itself is derived from the period the server actually manages, which has its own
 * test: a 12 TPS server must not be blind to a tick that is a whole period late.
 */
@Timeout(120)
class KairosOverdueTest {

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
            assertTrue(this.scheduler.join(15_000L), "halt must stop every thread of an elastic fund");
        }
        KairosGate.disarm();
    }

    @Test
    void aFullFundWarnsSaturatedOnceAndCountsTheSuppressedPasses() throws Exception {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler elastic = this.elastic(1, 2, clock);
        final CountDownLatch done = new CountDownLatch(3);
        final long base = clock.getAsLong();
        for (int index = 0; index < 3; ++index) {
            elastic.schedule(this.tick("t" + index, base - millis(200L), done));
        }

        try (KairosWarningCapture capture = KairosWarningCapture.attach()) {
            Assumptions.assumeTrue(capture != null, "log4j2 core is not on the test classpath");
            elastic.start();

            assertTrue(capture.await(1, 5_000L), "a tick that waits 200 ms must be reported");
            final String first = capture.warnings().get(0);
            assertTrue(first.contains("saturated"), first);
            assertTrue(first.contains("2/2"), "the warning must carry live/max, was: " + first);
            assertTrue(first.contains("region unknown (tick"), "a fake tick has no region, was: " + first);

            Thread.sleep(300L);
            assertEquals(1, capture.warnings().size(), "the quiet window must swallow the following passes");

            clock.advance(TimeUnit.SECONDS.toNanos(11L));
            assertTrue(capture.await(2, 5_000L), "the next window must be allowed to warn again");
            final String second = capture.warnings().get(1);
            assertTrue(second.contains("suppressed"), "the next warning must say what it swallowed: " + second);
        }
        final KairosSnapshot snapshot = elastic.snapshot();
        assertTrue(snapshot.degradedReasons().contains(Degraded.QUEUED_OVERDUE));
        assertTrue(snapshot.overdueTicks() > 0, "the snapshot must show the same overdue ticks");
        assertTrue(snapshot.oldestOverdueMillis() >= 100L);
    }

    @Test
    void aFundThatStillHasRoomBlamesStarvationNotSaturation() throws Exception {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler elastic = this.elastic(1, 4, clock);
        final CountDownLatch done = new CountDownLatch(3);
        final long base = clock.getAsLong();
        for (int index = 0; index < 3; ++index) {
            elastic.schedule(this.tick("t" + index, base - millis(200L), done));
        }

        try (KairosWarningCapture capture = KairosWarningCapture.attach()) {
            Assumptions.assumeTrue(capture != null, "log4j2 core is not on the test classpath");
            elastic.start();

            assertTrue(capture.await(1, 5_000L), "the overdue tick must be reported");
            final String warning = capture.warnings().get(0);
            assertTrue(warning.contains("starvation"), warning);
            assertTrue(warning.contains("2/4"), "the warning must carry live/max, was: " + warning);
            assertFalse(warning.contains("saturated"), warning);
        }
    }

    @Test
    void aFixedFundHasNoDetectorAtAll() throws Exception {
        KairosGate.arm(true, new KairosTuning(false, 2, 5_000, 50, 20L));
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fixed = this.scheduler =
            new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline(), KairosTickControl.none(), clock);
        final CountDownLatch done = new CountDownLatch(3);
        final long base = clock.getAsLong();
        for (int index = 0; index < 3; ++index) {
            fixed.schedule(this.tick("t" + index, base - millis(200L), done));
        }

        try (KairosWarningCapture capture = KairosWarningCapture.attach()) {
            Assumptions.assumeTrue(capture != null, "log4j2 core is not on the test classpath");
            fixed.start();
            Thread.sleep(400L);

            assertTrue(capture.warnings().isEmpty(), "a fixed fund has no supervisor and therefore no detector");
        }
        assertTrue(fixed.snapshot().overdueTicks() > 0, "the snapshot still reports the stale tick");
    }

    @Test
    void theThresholdFollowsTheObservedPeriod() throws InterruptedException {
        // a server that manages 12 TPS ticks every 83 ms; two periods are 166 ms, not the 100 ms constant
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        KairosGate.arm(true, new KairosTuning(false, -1, 5_000, 10, 20L));
        final KairosScheduler fixed = this.scheduler =
            new KairosScheduler(1, this.threads, tick -> tick instanceof CatchUpTick model
                ? model.scheduledStart() : ((FakeTick) tick).deadline(), KairosTickControl.none(), clock);
        final CountDownLatch unused = new CountDownLatch(0);
        // each body costs the region 83 ms of the model clock, and the tick stops after two bodies
        final CatchUpTick slow = new CatchUpTick(
            "slow", TimeUnit.SECONDS.toNanos(1L), 0L, clock, clock, millis(83L)
        ).stopAfter(2);
        fixed.schedule(slow);
        fixed.start();
        assertTrue(slow.awaitBodies(2, 10_000L), "the tick must run twice, so the period has one sample");

        final KairosSnapshot sampled = fixed.snapshot();
        assertEquals(166L, sampled.overdueThresholdMillis(), "two observed periods, not the constant");

        // keep the only permit busy, so the next tick really waits in the queue
        final FakeTick holder = this.tick("holder", clock.getAsLong(), unused).runFor(millis(400L));
        fixed.schedule(holder);
        assertTrue(holder.entered().await(5, TimeUnit.SECONDS), "the holder must own the permit");

        // a tick that waited 110 ms is late at 20 TPS but not at 12 TPS
        final FakeTick waiting = this.tick("waiting", clock.getAsLong() - millis(110L), unused);
        fixed.schedule(waiting);
        Thread.sleep(30L);
        assertEquals(0, fixed.snapshot().overdueTicks(), "110 ms is below the derived threshold");
        clock.advance(millis(70L));
        assertEquals(1, fixed.snapshot().overdueTicks(), "180 ms is past it");
    }

    private KairosScheduler elastic(final int min, final int max, final MutableNanoClock clock) {
        KairosGate.arm(true, new KairosTuning(true, max, 5_000, 50, 20L));
        return this.scheduler = new KairosScheduler(
            min, this.threads, tick -> ((FakeTick) tick).deadline(), KairosTickControl.none(), clock
        );
    }

    private FakeTick tick(final String label, final long deadline, final CountDownLatch done) {
        return new FakeTick(label, deadline, this.order, done).runFor(millis(1_500L));
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
