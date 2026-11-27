package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Soft affinity and stealing, with real runners and a controlled timeline.
 *
 * <p>Both tests of a pair build the same shape: two permits are busy, one of them runs a tick that
 * wants to be scheduled again while already late, and a second, earlier tick waits in the one queue.
 * The owner therefore keeps the late tick for itself and takes the earlier one, and a permit frees up
 * while the kept tick is late. Whether that tick comes back to its own runner or is taken by the free
 * permit is then a pure consequence of {@code affinity.local-queue-size} and
 * {@code steal.threshold-millis}.
 *
 * <p>Every test also asserts the tick ran exactly as many times as it asked to: moving unleased ticks
 * between backlogs must never run one twice or lose one.
 */
@Timeout(120)
class KairosAffinityTest {

    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    private RecordingThreadFactory threads;
    private @Nullable KairosScheduler scheduler;

    @BeforeEach
    void newFactory() {
        this.threads = new RecordingThreadFactory();
    }

    @AfterEach
    void stopEverything() {
        final KairosScheduler running = this.scheduler;
        if (running != null) {
            running.halt();
            assertTrue(running.join(15_000L), "halt must stop every thread of the fund");
        }
        assertFalse(this.threads.anyAlive(), "no thread of the fund may survive halt");
        KairosGate.disarm();
    }

    @Test
    void aTickComesBackToTheRunnerThatTickedIt() throws InterruptedException {
        final KairosScheduler fund = this.fund(new KairosKernel(1, 0, 4, 3, 0), false, 2);
        final long now = System.nanoTime();
        final CountDownLatch done = new CountDownLatch(4);
        final FakeTick left = this.tick("left", now, done).runFor(millis(200L))
            .thenRescheduleAt(now - millis(10L), 1);
        final FakeTick right = this.tick("right", now + millis(1L), done).runFor(millis(200L))
            .thenRescheduleAt(now - millis(10L), 1);
        fund.schedule(left);
        fund.schedule(right);
        fund.start();

        assertTrue(done.await(10L, TimeUnit.SECONDS), "both ticks must run twice");
        assertEquals(2, left.runs());
        assertEquals(2, right.runs());
        assertEquals(1, Set.copyOf(left.threadNames()).size(), "a kept tick must come back to its own runner");
        assertEquals(1, Set.copyOf(right.threadNames()).size(), "and so must the other one");
        assertTrue(fund.snapshot().affinityHits() >= 2L, "the fund must record the hits it served");
        assertEquals(0L, fund.snapshot().stolenTicks(), "nothing waited long enough to be stolen");
    }

    @Test
    void aLateTickIsStolenByTheFreePermit() throws InterruptedException {
        final KairosScheduler fund = this.fund(new KairosKernel(1, 0, 4, 3, 0), false, 2);
        final Scenario scenario = this.scenario(fund);

        assertTrue(this.awaitRuns(scenario.kept, 2, 10_000L), "the late tick must run a second time");
        final List<String> threadsOfKept = scenario.kept.threadNames();
        assertEquals(2, threadsOfKept.size(), "exactly once per acquisition, never a double tick");
        assertNotEquals(threadsOfKept.get(0), threadsOfKept.get(1), "a steal runs the tick on the other runner");
        assertEquals(1L, fund.snapshot().stolenTicks(), "the steal counter must match");
    }

    @Test
    void aTickThatIsNotLateEnoughStaysWithItsOwner() throws InterruptedException {
        // ten minutes of lateness needed: the kept tick is late, but nowhere near the threshold
        final KairosScheduler fund = this.fund(new KairosKernel(1, 0, 4, 600_000, 0), false, 2);
        final Scenario scenario = this.scenario(fund);

        assertTrue(this.awaitRuns(scenario.kept, 2, 10_000L), "the tick must still run twice");
        final List<String> threadsOfKept = scenario.kept.threadNames();
        assertEquals(2, threadsOfKept.size());
        assertEquals(threadsOfKept.get(0), threadsOfKept.get(1), "the owner keeps a tick that is not late enough");
        assertEquals(0L, fund.snapshot().stolenTicks());
    }

    @Test
    void everyTickStillRunsOnceWhileTheFundShrinks() throws InterruptedException {
        // one thread floor, two threads of headroom, affinity on: a retired keeper must not lose a tick
        final KairosScheduler fund = this.fund(new KairosKernel(1, 0, 2, 3, 0), true, 1);
        final long now = System.nanoTime();
        final int ticks = 6;
        final int bodiesEach = 3;
        final CountDownLatch done = new CountDownLatch(ticks * bodiesEach);
        final List<FakeTick> scheduled = new ArrayList<>(ticks);
        for (int index = 0; index < ticks; ++index) {
            final FakeTick tick = this.tick("t" + index, now + millis(index), done)
                .runFor(millis(30L))
                .thenRescheduleAt(now - millis(5L + index), bodiesEach - 1);
            scheduled.add(tick);
            fund.schedule(tick);
        }
        fund.start();

        assertTrue(done.await(20L, TimeUnit.SECONDS), "every tick must run every time it asked to");
        for (final FakeTick tick : scheduled) {
            assertEquals(bodiesEach, tick.runs(), tick + " must run exactly " + bodiesEach + " time(s)");
        }
        assertTrue(fund.snapshot().affinityHits() > 0L, "affinity must actually have been used");
    }

    /**
     * The timeline shared by the steal pair.
     *
     * @param kept tick that is kept by its owner and is late by the time a permit frees up
     */
    private record Scenario(FakeTick kept) {
    }

    /**
     * Builds the timeline and returns the tick under test.
     *
     * <p>{@code kept} runs for 200 ms and immediately asks to be scheduled again while already late.
     * {@code blocker} holds the second permit for 600 ms. Once {@code kept} is executing, an earlier
     * tick is scheduled; it waits in the one queue because both permits are busy, and the owner prefers
     * it over its own kept tick. When {@code blocker} ends, the kept tick is overdue and a permit is
     * free, which is exactly the moment the affinity policy decides who runs it.
     *
     * @param fund scheduler to schedule into and start
     * @return the tick under test
     * @throws InterruptedException if the test thread is interrupted while waiting for the start
     */
    private Scenario scenario(final KairosScheduler fund) throws InterruptedException {
        final long t0 = System.nanoTime();
        final FakeTick kept = this.tick("kept", t0, new CountDownLatch(0)).runFor(millis(200L))
            .thenRescheduleAt(t0 - millis(50L), 1);
        final FakeTick blocker = this.tick("blocker", t0 + millis(5L), new CountDownLatch(0))
            .runFor(millis(600L));
        fund.schedule(kept);
        fund.schedule(blocker);
        fund.start();
        // wait until both permits are inside their bodies, so the earlier tick neither gets a permit nor
        // evicts the parked blocker (the scheduler may replace a tick that is only waiting for its start)
        assertTrue(this.awaitEntered(kept, 5_000L), "the kept tick must start");
        assertTrue(this.awaitEntered(blocker, 5_000L), "the blocker must start");
        final FakeTick earlier = this.tick("earlier", t0 - millis(150L), new CountDownLatch(0))
            .runFor(millis(550L));
        fund.schedule(earlier);
        return new Scenario(kept);
    }

    private KairosScheduler fund(final KairosKernel kernel, final boolean elastic, final int min) {
        KairosGate.arm(true, new KairosTuning(elastic, 2, 100, 50, 20L));
        KairosGate.kernel(kernel);
        return this.scheduler = new KairosScheduler(min, this.threads, tick -> ((FakeTick) tick).deadline());
    }

    private FakeTick tick(final String label, final long deadline, final CountDownLatch done) {
        return new FakeTick(label, deadline, this.order, done);
    }

    private boolean awaitRuns(final FakeTick tick, final int expected, final long millis) throws InterruptedException {
        final long deadline = System.nanoTime() + millis(millis);
        while (tick.runs() < expected) {
            if (System.nanoTime() - deadline >= 0L) {
                return false;
            }
            Thread.sleep(2L);
        }
        return true;
    }

    private boolean awaitEntered(final FakeTick tick, final long millis) throws InterruptedException {
        return tick.entered().await(millis, TimeUnit.MILLISECONDS);
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
