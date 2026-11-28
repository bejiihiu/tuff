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
 * The scheduler's own state map against the permit counters.
 *
 * <p>K1 of the rollout plan is "queued + awaiting + executing == the ticks this scheduler holds". The
 * sum could not be checked while the scheduled set was private, so the old tests compared the sum with
 * itself. {@link KairosScheduler#scheduledTicks()} exposes the map size, and every step here asserts
 * both sides - which is what catches a cancel that removes the tick from a queue but leaves its entry
 * behind, pinning the handle (and, on a server, the region and its world) forever.
 */
@Timeout(120)
class KairosStateAccountingTest {

    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    private RecordingThreadFactory threads;
    private @Nullable KairosScheduler scheduler;

    @BeforeEach
    void newFactory() {
        this.threads = new RecordingThreadFactory();
        KairosGate.arm(true, new KairosTuning(false, -1, 5_000, 10, 20L));
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
    void cancellingAQueuedTickReleasesItsEntryAndAllowsTheHandleAgain() {
        final KairosScheduler fund = this.fund();
        final long now = System.nanoTime();
        final FakeTick awaiting = this.tick("awaiting", now + millis(2_000L));
        final FakeTick queued = this.tick("queued", now + millis(3_000L));
        fund.schedule(awaiting);
        fund.schedule(queued);
        assertAccounting(fund);
        assertEquals(2, fund.scheduledTicks());

        assertTrue(fund.cancel(queued), "a queued tick is cancellable");
        assertEquals(1, fund.scheduledTicks(), "the cancelled tick must leave the scheduler's map");
        assertAccounting(fund);

        // the entry is gone, so the same handle may be scheduled again; a leaked entry would answer
        // "already scheduled" from an ownership no queue can see
        fund.schedule(queued);
        assertEquals(2, fund.scheduledTicks());
        assertAccounting(fund);

        assertTrue(fund.cancel(queued));
        assertTrue(fund.cancel(awaiting));
        assertEquals(0, fund.scheduledTicks(), "cancelling everything must empty the map");
        assertAccounting(fund);
    }

    @Test
    void cancellingAnAwaitingTickReleasesItsEntryAndAllowsTheHandleAgain() {
        final KairosScheduler fund = this.fund();
        final FakeTick tick = this.tick("awaiting", System.nanoTime() + millis(2_000L));
        fund.schedule(tick);
        assertEquals(1, fund.scheduledTicks());
        assertAccounting(fund);

        assertTrue(fund.cancel(tick), "a tick that only waits for its deadline is cancellable");
        assertEquals(0, fund.scheduledTicks(), "the permit was withdrawn, the entry must go with it");
        assertAccounting(fund);

        fund.schedule(tick);
        assertEquals(1, fund.scheduledTicks());
        assertAccounting(fund);
        assertTrue(fund.cancel(tick));
        assertEquals(0, fund.scheduledTicks());
    }

    private KairosScheduler fund() {
        return this.scheduler = new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline());
    }

    private FakeTick tick(final String label, final long deadline) {
        return new FakeTick(label, deadline, this.order, new CountDownLatch(1));
    }

    private static void assertAccounting(final KairosScheduler fund) {
        final KairosSnapshot snapshot = fund.snapshot();
        assertEquals(
            snapshot.scheduledTicks(),
            snapshot.queuedTicks() + snapshot.awaitingTicks() + snapshot.executingTicks(),
            () -> "permit accounting drifted from the scheduled set: " + snapshot
        );
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
