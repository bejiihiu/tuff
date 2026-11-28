package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the fund does when a runner thread dies without a retire.
 *
 * <p>A runner can be killed by a throwable that escapes its branch: an idle-safe job that throws an
 * {@link Error}, a broken probe, an out of memory in somebody's body. The scheduler must then stop
 * treating that slot as a permit holder - a tick handed to a dead thread never runs - and a spawn that
 * reuses the slot must take over the backlog the dead runner kept, because nothing else can see it.
 */
@Timeout(120)
class KairosSlotRecoveryTest {

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
    void aDeadSlotDoesNotKeepItsPermitBit() throws InterruptedException {
        final FakeIdleSource source = new FakeIdleSource();
        KairosGate.arm(
            true,
            new KairosTuning(false, -1, 5_000, 10, 20L),
            new KairosServices(null, KairosSupervisorSource.plain(), source)
        );
        KairosGate.kernel(KairosKernel.defaults());
        KairosGate.idleWork(new KairosIdleWork(true, false, true, 5, 5, 100));
        final KairosScheduler fund = this.scheduler =
            new KairosScheduler(2, this.threads, tick -> ((FakeTick) tick).deadline());

        // the first created thread is slot 0; killing exactly that runner leaves the lowest permit bit
        // assigned to a thread that will never wake up for its tick
        final Thread slotZero = this.threads.created().getFirst();
        source.failingOnceOnThreadNamed(slotZero.getName(), new AssertionError("the runner must die here"));
        fund.start();
        this.awaitLive(fund, 1, 5_000L);
        this.awaitDead(slotZero, 5_000L);

        final FakeTick next = new FakeTick("next", System.nanoTime(), this.order, new CountDownLatch(1));
        fund.schedule(next);

        this.awaitRuns(next, 1);
        assertEquals(1, fund.liveThreads(), "the surviving runner keeps the fund alive");
        assertEquals(1, fund.getAliveThreads().length);
    }

    @Test
    void aSpawnThatReusesADeadSlotTakesOverItsPendingTick() throws InterruptedException {
        KairosGate.arm(
            true,
            new KairosTuning(true, 2, 60_000, 100, 20L),
            KairosServices.none()
        );
        KairosGate.kernel(new KairosKernel(1, 50, 4, 3, 0));
        // the mid-tick path is what runs on the runner while it waits for a deadline; a tick whose
        // task probe throws therefore kills the runner with the permit still attached to it
        KairosGate.idleWork(new KairosIdleWork(true, true, false, 5, 5, 100));
        final QuietFactory factory = new QuietFactory();
        final KairosScheduler fund = this.scheduler =
            new KairosScheduler(1, factory, tick -> ((FakeTick) tick).deadline());

        final long now = System.nanoTime();
        final FakeTick kept = new ExplodingTick("kept", now, this.order)
            .thenRescheduleAt(now + millis(400L), 1);
        fund.schedule(kept);
        fund.start();

        // the first body reschedules; the runner holds the next generation's permit while it waits
        this.awaitRuns(kept, 1);
        this.awaitLive(fund, 0, 5_000L);

        // the supervisor reuses the dead slot; the reused runner must inherit the pending tick
        this.awaitRuns(kept, 2);
        this.awaitLive(fund, 1, 5_000L);
        assertEquals(2, kept.runs(), "the inherited tick runs exactly once");
    }

    private void awaitLive(final KairosScheduler fund, final int expected, final long waitMillis) {
        final long deadline = System.nanoTime() + millis(waitMillis);
        while (fund.liveThreads() != expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("expected " + expected + " live threads, got " + fund.liveThreads());
            }
            Thread.onSpinWait();
        }
    }

    private static void awaitDead(final Thread thread, final long waitMillis) {
        final long deadline = System.nanoTime() + millis(waitMillis);
        while (thread.getState() != Thread.State.TERMINATED) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("the runner thread never terminated");
            }
            Thread.onSpinWait();
        }
    }

    private void awaitRuns(final FakeTick tick, final int expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (tick.runs() < expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("expected " + expected + " run(s), saw " + tick.runs());
            }
            Thread.onSpinWait();
        }
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }

    /**
     * A tick whose idle task probe kills the runner that is waiting for it.
     */
    private static final class ExplodingTick extends FakeTick {

        private ExplodingTick(final String label, final long deadline, final List<String> order) {
            super(label, deadline, order, new CountDownLatch(1));
        }

        @Override
        public boolean hasTasks() {
            throw new AssertionError("the runner must die here");
        }
    }

    /**
     * Builds daemon threads with a quiet failure handler, so a deliberate death does not spam the log.
     */
    private static final class QuietFactory implements ThreadFactory {

        private final AtomicInteger ids = new AtomicInteger();

        @Override
        public Thread newThread(final Runnable run) {
            final Thread thread = new Thread(run, "kairos-dead-slot-" + this.ids.getAndIncrement());
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((ignored, failure) -> {
                // the test decides what the death means; the default handler would print a stack trace
            });
            return thread;
        }
    }
}
