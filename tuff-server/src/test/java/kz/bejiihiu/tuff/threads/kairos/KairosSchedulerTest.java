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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behaviour of the fixed fund scheduler, without a server.
 *
 * <p>Every test is one of the guarantees the prototype promises to keep from EDF: deadline order,
 * the id tie break, a slow tick not starving the next deadline, halt/join, an honest no op on a
 * thread count change, and no thread left behind. The opt in is armed explicitly because a scheduler
 * that forgets it must refuse to start.
 */
@Timeout(120)
class KairosSchedulerTest {

    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    private RecordingThreadFactory threads;
    private @Nullable KairosScheduler scheduler;

    @BeforeEach
    void armTheGate() {
        KairosGate.arm(true);
        this.threads = new RecordingThreadFactory();
    }

    @AfterEach
    void stopEverything() {
        if (this.scheduler != null) {
            this.scheduler.halt();
            this.scheduler.join(5_000L);
        }
        KairosGate.disarm();
    }

    @Test
    void ticksRunInDeadlineOrder() throws InterruptedException {
        this.scheduler = this.scheduler(1);
        final long now = System.nanoTime();
        final CountDownLatch done = new CountDownLatch(3);
        final FakeTick first = this.tick("a", now + millis(30L), done);
        final FakeTick second = this.tick("b", now + millis(10L), done);
        final FakeTick third = this.tick("c", now + millis(20L), done);

        // scheduled out of order on purpose
        this.scheduler.schedule(first);
        this.scheduler.schedule(third);
        this.scheduler.schedule(second);
        this.scheduler.start();

        assertTrue(done.await(5L, TimeUnit.SECONDS), "every tick should run");
        assertEquals(List.of("b", "c", "a"), this.order);
        assertEquals(1, first.runs());
        assertEquals(1, second.runs());
        assertEquals(1, third.runs());
    }

    @Test
    void equalDeadlinesBreakTheTieByTickId() throws InterruptedException {
        this.scheduler = this.scheduler(1);
        final long deadline = System.nanoTime() + millis(10L);
        final CountDownLatch done = new CountDownLatch(3);
        final FakeTick first = this.tick("a", deadline, done);
        final FakeTick second = this.tick("b", deadline, done);
        final FakeTick third = this.tick("c", deadline, done);

        this.scheduler.schedule(first);
        this.scheduler.schedule(second);
        this.scheduler.schedule(third);
        this.scheduler.start();

        assertTrue(done.await(5L, TimeUnit.SECONDS), "every tick should run");
        assertEquals(List.of("a", "b", "c"), this.order, "the tie break is the tick id, ascending");
    }

    @Test
    void aSlowTickDoesNotStarveTheNextDeadline() throws InterruptedException {
        this.scheduler = this.scheduler(1);
        final long now = System.nanoTime();
        final CountDownLatch done = new CountDownLatch(2);
        final FakeTick slow = this.tick("slow", now - millis(5L), done).runFor(millis(150L));
        final FakeTick next = this.tick("next", now + millis(1L), done);

        this.scheduler.schedule(slow);
        this.scheduler.schedule(next);
        this.scheduler.start();

        assertTrue(done.await(5L, TimeUnit.SECONDS), "the second tick must run after the slow one, not be skipped");
        assertEquals(List.of("slow", "next"), this.order);
    }

    @Test
    void haltAndJoinStopEveryThread() throws InterruptedException {
        this.scheduler = this.scheduler(2);
        this.scheduler.start();
        this.awaitAlive(2);

        this.scheduler.halt();

        assertTrue(this.scheduler.join(5_000L), "join must return before the timeout");
        assertEquals(0, this.scheduler.getAliveThreads().length);
        assertEquals(0, this.scheduler.getCoreThreads().length);
        assertFalse(this.threads.anyAlive(), "no thread of the fund may survive halt");
    }

    @Test
    void joinHonoursItsTimeoutWhileATickIsRunning() throws InterruptedException {
        this.scheduler = this.scheduler(1);
        final FakeTick longTick = this.tick("long", System.nanoTime(), new CountDownLatch(0)).runFor(millis(800L));
        this.scheduler.schedule(longTick);
        this.scheduler.start();
        assertTrue(longTick.entered().await(2L, TimeUnit.SECONDS), "the tick should start");

        this.scheduler.halt();

        assertFalse(this.scheduler.join(50L), "a running tick must be allowed to finish, so the short join fails");
        assertTrue(this.scheduler.join(5_000L), "the second join must see the thread exit");
        assertFalse(this.threads.anyAlive());
    }

    @Test
    void aTickScheduledAfterThePoolWentIdleStillRuns() throws InterruptedException {
        this.scheduler = this.scheduler(1);
        this.scheduler.start();
        final CountDownLatch first = new CountDownLatch(1);
        this.scheduler.schedule(this.tick("first", System.nanoTime(), first));
        assertTrue(first.await(5L, TimeUnit.SECONDS), "the first tick should run");

        // the runner is parked now; a new arrival must find its permit again
        Thread.sleep(100L);
        final CountDownLatch second = new CountDownLatch(1);
        this.scheduler.schedule(this.tick("second", System.nanoTime(), second));

        assertTrue(second.await(5L, TimeUnit.SECONDS), "an idle runner must be handed the next tick");
        assertEquals(List.of("first", "second"), this.order);
    }

    @Test
    void setThreadsIsAnHonestNoOpWhileTheFundIsFixed() throws InterruptedException {
        this.scheduler = this.scheduler(2);
        this.scheduler.start();
        this.awaitAlive(2);

        this.scheduler.setThreads(4);
        this.scheduler.setThreads(1);

        assertEquals(2, this.scheduler.getAliveThreads().length, "a fixed fund cannot resize");
    }

    @Test
    void theSameTickCannotBeScheduledTwice() {
        this.scheduler = this.scheduler(1);
        final FakeTick tick = this.tick("a", System.nanoTime(), new CountDownLatch(0));

        this.scheduler.schedule(tick);

        assertThrows(IllegalStateException.class, () -> this.scheduler.schedule(tick));
    }

    @Test
    void cancelRemovesAQueuedTickButNotARunningOne() throws InterruptedException {
        this.scheduler = this.scheduler(1);
        final CountDownLatch done = new CountDownLatch(1);
        final FakeTick running = this.tick("running", System.nanoTime(), done).runFor(millis(400L));
        final FakeTick queued = this.tick("queued", System.nanoTime() + millis(100L), done);
        this.scheduler.schedule(running);
        this.scheduler.schedule(queued);

        assertTrue(this.scheduler.cancel(queued), "a tick that still waits can be cancelled");
        assertFalse(
            this.scheduler.cancel(this.tick("never", System.nanoTime(), new CountDownLatch(0))),
            "a tick this scheduler never saw is not cancellable"
        );

        this.scheduler.start();
        assertTrue(running.entered().await(2L, TimeUnit.SECONDS));
        assertFalse(this.scheduler.cancel(running), "a tick that is executing cannot be cancelled");
        assertTrue(done.await(5L, TimeUnit.SECONDS));

        assertEquals(0, queued.runs(), "a cancelled tick must never run");
        assertEquals(List.of("running"), this.order);
    }

    @Test
    void theFundIsNotCreatedBeforeTheOptInIsKnown() {
        KairosGate.disarm();
        this.scheduler = this.scheduler(1);

        final IllegalStateException refused = assertThrows(IllegalStateException.class, this.scheduler::start);

        assertTrue(refused.getMessage().contains("kairos-unstable"), refused.getMessage());
        assertEquals(0, this.scheduler.getAliveThreads().length);
        assertTrue(this.scheduler.join(1_000L), "a refused start leaves nothing to join");
        assertFalse(this.threads.anyAlive(), "a refused start must not start a thread");
    }

    @Test
    void joinWaitsForARetiredThreadThatIsStillAlive() throws InterruptedException {
        // the factory keeps a thread alive after its loop returns, which is the only way to hold a
        // retired runner in the state the shutdown contract is about: out of the fund, still running
        final SlowExitFactory factory = new SlowExitFactory();
        KairosGate.arm(true, new KairosTuning(true, 2, 50, 50, 20L));
        final KairosScheduler elastic = this.scheduler =
            new KairosScheduler(1, factory, tick -> ((FakeTick) tick).deadline());
        final CountDownLatch done = new CountDownLatch(2);
        final long now = System.nanoTime();
        elastic.schedule(this.tick("a", now, done).runFor(millis(120L)));
        elastic.schedule(this.tick("b", now, done).runFor(millis(120L)));
        elastic.start();
        this.awaitAlive(2);
        assertTrue(done.await(5L, TimeUnit.SECONDS));

        final long retireDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (elastic.retireCount() == 0L) {
            if (System.nanoTime() - retireDeadline >= 0L) {
                throw new AssertionError("the surplus runner was never retired");
            }
            Thread.sleep(5L);
        }
        // the retired runner is out of the fund but its thread has not exited
        assertEquals(1, elastic.getCoreThreads().length, "a retiring runner is not an executor any more");
        assertEquals(2, factory.created().size());

        elastic.halt();

        assertFalse(elastic.join(300L), "join must still wait for the retired thread");
        factory.allowExit();
        assertTrue(elastic.join(10_000L), "and return once that thread is done");
        assertFalse(factory.anyAlive(), "no thread of the fund may survive the join");
    }

    @Test
    void noThreadIsStartedAfterHalt() throws InterruptedException {
        KairosGate.arm(true, new KairosTuning(true, 4, 5_000, 50, 20L));
        final KairosScheduler elastic = this.scheduler =
            new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline());
        final CountDownLatch done = new CountDownLatch(8);
        final long now = System.nanoTime();
        for (int index = 0; index < 8; ++index) {
            elastic.schedule(this.tick("t" + index, now, done).runFor(millis(200L)));
        }
        elastic.start();

        elastic.halt();
        final int createdAtHalt = this.threads.created().size();

        assertTrue(elastic.join(10_000L));
        Thread.sleep(100L);
        assertEquals(createdAtHalt, this.threads.created().size(), "a spawn that lost the race must not start");
        assertEquals(0, elastic.getAliveThreads().length);
        assertFalse(this.threads.anyAlive());
    }

    private KairosScheduler scheduler(final int size) {
        return new KairosScheduler(size, this.threads, tick -> ((FakeTick) tick).deadline());
    }

    private FakeTick tick(final String label, final long deadline, final CountDownLatch done) {
        return new FakeTick(label, deadline, this.order, done);
    }

    private void awaitAlive(final int expected) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (this.scheduler.getAliveThreads().length != expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError(
                    "expected " + expected + " alive threads, got " + this.scheduler.getAliveThreads().length
                );
            }
            Thread.sleep(1L);
        }
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }

    /**
     * A factory whose threads outlive their runner loop until the test lets them go.
     *
     * <p>That is what a retired runner looks like during shutdown: it is no longer part of the fund,
     * and it is still a thread that the save must not race.
     */
    private static final class SlowExitFactory implements ThreadFactory {

        private final List<Thread> created = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch exit = new CountDownLatch(1);
        private final AtomicInteger ids = new AtomicInteger();

        @Override
        public Thread newThread(final Runnable run) {
            final Thread thread = new Thread(() -> {
                run.run();
                try {
                    this.exit.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }, "kairos-slow-exit-" + this.ids.getAndIncrement());
            thread.setDaemon(true);
            this.created.add(thread);
            return thread;
        }

        void allowExit() {
            this.exit.countDown();
        }

        List<Thread> created() {
            return List.copyOf(this.created);
        }

        boolean anyAlive() {
            return this.created.stream().anyMatch(Thread::isAlive);
        }
    }
}
