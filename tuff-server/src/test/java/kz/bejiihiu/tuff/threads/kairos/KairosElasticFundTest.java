package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behaviour of the elastic fund and its supervisor, without a server.
 *
 * <p>Each test is one promise of the package: growth only under demand, shrink only from a parked
 * runner, a live thread is never recreated, the floor and the ceiling hold, the spawn pace and the
 * retire cooldown are real, and a broken supervisor pass cannot take the fund down with it.
 */
@Timeout(120)
class KairosElasticFundTest {

    private static final String SUPERVISOR_NAME = "Tuff Kairos Supervisor";

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
            assertTrue(this.scheduler.join(10_000L), "halt must stop every thread of an elastic fund");
        }
        KairosGate.disarm();
    }

    @Test
    void theFundGrowsWhileTicksWaitForAPermit() throws InterruptedException {
        final KairosScheduler elastic = this.elastic(1, 4, 5_000, 50, 20L);
        final CountDownLatch done = new CountDownLatch(4);
        final long now = System.nanoTime();
        for (int index = 0; index < 4; ++index) {
            elastic.schedule(this.tick("t" + index, now, done).runFor(millis(400L)));
        }

        elastic.start();
        this.awaitLive(4, 5_000L);

        assertTrue(done.await(5L, TimeUnit.SECONDS), "all four ticks must run");
        assertEquals(3, elastic.spawnCount(), "three runners were added above the floor");
        assertEquals(4, elastic.liveThreads());
    }

    @Test
    void theFundShrinksBackToItsFloorWhenTheLoadIsGone() throws InterruptedException {
        final KairosScheduler elastic = this.elastic(1, 4, 100, 50, 20L);
        final CountDownLatch done = new CountDownLatch(4);
        final long now = System.nanoTime();
        for (int index = 0; index < 4; ++index) {
            elastic.schedule(this.tick("t" + index, now, done).runFor(millis(300L)));
        }
        elastic.start();
        this.awaitLive(4, 5_000L);
        assertTrue(done.await(5L, TimeUnit.SECONDS));

        this.awaitLive(1, 20_000L);
        this.awaitRetires(elastic, 3, 20_000L);

        assertEquals(3, elastic.retireCount(), "exactly the runners above the floor are retired");
        assertEquals(4, this.threads.created().size(), "shrink must not create a replacement thread");
    }

    @Test
    void growthNeverRecreatesALiveThread() throws InterruptedException {
        final KairosScheduler elastic = this.elastic(1, 3, 5_000, 50, 20L);
        final CountDownLatch first = new CountDownLatch(3);
        final long now = System.nanoTime();
        for (int index = 0; index < 3; ++index) {
            elastic.schedule(this.tick("a" + index, now, first).runFor(millis(250L)));
        }
        elastic.start();
        this.awaitLive(3, 5_000L);
        assertTrue(first.await(5L, TimeUnit.SECONDS));
        final Set<Thread> grown = Set.of(elastic.getAliveThreads());
        assertEquals(3, grown.size());

        final CountDownLatch second = new CountDownLatch(3);
        final long later = System.nanoTime();
        for (int index = 0; index < 3; ++index) {
            elastic.schedule(this.tick("b" + index, later, second).runFor(millis(200L)));
        }
        assertTrue(second.await(5L, TimeUnit.SECONDS));

        assertEquals(grown, Set.of(elastic.getAliveThreads()), "the same live threads must still be the fund");
        assertEquals(
            1 + elastic.spawnCount(),
            this.threads.created().size(),
            "a recreate would ask the factory for another thread"
        );
        assertEquals(2, elastic.spawnCount(), "one burst, one extra runner, no churn in between");
    }

    @Test
    void theCeilingAndTheFloorHoldUnderAnyLoad() throws InterruptedException {
        final KairosScheduler elastic = this.elastic(2, 3, 50, 50, 20L);
        final CountDownLatch done = new CountDownLatch(12);
        final long now = System.nanoTime();
        for (int index = 0; index < 12; ++index) {
            elastic.schedule(this.tick("t" + index, now, done).runFor(millis(150L)));
        }

        elastic.start();
        final long watchUntil = System.nanoTime() + millis(2_000L);
        while (System.nanoTime() - watchUntil < 0L) {
            final int live = elastic.liveThreads();
            assertTrue(live <= 3, "the fund must never pass its ceiling, saw " + live);
            assertTrue(live >= 2, "the floor must never be retired, saw " + live);
            Thread.sleep(5L);
        }
        assertEquals(1, elastic.spawnCount(), "only one runner fits above a floor of two");
        assertTrue(done.await(10L, TimeUnit.SECONDS));
    }

    @Test
    void theSpawnRatePacesGrowth() throws InterruptedException {
        KairosGate.arm(true, new KairosTuning(true, 8, 30_000, 1, 10L));
        final KairosScheduler elastic = this.scheduler =
            new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline());
        final CountDownLatch done = new CountDownLatch(6);
        final long now = System.nanoTime();
        for (int index = 0; index < 6; ++index) {
            elastic.schedule(this.tick("t" + index, now, done).runFor(millis(700L)));
        }
        elastic.start();

        Thread.sleep(150L);
        assertEquals(1, elastic.spawnCount(), "one spawn per second, the burst cannot jump over the pace");

        Thread.sleep(1_100L);
        assertEquals(2, elastic.spawnCount(), "the second second pays for exactly one more runner");
    }

    @Test
    void aRetireCannotRepeatBeforeTheCooldown() throws InterruptedException {
        final KairosScheduler elastic = this.elastic(1, 4, 50, 50, 20L);
        final CountDownLatch done = new CountDownLatch(4);
        final long now = System.nanoTime();
        for (int index = 0; index < 4; ++index) {
            elastic.schedule(this.tick("t" + index, now, done).runFor(millis(200L)));
        }
        elastic.start();
        this.awaitLive(4, 5_000L);
        assertTrue(done.await(5L, TimeUnit.SECONDS));

        final long firstRetire = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (elastic.retireCount() == 0L) {
            if (System.nanoTime() - firstRetire >= 0L) {
                throw new AssertionError("the idle surplus was never retired");
            }
            Thread.sleep(5L);
        }
        assertEquals(1L, elastic.retireCount());

        Thread.sleep(600L);
        assertEquals(1L, elastic.retireCount(), "two retires within a second would be a sawtooth");

        this.awaitLive(1, 20_000L);
        this.awaitRetires(elastic, 3, 20_000L);
        assertEquals(3L, elastic.retireCount());
    }

    @Test
    void aBrokenFactoryLeavesTheFundAndTheTicksAlive() throws InterruptedException {
        KairosGate.arm(true, new KairosTuning(true, 4, 5_000, 50, 20L));
        final FailingThreadFactory factory = new FailingThreadFactory(1);
        final KairosScheduler broken = this.scheduler =
            new KairosScheduler(1, factory, tick -> ((FakeTick) tick).deadline());
        final CountDownLatch done = new CountDownLatch(3);
        final long now = System.nanoTime();
        for (int index = 0; index < 3; ++index) {
            broken.schedule(this.tick("t" + index, now, done).runFor(millis(150L)));
        }

        broken.start();

        assertTrue(done.await(5L, TimeUnit.SECONDS), "a spawn that throws must not stop the ticks");
        assertEquals(1, broken.liveThreads(), "the fund keeps the size it had");
        assertEquals(0L, broken.spawnCount());
    }

    @Test
    void aFixedFundNeverGrowsAndNeverStartsASupervisor() throws InterruptedException {
        KairosGate.arm(true, new KairosTuning(false, 4, 100, 50, 10L));
        final KairosScheduler fixed = this.scheduler =
            new KairosScheduler(2, this.threads, tick -> ((FakeTick) tick).deadline());
        fixed.start();
        this.awaitLive(2, 2_000L);

        final CountDownLatch done = new CountDownLatch(4);
        final long now = System.nanoTime();
        for (int index = 0; index < 4; ++index) {
            fixed.schedule(this.tick("t" + index, now, done).runFor(millis(100L)));
        }
        assertTrue(done.await(5L, TimeUnit.SECONDS));

        assertEquals(2, fixed.maxThreads(), "elasticity off means min == max, whatever the ceiling says");
        assertFalse(fixed.elastic());
        assertEquals(0L, fixed.spawnCount());
        fixed.setThreads(4);
        assertEquals(2, fixed.getAliveThreads().length, "a fixed fund ignores a live thread count change");
        assertFalse(supervisorAlive(), "no control thread is created when the fund is fixed");
    }

    @Test
    void haltStopsTheSupervisorBeforeJoinReturns() throws InterruptedException {
        final KairosScheduler elastic = this.elastic(1, 4, 5_000, 50, 20L);
        final CountDownLatch done = new CountDownLatch(4);
        final long now = System.nanoTime();
        for (int index = 0; index < 4; ++index) {
            elastic.schedule(this.tick("t" + index, now, done).runFor(millis(200L)));
        }
        elastic.start();
        this.awaitLive(4, 5_000L);

        elastic.halt();

        assertTrue(elastic.join(10_000L));
        assertEquals(0, elastic.getAliveThreads().length);
        assertFalse(this.threads.anyAlive(), "no runner may survive halt");
        assertFalse(supervisorAlive(), "the supervisor must be joined, not left parked");
    }

    @Test
    void aLiveFloorChangeResizesWithoutRecreatingAThread() throws InterruptedException {
        final KairosScheduler elastic = this.elastic(1, 4, 5_000, 50, 20L);
        elastic.start();
        this.awaitLive(1, 2_000L);
        final Thread original = elastic.getAliveThreads()[0];

        elastic.setThreads(3);
        this.awaitLive(3, 5_000L);
        assertTrue(Set.of(elastic.getAliveThreads()).contains(original), "the first thread must survive a grow");
        assertEquals(3, elastic.minThreads());
        assertEquals(3, elastic.liveThreads());
        assertEquals(2L, elastic.spawnCount());

        elastic.setThreads(1);
        assertEquals(1, elastic.minThreads());
        this.awaitLive(1, 20_000L);
        assertEquals(2L, elastic.retireCount(), "the surplus above the new floor is retired, one per second");
        assertTrue(Set.of(elastic.getAliveThreads()).contains(original), "the floor thread is never recreated");
    }

    private KairosScheduler elastic(
        final int min,
        final int max,
        final int keepAliveMillis,
        final int spawnRate,
        final long periodMillis
    ) {
        KairosGate.arm(true, new KairosTuning(true, max, keepAliveMillis, spawnRate, periodMillis));
        return this.scheduler = new KairosScheduler(min, this.threads, tick -> ((FakeTick) tick).deadline());
    }

    private FakeTick tick(final String label, final long deadline, final CountDownLatch done) {
        return new FakeTick(label, deadline, this.order, done);
    }

    private void awaitLive(final int expected, final long waitMillis) throws InterruptedException {
        final KairosScheduler elastic = this.scheduler;
        final long deadline = System.nanoTime() + millis(waitMillis);
        while (elastic.liveThreads() != expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError(
                    "expected " + expected + " live threads, got " + elastic.liveThreads()
                );
            }
            Thread.sleep(2L);
        }
    }

    /**
     * Waits for the retire counter, which trails the live count by the moment the decision is recorded.
     *
     * <p>The counter is incremented before the runner is asked to leave, so a live count that already
     * reached the floor cannot legitimately be ahead of it; the wait only covers the supervisor's one
     * retire per second, not a race in the assertion.
     *
     * @param elastic    scheduler under test
     * @param expected   retire count to wait for
     * @param waitMillis budget of the wait
     * @throws InterruptedException if the test is interrupted
     * @throws AssertionError       if the retire count never arrives
     */
    private void awaitRetires(final KairosScheduler elastic, final long expected, final long waitMillis)
        throws InterruptedException {
        final long deadline = System.nanoTime() + millis(waitMillis);
        while (elastic.retireCount() != expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError(
                    "expected " + expected + " retires, got " + elastic.retireCount()
                        + " (live " + elastic.liveThreads() + ')'
                );
            }
            Thread.sleep(2L);
        }
    }

    private static boolean supervisorAlive() {
        for (final Thread thread : Thread.getAllStackTraces().keySet()) {
            if (SUPERVISOR_NAME.equals(thread.getName()) && thread.isAlive()) {
                return true;
            }
        }
        return false;
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }

    /**
     * A factory that works for a few threads and then throws, standing in for an OOM on spawn.
     */
    private static final class FailingThreadFactory implements ThreadFactory {

        private final AtomicInteger allowed;
        private final AtomicInteger ids = new AtomicInteger();

        private FailingThreadFactory(final int allowed) {
            this.allowed = new AtomicInteger(allowed);
        }

        @Override
        public Thread newThread(final Runnable run) {
            if (this.allowed.getAndDecrement() <= 0) {
                throw new OutOfMemoryError("no more threads in this test");
            }
            final Thread thread = new Thread(run, "kairos-failing-" + this.ids.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
