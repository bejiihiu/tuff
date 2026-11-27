package kz.bejiihiu.tuff.threads.soak;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.threads.kairos.KairosGate;
import kz.bejiihiu.tuff.threads.kairos.KairosSnapshot;
import kz.bejiihiu.tuff.threads.kairos.KairosSupervisorSource;
import kz.bejiihiu.tuff.threads.kairos.KairosTuning;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lifecycle faults of the elastic fund: a supervisor that dies, an allocator that refuses or stalls,
 * a halt racing a spawn or a retire, and a repeated halt.
 *
 * <p>Every test drives the real scheduler and asserts the plan's shutdown invariants with observations:
 * no thread survives a successful join, no tick starts after {@code halt()} returned, no tick body runs
 * twice, and the fund keeps serving work after its control thread is gone.
 */
@Tag("soak")
@Timeout(150)
class KairosLifecycleSoakTest {

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
    void aDeadSupervisorLeavesTheFundAndTheTicksAlive() throws InterruptedException {
        final KairosSupervisorSource dying = (name, body, handler) -> {
            final Thread thread = new Thread(() -> {
                // a supervisor that never runs a pass and leaves: the fund keeps its current size
            }, name);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(handler);
            return thread;
        };
        this.rig = new KairosRig(
            2, new KairosTuning(true, 4, 1_000, 50, 20L), SoakFixtures.FaultyThreadFactory.normal(), dying
        );
        final CountDownLatch done = new CountDownLatch(4);
        final long deadline = System.nanoTime();
        for (int index = 0; index < 4; ++index) {
            this.rig.tick("initial-" + index, deadline, 0, millis(20L), 0L, done, null);
        }
        this.rig.start();

        assertTrue(done.await(15L, TimeUnit.SECONDS), "the floor must serve every tick with a dead supervisor");
        SoakFixtures.await(
            "supervisor state DEAD",
            5_000L,
            () -> this.rig.snapshot().supervisor() == KairosSnapshot.SupervisorState.DEAD
        );
        assertEquals(2, this.rig.scheduler().liveThreads(), "the fund keeps the size it had when control died");
        assertEquals(0L, this.rig.scheduler().spawnCount(), "a dead supervisor cannot grow the fund");
        assertEquals(1, this.rig.ticks().getFirst().runs(), "each tick runs exactly once");

        final CountDownLatch afterDeath = new CountDownLatch(6);
        this.rig.tick("after-death", System.nanoTime(), 5, millis(5L), millis(1L), afterDeath, null);
        assertTrue(afterDeath.await(15L, TimeUnit.SECONDS), "the fund must keep making progress after control died");
        assertEquals(6, this.rig.ticks().getLast().runs(), "the late tick rescheduled five times");
    }

    @Test
    void aSpawnThatThrowsOutOfMemoryKeepsTicksRunning() throws InterruptedException {
        final SoakFixtures.FaultyThreadFactory factory = SoakFixtures.FaultyThreadFactory.failingAfter(1);
        this.rig = new KairosRig(1, new KairosTuning(true, 4, 5_000, 50, 5L), factory, null);
        final CountDownLatch done = new CountDownLatch(3);
        final long deadline = System.nanoTime();
        for (int index = 0; index < 3; ++index) {
            this.rig.tick("oom-" + index, deadline, 0, millis(30L), 0L, done, null);
        }
        this.rig.start();

        assertTrue(done.await(15L, TimeUnit.SECONDS), "an OOM from the factory must not stop the queued ticks");
        assertEquals(1, this.rig.scheduler().liveThreads(), "a failed spawn leaves the fund at its previous size");
        assertEquals(0L, this.rig.scheduler().spawnCount(), "no runner may be counted as spawned");
        this.rig.awaitQuiescent(10_000L);
        for (final SoakFixtures.SoakTick tick : this.rig.ticks()) {
            assertEquals(1, tick.runs(), "each tick must run exactly once: " + tick);
        }
    }

    @Test
    void haltDuringASpawnLeavesNoLiveThread() throws InterruptedException {
        final SoakFixtures.FaultyThreadFactory factory = SoakFixtures.FaultyThreadFactory.blockingAfter(1);
        this.rig = new KairosRig(1, new KairosTuning(true, 4, 5_000, 100, 5L), factory, null);
        final CountDownLatch done = new CountDownLatch(3);
        final long deadline = System.nanoTime();
        final SoakFixtures.SoakTick pinned =
            this.rig.tick("pinned", deadline, 0, millis(300L), 0L, done, null);
        for (int index = 0; index < 2; ++index) {
            this.rig.tick("queued-" + index, deadline, 0, 0L, 0L, done, null);
        }
        this.rig.start();
        assertTrue(factory.awaitEntered(10_000L), "the spawn must be in flight before the halt");

        final long before = System.nanoTime();
        this.rig.halt();
        final long haltMillis = (System.nanoTime() - before) / 1_000_000L;
        assertTrue(haltMillis < 2_000L, "halt must not wait for a spawn, took " + haltMillis + " ms");

        factory.release();
        this.rig.haltAndJoin(15_000L);

        assertTrue(pinned.runs() <= 1, "the pinned tick ran at most once");
        for (final SoakFixtures.SoakTick tick : this.rig.ticks()) {
            assertEquals(0, tick.startsAfterHalt(), "no tick may start after halt returned: " + tick);
            assertTrue(tick.runs() <= 1, "no tick may run twice: " + tick + " runs " + tick.runs());
        }
        assertFalse(factory.anyAlive(), "the spawned thread must be dead before join returns");
    }

    @Test
    void haltAfterARetireLeavesNoLiveThread() throws InterruptedException {
        this.rig = new KairosRig(
            1, new KairosTuning(true, 3, 1, 100, 1L), SoakFixtures.FaultyThreadFactory.normal(), null
        );
        this.rig.start();
        final CountDownLatch done = new CountDownLatch(3);
        final long deadline = System.nanoTime();
        for (int index = 0; index < 3; ++index) {
            this.rig.tick("grow-" + index, deadline, 0, millis(80L), 0L, done, null);
        }
        assertTrue(done.await(15L, TimeUnit.SECONDS), "the burst must be served");
        SoakFixtures.await("at least one retire", 20_000L, () -> this.rig.scheduler().retireCount() >= 1L);

        this.rig.halt();
        this.rig.haltAndJoin(15_000L);
        assertTrue(this.rig.scheduler().retireCount() >= 1L, "the halt must happen after an observed retire");
        for (final SoakFixtures.SoakTick tick : this.rig.ticks()) {
            assertEquals(0, tick.startsAfterHalt(), "no tick may start after halt returned");
        }
    }

    @Test
    void repeatedHaltIsIdempotentAndAlwaysConverges() throws InterruptedException {
        this.rig = new KairosRig(2, KairosTuning.fixed(), SoakFixtures.FaultyThreadFactory.normal(), null);
        this.rig.start();
        final CountDownLatch started = new CountDownLatch(1);
        this.rig.tick("looping", System.nanoTime(), 50, 0L, millis(1L), started, null);
        assertTrue(started.await(10L, TimeUnit.SECONDS), "the looping tick must start before the halt storm");

        final AtomicInteger halts = new AtomicInteger();
        final List<Thread> callers = new ArrayList<>();
        for (int index = 0; index < 4; ++index) {
            final int id = index;
            callers.add(Thread.ofPlatform().name("halt-caller-" + id).start(() -> {
                for (int round = 0; round < 50; ++round) {
                    this.rig.scheduler().halt();
                    halts.incrementAndGet();
                }
            }));
        }
        for (final Thread caller : callers) {
            caller.join(10_000L);
        }
        this.rig.haltAndJoin(15_000L);
        assertEquals(200, halts.get(), "every concurrent halt returned");
        for (final SoakFixtures.SoakTick tick : this.rig.ticks()) {
            assertEquals(0, tick.startsAfterHalt(), "a halted loop never starts another execution");
        }
    }

    @Test
    void noThreadIsStartedAfterHaltReturns() throws InterruptedException {
        this.rig = new KairosRig(2, KairosTuning.fixed(), SoakFixtures.FaultyThreadFactory.normal(), null);
        this.rig.start();
        this.rig.haltAndJoin(15_000L);
        final int created = this.rig.threads().created().size();
        final int starts = this.rig.threads().starts();
        assertEquals(2, created, "a fixed fund of two allocates exactly two threads");

        final CountDownLatch never = new CountDownLatch(1);
        for (int index = 0; index < 4; ++index) {
            this.rig.tick("post-halt-" + index, System.nanoTime(), 0, 0L, 0L, never, null);
        }
        Thread.sleep(300L);

        assertEquals(created, this.rig.threads().created().size(), "a halted fund must not allocate a thread");
        assertEquals(starts, this.rig.threads().starts(), "a halted fund must not start a thread");
        for (final SoakFixtures.SoakTick tick : this.rig.ticks()) {
            assertEquals(0, tick.runs(), "nothing scheduled after halt may execute");
        }
    }

    @Test
    void reArmingTheGateStartsAFreshFund() throws InterruptedException {
        this.rig = new KairosRig(1, KairosTuning.fixed(), SoakFixtures.FaultyThreadFactory.normal(), null);
        this.rig.start();
        this.rig.haltAndJoin(15_000L);
        final List<Thread> firstGeneration = List.copyOf(this.rig.threads().created());
        assertFalse(firstGeneration.stream().anyMatch(Thread::isAlive), "the first fund must be dead");
        KairosGate.disarm();

        final KairosRig second = new KairosRig(
            2, KairosTuning.fixed(), SoakFixtures.FaultyThreadFactory.normal(), null
        );
        try {
            second.start();
            final CountDownLatch done = new CountDownLatch(2);
            for (int index = 0; index < 2; ++index) {
                second.tick("fresh-" + index, System.nanoTime(), 0, 0L, 0L, done, null);
            }
            assertTrue(done.await(10L, TimeUnit.SECONDS), "a re-armed gate must start a working fund");
            assertEquals(2, second.scheduler().liveThreads());
            assertTrue(KairosGate.armed(), "the gate stays armed while the second fund runs");
        } finally {
            second.close();
        }
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
