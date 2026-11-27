package kz.bejiihiu.tuff.threads.kairos;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.kairos.KairosSnapshot.Degraded;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Where the ceiling comes from and who builds the supervisor, with fakes standing in for the thread
 * layer.
 *
 * <p>The point of the budget tests is that Kairos has no second copy of the processor arithmetic: the
 * port answers, the scheduler uses the answer, and the core count is only the fallback. The point of the
 * supervisor tests is that the control thread is a seam too - a registered thread when the layer
 * provides one, a documented plain daemon when it does not.
 */
@Timeout(120)
class KairosBudgetTest {

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
            assertTrue(this.scheduler.join(10_000L));
        }
        KairosGate.disarm();
    }

    @Test
    void theCeilingIsWhateverTheBudgetPortSays() {
        this.scheduler = this.elastic(new KairosServices(floor -> 7, KairosSupervisorSource.plain()), 2);

        assertEquals(7, this.scheduler.maxThreads());
        assertTrue(this.scheduler.snapshot().budgetSource().contains("thread layer budget"));
    }

    @Test
    void aBudgetBelowTheFloorDegradesInsteadOfOversubscribing() {
        this.scheduler = this.elastic(new KairosServices(floor -> 1, KairosSupervisorSource.plain()), 2);

        assertEquals(2, this.scheduler.maxThreads(), "the floor wins over a budget that cannot carry it");
        final KairosSnapshot snapshot = this.scheduler.snapshot();
        assertTrue(snapshot.degradedReasons().contains(Degraded.NO_HEADROOM), snapshot.degradedReasons().toString());
    }

    @Test
    void aFailedPortFallsBackToTheCoreCount() {
        final KairosBudgetPort broken = floor -> {
            throw new IllegalStateException("controller is gone");
        };
        this.scheduler = this.elastic(new KairosServices(broken, KairosSupervisorSource.plain()), 1);

        final int cores = Runtime.getRuntime().availableProcessors();
        assertEquals(Math.max(1, (int) Math.floor(cores * 0.8d)), this.scheduler.maxThreads());
        assertTrue(this.scheduler.snapshot().budgetSource().startsWith("floor("));
    }

    @Test
    void withoutARuntimeTheCoreCountIsTheFallback() {
        KairosGate.arm(true, new KairosTuning(true, -1, 5_000, 10, 20L));
        this.scheduler = new KairosScheduler(
            1, this.threads, tick -> ((FakeTick) tick).deadline()
        );
        this.scheduler.start();

        final int cores = Runtime.getRuntime().availableProcessors();
        assertEquals(Math.max(1, (int) Math.floor(cores * 0.8d)), this.scheduler.maxThreads());
    }

    @Test
    void theSupervisorComesFromTheInjectedSourceAndIsJoined() {
        final AtomicInteger created = new AtomicInteger();
        final AtomicReference<Thread> supplied = new AtomicReference<>();
        final KairosSupervisorSource source = (name, body, handler) -> {
            created.incrementAndGet();
            final Thread thread = new Thread(body, "kairos-registered-supervisor");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(handler);
            supplied.set(thread);
            return thread;
        };
        this.scheduler = this.elastic(new KairosServices(null, source), 1);
        this.awaitLive(1);

        assertEquals(1, created.get(), "an elastic fund asks for exactly one control thread");
        assertNotNull(supplied.get());
        assertTrue(supplied.get().isAlive());
        assertFalse(plainSupervisorAlive(), "the seam must replace the plain daemon, not duplicate it");

        this.scheduler.halt();
        assertTrue(this.scheduler.join(5_000L));
        assertFalse(supplied.get().isAlive(), "the registered supervisor must be joined like any other runner");
    }

    @Test
    void withoutARuntimeTheSupervisorIsStillAPlainDaemon() throws InterruptedException {
        KairosGate.arm(true, new KairosTuning(true, 3, 5_000, 10, 20L));
        this.scheduler = new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline());
        this.scheduler.start();
        this.awaitLive(1);

        assertTrue(plainSupervisorAlive(), "losing the supervisor would freeze the fund, so a daemon must exist");
        assertEquals(KairosSnapshot.SupervisorState.RUNNING, this.scheduler.snapshot().supervisor());
    }

    @Test
    void aFixedFundNeverAsksForASupervisor() {
        final AtomicInteger created = new AtomicInteger();
        final KairosSupervisorSource counting = (name, body, handler) -> {
            created.incrementAndGet();
            return null;
        };
        KairosGate.arm(true, new KairosTuning(false, 2, 5_000, 10, 20L),
            new KairosServices(null, counting));
        this.scheduler = new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline());
        this.scheduler.start();
        this.awaitLive(1);

        assertEquals(0, created.get());
        assertEquals(KairosSnapshot.SupervisorState.OFF, this.scheduler.snapshot().supervisor());
    }

    private KairosScheduler elastic(final KairosServices services, final int floor) {
        KairosGate.arm(true, new KairosTuning(true, -1, 5_000, 10, 20L), services);
        final KairosScheduler created = new KairosScheduler(
            floor, this.threads, tick -> ((FakeTick) tick).deadline()
        );
        created.start();
        return this.scheduler = created;
    }

    private void awaitLive(final int expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (this.scheduler.liveThreads() != expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("expected " + expected + " live threads, got "
                    + this.scheduler.liveThreads());
            }
            Thread.onSpinWait();
        }
    }

    private static boolean plainSupervisorAlive() {
        for (final Thread thread : Thread.getAllStackTraces().keySet()) {
            if ("Tuff Kairos Supervisor".equals(thread.getName()) && thread.isAlive()) {
                return true;
            }
        }
        return false;
    }
}
