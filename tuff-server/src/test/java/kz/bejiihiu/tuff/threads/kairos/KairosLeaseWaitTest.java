package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * Permit waiting: measured on the grant, reported by the supervisor, never paid for by a tick thread.
 *
 * <p>Two waits are counted apart. {@code queued} is the distance between a tick's deadline and the
 * moment a permit arrived, which is the shortage signal; {@code parked} is the extra time after both
 * the permit and the deadline existed, which is contention. The timeout only decides whether the
 * supervisor writes a line, so a server that leaves it at zero still sees both numbers in
 * {@code /tuff scheduler}.
 */
@Timeout(120)
class KairosLeaseWaitTest {

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
    void theSnapshotAccountsForTheWaitEvenWithoutATimeout() throws InterruptedException {
        final KairosScheduler fixed = this.fund(new KairosKernel(1, 0, 0, 3, 0), false);
        final long now = System.nanoTime();
        final CountDownLatch done = new CountDownLatch(1);
        fixed.schedule(this.tick("late", now - millis(300L), done).runFor(millis(1L)));
        fixed.start();
        assertTrue(done.await(5L, TimeUnit.SECONDS), "the late tick must run");

        final KairosSnapshot.LeaseWait lease = fixed.snapshot().leaseWait();
        assertEquals(1L, lease.grants(), "one permit was handed out and measured");
        assertTrue(lease.queuedMaxMillis() >= 290L, "the wait before the permit must be recorded, got " + lease);
        assertEquals(0L, lease.timeouts(), "no timeout was asked for, so nothing can time out");
        assertFalse(lease.armed());
    }

    @Test
    void aTimeoutWarnsOncePerQuietWindow() throws Exception {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        KairosGate.arm(true, new KairosTuning(true, 2, 5_000, 50, 20L));
        KairosGate.kernel(new KairosKernel(1, 0, 0, 3, 100));
        final KairosScheduler elastic = this.scheduler = new KairosScheduler(
            1,
            this.threads,
            tick -> ((FakeTick) tick).deadline(),
            KairosTickControl.none(),
            clock
        );
        elastic.start();

        try (KairosWarningCapture capture = KairosWarningCapture.attach()) {
            Assumptions.assumeTrue(capture != null, "log4j2 core is not on the test classpath");

            elastic.schedule(this.tick("first", System.nanoTime() - millis(400L), new CountDownLatch(1)));
            assertTrue(capture.await(1, 5_000L), "a permit that arrived 400 ms late must be reported");
            final String warning = capture.warnings().get(0);
            assertTrue(warning.contains("lease wait above the 100 ms timeout"), warning);
            assertTrue(warning.contains("never blocked"), "the line must say the tick body is not blocked: " + warning);

            // a second timeout inside the ten second window is counted, not written
            elastic.schedule(this.tick("second", System.nanoTime() - millis(400L), new CountDownLatch(1)));
            Thread.sleep(200L);
            assertEquals(1, capture.warnings().size(), "the quiet window must swallow the second line");

            clock.advance(TimeUnit.SECONDS.toNanos(11L));
            elastic.schedule(this.tick("third", System.nanoTime() - millis(400L), new CountDownLatch(1)));
            assertTrue(capture.await(2, 5_000L), "the next window must be allowed to warn again");
            assertTrue(capture.warnings().get(1).contains("suppressed"), capture.warnings().get(1));
        }
        assertTrue(elastic.snapshot().leaseWait().timeouts() >= 2L, "the timeouts are still counted");
    }

    @Test
    void aTickBodyCanTakeASnapshotAndScheduleWithoutBlocking() throws InterruptedException {
        final KairosScheduler fixed = this.fund(new KairosKernel(1, 0, 0, 3, 0), false);
        final AtomicLong bodyNanos = new AtomicLong(-1L);
        final CountDownLatch done = new CountDownLatch(1);
        final FakeTick body = new FakeTick("body", System.nanoTime(), this.order, done) {
            @Override
            public boolean runTick() {
                final long started = System.nanoTime();
                // the two calls a region may make from its own thread; neither may wait for a permit
                fixed.snapshot();
                fixed.schedule(new FakeTick("nested", System.nanoTime() + millis(50L), KairosLeaseWaitTest.this.order,
                    new CountDownLatch(1)));
                bodyNanos.set(System.nanoTime() - started);
                return super.runTick();
            }
        };
        fixed.schedule(body);
        fixed.start();

        assertTrue(done.await(5L, TimeUnit.SECONDS), "the body must finish");
        assertTrue(bodyNanos.get() >= 0L);
        assertTrue(bodyNanos.get() < millis(200L), "a tick thread must not wait for a lock, took " + bodyNanos.get());
    }

    private KairosScheduler fund(final KairosKernel kernel, final boolean elastic) {
        KairosGate.arm(true, new KairosTuning(elastic, 2, 100, 50, 20L));
        KairosGate.kernel(kernel);
        return this.scheduler = new KairosScheduler(
            1, this.threads, tick -> ((FakeTick) tick).deadline(), KairosTickControl.none()
        );
    }

    private FakeTick tick(final String label, final long deadline, final CountDownLatch done) {
        return new FakeTick(label, deadline, this.order, done);
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
