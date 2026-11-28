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
 * The bounded ring behind {@code /tuff scheduler leases}.
 *
 * <p>The ring is written on the tick path, one small sample per grant, and read by a command. These
 * tests prove the two properties an operator depends on: a sample names the region and the tick that
 * waited, and a long-running server keeps a fixed amount of history instead of growing one entry per
 * tick.
 */
@Timeout(120)
class KairosLeaseRingTest {

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
    void aMeasuredWaitNamesItsRegionAndItsTick() throws InterruptedException {
        final KairosScheduler fixed = this.fund();
        final CountDownLatch done = new CountDownLatch(1);
        final RegionFakeTick tick = new RegionFakeTick(
            "late", System.nanoTime() - millis(50L), this.order, done, 7L
        );
        fixed.schedule(tick);
        fixed.start();

        assertTrue(done.await(10L, TimeUnit.SECONDS), "the late tick must run");
        final KairosSnapshot.LeaseWait lease = fixed.snapshot().leaseWait();
        assertEquals(1L, lease.grants());
        assertEquals(1, lease.recent().size());
        final KairosSnapshot.LeaseWait.Sample sample = lease.recent().get(0);
        assertEquals(7L, sample.regionId(), "the sample must carry the region that waited");
        assertEquals(tick.id, sample.tickId(), "the sample must carry the tick that waited");
        assertTrue(sample.queuedMillis() >= 40L, "a tick 50 ms past its deadline must show a wait: " + sample);
        assertTrue(sample.ageMillis() >= 0L, "the age of a fresh sample cannot be negative");
    }

    @Test
    void aTickThatIsNotARegionSaysSoWithoutFailing() throws InterruptedException {
        final KairosScheduler fixed = this.fund();
        final CountDownLatch done = new CountDownLatch(1);
        fixed.schedule(new FakeTick("plain", System.nanoTime() - millis(20L), this.order, done));
        fixed.start();

        assertTrue(done.await(10L, TimeUnit.SECONDS), "the tick must run");
        final KairosSnapshot.LeaseWait.Sample sample = fixed.snapshot().leaseWait().recent().get(0);
        assertEquals(-1L, sample.regionId(), "a tick without a region must not invent one");
    }

    @Test
    void theRingKeepsAFixedNumberOfRecentWaits() throws InterruptedException {
        final KairosScheduler fixed = this.fund();
        final int grants = KairosScheduler.LEASE_RING_SIZE + 8;
        final CountDownLatch done = new CountDownLatch(grants);
        final long base = System.nanoTime();
        for (int index = 0; index < grants; index++) {
            fixed.schedule(new RegionFakeTick(
                "t" + index,
                base - millis(10L * (index + 1)),
                this.order,
                done,
                index % 3L
            ));
        }
        fixed.start();

        assertTrue(done.await(20L, TimeUnit.SECONDS), "every queued tick must run");
        final KairosSnapshot.LeaseWait lease = fixed.snapshot().leaseWait();
        assertEquals(grants, lease.grants(), "every grant is still counted");
        assertEquals(KairosScheduler.LEASE_RING_SIZE, lease.recent().size(),
            "the ring must keep exactly its capacity, not one entry per grant");
        assertEquals(
            KairosScheduler.LEASE_RING_SIZE,
            lease.recent().stream().map(KairosSnapshot.LeaseWait.Sample::tickId).distinct().count(),
            "the capacity samples must be the latest distinct grants"
        );
        assertTrue(lease.recent().stream().allMatch(sample -> sample.regionId() >= 0L));
    }

    private KairosScheduler fund() {
        KairosGate.arm(true, new KairosTuning(false, 1, 5_000, 10, 20L));
        KairosGate.kernel(new KairosKernel(1, 0, 0, 0, 0));
        return this.scheduler = new KairosScheduler(
            1, this.threads, tick -> ((FakeTick) tick).deadline(), KairosTickControl.none()
        );
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }

    /**
     * The shape Folia's handle has: a public {@code region} field whose object has a public
     * {@code id}. Reflection finds both exactly like it does on a real tick.
     */
    static final class RegionFakeTick extends FakeTick {

        public final Region region;

        private RegionFakeTick(
            final String label,
            final long deadline,
            final List<String> order,
            final CountDownLatch done,
            final long regionId
        ) {
            super(label, deadline, order, done);
            this.region = new Region(regionId);
        }
    }

    /**
     * Stand-in for the region object, with the one field the scheduler reads.
     */
    static final class Region {

        public final long id;

        private Region(final long id) {
            this.id = id;
        }
    }
}
