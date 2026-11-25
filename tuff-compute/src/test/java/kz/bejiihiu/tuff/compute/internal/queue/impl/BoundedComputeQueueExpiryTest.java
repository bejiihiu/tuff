package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue.OfferResult;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class BoundedComputeQueueExpiryTest {

    private TestSupport.TestClock clock;
    private long sequence;

    @BeforeEach
    void setUp() {
        this.clock = new TestSupport.TestClock();
        this.sequence = 0L;
    }

    @Test
    void zeroLimitNeverExpires() {
        final BoundedComputeQueue queue = this.queue(8);
        final QueueEntry entry = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(entry));

        this.clock.advance(Duration.ofDays(1));

        assertFalse(queue.hasExpired(this.clock.nanoTime()));
        final List<QueueEntry> expired = new ArrayList<>();
        assertEquals(0, queue.drainExpired(this.clock.nanoTime(), expired));
        assertTrue(expired.isEmpty());
        assertEquals(1, queue.size());
        assertSame(entry, queue.poll(admitted -> true));
    }

    @Test
    void boundaryIsInclusive() {
        final BoundedComputeQueue queue = this.queue(8);
        queue.registerConsumer("a", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT, Duration.ofSeconds(1)));
        final QueueEntry entry = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(entry));

        this.clock.advance(Duration.ofNanos(999_999_999L));
        assertFalse(queue.hasExpired(this.clock.nanoTime()));
        final List<QueueEntry> before = new ArrayList<>();
        assertEquals(0, queue.drainExpired(this.clock.nanoTime(), before));
        assertEquals(1, queue.size());

        this.clock.advance(Duration.ofNanos(1L));
        assertTrue(queue.hasExpired(this.clock.nanoTime()));
        final List<QueueEntry> after = new ArrayList<>();
        assertEquals(1, queue.drainExpired(this.clock.nanoTime(), after));
        assertEquals(List.of(entry), after);
        assertTrue(queue.isEmpty());
        assertFalse(queue.hasExpired(this.clock.nanoTime()));
    }

    @Test
    void drainTakesOnlyExpiredEntriesAndLeavesTheRest() {
        final BoundedComputeQueue queue = this.queue(8);
        queue.registerConsumer("a", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT, Duration.ofSeconds(1)));

        final QueueEntry aExpired = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(aExpired));
        this.clock.advance(Duration.ofMillis(1500));
        final QueueEntry aAlive = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry bAlive = this.entry("b", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(aAlive));
        assertEquals(OfferResult.ACCEPTED, queue.offer(bAlive));

        this.clock.advance(Duration.ofMillis(500));
        assertTrue(queue.hasExpired(this.clock.nanoTime()));
        final List<QueueEntry> expired = new ArrayList<>();
        assertEquals(1, queue.drainExpired(this.clock.nanoTime(), expired));
        assertEquals(List.of(aExpired), expired);

        assertEquals(2, queue.size());
        assertEquals(1, queue.size("a"));
        assertFalse(queue.hasExpired(this.clock.nanoTime()));
        assertNotNull(queue.poll(admitted -> true));
        assertNotNull(queue.poll(admitted -> true));
        assertTrue(queue.isEmpty());
    }

    @Test
    void drainUsesDispatchOrder() {
        final BoundedComputeQueue queue = this.queue(8);
        final ConsumerPolicy expiring = TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT, Duration.ofSeconds(1));
        queue.registerConsumer("a", expiring);
        queue.registerConsumer("b", expiring);

        final QueueEntry aLow = this.entry("a", ComputePriority.LOW);
        final QueueEntry bNormal = this.entry("b", ComputePriority.NORMAL);
        final QueueEntry aHigh = this.entry("a", ComputePriority.HIGH);
        assertEquals(OfferResult.ACCEPTED, queue.offer(aLow));
        assertEquals(OfferResult.ACCEPTED, queue.offer(bNormal));
        assertEquals(OfferResult.ACCEPTED, queue.offer(aHigh));

        this.clock.advance(Duration.ofSeconds(2));
        final List<QueueEntry> expired = new ArrayList<>();
        assertEquals(3, queue.drainExpired(this.clock.nanoTime(), expired));
        assertEquals(List.of(aHigh, bNormal, aLow), expired);
        assertTrue(queue.isEmpty());
    }

    @Test
    void shorteningThePolicyExpiresAlreadyQueuedWork() {
        final BoundedComputeQueue queue = this.queue(8);
        queue.registerConsumer("a", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT, Duration.ofHours(1)));
        final QueueEntry entry = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(entry));

        this.clock.advance(Duration.ofSeconds(2));
        assertFalse(queue.hasExpired(this.clock.nanoTime()));
        queue.updatePolicy("a", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT, Duration.ofSeconds(1)));
        assertTrue(queue.hasExpired(this.clock.nanoTime()));

        final List<QueueEntry> expired = new ArrayList<>();
        assertEquals(1, queue.drainExpired(this.clock.nanoTime(), expired));
        assertEquals(List.of(entry), expired);
        assertTrue(queue.isEmpty());
    }

    @Test
    void drainExpiredDoesNotTouchTheEvictionListenerOrTheHandle() {
        final BoundedComputeQueue queue = this.queue(8);
        queue.registerConsumer("a", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT, Duration.ofSeconds(1)));
        queue.setEvictionListener(evicted -> fail("expiry must not be reported as eviction"));

        final QueueEntry entry = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(entry));
        this.clock.advance(Duration.ofSeconds(2));

        final List<QueueEntry> expired = new ArrayList<>();
        assertEquals(1, queue.drainExpired(this.clock.nanoTime(), expired));
        assertEquals(List.of(entry), expired);
        assertFalse(entry.handle().future().isDone());
        assertEquals(ComputeTaskHandle.State.QUEUED, entry.handle().state());
    }

    @Test
    @Timeout(60)
    void aSingleExpiredEntryIsFoundWithoutWalkingTheQueue() {
        final BoundedComputeQueue queue = this.queue(200_000);
        queue.registerConsumer("a", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT, Duration.ofHours(1)));

        final long now = this.clock.nanoTime();
        final QueueEntry expiredEntry = TestSupport.entry(
            1L,
            "a",
            ComputePriority.NORMAL,
            1L,
            now - Duration.ofHours(2).toNanos()
        );
        assertEquals(OfferResult.ACCEPTED, queue.offer(expiredEntry));
        for (int index = 0; index < 100_000; index++) {
            final QueueEntry alive = TestSupport.entry(
                2L + index,
                "a",
                ComputePriority.NORMAL,
                2L + index,
                now
            );
            assertEquals(OfferResult.ACCEPTED, queue.offer(alive));
        }

        assertTrue(queue.hasExpired(now));
        final List<QueueEntry> expired = new ArrayList<>();
        final long started = System.nanoTime();
        assertEquals(1, queue.drainExpired(now, expired));
        final long elapsedMillis = (System.nanoTime() - started) / 1_000_000L;

        assertEquals(List.of(expiredEntry), expired);
        assertEquals(100_000, queue.size());
        assertTrue(elapsedMillis < 2_000L, "drainExpired took " + elapsedMillis + " ms");
        assertFalse(queue.hasExpired(now));
    }

    /**
     * Creates a queue with the default consumer policy and no expiry on unknown consumers.
     *
     * @param capacity queue capacity
     * @return the queue
     */
    private BoundedComputeQueue queue(final int capacity) {
        return new BoundedComputeQueue(
            capacity,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR, false, 1, false),
            ConsumerPolicy.defaults(),
            this.clock
        );
    }

    /**
     * Creates an entry at the current clock reading.
     *
     * @param consumerId consumer of the entry
     * @param priority   submitted priority
     * @return the entry
     */
    private QueueEntry entry(final String consumerId, final ComputePriority priority) {
        this.sequence++;
        return TestSupport.entry(this.sequence, consumerId, priority, this.sequence, this.clock.nanoTime());
    }
}
