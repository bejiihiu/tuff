package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue.OfferResult;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.queue.QueueSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class BoundedComputeQueueEvictionTest {

    private TestSupport.TestClock clock;
    private long sequence;

    @BeforeEach
    void setUp() {
        this.clock = new TestSupport.TestClock();
        this.sequence = 0L;
    }

    @Test
    void listenerSeesTheEntryAfterUnlinkAndBeforeTheNewOneIsQueued() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(1, 1.0d, OverflowPolicy.DROP_OLDEST));
        final QueueEntry old = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(old));

        final AtomicInteger calls = new AtomicInteger();
        final List<String> observed = new ArrayList<>();
        queue.setEvictionListener(evicted -> {
            calls.incrementAndGet();
            assertSame(old, evicted);
            assertNull(queue.peek());
            final QueueSnapshot inside = queue.snapshot();
            observed.add("size=" + inside.size() + ",depth=" + queue.size("a")
                + ",evicted=" + inside.evicted());
        });

        final QueueEntry fresh = this.entry("a", ComputePriority.HIGH);
        assertEquals(OfferResult.ACCEPTED, queue.offer(fresh));

        assertEquals(1, calls.get());
        assertEquals(List.of("size=0,depth=0,evicted=1"), observed);
        assertEquals(1, queue.size());
        assertSame(fresh, queue.peek());
        assertSame(fresh, queue.poll(admitted -> true));
        assertNull(queue.poll(admitted -> true));
        assertFalse(queue.remove(old));
    }

    @Test
    void dropOldestWithoutListenerThrowsAndLosesNothing() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(1, 1.0d, OverflowPolicy.DROP_OLDEST));
        final QueueEntry old = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(old));

        final QueueEntry fresh = this.entry("a", ComputePriority.HIGH);
        assertThrows(IllegalStateException.class, () -> queue.offer(fresh));

        assertEquals(1, queue.size());
        assertEquals(1, queue.size("a"));
        assertSame(old, queue.peek());
        final QueueSnapshot snapshot = queue.snapshot();
        assertEquals(1L, snapshot.accepted());
        assertEquals(0L, snapshot.evicted());
        assertEquals(0L, snapshot.rejectedConsumerFull());
        assertSame(old, queue.poll(admitted -> true));
        assertNull(queue.poll(admitted -> true));
    }

    @Test
    void dropOldestWithoutListenerAtGlobalCapacityThrows() {
        final BoundedComputeQueue queue = this.queue(2, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        queue.registerConsumer("b", TestSupport.policy(-1, 1.0d, OverflowPolicy.DROP_OLDEST));
        final QueueEntry a = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry bOld = this.entry("b", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(a));
        assertEquals(OfferResult.ACCEPTED, queue.offer(bOld));

        assertThrows(IllegalStateException.class, () -> queue.offer(this.entry("b", ComputePriority.HIGH)));

        assertEquals(2, queue.size());
        assertEquals(1, queue.size("a"));
        assertEquals(1, queue.size("b"));
        assertSame(a, queue.poll(admitted -> true));
        assertSame(bOld, queue.poll(admitted -> true));
        assertNull(queue.poll(admitted -> true));
    }

    @Test
    void replacingTheListenerSendsNewEvictionsToTheNewOne() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(1, 1.0d, OverflowPolicy.DROP_OLDEST));
        final List<QueueEntry> first = new ArrayList<>();
        final List<QueueEntry> second = new ArrayList<>();
        queue.setEvictionListener(first::add);

        final QueueEntry one = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry two = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry three = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(one));
        assertEquals(OfferResult.ACCEPTED, queue.offer(two));

        queue.setEvictionListener(second::add);
        assertEquals(OfferResult.ACCEPTED, queue.offer(three));

        assertEquals(List.of(one), first);
        assertEquals(List.of(two), second);
        assertEquals(1, queue.size());
        assertSame(three, queue.peek());
    }

    @Test
    void listenerIsNotCalledByPollRemoveDrainOrClear() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        queue.setEvictionListener(evicted -> fail("eviction listener must not run for " + evicted.taskId()));

        final QueueEntry a = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry b = this.entry("b", ComputePriority.HIGH);
        final QueueEntry c = this.entry("c", ComputePriority.LOW);
        assertEquals(OfferResult.ACCEPTED, queue.offer(a));
        assertEquals(OfferResult.ACCEPTED, queue.offer(b));
        assertEquals(OfferResult.ACCEPTED, queue.offer(c));

        assertTrue(queue.remove(b));
        assertSame(a, queue.poll(admitted -> true));
        final List<QueueEntry> drained = new ArrayList<>();
        assertEquals(1, queue.drainTo(drained, 1));
        assertEquals(List.of(c), drained);
        assertTrue(queue.clear().isEmpty());
    }

    @Test
    void clearReturnsExactlyWhatPollingWouldReturn() {
        final BoundedComputeQueue cleared = this.queue(16, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        final BoundedComputeQueue polled = this.queue(16, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        final List<QueueEntry> entries = List.of(
            this.entry("a", ComputePriority.NORMAL),
            this.entry("b", ComputePriority.HIGH),
            this.entry("a", ComputePriority.LOW),
            this.entry("b", ComputePriority.NORMAL),
            this.entry("a", ComputePriority.CRITICAL),
            this.entry("b", ComputePriority.LOW)
        );
        for (final QueueEntry entry : entries) {
            assertEquals(OfferResult.ACCEPTED, cleared.offer(entry));
            assertEquals(OfferResult.ACCEPTED, polled.offer(entry));
        }

        final Collection<QueueEntry> clearedEntries = cleared.clear();
        final List<QueueEntry> polledEntries = new ArrayList<>();
        QueueEntry next = polled.poll(admitted -> true);
        while (next != null) {
            polledEntries.add(next);
            next = polled.poll(admitted -> true);
        }

        assertEquals(polledEntries, List.copyOf(clearedEntries));
        assertEquals(entries.size(), clearedEntries.size());
        assertTrue(cleared.isEmpty());
        assertTrue(polled.isEmpty());
    }

    /**
     * Creates a queue with the given capacity and default consumer policy.
     *
     * @param capacity queue capacity
     * @param policy   default consumer policy
     * @return the queue
     */
    private BoundedComputeQueue queue(final int capacity, final ConsumerPolicy policy) {
        return new BoundedComputeQueue(
            capacity,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR, false, 1, false),
            policy,
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
