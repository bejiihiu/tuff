package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedComputeQueueTest {

    private TestSupport.TestClock clock;
    private long sequence;

    @BeforeEach
    void setUp() {
        this.clock = new TestSupport.TestClock();
        this.sequence = 0L;
    }

    @Test
    void acceptsUpToCapacityAndRefusesTheRest() {
        final BoundedComputeQueue queue = this.queue(2, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));

        assertEquals(OfferResult.ACCEPTED, queue.offer(this.entry("a", ComputePriority.NORMAL)));
        assertEquals(OfferResult.ACCEPTED, queue.offer(this.entry("b", ComputePriority.NORMAL)));
        assertEquals(OfferResult.QUEUE_FULL, queue.offer(this.entry("a", ComputePriority.NORMAL)));

        assertEquals(2, queue.size());
        assertEquals(2, queue.capacity());
        final QueueSnapshot snapshot = queue.snapshot();
        assertEquals(2, snapshot.size());
        assertEquals(1L, snapshot.rejectedQueueFull());
        assertEquals(2L, snapshot.accepted());
        assertNotNull(queue.poll(admitted -> true));
        assertNotNull(queue.poll(admitted -> true));
        assertNull(queue.poll(admitted -> true));
        assertTrue(queue.isEmpty());
    }

    @Test
    void refusesWhenConsumerBoundIsReached() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(1, 1.0d, OverflowPolicy.REJECT));

        assertEquals(OfferResult.ACCEPTED, queue.offer(this.entry("a", ComputePriority.NORMAL)));
        assertEquals(OfferResult.CONSUMER_QUEUE_FULL, queue.offer(this.entry("a", ComputePriority.HIGH)));
        assertEquals(1, queue.size("a"));
        assertEquals(1L, queue.snapshot().rejectedConsumerFull());
    }

    @Test
    void dropOldestEvictsOnlyTheOwnOldestEntry() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(2, 1.0d, OverflowPolicy.DROP_OLDEST));
        queue.registerConsumer("b", TestSupport.policy(2, 1.0d, OverflowPolicy.DROP_OLDEST));

        final QueueEntry aNormal = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry aHigh = this.entry("a", ComputePriority.HIGH);
        final QueueEntry bNormal = this.entry("b", ComputePriority.NORMAL);
        final QueueEntry bHigh = this.entry("b", ComputePriority.HIGH);
        assertEquals(OfferResult.ACCEPTED, queue.offer(aNormal));
        assertEquals(OfferResult.ACCEPTED, queue.offer(aHigh));
        assertEquals(OfferResult.ACCEPTED, queue.offer(bNormal));
        assertEquals(OfferResult.ACCEPTED, queue.offer(bHigh));

        final QueueEntry aCritical = this.entry("a", ComputePriority.CRITICAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(aCritical));

        assertEquals(4, queue.size());
        assertEquals(2, queue.size("a"));
        assertEquals(2, queue.size("b"));
        assertEquals(1L, queue.snapshot().evicted());

        final List<QueueEntry> remaining = new ArrayList<>();
        assertEquals(4, queue.drainTo(remaining, 10));
        assertEquals(Set.of(aHigh, aCritical, bNormal, bHigh), Set.copyOf(remaining));
        assertFalse(remaining.contains(aNormal));
        assertNull(queue.poll(admitted -> true));
    }

    @Test
    void dropOldestAtGlobalCapacityEvictsOnlyItsOwnEntry() {
        final BoundedComputeQueue queue = this.queue(2, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        queue.registerConsumer("b", TestSupport.policy(-1, 1.0d, OverflowPolicy.DROP_OLDEST));

        final QueueEntry a = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry bOld = this.entry("b", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(a));
        assertEquals(OfferResult.ACCEPTED, queue.offer(bOld));

        final QueueEntry bNew = this.entry("b", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(bNew));

        assertEquals(2, queue.size());
        assertEquals(1, queue.size("a"));
        assertEquals(1, queue.size("b"));
        assertEquals(1L, queue.snapshot().evicted());

        assertEquals(a, queue.poll(admitted -> true));
        assertEquals(bNew, queue.poll(admitted -> true));
        assertNull(queue.poll(admitted -> true));
    }

    @Test
    void dropOldestWithoutOwnEntryIsRejected() {
        final BoundedComputeQueue queue = this.queue(1, TestSupport.policy(-1, 1.0d, OverflowPolicy.DROP_OLDEST));
        queue.registerConsumer("b", TestSupport.policy(-1, 1.0d, OverflowPolicy.DROP_OLDEST));

        assertEquals(OfferResult.ACCEPTED, queue.offer(this.entry("a", ComputePriority.NORMAL)));
        assertEquals(OfferResult.QUEUE_FULL, queue.offer(this.entry("b", ComputePriority.NORMAL)));
        assertEquals(1, queue.size());
        assertEquals(0, queue.size("b"));
        assertEquals(1L, queue.snapshot().rejectedQueueFull());
    }

    @Test
    void closedQueueRefusesOffersButStillDrains() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        final QueueEntry queued = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(queued));

        queue.close();
        queue.close();

        assertTrue(queue.isClosed());
        assertEquals(OfferResult.CLOSED, queue.offer(this.entry("a", ComputePriority.HIGH)));
        assertEquals(1, queue.size());
        assertEquals(queued, queue.poll(admitted -> true));
        assertNull(queue.poll(admitted -> true));
    }

    @Test
    void removeDropsExactlyTheRequestedEntry() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        final QueueEntry first = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry middle = this.entry("a", ComputePriority.HIGH);
        final QueueEntry last = this.entry("a", ComputePriority.LOW);
        queue.offer(first);
        queue.offer(middle);
        queue.offer(last);

        assertTrue(queue.remove(middle));
        assertFalse(queue.remove(middle));
        assertEquals(2, queue.size());
        assertEquals(first, queue.poll(admitted -> true));
        assertEquals(last, queue.poll(admitted -> true));
        assertFalse(queue.remove(last));
    }

    @Test
    void removeIgnoresUnknownEntriesAndProbesTheRightBucket() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        final QueueEntry low = this.entry("a", ComputePriority.LOW);
        final QueueEntry normal = this.entry("a", ComputePriority.NORMAL);
        queue.offer(low);
        queue.offer(normal);

        final QueueEntry neverQueued = this.entry("a", ComputePriority.NORMAL);
        assertFalse(queue.remove(neverQueued));
        assertFalse(queue.remove(this.entry("missing", ComputePriority.NORMAL)));
        assertTrue(queue.remove(normal));
        assertEquals(1, queue.size());
        assertEquals(low, queue.poll(admitted -> true));
    }

    @Test
    void drainMovesHighestPriorityThenOldestFirst() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        final QueueEntry low = this.entry("a", ComputePriority.LOW);
        final QueueEntry high = this.entry("a", ComputePriority.HIGH);
        final QueueEntry normal = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry critical = this.entry("a", ComputePriority.CRITICAL);
        queue.offer(low);
        queue.offer(high);
        queue.offer(normal);
        queue.offer(critical);

        final List<QueueEntry> drained = new ArrayList<>();
        assertEquals(2, queue.drainTo(drained, 2));
        assertEquals(List.of(critical, high), drained);
        assertEquals(2, queue.size());

        assertEquals(2, queue.drainTo(drained, 10));
        assertEquals(List.of(critical, high, normal, low), drained);
        assertEquals(0, queue.drainTo(drained, 1));
        assertTrue(queue.isEmpty());
        assertThrows(IllegalArgumentException.class, () -> queue.drainTo(drained, 0));
    }

    @Test
    void clearRemovesEverythingWithoutResettingLifetimeCounters() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        queue.offer(this.entry("a", ComputePriority.NORMAL));
        queue.offer(this.entry("b", ComputePriority.HIGH));

        queue.clear();

        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertNull(queue.poll(admitted -> true));
        final QueueSnapshot snapshot = queue.snapshot();
        assertEquals(0, snapshot.size());
        assertTrue(snapshot.perConsumerSize().isEmpty());
        assertEquals(2L, snapshot.accepted());
        assertEquals(0L, snapshot.oldestWaitNanos());
    }

    @Test
    void unregisterReturnsItsEntriesInDispatchOrder() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        final QueueEntry aNormal = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry aCritical = this.entry("a", ComputePriority.CRITICAL);
        final QueueEntry aHigh = this.entry("a", ComputePriority.HIGH);
        final QueueEntry bNormal = this.entry("b", ComputePriority.NORMAL);
        queue.offer(aNormal);
        queue.offer(aCritical);
        queue.offer(aHigh);
        queue.offer(bNormal);

        final Collection<QueueEntry> removed = queue.unregisterConsumer("a");

        assertEquals(List.of(aCritical, aHigh, aNormal), List.copyOf(removed));
        assertEquals(1, queue.size());
        assertEquals(0, queue.size("a"));
        assertTrue(queue.unregisterConsumer("a").isEmpty());
        assertTrue(queue.unregisterConsumer("never-registered").isEmpty());

        final QueueEntry afterReRegister = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(afterReRegister));
        assertEquals(bNormal, queue.poll(admitted -> true));
        assertEquals(afterReRegister, queue.poll(admitted -> true));
    }

    @Test
    void snapshotReportsRealGaugesAndCounters() {
        final BoundedComputeQueue queue = this.queue(4, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        queue.offer(this.entry("a", ComputePriority.NORMAL));
        queue.offer(this.entry("a", ComputePriority.LOW));
        queue.offer(this.entry("b", ComputePriority.HIGH));

        this.clock.advance(Duration.ofSeconds(5));
        final QueueSnapshot snapshot = queue.snapshot();

        assertEquals(5_000_000_000L, snapshot.capturedAtNanos());
        assertEquals(3, snapshot.size());
        assertEquals(4, snapshot.capacity());
        assertEquals(0.75d, snapshot.utilization());
        assertEquals(3L, snapshot.accepted());
        assertEquals(Duration.ofSeconds(5), snapshot.oldestWait());
        assertEquals(Map.of("a", 2, "b", 1), snapshot.perConsumerSize());
    }

    @Test
    void peekDoesNotRemoveAndRespectsOrder() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        final QueueEntry low = this.entry("a", ComputePriority.LOW);
        final QueueEntry high = this.entry("a", ComputePriority.HIGH);
        queue.offer(low);
        queue.offer(high);

        assertSame(high, queue.peek());
        assertSame(high, queue.peek());
        assertEquals(2, queue.size());
        assertSame(high, queue.poll(admitted -> true));
        assertSame(low, queue.poll(admitted -> true));
        assertNull(queue.peek());
    }

    @Test
    void unknownConsumersAreRegisteredWithTheDefaultPolicy() {
        final ConsumerPolicy defaults = ConsumerPolicy.defaults();
        final BoundedComputeQueue queue = new BoundedComputeQueue(
            8,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR, false, 1, false),
            defaults,
            this.clock
        );

        assertEquals(OfferResult.ACCEPTED, queue.offer(this.entry("dynamic", ComputePriority.NORMAL)));
        assertEquals(1, queue.size("dynamic"));
        assertEquals(0, queue.size("never-seen"));
    }

    @Test
    void policyUpdateAppliesToFutureOffersOnly() {
        final BoundedComputeQueue queue = this.queue(8, TestSupport.policy(1, 1.0d, OverflowPolicy.REJECT));
        final QueueEntry first = this.entry("a", ComputePriority.NORMAL);
        assertEquals(OfferResult.ACCEPTED, queue.offer(first));

        queue.updatePolicy("a", TestSupport.policy(0, 1.0d, OverflowPolicy.REJECT));

        assertEquals(OfferResult.CONSUMER_QUEUE_FULL, queue.offer(this.entry("a", ComputePriority.HIGH)));
        assertEquals(1, queue.size("a"));
        assertEquals(first, queue.poll(admitted -> true));
    }

    @Test
    void stealIsDisabledUnlessConfigured() {
        final BoundedComputeQueue disabled = this.queue(8, TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        disabled.offer(this.entry("a", ComputePriority.NORMAL));
        assertNull(disabled.steal(0, admitted -> true));

        final BoundedComputeQueue enabled = new BoundedComputeQueue(
            8,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR, false, 1, true),
            TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT),
            this.clock
        );
        final QueueEntry a = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry b = this.entry("b", ComputePriority.NORMAL);
        enabled.offer(a);
        enabled.offer(b);

        assertNull(enabled.steal(0, admitted -> false));
        assertNotNull(enabled.steal(-3, admitted -> true));
        assertNotNull(enabled.steal(7, admitted -> true));
        assertTrue(enabled.isEmpty());
    }

    /**
     * Creates a queue with REJECT default policy and the given capacity and per consumer defaults.
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
     * Creates an entry for a consumer at the current clock reading.
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
