package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue.ConsumerAdmission;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedComputeQueueOrderingTest {

    private static final ConsumerAdmission ADMIT_ALL = consumerId -> true;

    private TestSupport.TestClock clock = new TestSupport.TestClock();
    private long sequence;

    @Test
    void dispatchesHighestPriorityFirst() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            false,
            1
        );
        final QueueEntry low = this.entry("a", ComputePriority.LOW);
        final QueueEntry normal = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry critical = this.entry("a", ComputePriority.CRITICAL);
        final QueueEntry high = this.entry("a", ComputePriority.HIGH);
        queue.offer(low);
        queue.offer(normal);
        queue.offer(critical);
        queue.offer(high);

        assertEquals(critical, queue.poll(ADMIT_ALL));
        assertEquals(high, queue.poll(ADMIT_ALL));
        assertEquals(normal, queue.poll(ADMIT_ALL));
        assertEquals(low, queue.poll(ADMIT_ALL));
        assertNull(queue.poll(ADMIT_ALL));
    }

    @Test
    void equalPriorityIsFifoInsideOneConsumer() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            false,
            1
        );
        final QueueEntry first = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry second = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry third = this.entry("a", ComputePriority.NORMAL);
        queue.offer(first);
        queue.offer(second);
        queue.offer(third);

        assertEquals(first, queue.poll(ADMIT_ALL));
        assertEquals(second, queue.poll(ADMIT_ALL));
        assertEquals(third, queue.poll(ADMIT_ALL));
    }

    @Test
    void agingLiftsAWaitingLowTaskAboveAFreshHighTask() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            true,
            3
        );
        final QueueEntry low = this.entry("a", ComputePriority.LOW);
        queue.offer(low);

        this.clock.advance(Duration.ofSeconds(3));
        final QueueEntry high = this.entry("a", ComputePriority.HIGH);
        queue.offer(high);

        assertSame(low, queue.peek());
        assertEquals(low, queue.poll(ADMIT_ALL));
        assertEquals(high, queue.poll(ADMIT_ALL));
    }

    @Test
    void agingRaisesLowPriorityAcrossConsumersToo() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            true,
            3
        );
        final QueueEntry low = this.entry("a", ComputePriority.LOW);
        queue.offer(low);

        this.clock.advance(Duration.ofSeconds(3));
        final QueueEntry high = this.entry("b", ComputePriority.HIGH);
        queue.offer(high);

        assertEquals(low, queue.poll(ADMIT_ALL));
        assertEquals(high, queue.poll(ADMIT_ALL));
    }

    @Test
    void agingIsCappedByMaxAgingBoost() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            true,
            1
        );
        final QueueEntry low = this.entry("a", ComputePriority.LOW);
        queue.offer(low);

        this.clock.advance(Duration.ofSeconds(1));
        final QueueEntry high = this.entry("a", ComputePriority.HIGH);
        queue.offer(high);

        // low is at level 1, high at 2, so the cap keeps high in front
        assertEquals(high, queue.poll(ADMIT_ALL));
        assertEquals(low, queue.poll(ADMIT_ALL));
    }

    @Test
    void disabledAgingKeepsStrictPriority() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            false,
            3
        );
        final QueueEntry low = this.entry("a", ComputePriority.LOW);
        queue.offer(low);

        this.clock.advance(Duration.ofMinutes(10));
        final QueueEntry high = this.entry("a", ComputePriority.HIGH);
        queue.offer(high);

        assertEquals(high, queue.poll(ADMIT_ALL));
        assertEquals(low, queue.poll(ADMIT_ALL));
    }

    @Test
    void weightedFairnessGivesTurnsInProportionToWeight() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            false,
            1
        );
        queue.registerConsumer("heavy", TestSupport.policy(-1, 2.0d, OverflowPolicy.REJECT));
        queue.registerConsumer("light", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));

        for (int index = 0; index < 2000; index++) {
            queue.offer(this.entry("heavy", ComputePriority.NORMAL));
        }
        for (int index = 0; index < 2000; index++) {
            queue.offer(this.entry("light", ComputePriority.NORMAL));
        }

        int heavy = 0;
        int light = 0;
        for (int index = 0; index < 600; index++) {
            final QueueEntry entry = queue.poll(ADMIT_ALL);
            if (entry == null) {
                break;
            }
            if (entry.consumerId().equals("heavy")) {
                heavy++;
            } else {
                light++;
            }
        }

        assertEquals(600, heavy + light);
        assertTrue(heavy >= 390 && heavy <= 410, "heavy got " + heavy + " turns");
        assertTrue(light >= 190 && light <= 210, "light got " + light + " turns");
    }

    @Test
    void idleConsumerDoesNotGetAnUnboundedBurstWhenItReturns() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            false,
            1
        );
        for (int index = 0; index < 200; index++) {
            queue.offer(this.entry("a", ComputePriority.NORMAL));
        }
        for (int index = 0; index < 100; index++) {
            assertEquals("a", queue.poll(ADMIT_ALL).consumerId());
        }

        queue.offer(this.entry("b", ComputePriority.NORMAL));

        // b is aligned to a's virtual service time, so the next turn goes to the older entry and the
        // one after that to b. Without the alignment b would receive a hundred turn burst.
        assertEquals("a", queue.poll(ADMIT_ALL).consumerId());
        assertEquals("b", queue.poll(ADMIT_ALL).consumerId());
    }

    @Test
    void equalWeightsSplitTurnsEvenly() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            false,
            1
        );
        for (int index = 0; index < 500; index++) {
            queue.offer(this.entry("a", ComputePriority.NORMAL));
        }
        for (int index = 0; index < 500; index++) {
            queue.offer(this.entry("b", ComputePriority.NORMAL));
        }

        int a = 0;
        int b = 0;
        for (int index = 0; index < 200; index++) {
            final QueueEntry entry = queue.poll(ADMIT_ALL);
            if (entry == null) {
                break;
            }
            if (entry.consumerId().equals("a")) {
                a++;
            } else {
                b++;
            }
        }

        assertEquals(200, a + b);
        assertEquals(100, a);
        assertEquals(100, b);
    }

    @Test
    void fairnessNeverOverridesPriority() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            false,
            1
        );
        queue.registerConsumer("greedy", TestSupport.policy(-1, 100.0d, OverflowPolicy.REJECT));
        queue.registerConsumer("urgent", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));

        final QueueEntry greedy = this.entry("greedy", ComputePriority.NORMAL);
        final QueueEntry urgent = this.entry("urgent", ComputePriority.HIGH);
        queue.offer(greedy);
        queue.offer(urgent);

        assertEquals(urgent, queue.poll(ADMIT_ALL));
        assertEquals(greedy, queue.poll(ADMIT_ALL));
    }

    @Test
    void priorityStrategyIgnoresWeights() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.PRIORITY,
            false,
            1
        );
        queue.registerConsumer("heavy", TestSupport.policy(-1, 2.0d, OverflowPolicy.REJECT));

        final QueueEntry a1 = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry heavy1 = this.entry("heavy", ComputePriority.NORMAL);
        final QueueEntry a2 = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry heavy2 = this.entry("heavy", ComputePriority.NORMAL);
        queue.offer(a1);
        queue.offer(heavy1);
        queue.offer(a2);
        queue.offer(heavy2);

        assertEquals(a1, queue.poll(ADMIT_ALL));
        assertEquals(heavy1, queue.poll(ADMIT_ALL));
        assertEquals(a2, queue.poll(ADMIT_ALL));
        assertEquals(heavy2, queue.poll(ADMIT_ALL));
    }

    @Test
    void fifoStrategyIgnoresPriority() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.FIFO,
            false,
            1
        );
        final QueueEntry normal = this.entry("a", ComputePriority.NORMAL);
        final QueueEntry critical = this.entry("a", ComputePriority.CRITICAL);
        queue.offer(normal);
        queue.offer(critical);

        assertEquals(normal, queue.poll(ADMIT_ALL));
        assertEquals(critical, queue.poll(ADMIT_ALL));
    }

    @Test
    void admissionSkipsTheWholeBlockedConsumerWithoutDroppingIt() {
        final BoundedComputeQueue queue = this.queue(
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            false,
            1
        );
        final QueueEntry blockedHigh = this.entry("blocked", ComputePriority.HIGH);
        final QueueEntry blockedNormal = this.entry("blocked", ComputePriority.NORMAL);
        final QueueEntry open = this.entry("open", ComputePriority.NORMAL);
        queue.offer(blockedHigh);
        queue.offer(blockedNormal);
        queue.offer(open);

        final ConsumerAdmission refuseBlocked = consumerId -> !consumerId.equals("blocked");
        assertEquals(open, queue.poll(refuseBlocked));
        assertEquals(2, queue.size("blocked"));
        assertNull(queue.poll(refuseBlocked));

        assertEquals(blockedHigh, queue.poll(ADMIT_ALL));
        assertEquals(blockedNormal, queue.poll(ADMIT_ALL));
        assertTrue(queue.isEmpty());
    }

    /**
     * Creates a queue with the default consumer policy and the given ordering configuration.
     *
     * @param strategy ordering strategy
     * @param aging    whether aging is enabled
     * @param boost    maximum aging boost
     * @return the queue
     */
    private BoundedComputeQueue queue(
        final ComputeSettings.Scheduling.Strategy strategy,
        final boolean aging,
        final int boost
    ) {
        return new BoundedComputeQueue(
            10_000,
            TestSupport.scheduling(strategy, aging, boost, false),
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
