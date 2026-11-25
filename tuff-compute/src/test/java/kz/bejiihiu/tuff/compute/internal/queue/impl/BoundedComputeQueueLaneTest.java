package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.LaneAwareQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Level masks are what keep LOW work off the platform lane and spilled NORMAL work off the default
 * virtual mask, without walking the queue.
 */
class BoundedComputeQueueLaneTest {

    private static final int LOW = 1 << ComputePriority.LOW.level();
    private static final int NORMAL = 1 << ComputePriority.NORMAL.level();
    private static final int HIGH = 1 << ComputePriority.HIGH.level();
    private static final int PLATFORM = NORMAL | HIGH | (1 << ComputePriority.CRITICAL.level());
    private static final int VIRTUAL = LOW | NORMAL;

    @Test
    void maskKeepsLowOffThePlatformLaneAndPreservesFifoInsideALane() {
        final TestSupport.TestClock clock = new TestSupport.TestClock();
        final BoundedComputeQueue queue = new BoundedComputeQueue(
            32,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.FIFO, false, 3, false),
            ConsumerPolicy.defaults(),
            clock
        );
        queue.registerConsumer("c", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));

        queue.offer(TestSupport.entry(1L, "c", ComputePriority.LOW, 1L, 0L));
        queue.offer(TestSupport.entry(2L, "c", ComputePriority.NORMAL, 2L, 0L));
        queue.offer(TestSupport.entry(3L, "c", ComputePriority.LOW, 3L, 0L));
        queue.offer(TestSupport.entry(4L, "c", ComputePriority.HIGH, 4L, 0L));

        assertEquals(2, queue.queuedAtLevel(ComputePriority.LOW.level()));
        assertEquals(1, queue.queuedAtLevel(ComputePriority.NORMAL.level()));
        assertEquals(3, queue.queuedInLane(LaneAwareQueue.Lane.VIRTUAL, LOW | NORMAL));

        final QueueEntry firstPlatform = queue.poll(LaneAwareQueue.Lane.PLATFORM, PLATFORM, consumer -> true);
        assertNotNull(firstPlatform);
        assertEquals(2L, firstPlatform.taskId(), "platform lane must take the oldest non-LOW entry");

        final QueueEntry firstLow = queue.poll(LaneAwareQueue.Lane.VIRTUAL, LOW, consumer -> true);
        assertNotNull(firstLow);
        assertEquals(1L, firstLow.taskId());
        final QueueEntry secondLow = queue.poll(LaneAwareQueue.Lane.VIRTUAL, LOW, consumer -> true);
        assertNotNull(secondLow);
        assertEquals(3L, secondLow.taskId(), "LOW tasks must not overtake each other");
        assertNull(queue.poll(LaneAwareQueue.Lane.VIRTUAL, LOW, consumer -> true));

        final QueueEntry high = queue.poll(LaneAwareQueue.Lane.PLATFORM, PLATFORM, consumer -> true);
        assertNotNull(high);
        assertEquals(4L, high.taskId());
        assertNull(queue.poll(LaneAwareQueue.Lane.PLATFORM, PLATFORM, consumer -> true));
    }

    @Test
    void oldestWaitIsComputedFromTheLevelHead() {
        final TestSupport.TestClock clock = new TestSupport.TestClock();
        final BoundedComputeQueue queue = new BoundedComputeQueue(
            32,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.FIFO, false, 3, false),
            ConsumerPolicy.defaults(),
            clock
        );
        queue.registerConsumer("c", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        queue.offer(TestSupport.entry(1L, "c", ComputePriority.NORMAL, 1L, 0L));
        clock.advance(Duration.ofMillis(250L));

        assertEquals(Duration.ofMillis(250L).toNanos(), queue.oldestWaitNanos(ComputePriority.NORMAL.level(), clock.nanoTime()));
        assertEquals(0L, queue.oldestWaitNanos(ComputePriority.LOW.level(), clock.nanoTime()));
    }
}
