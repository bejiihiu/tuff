package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.queue.impl.BoundedComputeQueue;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.jspecify.annotations.Nullable;

/**
 * Thin adapter over the real {@link BoundedComputeQueue}.
 *
 * <p>Tests use the production queue so the pool is exercised against the same ordering, capacity
 * and stealing code the engine will use. The adapter only assigns ids/sequences and builds the
 * {@link QueueEntry} the queue needs.
 */
final class TestTaskQueue {

    private static final ComputeQueue.ConsumerAdmission ALWAYS_ADMIT = consumerId -> true;
    private static final String CONSUMER = "default";

    private final BoundedComputeQueue queue;
    private final AtomicLong ids = new AtomicLong();
    private final AtomicLong sequences = new AtomicLong();

    TestTaskQueue() {
        this(false);
    }

    TestTaskQueue(final boolean workStealing) {
        final ComputeSettings.Scheduling scheduling = new ComputeSettings.Scheduling(
            ComputeSettings.Scheduling.Strategy.FIFO,
            workStealing,
            false,
            Duration.ofSeconds(1),
            1,
            Duration.ofMillis(1));
        this.queue = new BoundedComputeQueue(1_000_000, scheduling, ConsumerPolicy.defaults(), ComputeClock.system());
    }

    void submit(final TestTask task) {
        final long id = this.ids.incrementAndGet();
        final QueueEntry entry = new QueueEntry(
            id,
            CONSUMER,
            task.handle.taskType(),
            ComputePriority.NORMAL,
            this.sequences.incrementAndGet(),
            System.nanoTime(),
            task,
            task.handle);
        final ComputeQueue.OfferResult result = this.queue.offer(entry);
        if (result != ComputeQueue.OfferResult.ACCEPTED) {
            throw new IllegalStateException("test queue refused entry: " + result);
        }
    }

    @Nullable QueueEntry poll(final int workerIndex) {
        return this.queue.poll(ALWAYS_ADMIT);
    }

    @Nullable QueueEntry steal(final int thiefIndex) {
        return this.queue.steal(thiefIndex, ALWAYS_ADMIT);
    }
}
