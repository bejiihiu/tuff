package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue.OfferResult;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One incoming submission may evict at most one of its own queued entries, even when competitors keep
 * stealing the freed slot.
 */
@Timeout(180)
class BoundedComputeQueueDropOldestConcurrencyTest {

    private static final int PRODUCERS = 8;
    private static final int PER_PRODUCER = 4_000;
    private static final int TOTAL = PRODUCERS * PER_PRODUCER;

    @Test
    void oneOfferEvictsAtMostOneEntryPerProducerThread() throws Exception {
        final BoundedComputeQueue queue = new BoundedComputeQueue(
            4096,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR, false, 1, false),
            TestSupport.policy(8, 1.0d, OverflowPolicy.DROP_OLDEST),
            ComputeClock.system()
        );
        queue.registerConsumer("hot", TestSupport.policy(8, 1.0d, OverflowPolicy.DROP_OLDEST));

        final AtomicIntegerArray delivered = new AtomicIntegerArray(TOTAL);
        final AtomicInteger callbacks = new AtomicInteger();
        final Map<Thread, AtomicInteger> evictionsByThread = new ConcurrentHashMap<>();
        queue.setEvictionListener(evicted -> {
            delivered.incrementAndGet((int) evicted.taskId());
            callbacks.incrementAndGet();
            evictionsByThread.computeIfAbsent(Thread.currentThread(), ignored -> new AtomicInteger()).incrementAndGet();
        });

        final CyclicBarrier start = new CyclicBarrier(PRODUCERS);
        final List<Thread> threads = new ArrayList<>(PRODUCERS);
        final AtomicInteger multiEvictions = new AtomicInteger();
        for (int producer = 0; producer < PRODUCERS; producer++) {
            final int producerIndex = producer;
            final Thread thread = Thread.ofPlatform().daemon(true).unstarted(() -> {
                await(start);
                final AtomicInteger mine = evictionsByThread.computeIfAbsent(
                    Thread.currentThread(),
                    ignored -> new AtomicInteger()
                );
                for (int index = 0; index < PER_PRODUCER; index++) {
                    final long taskId = (long) producerIndex * PER_PRODUCER + index;
                    final int before = mine.get();
                    queue.offer(TestSupport.entry(
                        taskId,
                        "hot",
                        ComputePriority.NORMAL,
                        taskId,
                        System.nanoTime()
                    ));
                    if (mine.get() - before > 1) {
                        multiEvictions.incrementAndGet();
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (final Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(120));
            assertTrue(!thread.isAlive(), "producer did not finish");
        }

        assertEquals(0, multiEvictions.get(), "one offer must never evict more than one entry");
        assertEquals(queue.snapshot().evicted(), callbacks.get(), "every eviction must be reported exactly once");
        for (int taskId = 0; taskId < TOTAL; taskId++) {
            assertTrue(delivered.get(taskId) <= 1, "task " + taskId + " was evicted " + delivered.get(taskId) + " times");
        }
        assertEquals(OfferResult.ACCEPTED, queue.offer(TestSupport.entry(
            TOTAL + 1L,
            "hot",
            ComputePriority.NORMAL,
            TOTAL + 1L,
            System.nanoTime()
        )));
    }

    private static void await(final CyclicBarrier barrier) {
        try {
            barrier.await(30L, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException("test barrier failed", failure);
        }
    }
}
