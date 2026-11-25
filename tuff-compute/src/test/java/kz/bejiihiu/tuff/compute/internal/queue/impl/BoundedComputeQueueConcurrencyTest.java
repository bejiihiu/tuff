package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue.OfferResult;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedComputeQueueConcurrencyTest {

    private static final int PRODUCERS = 8;
    private static final int PER_PRODUCER = 50_000;
    private static final int CONSUMERS = 4;
    private static final int TOTAL = PRODUCERS * PER_PRODUCER;
    private static final ComputePriority[] PRIORITIES = ComputePriority.values();

    @Test
    @Timeout(180)
    void manyProducersAndConsumersLoseNothing() throws Exception {
        final BoundedComputeQueue queue = new BoundedComputeQueue(
            8192,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR, true, 3, true),
            ConsumerPolicy.defaults(),
            ComputeClock.system()
        );
        final AtomicIntegerArray seen = new AtomicIntegerArray(TOTAL);
        final AtomicInteger producersLeft = new AtomicInteger(PRODUCERS);
        final AtomicLong consumed = new AtomicLong();
        final CyclicBarrier start = new CyclicBarrier(PRODUCERS + CONSUMERS);
        final List<Thread> workers = new ArrayList<>(PRODUCERS + CONSUMERS);

        for (int producer = 0; producer < PRODUCERS; producer++) {
            final int producerIndex = producer;
            final Thread thread = daemon("producer-" + producer, () -> {
                await(start);
                for (int index = 0; index < PER_PRODUCER; index++) {
                    final long taskId = (long) producerIndex * PER_PRODUCER + index;
                    offerUntilAccepted(queue, newEntry(taskId));
                }
                producersLeft.decrementAndGet();
            });
            workers.add(thread);
            thread.start();
        }
        for (int consumer = 0; consumer < CONSUMERS; consumer++) {
            final Thread thread = daemon("consumer-" + consumer, () -> {
                await(start);
                long claimed = 0L;
                while (true) {
                    final QueueEntry entry = queue.poll(consumerId -> true);
                    if (entry != null) {
                        seen.incrementAndGet((int) entry.taskId());
                        claimed++;
                        continue;
                    }
                    if (producersLeft.get() == 0 && queue.isEmpty()) {
                        break;
                    }
                    Thread.onSpinWait();
                }
                consumed.addAndGet(claimed);
            });
            workers.add(thread);
            thread.start();
        }

        joinAll(workers);

        assertEquals(TOTAL, consumed.get());
        for (int index = 0; index < TOTAL; index++) {
            assertEquals(1, seen.get(index), "task " + index + " was delivered " + seen.get(index) + " times");
        }
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertEquals(0, queue.snapshot().size());
        assertEquals(TOTAL, queue.snapshot().accepted());
    }

    @Test
    @Timeout(180)
    void unregisterDuringTrafficAccountsForEveryEntry() throws Exception {
        final int perProducer = 5_000;
        final int total = 4 * perProducer;
        final BoundedComputeQueue queue = new BoundedComputeQueue(
            4096,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR, false, 1, false),
            ConsumerPolicy.defaults(),
            ComputeClock.system()
        );
        final AtomicIntegerArray seen = new AtomicIntegerArray(total);
        final AtomicInteger producersLeft = new AtomicInteger(4);
        final CyclicBarrier start = new CyclicBarrier(6);
        final CountDownLatch unregister = new CountDownLatch(1);
        final List<Thread> workers = new ArrayList<>();
        final List<QueueEntry> detached = new ArrayList<>();

        for (int producer = 0; producer < 4; producer++) {
            final int producerIndex = producer;
            final Thread thread = daemon("unregister-producer-" + producer, () -> {
                await(start);
                for (int index = 0; index < perProducer; index++) {
                    final long taskId = (long) producerIndex * perProducer + index;
                    offerUntilAccepted(queue, newEntry(taskId));
                    if (producerIndex == 0 && index == 999) {
                        unregister.countDown();
                    }
                }
                producersLeft.decrementAndGet();
            });
            workers.add(thread);
            thread.start();
        }

        final Thread unregisterThread = daemon("unregister", () -> {
            await(start);
            try {
                if (!unregister.await(60, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("producers never reached the unregister point");
                }
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                return;
            }
            synchronized (detached) {
                detached.addAll(queue.unregisterConsumer("c0"));
            }
        });
        workers.add(unregisterThread);
        unregisterThread.start();

        final Thread drainer = daemon("unregister-drainer", () -> {
            await(start);
            while (true) {
                final QueueEntry entry = queue.poll(consumerId -> true);
                if (entry != null) {
                    seen.incrementAndGet((int) entry.taskId());
                    continue;
                }
                if (producersLeft.get() == 0 && queue.isEmpty()) {
                    break;
                }
                Thread.onSpinWait();
            }
        });
        workers.add(drainer);
        drainer.start();

        joinAll(workers);

        synchronized (detached) {
            for (final QueueEntry entry : detached) {
                seen.incrementAndGet((int) entry.taskId());
            }
        }

        assertEquals(total, countSeen(seen, total));
        for (int index = 0; index < total; index++) {
            assertEquals(1, seen.get(index), "task " + index + " was delivered " + seen.get(index) + " times");
        }
        assertTrue(queue.isEmpty());
    }

    @Test
    @Timeout(180)
    void cancelDuringTrafficDeliversEachEntryExactlyOnce() throws Exception {
        final int perProducer = 10_000;
        final int total = 2 * perProducer;
        final BoundedComputeQueue queue = new BoundedComputeQueue(
            2048,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR, false, 1, false),
            ConsumerPolicy.defaults(),
            ComputeClock.system()
        );
        final AtomicIntegerArray seen = new AtomicIntegerArray(total);
        final AtomicInteger producersLeft = new AtomicInteger(2);
        final CyclicBarrier start = new CyclicBarrier(4);
        final ConcurrentLinkedQueue<QueueEntry> accepted = new ConcurrentLinkedQueue<>();
        final List<Thread> workers = new ArrayList<>();

        for (int producer = 0; producer < 2; producer++) {
            final int producerIndex = producer;
            final Thread thread = daemon("cancel-producer-" + producer, () -> {
                await(start);
                for (int index = 0; index < perProducer; index++) {
                    final long taskId = (long) producerIndex * perProducer + index;
                    final QueueEntry entry = newEntry(taskId);
                    offerUntilAccepted(queue, entry);
                    accepted.add(entry);
                }
                producersLeft.decrementAndGet();
            });
            workers.add(thread);
            thread.start();
        }

        final Thread canceller = daemon("canceller", () -> {
            await(start);
            while (true) {
                final QueueEntry entry = accepted.poll();
                if (entry != null) {
                    if (queue.remove(entry)) {
                        seen.incrementAndGet((int) entry.taskId());
                    }
                    continue;
                }
                if (producersLeft.get() == 0) {
                    break;
                }
                Thread.onSpinWait();
            }
        });
        workers.add(canceller);
        canceller.start();

        final Thread drainer = daemon("cancel-drainer", () -> {
            await(start);
            while (true) {
                final QueueEntry entry = queue.poll(consumerId -> true);
                if (entry != null) {
                    seen.incrementAndGet((int) entry.taskId());
                    continue;
                }
                if (producersLeft.get() == 0 && queue.isEmpty()) {
                    break;
                }
                Thread.onSpinWait();
            }
        });
        workers.add(drainer);
        drainer.start();

        joinAll(workers);

        assertEquals(total, countSeen(seen, total));
        for (int index = 0; index < total; index++) {
            assertEquals(1, seen.get(index), "task " + index + " was delivered " + seen.get(index) + " times");
        }
        assertTrue(queue.isEmpty());
        assertEquals(total, queue.snapshot().accepted());
        assertNotNull(queue.snapshot().perConsumerSize());
    }

    /**
     * Creates an entry whose consumer and priority vary with the task id.
     *
     * @param taskId globally unique task id
     * @return the entry
     */
    private static QueueEntry newEntry(final long taskId) {
        return TestSupport.entry(
            taskId,
            "c" + (taskId % 4),
            PRIORITIES[(int) (taskId % PRIORITIES.length)],
            taskId,
            System.nanoTime()
        );
    }

    /**
     * Offers until the bounded queue accepts, which is the producer side of backpressure.
     *
     * @param queue queue to offer to
     * @param entry entry to offer
     * @throws IllegalStateException if the queue was closed while the test was running
     */
    private static void offerUntilAccepted(final BoundedComputeQueue queue, final QueueEntry entry) {
        while (true) {
            final OfferResult result = queue.offer(entry);
            if (result == OfferResult.ACCEPTED) {
                return;
            }
            if (result == OfferResult.CLOSED) {
                throw new IllegalStateException("queue closed while the test was running");
            }
            Thread.onSpinWait();
        }
    }

    /**
     * Counts how many slots of the seen array are non zero.
     *
     * @param seen  delivery counters
     * @param total number of slots
     * @return the number of delivered tasks
     */
    private static int countSeen(final AtomicIntegerArray seen, final int total) {
        int delivered = 0;
        for (int index = 0; index < total; index++) {
            if (seen.get(index) != 0) {
                delivered++;
            }
        }
        return delivered;
    }

    /**
     * Awaits a barrier, converting the checked failures into an unchecked one.
     *
     * @param barrier barrier to await
     * @throws IllegalStateException if the barrier was broken or the wait was interrupted
     */
    private static void await(final CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (final Exception failure) {
            throw new IllegalStateException("test barrier failed", failure);
        }
    }

    /**
     * Joins every worker with a generous ceiling.
     *
     * @param workers workers to join
     * @throws InterruptedException if the test thread was interrupted
     */
    private static void joinAll(final List<Thread> workers) throws InterruptedException {
        for (final Thread worker : workers) {
            worker.join(TimeUnit.SECONDS.toMillis(120));
            assertTrue(!worker.isAlive(), worker.getName() + " did not finish");
        }
    }

    /**
     * Creates a daemon thread so a failing assertion cannot keep the JVM alive.
     *
     * @param name thread name
     * @param body body to run
     * @return the unstarted thread
     */
    private static Thread daemon(final String name, final Runnable body) {
        return Thread.ofPlatform().daemon(true).name(name).unstarted(body);
    }
}
