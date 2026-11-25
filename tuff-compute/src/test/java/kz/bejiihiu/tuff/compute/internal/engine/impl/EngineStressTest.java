package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * High volume mixed workload: four producers, four consumers, cancellations and rejections.
 *
 * <p>This is the test that catches ownership leaks and lost entries: every accepted task must be
 * executed at most once, every handle must reach a terminal state, and the counter invariant must
 * still hold afterwards.
 */
@Timeout(180)
class EngineStressTest {

    private static final int TASKS = 100_000;
    private static final int PRODUCERS = 4;
    private static final String[] CONSUMERS = {"stress-a", "stress-b", "stress-c", "stress-d"};

    @Test
    void hundredThousandTasksWithCancellationsAndRejectionsStayConsistent() throws Exception {
        final EngineTestSupport.SettingsBuilder builder =
            settings().fixedWorkers(4).capacity(64).grace(Duration.ofSeconds(10));
        final String prefix = builder.prefix();
        final AsyncComputeScheduler compute = startedScheduler(builder.build(), new EngineTestSupport.FakeClock());
        final AtomicIntegerArray executions = new AtomicIntegerArray(TASKS);
        final ConcurrentLinkedQueue<ComputeTaskHandle<Integer>> accepted = new ConcurrentLinkedQueue<>();
        final AtomicInteger rejected = new AtomicInteger();
        final AtomicLong cancels = new AtomicLong();
        try {
            for (final String consumer : CONSUMERS) {
                compute.registerConsumer(ComputeConsumer.of(
                    consumer,
                    Set.of(ComputeTaskType.GENERAL),
                    ComputePriority.CRITICAL,
                    ConsumerPolicy.defaults()
                ));
            }

            final CountDownLatch start = new CountDownLatch(1);
            final Thread[] producers = new Thread[PRODUCERS];
            for (int producer = 0; producer < PRODUCERS; producer++) {
                final int producerIndex = producer;
                producers[producer] = new Thread(() -> this.produce(compute, executions, accepted, rejected, cancels, start, producerIndex));
                producers[producer].setName("stress-producer-" + producer);
                producers[producer].start();
            }
            start.countDown();
            for (final Thread producer : producers) {
                producer.join(TimeUnit.SECONDS.toMillis(60L));
            }
            for (final Thread producer : producers) {
                assertFalse(producer.isAlive(), "producer did not finish in time");
            }

            assertEventually("all accepted tasks reach a terminal state", Duration.ofSeconds(90), () -> {
                for (final ComputeTaskHandle<Integer> handle : accepted) {
                    if (!handle.isDone()) {
                        return false;
                    }
                }
                return true;
            });
            assertEventually("the queue drains", Duration.ofSeconds(30), () -> compute.metrics().queuedTasks() == 0);

            for (int task = 0; task < TASKS; task++) {
                assertTrue(executions.get(task) <= 1, "task " + task + " executed more than once");
            }
            assertTrue(rejected.get() > 0, "the small queue must have produced rejections");
            assertTrue(cancels.get() > 0, "the workload must have produced cancellations");

            final long settleDeadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            SchedulerMetrics last = compute.metrics();
            boolean settled = false;
            while (!settled && System.nanoTime() < settleDeadline) {
                last = compute.metrics();
                settled = last.submittedTasks() == last.completedTasks() + last.failedTasks()
                    + last.cancelledTasks() + last.queuedTasks() + last.activeWorkers();
                if (!settled) {
                    Thread.sleep(1L);
                }
            }
            assertTrue(settled, "counters must settle: submitted=" + last.submittedTasks()
                + " completed=" + last.completedTasks() + " failed=" + last.failedTasks()
                + " cancelled=" + last.cancelledTasks() + " queued=" + last.queuedTasks()
                + " active=" + last.activeWorkers() + " rejected=" + last.rejectedTasks());
            final SchedulerMetrics metrics = compute.metrics();
            assertEquals(TASKS, metrics.submittedTasks());
            assertTrue(metrics.completedTasks() > TASKS / 2L, "most tasks must actually complete");
            assertEquals(
                metrics.submittedTasks(),
                metrics.completedTasks() + metrics.failedTasks() + metrics.cancelledTasks()
                    + metrics.queuedTasks() + metrics.activeWorkers()
            );
            assertTrue(metrics.rejectedTasks() > 0, "rejections are counted separately");
            long executed = 0L;
            for (int task = 0; task < TASKS; task++) {
                executed += executions.get(task);
            }
            assertTrue(executed >= metrics.completedTasks(), "every completed task ran at least once");
        } finally {
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(30)));
        }
        EngineTestSupport.awaitNoEngineThreads(prefix, Duration.ofSeconds(10));
    }

    private void produce(
        final AsyncComputeScheduler compute,
        final AtomicIntegerArray executions,
        final ConcurrentLinkedQueue<ComputeTaskHandle<Integer>> accepted,
        final AtomicInteger rejected,
        final AtomicLong cancels,
        final CountDownLatch start,
        final int producerIndex
    ) {
        final String consumer = CONSUMERS[producerIndex];
        final int perProducer = TASKS / PRODUCERS;
        final ComputePriority[] priorities = ComputePriority.values();
        int localAccepted = 0;
        try {
            start.await();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            for (int offset = 0; offset < perProducer; offset++) {
                final int taskId = producerIndex * perProducer + offset;
                final ComputePriority priority = priorities[taskId % priorities.length];
                ComputeTaskHandle<Integer> handle = null;
                while (handle == null) {
                    try {
                        handle = compute.enqueue(new ComputeRequest<>(
                            consumer,
                            ComputeTaskType.GENERAL,
                            priority,
                            () -> work(taskId, executions)
                        ));
                    } catch (ComputeRejectedException _) {
                        rejected.incrementAndGet();
                        Thread.yield();
                    }
                }
                accepted.add(handle);
                localAccepted++;
                if ((taskId & 15) == 0) {
                    handle.cancel();
                    cancels.incrementAndGet();
                }
            }
        } catch (Throwable escaped) {
            System.out.println("producer " + producerIndex + " died after " + localAccepted + " accepted");
            escaped.printStackTrace(System.out);
        }
    }

    private static int work(final int taskId, final AtomicIntegerArray executions) {
        executions.incrementAndGet(taskId);
        int value = taskId;
        for (int step = 0; step < 2048; step++) {
            value = value * 31 + step;
        }
        return value;
    }
}
