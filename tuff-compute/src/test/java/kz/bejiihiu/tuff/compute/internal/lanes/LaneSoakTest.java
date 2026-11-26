package kz.bejiihiu.tuff.compute.internal.lanes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.NEVER;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.SettingsBuilder;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertSettled;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertStateTerminal;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.awaitNoThreads;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.bounded;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.cancellationOf;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.expiring;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Thirty seconds of mixed lane traffic with cancellations, rejections, evictions, expiry and
 * concurrent scheduler lifecycles.
 *
 * <p>This is the test that would catch a lost terminal transition: every handle the scheduler
 * accepted must be terminal afterwards, and the counters must add up to that same set.
 */
@Timeout(180)
class LaneSoakTest {

    private static final Duration TRAFFIC = Duration.ofSeconds(30L);

    @Test
    void mixedTrafficNeverLosesATaskOrLeavesAThreadBehind() throws Exception {
        final SettingsBuilder settings = SettingsBuilder.of("soak")
            .fixedWorkers(3)
            .capacity(96)
            .overflow(OverflowPolicy.DROP_OLDEST)
            .virtualLane(true, true, 4, millis(20L), 4)
            .grace(Duration.ofSeconds(10L));
        final AsyncComputeScheduler compute = start(settings, ComputeClock.system());
        compute.registerConsumer(ComputeConsumer.of("bulk", bounded(2, 24, OverflowPolicy.DROP_OLDEST)));
        compute.registerConsumer(ComputeConsumer.of("expiring", expiring(2, millis(10L))));
        compute.registerConsumer(ComputeConsumer.of("tiny", bounded(1, 2, OverflowPolicy.REJECT)));
        compute.registerConsumer(ComputeConsumer.of("evict", bounded(1, 1, OverflowPolicy.DROP_OLDEST)));

        final ConcurrentLinkedQueue<ComputeTaskHandle<?>> handles = new ConcurrentLinkedQueue<>();
        final AtomicLong accepted = new AtomicLong();
        final AtomicLong rejected = new AtomicLong();
        final AtomicLong ran = new AtomicLong();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final long deadline = System.nanoTime() + TRAFFIC.toNanos();
        final List<Thread> workers = new ArrayList<>();
        final Set<String> cyclePrefixes = ConcurrentHashMap.newKeySet();

        for (final String consumer : List.of("bulk", "expiring", "tiny", "evict")) {
            final Thread producer = new Thread(() -> this.produce(compute, consumer, deadline, handles, accepted,
                rejected, ran, failure), "soak-producer-" + consumer);
            producer.setDaemon(true);
            workers.add(producer);
        }
        final Thread canceller = new Thread(() -> this.cancelRandom(handles, deadline, failure), "soak-canceller");
        canceller.setDaemon(true);
        final Thread cycles = new Thread(() -> this.runLifecycles(deadline, cyclePrefixes, failure), "soak-cycles");
        cycles.setDaemon(true);
        workers.add(canceller);
        workers.add(cycles);

        try {
            for (final Thread worker : workers) {
                worker.start();
            }
            for (final Thread worker : workers) {
                worker.join(TimeUnit.SECONDS.toMillis(60L));
                assertTrue(!worker.isAlive(), "a soak thread did not stop: " + worker.getName());
            }
            assertNull(failure.get(), "a soak thread failed: " + failure.get());

            assertTrue(accepted.get() > 500L, "the soak did not accept enough work: " + accepted.get());
            assertTrue(rejected.get() > 0L, "the bounded consumer never refused work");
            assertTrue(ran.get() > 100L);

            // every accepted handle must be terminal, whatever path it took
            assertEventually("every accepted handle reached a terminal state", Duration.ofSeconds(60L), () -> {
                for (final ComputeTaskHandle<?> handle : handles) {
                    if (!handle.isDone()) {
                        return false;
                    }
                }
                return true;
            });

            compute.shutdown(Duration.ofSeconds(15L));
            assertStateTerminal(compute, Duration.ofSeconds(30L));
            assertSettled(compute, Duration.ofSeconds(30L));
            assertEventually("accepted submissions match the submitted counter", NEVER,
                () -> compute.metrics().submittedTasks() == accepted.get());

            final SchedulerMetrics metrics = compute.metrics();
            assertEquals(accepted.get(), metrics.submittedTasks());
            assertTrue(metrics.cancelledTasks() > 0L, "no task was cancelled");
            assertTrue(metrics.failedTasks() > 0L, "no task failed");
            assertTrue(metrics.expiredTasks() > 0L, "the expiring consumer never expired a task");
            assertEquals(rejected.get(), metrics.rejectedTasks(), "rejections were counted twice or lost");

            boolean evicted = false;
            for (final ComputeTaskHandle<?> handle : handles) {
                if (handle.isCancelled()
                    && cancellationOf(handle.future()).reason() == ComputeCancelledException.Reason.EVICTED) {
                    evicted = true;
                    break;
                }
            }
            assertTrue(evicted, "drop-oldest never evicted a queued task");

            assertEquals(accepted.get(), handles.size());
            awaitNoThreads(settings.prefix(), Duration.ofSeconds(30L));
            for (final String prefix : cyclePrefixes) {
                awaitNoThreads(prefix, Duration.ofSeconds(15L));
            }
        } finally {
            compute.shutdown(Duration.ofSeconds(15L));
        }
    }

    private void produce(
        final AsyncComputeScheduler compute,
        final String consumer,
        final long deadline,
        final ConcurrentLinkedQueue<ComputeTaskHandle<?>> handles,
        final AtomicLong accepted,
        final AtomicLong rejected,
        final AtomicLong ran,
        final AtomicReference<Throwable> failure
    ) {
        try {
            long index = 0L;
            while (System.nanoTime() < deadline && failure.get() == null) {
                final long sequence = index++;
                final ComputePriority priority = switch ((int) (sequence % 20)) {
                    case 0, 1, 2, 3, 4 -> ComputePriority.LOW;
                    case 5, 6 -> ComputePriority.HIGH;
                    case 7 -> ComputePriority.CRITICAL;
                    default -> ComputePriority.NORMAL;
                };
                final boolean fails = sequence % 53 == 0;
                try {
                    final ComputeTaskHandle<Long> handle = compute.enqueue(new ComputeRequest<>(
                        consumer,
                        ComputeTaskType.GENERAL,
                        priority,
                        () -> {
                            ran.incrementAndGet();
                            // synthetic workload, not a synchronisation wait
                            LockSupport.parkNanos(5_000_000L);
                            if (fails) {
                                throw new IllegalStateException("planned soak failure " + sequence);
                            }
                            return sequence;
                        },
                        consumer + '-' + sequence
                    ));
                    accepted.incrementAndGet();
                    handles.add(handle);
                    if (sequence % 31 == 0) {
                        handle.cancel();
                    }
                } catch (ComputeRejectedException _) {
                    rejected.incrementAndGet();
                }
                // keep the producers from flooding the heap with handles; the slow task bodies are
                // what actually saturate the queues
                LockSupport.parkNanos(1_000_000L);
            }
        } catch (final Throwable thrown) {
            failure.compareAndSet(null, thrown);
        }
    }

    private void cancelRandom(
        final ConcurrentLinkedQueue<ComputeTaskHandle<?>> handles,
        final long deadline,
        final AtomicReference<Throwable> failure
    ) {
        try {
            long seed = 0x5DEECE66DL;
            while (System.nanoTime() < deadline && failure.get() == null) {
                final int size = handles.size();
                if (size > 0) {
                    seed = seed * 6364136223846793005L + 1442695040888963407L;
                    int target = (int) Math.floorMod(seed >>> 17, size);
                    for (final ComputeTaskHandle<?> handle : handles) {
                        if (target-- <= 0) {
                            handle.cancel();
                            break;
                        }
                    }
                }
                LockSupport.parkNanos(5_000_000L);
            }
        } catch (final Throwable thrown) {
            failure.compareAndSet(null, thrown);
        }
    }

    private void runLifecycles(
        final long deadline,
        final Set<String> prefixes,
        final AtomicReference<Throwable> failure
    ) {
        try {
            int cycle = 0;
            while (System.nanoTime() < deadline && failure.get() == null) {
                final SettingsBuilder settings = SettingsBuilder.of("soak-cycle-" + cycle++)
                    .fixedWorkers(2)
                    .capacity(64)
                    .virtualLane(true, true, 2, millis(10L), 2)
                    .grace(Duration.ofSeconds(2L));
                final String prefix = settings.prefix();
                prefixes.add(prefix);
                final AsyncComputeScheduler scheduler = start(settings, ComputeClock.system());
                try {
                    final List<ComputeTaskHandle<?>> cycleHandles = new ArrayList<>();
                    for (int i = 0; i < 40; i++) {
                        final ComputePriority priority = i % 3 == 0 ? ComputePriority.LOW : ComputePriority.NORMAL;
                        cycleHandles.add(scheduler.enqueue(new ComputeRequest<>(
                            AsyncComputeScheduler.DEFAULT_CONSUMER_ID,
                            ComputeTaskType.GENERAL,
                            priority,
                            () -> {
                                LockSupport.parkNanos(1_000_000L);
                                return "cycle";
                            }
                        )));
                        if (i % 7 == 0) {
                            cycleHandles.get(i).cancel();
                        }
                    }
                    scheduler.shutdown(Duration.ofSeconds(2L));
                    if (!scheduler.awaitTermination(Duration.ofSeconds(15L))) {
                        throw new AssertionError("a soak lifecycle did not terminate in time");
                    }
                } finally {
                    scheduler.shutdown(Duration.ofSeconds(2L));
                }
                awaitNoThreads(prefix, Duration.ofSeconds(15L));
                LockSupport.parkNanos(1_000_000_000L);
            }
        } catch (final Throwable thrown) {
            failure.compareAndSet(null, thrown);
        }
    }
}
