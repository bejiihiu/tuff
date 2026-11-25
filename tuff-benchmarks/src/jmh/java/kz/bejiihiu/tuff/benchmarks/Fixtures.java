package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;

/**
 * Shared fixtures for the compute benchmarks.
 *
 * <p>Everything here is benchmark scaffolding: settings factories, task bodies with a known size and
 * a few bounded waits. The waits spin on purpose - a benchmark that sleeps is measuring the OS timer
 * instead of the code under test, and a silent infinite wait would hang the whole JMH fork.
 */
final class Fixtures {

    /** Consumer id used by the single-consumer scenarios. */
    static final String CONSUMER = "bench";

    /** Thread name prefix of every benchmark scheduler, also used for the leak check. */
    static final String PREFIX = "jmh-compute";

    /** Server-wide metrics sampler thread prefix; the engine uses a fixed name for it. */
    static final String METRICS_PREFIX = "Tuff Compute Metrics";

    /** Longest a fixture waits for a worker to reach a state before giving up. */
    private static final Duration READY_TIMEOUT = Duration.ofSeconds(10L);

    /** Empty task body: measures the scheduler around it, not the work. */
    static final ComputeTask<Boolean> NOOP = () -> Boolean.TRUE;

    /** ~0.5-0.7 us of dependent integer work, on the order of a tiny game-logic query. */
    static final ComputeTask<Long> SHORT = () -> mix(256);

    private Fixtures() {
    }

    /**
     * Builds a settings tree for a benchmark run.
     *
     * @param workers       fixed worker count
     * @param capacity      global queue capacity
     * @param overflow      overflow policy of the default consumer
     * @param strategy      dispatch strategy
     * @param aging         whether priority aging is enabled
     * @param agingInterval aging step interval
     * @param idleWait      how long an idle worker parks between polls
     * @param metrics       whether the metrics collector (and its sampler thread) is enabled
     * @return immutable settings
     */
    static ComputeSettings settings(
        final int workers,
        final int capacity,
        final OverflowPolicy overflow,
        final ComputeSettings.Scheduling.Strategy strategy,
        final boolean aging,
        final Duration agingInterval,
        final Duration idleWait,
        final boolean metrics
    ) {
        return new ComputeSettings(
            true,
            new ComputeSettings.Workers(workers, 1, -1, Duration.ofSeconds(30L), PREFIX),
            new ComputeSettings.QueueSettings(capacity, overflow),
            new ComputeSettings.Scheduling(strategy, false, aging, agingInterval, 3, idleWait),
            new ComputeSettings.Consumers(ConsumerPolicy.defaults(), Map.of(), true),
            new ComputeSettings.Metrics(metrics, false, Duration.ofSeconds(10L), false, Duration.ofSeconds(1L)),
            ComputeSettings.Adaptive.defaults(),
            Duration.ofSeconds(1L)
        );
    }

    /**
     * Settings for scenarios that need a saturated pool: weighted fair scheduling, aging off, a one
     * millisecond idle wait and metrics off.
     *
     * <p>The idle wait is short but not zero: a zero wait turns idle workers into a spin loop that
     * fights the submitters for the queue and the concurrency gate. What an idle pool costs is
     * measured by the latency benchmark instead.
     *
     * @param workers  fixed worker count
     * @param capacity global queue capacity
     * @return immutable settings
     */
    static ComputeSettings saturated(final int workers, final int capacity) {
        return settings(
            workers,
            capacity,
            OverflowPolicy.REJECT,
            ComputeSettings.Scheduling.Strategy.FIFO,
            false,
            Duration.ofMillis(1L),
            Duration.ofMillis(1L),
            false
        );
    }

    /**
     * Submits a task that holds one worker until the gate opens.
     *
     * <p>The returned handle is the one the caller must {@link CountDownLatch#countDown()} to release.
     * The method only returns once the worker reports {@code RUNNING}, so a queue filled afterwards is
     * really queued and not racing with the worker.
     *
     * @param scheduler scheduler to block
     * @param gate      latch the blocking body waits on
     * @return handle of the blocking task
     */
    static ComputeTaskHandle<Boolean> blockWorker(final AsyncComputeScheduler scheduler, final CountDownLatch gate) {
        return blockWorker(scheduler, CONSUMER, gate);
    }

    /**
     * Submits a task under the given consumer that holds one worker until the gate opens.
     *
     * @param scheduler  scheduler to block
     * @param consumerId consumer the blocking task belongs to
     * @param gate       latch the blocking body waits on
     * @return handle of the blocking task
     */
    static ComputeTaskHandle<Boolean> blockWorker(
        final AsyncComputeScheduler scheduler,
        final String consumerId,
        final CountDownLatch gate
    ) {
        final ComputeTaskHandle<Boolean> blocker = scheduler.enqueue(new ComputeRequest<>(
            consumerId, ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                gate.await();
                return Boolean.TRUE;
            }
        ));
        awaitState(blocker, ComputeTaskHandle.State.RUNNING);
        return blocker;
    }

    /**
     * Spins until the handle reaches the expected state.
     *
     * @param handle   handle to observe
     * @param expected expected state
     */
    static void awaitState(final ComputeTaskHandle<?> handle, final ComputeTaskHandle.State expected) {
        final long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
        while (handle.state() != expected) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("handle did not reach " + expected + " in " + READY_TIMEOUT
                    + ", state=" + handle.state());
            }
            Thread.onSpinWait();
        }
    }

    /**
     * Spins until a condition holds, with a hard deadline.
     *
     * @param condition condition to poll
     * @param what      description used in the failure message
     */
    static void await(final java.util.function.BooleanSupplier condition, final String what) {
        final long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    /**
     * Counts live threads owned by a benchmark scheduler.
     *
     * @return worker, janitor, adaptive, shutdown and metrics sampler threads that still exist
     */
    static int schedulerThreads() {
        int count = 0;
        for (final Thread thread : Thread.getAllStackTraces().keySet()) {
            if (!thread.isAlive()) {
                continue;
            }
            final String name = thread.getName();
            if (name.startsWith(PREFIX) || name.startsWith(METRICS_PREFIX)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Waits until no thread owned by a benchmark scheduler is left.
     *
     * @return {@code true} if the threads disappeared within the ready timeout
     */
    static boolean awaitNoSchedulerThreads() {
        final long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (schedulerThreads() == 0) {
                return true;
            }
            Thread.onSpinWait();
        }
        return schedulerThreads() == 0;
    }

    /**
     * Dependent integer mix, used to give a task body a stable cost.
     *
     * @param iterations number of mix rounds
     * @return a value that depends on every round, so the JIT cannot drop the loop
     */
    static long mix(final int iterations) {
        long value = 0x9E3779B97F4A7C15L;
        for (int i = 0; i < iterations; i++) {
            value = value * 6364136223846793005L + 1442695040888963407L;
            value ^= value >>> 29;
        }
        return value;
    }

    /**
     * Joins a future that is expected to be cancelled.
     *
     * @param future future to settle
     */
    static void settle(final CompletableFuture<?> future) {
        try {
            future.join();
        } catch (RuntimeException expected) {
            // cancellation shows up as a runtime exception here
        }
    }
}
