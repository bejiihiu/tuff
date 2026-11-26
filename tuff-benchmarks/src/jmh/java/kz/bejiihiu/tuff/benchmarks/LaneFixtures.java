package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;

/**
 * Settings and task bodies for the execution-lane benchmarks.
 *
 * <p>Everything here is scaffolding. The interesting part is that the platform pool size and the
 * virtual lane limit are independent knobs, while the per-consumer quota is resolved against the
 * platform pool: a single consumer cannot run more than {@code workers} tasks at a time across both
 * lanes. The benchmarks measure that ceiling instead of assuming it away.
 */
final class LaneFixtures {

    /** Empty body: measures the scheduler around it, not the work. */
    static final ComputeTask<Boolean> NOOP = Fixtures.NOOP;

    /**
     * How long a simulated IO task blocks.
     *
     * <p>One millisecond, not fifty microseconds: a timed park on a virtual thread is quantized to
     * roughly half a millisecond on this JDK and OS, so a shorter wait would compare the timer of
     * two different mechanisms instead of their throughput under the same block.
     */
    static final long IO_BLOCK_NANOS = 1_000_000L;

    /** A shared 1 ms blocking body; no per invocation capture, like the older benchmarks. */
    static final ComputeTask<Boolean> IO_1MS = ioTask(IO_BLOCK_NANOS);

    /** A shared 4-6 us CPU body. */
    static final ComputeTask<Long> CPU_4K = () -> Fixtures.mix(4096);

    private LaneFixtures() {
    }

    /**
     * Builds a settings tree with an explicit lane configuration.
     *
     * @param workers   fixed platform worker count
     * @param capacity  global queue capacity
     * @param overflow  default overflow policy
     * @param lanes     lane configuration
     * @param overrides per consumer policy overrides
     * @param idleWait  idle park interval of a platform worker
     * @param metrics   whether the metrics collector is enabled
     * @return immutable settings
     */
    static ComputeSettings settings(
        final int workers,
        final int capacity,
        final OverflowPolicy overflow,
        final ComputeSettings.Lanes lanes,
        final Map<String, ConsumerPolicy> overrides,
        final Duration idleWait,
        final boolean metrics
    ) {
        return new ComputeSettings(
            true,
            // maxThreads is pinned to the same value as threads: with -1 the pool ceiling is the
            // processor budget and a fixed count above it would be silently clamped, which would
            // make the lane comparison measure the machine instead of the setting
            new ComputeSettings.Workers(workers, 1, workers, Duration.ofSeconds(30L), Fixtures.PREFIX),
            new ComputeSettings.QueueSettings(capacity, overflow),
            new ComputeSettings.Scheduling(
                ComputeSettings.Scheduling.Strategy.FIFO,
                false,
                false,
                Duration.ofMillis(1L),
                3,
                idleWait
            ),
            new ComputeSettings.Consumers(ConsumerPolicy.defaults(), overrides, true),
            new ComputeSettings.Metrics(metrics, false, Duration.ofSeconds(10L), false, Duration.ofSeconds(1L)),
            ComputeSettings.Adaptive.defaults(),
            lanes,
            Duration.ofSeconds(1L)
        );
    }

    /**
     * Builds a scheduler with the virtual lane enabled and LOW routed to it.
     *
     * @param workers       fixed platform worker count
     * @param capacity      global queue capacity
     * @param maxConcurrent virtual lane hard limit
     * @param normalSpill   whether NORMAL may spill
     * @param spillWait     NORMAL wait before spill becomes eligible
     * @param spillDepth    NORMAL queue depth that opens spill, {@code 0} disables the depth trigger
     * @return immutable settings
     */
    static ComputeSettings virtual(
        final int workers,
        final int capacity,
        final int maxConcurrent,
        final boolean normalSpill,
        final Duration spillWait,
        final int spillDepth
    ) {
        return settings(
            workers,
            capacity,
            OverflowPolicy.REJECT,
            new ComputeSettings.Lanes(new ComputeSettings.Lanes.Virtual(
                true, true, normalSpill, maxConcurrent, spillWait, spillDepth
            )),
            Map.of(),
            Duration.ofMillis(1L),
            false
        );
    }

    /**
     * Builds a scheduler with the virtual lane switched off, so LOW runs on the platform pool.
     *
     * <p>This is the pre-lane behaviour and the baseline for the lane comparisons.
     *
     * @param workers  fixed platform worker count
     * @param capacity global queue capacity
     * @return immutable settings
     */
    static ComputeSettings platform(final int workers, final int capacity) {
        return settings(
            workers,
            capacity,
            OverflowPolicy.REJECT,
            ComputeSettings.Lanes.withoutVirtual(),
            Map.of(),
            Duration.ofMillis(1L),
            false
        );
    }

    /**
     * A task that blocks for a fixed time, standing in for a network or disk call.
     *
     * <p>It uses {@link Thread#sleep(Duration)} and not {@code LockSupport.parkNanos}: the pool
     * unparks every worker on a submission, and a leftover unpark permit makes the next
     * {@code parkNanos} return immediately. A benchmark body built on park would therefore not
     * block at all on the platform lane while it does block on a virtual thread, which would be a
     * measurement artefact rather than a property of the lane.
     *
     * @param blockNanos how long the body blocks
     * @return the task
     */
    static ComputeTask<Boolean> ioTask(final long blockNanos) {
        return () -> {
            Thread.sleep(Duration.ofNanos(blockNanos));
            return Boolean.TRUE;
        };
    }

    /**
     * A blocking task that feeds the concurrency probe and opens a batch latch.
     *
     * @param blockNanos how long the body blocks
     * @param probe      concurrency probe
     * @param done       latch counted down by this body
     * @return the task
     */
    static ComputeTask<Boolean> ioProbeTask(
        final long blockNanos,
        final ConcurrencyProbe probe,
        final java.util.concurrent.CountDownLatch done
    ) {
        return () -> {
            probe.enter();
            try {
                Thread.sleep(Duration.ofNanos(blockNanos));
            } finally {
                probe.exit();
                done.countDown();
            }
            return Boolean.TRUE;
        };
    }

    /**
     * Fixed-capacity sample buffer usable from platform and virtual threads alike.
     *
     * <p>{@link QueueWaitSampler} keeps one ring per thread, which is fine for a fixed pool and
     * ruinous for one virtual thread per task: every ring is a 128 KB array and a new thread would
     * allocate a new one. This buffer is one shared array with an atomic cursor; the overflow is
     * simply dropped.
     */
    static final class WaitSamples {

        private static final int CAPACITY = 1 << 20;

        private final long[] values = new long[CAPACITY];
        private final AtomicInteger cursor = new AtomicInteger();

        /**
         * Records one queue wait.
         *
         * @param nanos queue wait in nanoseconds
         */
        void record(final long nanos) {
            final int index = this.cursor.getAndIncrement();
            if (index < CAPACITY) {
                this.values[index] = nanos;
            }
        }

        /**
         * Returns the recorded samples, ascending.
         *
         * @return the samples recorded so far
         */
        long[] sorted() {
            final int size = Math.min(this.cursor.get(), CAPACITY);
            final long[] copy = java.util.Arrays.copyOf(this.values, size);
            java.util.Arrays.sort(copy);
            return copy;
        }
    }

    /**
     * Tracks how many task bodies run at the same time and the highest value seen.
     *
     * <p>It is the only way a benchmark can observe the real concurrency ceiling: the scheduler
     * reports running counters, but a body-side gauge proves what the tasks actually saw.
     */
    static final class ConcurrencyProbe {

        private final AtomicInteger current = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();

        /**
         * Marks the start of a task body.
         */
        void enter() {
            final int now = this.current.incrementAndGet();
            this.peak.accumulateAndGet(now, Math::max);
        }

        /**
         * Marks the end of a task body.
         */
        void exit() {
            this.current.decrementAndGet();
        }

        /**
         * Returns the raw current concurrency.
         *
         * @return bodies inside a task right now
         */
        int current() {
            return this.current.get();
        }

        /**
         * Returns the highest concurrency since the last reset.
         *
         * @return peak simultaneous bodies
         */
        int peak() {
            return this.peak.get();
        }

        /**
         * Clears both gauges.
         */
        void reset() {
            this.current.set(0);
            this.peak.set(0);
        }
    }
}
