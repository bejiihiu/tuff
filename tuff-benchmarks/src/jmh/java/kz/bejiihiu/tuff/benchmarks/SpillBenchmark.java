package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * NORMAL work on a saturated platform pool, with and without spill to the virtual lane.
 *
 * <p>Eight workers and four producers of 1 ms blocking tasks keep the pool permanently saturated.
 * With spill enabled the lane may take NORMAL entries once they waited {@value #SPILL_WAIT_MILLIS} ms
 * or the NORMAL queue is at least {@value #SPILL_DEPTH} deep. The iteration report prints how many
 * tasks the lane actually started as spilled and what queue wait the producers saw, because a spill
 * that never happens and a spill that happens after a 25 ms pump tick look very different in a
 * throughput number alone.
 *
 * <p>One op is one submitted and executed task.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(4)
public class SpillBenchmark {

    /** Platform workers, also the per-consumer quota ceiling. */
    static final int WORKERS = 8;

    /** Tasks per producer per invocation. */
    static final int BATCH = 128;

    /** Virtual lane hard limit; deliberately far above the quota. */
    static final int MAX_CONCURRENT = 64;

    /** NORMAL wait that opens spill. */
    static final long SPILL_WAIT_MILLIS = 5L;

    /** NORMAL queue depth that opens spill. */
    static final int SPILL_DEPTH = 32;

    /** Whether NORMAL may spill. */
    @Param({"true", "false"})
    private boolean spill;

    private AsyncComputeScheduler scheduler;
    private LaneFixtures.WaitSamples waits;
    private long lastSpilled;

    /**
     * Starts an eight worker pool with the virtual lane present and metrics off.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(LaneFixtures.virtual(
            WORKERS,
            65536,
            MAX_CONCURRENT,
            this.spill,
            Duration.ofMillis(SPILL_WAIT_MILLIS),
            SPILL_DEPTH
        ));
        this.scheduler.start();
        this.waits = new LaneFixtures.WaitSamples();
    }

    /**
     * Stops the scheduler.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        this.scheduler.shutdown(Duration.ZERO);
        this.scheduler.awaitTermination(Duration.ofSeconds(10L));
    }

    /**
     * Prints the cumulative queue wait distribution and the spill delta of this iteration.
     */
    @TearDown(Level.Iteration)
    public void report() {
        final SchedulerMetrics.VirtualLane lane = this.scheduler.metrics().lanes().virtual();
        final long spilled = lane.spilled();
        final long[] samples = this.waits.sorted();
        System.out.printf(Locale.ROOT,
            "SPILL on=%s spilledDelta=%d started=%d n=%d waitP50=%.0fus waitP90=%.0fus waitP99=%.0fus%n",
            this.spill,
            spilled - this.lastSpilled,
            lane.submitted(),
            samples.length,
            QueueWaitSampler.percentile(samples, 0.50d) / 1000.0d,
            QueueWaitSampler.percentile(samples, 0.90d) / 1000.0d,
            QueueWaitSampler.percentile(samples, 0.99d) / 1000.0d);
        this.lastSpilled = spilled;
    }

    /**
     * Submits a batch of 1 ms NORMAL tasks and waits for all of them.
     *
     * @param blackhole consumes a marker
     * @throws InterruptedException if the wait for the batch is interrupted
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void ioThroughput(final Blackhole blackhole) throws InterruptedException {
        final CountDownLatch done = new CountDownLatch(BATCH);
        final CompletableFuture<?>[] futures = new CompletableFuture<?>[BATCH];
        for (int index = 0; index < BATCH; index++) {
            futures[index] = this.scheduler.submit(ComputePriority.NORMAL,
                new WaitProbe(System.nanoTime(), this.waits, done));
        }
        done.await();
        blackhole.consume(futures[BATCH - 1]);
    }

    /**
     * A 1 ms blocking task that records how long it waited before it started.
     *
     * @param submittedAt when the producer submitted it
     * @param waits       shared sample buffer
     * @param done        batch latch
     */
    private record WaitProbe(
        long submittedAt,
        LaneFixtures.WaitSamples waits,
        CountDownLatch done
    ) implements ComputeTask<Boolean> {

        @Override
        public Boolean execute() throws InterruptedException {
            this.waits.record(System.nanoTime() - this.submittedAt);
            try {
                Thread.sleep(java.time.Duration.ofNanos(LaneFixtures.IO_BLOCK_NANOS));
            } finally {
                this.done.countDown();
            }
            return Boolean.TRUE;
        }
    }
}
