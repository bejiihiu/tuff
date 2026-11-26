package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
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
 * Throughput of LOW work on the virtual lane, blocking and CPU bound.
 *
 * <p>The platform pool is fixed at {@value #WORKERS} workers, so the pool wide quota allows at most
 * that many concurrent tasks for one consumer. The lane limit is the parameter: at 8 it is the
 * binding limit, at 32 it meets the quota, above 32 it cannot raise concurrency any further. The
 * observed peak is printed by {@link VirtualLaneCeilingBenchmark}; this class measures the completion
 * rate with the same one join per batch shape as the rest of the suite.
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
public class VirtualLaneBenchmark {

    /** Platform workers, also the per-consumer quota ceiling for a fixed pool. */
    static final int WORKERS = 8;

    /** Tasks per producer per invocation. */
    static final int BATCH = 256;

    /** Virtual lane hard limit under test. */
    @Param({"8", "32", "128"})
    private int maxConcurrent;

    private AsyncComputeScheduler scheduler;
    private ThreadLocal<CompletableFuture<?>[]> batches;

    /**
     * Starts a fixed 32 worker pool with LOW routed to the virtual lane and metrics off.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(LaneFixtures.virtual(
            WORKERS, 65536, this.maxConcurrent, false, Duration.ZERO, 0
        ));
        this.scheduler.start();
        this.batches = ThreadLocal.withInitial(() -> new CompletableFuture<?>[BATCH]);
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
     * Prints the lane counters of the iteration.
     */
    @TearDown(Level.Iteration)
    public void reportLane() {
        final SchedulerMetrics.VirtualLane lane = this.scheduler.metrics().lanes().virtual();
        System.out.printf(Locale.ROOT,
            "VT-LANE limit=%d submitted=%d completed=%d failed=%d spilled=%d%n",
            this.maxConcurrent,
            lane.submitted(),
            lane.completed(),
            lane.failed(),
            lane.spilled());
    }

    /**
     * Submits a batch of 1 ms blocking tasks and waits for the last one.
     *
     * @param blackhole consumes the settled value
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void ioThroughput(final Blackhole blackhole) {
        final CompletableFuture<?>[] futures = this.batches.get();
        for (int index = 0; index < BATCH; index++) {
            futures[index] = this.scheduler.submit(ComputePriority.LOW, LaneFixtures.IO_1MS);
        }
        blackhole.consume(futures[BATCH - 1].join());
    }

    /**
     * Submits a batch of 4-6 us CPU tasks and waits for the last one.
     *
     * @param blackhole consumes the settled value
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void cpuThroughput(final Blackhole blackhole) {
        final CompletableFuture<?>[] futures = this.batches.get();
        for (int index = 0; index < BATCH; index++) {
            futures[index] = this.scheduler.submit(ComputePriority.LOW, LaneFixtures.CPU_4K);
        }
        blackhole.consume(futures[BATCH - 1].join());
    }
}
