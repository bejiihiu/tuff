package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
 * Throughput of one pool as the worker count grows.
 *
 * <p>Each of the four producer threads submits a batch of {@value #BATCH} tasks of about 4-6 us and
 * joins its own last future. One consumer, one priority and the FIFO strategy make the last submitted
 * task the last to complete, so one join suffices and no per-task wake-up is measured. Submitting
 * overlaps with execution and the queue stays deep, so the score is the pool's completion rate.
 *
 * <p>One op is one submitted, executed and joined task.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(4)
public class ScalingBenchmark {

    /** Tasks submitted per producer per invocation. */
    static final int BATCH = 8192;

    private AsyncComputeScheduler scheduler;
    private ThreadLocal<CompletableFuture<?>[]> batches;

    /** Fixed worker count of the pool under test. */
    @Param({"1", "2", "4", "8"})
    private int workers;

    /**
     * Starts a fixed-size pool with metrics off.
     */
    @Setup
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.saturated(this.workers, 65536));
        this.scheduler.start();
        this.batches = ThreadLocal.withInitial(() -> new CompletableFuture<?>[BATCH]);
    }

    /**
     * Stops the scheduler.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown
    public void tearDown() throws InterruptedException {
        this.scheduler.shutdown(Duration.ZERO);
        this.scheduler.awaitTermination(Duration.ofSeconds(10L));
    }

    /**
     * Submits a batch of medium CPU tasks and waits for the last one.
     *
     * @param blackhole consumes the settled value
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void batchCpu(final Blackhole blackhole) {
        final CompletableFuture<?>[] futures = this.batches.get();
        for (int index = 0; index < BATCH; index++) {
            futures[index] = this.scheduler.submit(Bodies.CPU);
        }
        blackhole.consume(futures[BATCH - 1].join());
    }
}
