package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Cost of submitting and executing one task on a single worker pool.
 *
 * <p>Each invocation submits a batch of {@value #BATCH} tasks as fast as it can and then joins the
 * last future. The batch uses one consumer, one priority and the FIFO strategy, so the last
 * submitted task is also the last one to complete; one join is one park, not one per task. Submit and
 * execution overlap, so the score is the rate at which the worker accepts and finishes tasks.
 *
 * <p>One op is one submitted, executed and joined task.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class SubmitOverheadBenchmark {

    /** Tasks submitted per benchmark invocation. */
    static final int BATCH = 8192;

    private AsyncComputeScheduler scheduler;
    private CompletableFuture<?>[] batch;

    /**
     * Starts one worker with metrics off.
     */
    @Setup
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.saturated(1, 16384));
        this.scheduler.start();
        this.batch = new CompletableFuture<?>[BATCH];
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
     * Submits a batch of empty tasks and waits for the last one.
     *
     * @param blackhole consumes the settled value
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void batchNoop(final Blackhole blackhole) {
        this.run(Bodies.NOOP, blackhole);
    }

    /**
     * Submits a batch of roughly 0.6 us tasks and waits for the last one.
     *
     * @param blackhole consumes the settled value
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void batchShort(final Blackhole blackhole) {
        this.run(Bodies.SHORT, blackhole);
    }

    private void run(final ComputeTask<?> body, final Blackhole blackhole) {
        final CompletableFuture<?>[] futures = this.batch;
        for (int index = 0; index < BATCH; index++) {
            futures[index] = this.scheduler.submit(body);
        }
        blackhole.consume(futures[BATCH - 1].join());
    }
}
