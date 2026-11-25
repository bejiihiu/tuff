package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
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
 * Cost of picking the next task when the queue is deep.
 *
 * <p>One worker is held by a spinning blocking task, ten thousand tasks are queued across all four
 * priorities and {@code consumers} consumers, then the worker is released and the whole backlog is
 * drained on a single thread. Nothing else runs, so the score is the serial dispatch cost of the
 * queue plus the handle/future bookkeeping.
 *
 * <p>One invocation is one full drain, not one task; the operations-per-invocation annotation makes
 * the reported score the cost of a single task in the drain.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@Threads(1)
public class QueueDepthDispatchBenchmark {

    /** Number of registered consumers the workload is spread over. */
    @Param({"1", "4", "16"})
    private int consumers;

    /** Number of queued tasks the drain has to get through. */
    private static final int DEPTH = 10_000;

    private AsyncComputeScheduler scheduler;
    private DrainFixture.Filled filled;

    /**
     * Starts a one worker pool and registers the consumers.
     */
    @Setup(org.openjdk.jmh.annotations.Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.settings(
            1,
            65536,
            OverflowPolicy.REJECT,
            ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
            true,
            Duration.ofMillis(1L),
            Duration.ofMillis(1L),
            false
        ));
        this.scheduler.start();
        DrainFixture.registerConsumers(this.scheduler, this.consumers);
    }

    /**
     * Refills the queue behind a blocked worker before every drain.
     */
    @Setup(org.openjdk.jmh.annotations.Level.Iteration)
    public void fill() {
        this.filled = DrainFixture.fill(this.scheduler, this.consumers, DEPTH);
    }

    /**
     * Stops the scheduler.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown(org.openjdk.jmh.annotations.Level.Trial)
    public void tearDown() throws InterruptedException {
        this.scheduler.shutdown(Duration.ZERO);
        this.scheduler.awaitTermination(Duration.ofSeconds(10L));
    }

    /**
     * Releases the held worker and waits until the pre-filled backlog is drained.
     *
     * @param blackhole consumes the drain future
     */
    @Benchmark
    @OperationsPerInvocation(DEPTH)
    public void drain(final Blackhole blackhole) {
        this.filled.release();
        blackhole.consume(this.filled.awaitDrained());
    }
}
