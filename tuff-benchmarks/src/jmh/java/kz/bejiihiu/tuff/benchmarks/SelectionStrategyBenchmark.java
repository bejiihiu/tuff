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
 * Overhead of the fairness machinery: strategy and priority aging.
 *
 * <p>Same shape as the depth benchmark - ten thousand queued tasks, four consumers, one worker and a
 * timed drain - but here the dispatch strategy changes. Aging uses a one millisecond interval so the
 * backlog really ages during a drain instead of the code path being cold.
 *
 * <p>One invocation is one full drain; the operations-per-invocation annotation makes the reported
 * score the cost of one task in the drain.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@Threads(1)
public class SelectionStrategyBenchmark {

    /** Fixed number of queued tasks per drain. */
    static final int DEPTH = 10_000;

    /** Number of registered consumers. */
    static final int CONSUMERS = 4;

    /** Dispatch strategy under test. */
    @Param({"FIFO", "PRIORITY", "WEIGHTED_FAIR"})
    private ComputeSettings.Scheduling.Strategy strategy;

    /** Whether priority aging is enabled. */
    @Param({"false", "true"})
    private boolean aging;

    private AsyncComputeScheduler scheduler;
    private DrainFixture.Filled filled;

    /**
     * Starts a one worker pool with the configured strategy.
     */
    @Setup(org.openjdk.jmh.annotations.Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.settings(
            1,
            65536,
            OverflowPolicy.REJECT,
            this.strategy,
            this.aging,
            Duration.ofMillis(1L),
            Duration.ofMillis(1L),
            false
        ));
        this.scheduler.start();
        DrainFixture.registerConsumers(this.scheduler, CONSUMERS);
    }

    /**
     * Refills the queue behind a blocked worker before every drain.
     */
    @Setup(org.openjdk.jmh.annotations.Level.Iteration)
    public void fill() {
        this.filled = DrainFixture.fill(this.scheduler, CONSUMERS, DEPTH);
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
