package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
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
 * Cost of a full scheduler lifecycle with no work: create, start, shutdown with a zero grace period,
 * wait for termination.
 *
 * <p>This also checks the promise that nothing of the scheduler survives termination. After every
 * iteration the benchmark counts threads whose name belongs to the scheduler (workers, janitor,
 * adaptive timer, shutdown thread and the metrics sampler) and prints the worst count it saw.
 *
 * <p>One op is one complete start/stop cycle.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@Fork(1)
@Threads(1)
public class LifecycleBenchmark {

    /** Whether the metrics collector and its sampler thread are part of the cycle. */
    @Param({"false", "true"})
    private boolean metrics;

    private int leakedThreads;

    /**
     * Resets the leak counter.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.leakedThreads = 0;
    }

    /**
     * Creates a two worker scheduler, starts it and shuts it down immediately.
     *
     * @param blackhole consumes the scheduler and the termination flag
     * @return {@code true} if the scheduler terminated within five seconds
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @Benchmark
    public boolean startStopCycle(final Blackhole blackhole) throws InterruptedException {
        final AsyncComputeScheduler scheduler = ComputeSchedulers.create(Fixtures.settings(
            2,
            1024,
            OverflowPolicy.REJECT,
            ComputeSettings.Scheduling.Strategy.FIFO,
            false,
            Duration.ofMillis(1L),
            Duration.ofMillis(1L),
            this.metrics
        ));
        scheduler.start();
        scheduler.shutdown(Duration.ZERO);
        final boolean terminated = scheduler.awaitTermination(Duration.ofSeconds(5L));
        blackhole.consume(scheduler);
        return terminated;
    }

    /**
     * Counts what is left after the cycle.
     */
    @TearDown(Level.Iteration)
    public void checkThreads() {
        if (!Fixtures.awaitNoSchedulerThreads()) {
            this.leakedThreads = Math.max(this.leakedThreads, Fixtures.schedulerThreads());
        }
    }

    /**
     * Prints the leak check result once per trial.
     */
    @TearDown(Level.Trial)
    public void report() {
        System.out.printf(Locale.ROOT, "LIFECYCLE metrics=%s leaked-threads=%d%n", this.metrics, this.leakedThreads);
    }
}
