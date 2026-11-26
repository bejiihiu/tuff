package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTask;
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
 * Round trip of one LOW task on an otherwise idle virtual lane.
 *
 * <p>One op is submit and wait for the result. The pump parks on a condition between checks, so the
 * score is the submit-to-wake-up path plus the 1 ms body. Sample mode reports the percentiles; a
 * tail far above the median means the pump missed a wake-up and waited for its 25 ms re-evaluation.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class VirtualLaneLatencyBenchmark {

    /** Virtual lane hard limit under test. */
    @Param({"8", "32", "128"})
    private int maxConcurrent;

    private AsyncComputeScheduler scheduler;
    private ComputeTask<Boolean> task;

    /**
     * Starts a 32 worker pool with the virtual lane and no metrics.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(LaneFixtures.virtual(
            VirtualLaneBenchmark.WORKERS, 65536, this.maxConcurrent, false, Duration.ZERO, 0
        ));
        this.scheduler.start();
        this.task = LaneFixtures.ioTask(LaneFixtures.IO_BLOCK_NANOS);
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
     * Submits one 1 ms LOW task and waits for the result.
     *
     * @param blackhole consumes the result
     */
    @Benchmark
    public void ioRoundTrip(final Blackhole blackhole) {
        blackhole.consume(this.scheduler.submit(ComputePriority.LOW, this.task).join());
    }

    /**
     * Submits one empty LOW task and waits for the result.
     *
     * <p>The difference from {@link #ioRoundTrip} is the time the body spends parked on a virtual
     * thread, not the lane plumbing.
     *
     * @param blackhole consumes the result
     */
    @Benchmark
    public void noopRoundTrip(final Blackhole blackhole) {
        blackhole.consume(this.scheduler.submit(ComputePriority.LOW, LaneFixtures.NOOP).join());
    }
}
