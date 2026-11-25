package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
 * End-to-end latency of a single task on a pool that has nothing else to do.
 *
 * <p>One op is submit + wait for the only worker to finish. The worker parks between polls for
 * {@code scheduling.idle-wait}, and a submission does not wake it, so the score is dominated by the
 * idle wait unless it is configured small. The three parameter values show exactly that dependency:
 * the same dispatch path measured with a 1 us, a 50 us and the default 50 ms pump interval.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class IdleLatencyBenchmark {

    /** Idle park interval of the single worker, in microseconds. */
    @Param({"1", "50", "50000"})
    private int idleWaitMicros;

    private AsyncComputeScheduler scheduler;
    private ComputeTask<Boolean> noop;
    private ComputeTask<Long> shortTask;

    /**
     * Starts one worker with the configured idle wait and metrics off.
     */
    @Setup
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.settings(
            1,
            65536,
            kz.bejiihiu.tuff.compute.OverflowPolicy.REJECT,
            kz.bejiihiu.tuff.compute.ComputeSettings.Scheduling.Strategy.FIFO,
            false,
            Duration.ofMillis(1L),
            Duration.ofNanos(this.idleWaitMicros * 1000L),
            false
        ));
        this.scheduler.start();
        this.noop = Fixtures.NOOP;
        this.shortTask = Fixtures.SHORT;
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
     * Submits an empty task to an idle pool and waits for the result.
     *
     * @param blackhole consumes the result
     */
    @Benchmark
    public void roundTripNoop(final Blackhole blackhole) {
        blackhole.consume(this.scheduler.submit(this.noop).join());
    }

    /**
     * Submits a roughly 200 ns task to an idle pool and waits for the result.
     *
     * @param blackhole consumes the result
     */
    @Benchmark
    public void roundTripShort(final Blackhole blackhole) {
        blackhole.consume(this.scheduler.submit(this.shortTask).join());
    }
}
