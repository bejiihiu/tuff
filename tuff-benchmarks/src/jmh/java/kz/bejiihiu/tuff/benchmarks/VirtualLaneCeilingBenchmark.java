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
 * One wave of exactly {@code maxConcurrent} LOW blocking tasks.
 *
 * <p>If the lane kept its promise, the wave takes one task duration (2 ms) plus overhead. If the
 * pool wide quota is the real ceiling, a limit above the pool size takes several waves. The observed
 * peak from inside the task bodies is printed per iteration, together with the JVM platform thread
 * count, which is how "no thread explosion" is checked: virtual tasks must not add platform threads.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@Threads(1)
public class VirtualLaneCeilingBenchmark {

    /** How long each task in the wave blocks. Long enough that thread start does not dominate. */
    static final long BLOCK_NANOS = 20_000_000L;

    /** Virtual lane hard limit, equal to the number of tasks in the wave. */
    @Param({"8", "32", "128"})
    private int maxConcurrent;

    private AsyncComputeScheduler scheduler;
    private LaneFixtures.ConcurrencyProbe probe;

    /**
     * Starts a 32 worker pool with the virtual lane and metrics off.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(LaneFixtures.virtual(
            VirtualLaneBenchmark.WORKERS, 65536, this.maxConcurrent, false, Duration.ZERO, 0
        ));
        this.scheduler.start();
        this.probe = new LaneFixtures.ConcurrencyProbe();
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
     * Prints what the tasks saw and the current platform thread count.
     */
    @TearDown(Level.Iteration)
    public void report() {
        final SchedulerMetrics.VirtualLane lane = this.scheduler.metrics().lanes().virtual();
        System.out.printf(Locale.ROOT,
            "VT-CEILING limit=%d tasks=%d peakBodies=%d started=%d platformThreads=%d%n",
            this.maxConcurrent,
            this.maxConcurrent,
            this.probe.peak(),
            lane.submitted(),
            Thread.activeCount());
        this.probe.reset();
    }

    /**
     * Submits one full wave and waits for every task.
     *
     * @param blackhole consumes a marker
     * @throws InterruptedException if the wait for the wave is interrupted
     */
    @Benchmark
    public void wave(final Blackhole blackhole) throws InterruptedException {
        final int tasks = this.maxConcurrent;
        final CountDownLatch done = new CountDownLatch(tasks);
        final ComputeTask<Boolean> task = LaneFixtures.ioProbeTask(BLOCK_NANOS, this.probe, done);
        final CompletableFuture<?>[] futures = new CompletableFuture<?>[tasks];
        for (int index = 0; index < tasks; index++) {
            futures[index] = this.scheduler.submit(ComputePriority.LOW, task);
        }
        done.await();
        blackhole.consume(futures[tasks - 1]);
    }
}
