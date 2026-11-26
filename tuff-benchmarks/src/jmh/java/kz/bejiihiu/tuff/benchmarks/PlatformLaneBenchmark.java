package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
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
 * The same LOW workloads as {@link VirtualLaneBenchmark} with the virtual lane switched off.
 *
 * <p>LOW then runs on the platform pool exactly as it did before the lane existed. The worker count
 * and therefore the per-consumer quota are the parameter, so the same class also shows how the
 * submission wake-up scales with the number of workers.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(4)
public class PlatformLaneBenchmark {

    /** Platform workers; also the per-consumer quota ceiling. */
    static final int WORKERS = VirtualLaneBenchmark.WORKERS;

    /** Tasks per producer per invocation. */
    static final int BATCH = VirtualLaneBenchmark.BATCH;

    /** Fixed worker count under test. */
    @Param({"4", "8", "16", "32"})
    private int workers;

    private AsyncComputeScheduler scheduler;
    private ThreadLocal<CompletableFuture<?>[]> batches;

    /**
     * Starts a fixed size pool with no virtual lane.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(LaneFixtures.platform(this.workers, 65536));
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
