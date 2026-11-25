package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
 * Throughput of a saturated eight worker pool plus the queue-wait distribution that comes with it.
 *
 * <p>Four producers each push a batch of {@value #BATCH} timestamped tasks, so tens of thousands of
 * tasks are queued while the pool is saturated. Every task records how long it had to wait before a
 * worker picked it up, so one run gives both the rate and the latency the producers actually see at
 * this backlog. The teardown prints the percentiles the workers collected.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(4)
public class UnderLoadLatencyBenchmark {

    /** Tasks submitted per producer per invocation. */
    static final int BATCH = 8192;

    private AsyncComputeScheduler scheduler;
    private QueueWaitSampler sampler;
    private ThreadLocal<CompletableFuture<?>[]> batches;

    /**
     * Starts an eight worker pool with metrics off.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.saturated(8, 65536));
        this.scheduler.start();
        this.sampler = new QueueWaitSampler();
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
     * Prints the queue wait distribution of the current iteration.
     */
    @TearDown(Level.Iteration)
    public void reportQueueWait() {
        final long[] samples = this.sampler.snapshot();
        System.out.printf(Locale.ROOT,
            "QUEUE-WAIT n=%d p50=%.0fus p99=%.0fus p999=%.0fus max=%.0fus%n",
            samples.length,
            QueueWaitSampler.percentile(samples, 0.50d) / 1000.0d,
            QueueWaitSampler.percentile(samples, 0.99d) / 1000.0d,
            QueueWaitSampler.percentile(samples, 0.999d) / 1000.0d,
            QueueWaitSampler.percentile(samples, 1.0d) / 1000.0d);
    }

    /**
     * Submits a batch of timestamped CPU tasks and waits for the last one.
     *
     * @param blackhole consumes the settled value
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void batchCpu(final Blackhole blackhole) {
        final CompletableFuture<?>[] futures = this.batches.get();
        for (int index = 0; index < BATCH; index++) {
            futures[index] = this.scheduler.submit(new QueueProbe(System.nanoTime(), this.sampler));
        }
        blackhole.consume(futures[BATCH - 1].join());
    }

    /**
     * CPU task that records how long it waited before it ran.
     */
    private record QueueProbe(long submittedAtNanos, QueueWaitSampler sampler) implements ComputeTask<Boolean> {

        @Override
        public Boolean execute() {
            this.sampler.record(System.nanoTime() - this.submittedAtNanos);
            return Fixtures.mix(4096) != 0L;
        }
    }
}
