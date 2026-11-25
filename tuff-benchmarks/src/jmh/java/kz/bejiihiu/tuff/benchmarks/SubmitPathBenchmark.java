package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
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
 * Cost of the submission path alone, with nothing executing.
 *
 * <p>One worker is held by a spinning task, so every submission stays in the queue: no execution, no
 * future completion and no scheduler wake-up is measured, only validation, the handle and entry
 * allocation, the queue insert and the metrics hook (off in this setup).
 *
 * <p>The queue is drained by cancelling the batch in the teardown, which is not part of the score.
 * One op is one accepted submission.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@Threads(1)
public class SubmitPathBenchmark {

    /** Tasks submitted per invocation. */
    static final int BATCH = 20_000;

    private AsyncComputeScheduler scheduler;
    private CountDownLatch gate;
    private ComputeTaskHandle<Boolean> blocker;
    private CompletableFuture<?>[] submitted;

    /**
     * Starts a one worker pool and holds the worker.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.settings(
            1,
            BATCH + 4096,
            OverflowPolicy.REJECT,
            ComputeSettings.Scheduling.Strategy.FIFO,
            false,
            Duration.ofMillis(1L),
            Duration.ofMillis(1L),
            false
        ));
        this.scheduler.start();
        this.gate = new CountDownLatch(1);
        this.blocker = Fixtures.blockWorker(this.scheduler, this.gate);
        this.submitted = new CompletableFuture<?>[BATCH];
    }

    /**
     * Empties the queue again so the next iteration starts from the same state.
     */
    @TearDown(Level.Iteration)
    public void drain() {
        for (final CompletableFuture<?> future : this.submitted) {
            future.cancel(false);
        }
    }

    /**
     * Releases the worker and stops the pool.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        this.gate.countDown();
        Fixtures.settle(this.blocker.future());
        this.scheduler.shutdown(Duration.ZERO);
        this.scheduler.awaitTermination(Duration.ofSeconds(10L));
    }

    /**
     * Submits the whole batch while the only worker is busy.
     *
     * @param blackhole consumes the number of accepted submissions
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void enqueueOnly(final Blackhole blackhole) {
        int accepted = 0;
        for (int index = 0; index < BATCH; index++) {
            this.submitted[index] = this.scheduler.enqueue(new ComputeRequest<>(
                Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.NORMAL, Fixtures.NOOP)).future();
            accepted++;
        }
        blackhole.consume(accepted);
    }
}
