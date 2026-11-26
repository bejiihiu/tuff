package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Cost of cancelling a LOW task that is already running on a virtual thread.
 *
 * <p>The body blocks on a latch, so it is reliably RUNNING when the measured {@code cancel()} runs
 * and never finishes on its own. Cancellation does not interrupt it: the flag is set, the result is
 * discarded when the latch opens, and the future completes as cancelled. Sample mode gives the tail,
 * which is where the queue and carry probes show up.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class VirtualLaneRunningCancelBenchmark {

    private AsyncComputeScheduler scheduler;
    private ComputeTaskHandle<?> running;
    private CountDownLatch gate;

    /**
     * Starts a 32 worker pool with an eight slot virtual lane.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(LaneFixtures.virtual(
            VirtualLaneBenchmark.WORKERS, 65536, 8, false, Duration.ZERO, 0
        ));
        this.scheduler.start();
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
     * Submits one gated LOW task and waits until it is running.
     */
    @Setup(Level.Invocation)
    public void startTask() {
        this.gate = new CountDownLatch(1);
        final CountDownLatch body = this.gate;
        final ComputeTask<Boolean> task = () -> {
            body.await();
            return Boolean.TRUE;
        };
        this.running = this.scheduler.enqueue(new ComputeRequest<>(
            Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.LOW, task
        ));
        Fixtures.awaitState(this.running, ComputeTaskHandle.State.RUNNING);
    }

    /**
     * Releases the body and lets the handle reach its cancelled terminal state.
     */
    @TearDown(Level.Invocation)
    public void finishTask() {
        this.gate.countDown();
        final CompletableFuture<?> future = this.running.future();
        Fixtures.settle(future);
    }

    /**
     * Cancels the running task.
     *
     * @param blackhole consumes whether the cancellation was accepted
     */
    @Benchmark
    public void cancelRunning(final Blackhole blackhole) {
        blackhole.consume(this.running.cancel());
    }
}
