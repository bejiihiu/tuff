package kz.bejiihiu.tuff.benchmarks;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
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
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Cost of asking a task that is already running to stop.
 *
 * <p>Workers are never interrupted, so a running cancellation only flips the flag and records the
 * reason; the worker notices after the body returns. The call still probes the queue and the
 * dispatcher carry slots to find out where the task lives, so it is not a bare compare-and-set.
 *
 * <p>A single call is cheaper than a microsecond, and measuring one call per JMH invocation puts
 * the harness (park wake-up, cold caches) in the same order of magnitude. The measured method
 * therefore times the calls itself with {@link System#nanoTime()} and prints the distribution; the
 * JMH score of that method is the whole prepare-run-stop cycle, not the call. {@code cancelFinished}
 * uses the identical machinery on an already completed handle and is the floor to compare against.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@Threads(1)
public class RunningCancellationBenchmark {

    /** Cancels timed per invocation. */
    static final int GENERATIONS = 512;

    private AsyncComputeScheduler scheduler;
    private QueueWaitSampler sampler;
    private AtomicBoolean gate;
    private ComputeTaskHandle<Boolean> finished;

    /**
     * Starts a one worker pool whose worker parks for a microsecond, so starting a task does not
     * wait on the idle pump.
     */
    @Setup(Level.Trial)
    public void setUp() {
        this.scheduler = ComputeSchedulers.create(Fixtures.settings(
            1,
            65536,
            OverflowPolicy.REJECT,
            ComputeSettings.Scheduling.Strategy.FIFO,
            false,
            Duration.ofMillis(1L),
            Duration.ofNanos(1000L),
            false
        ));
        this.scheduler.start();
        this.sampler = new QueueWaitSampler();
        this.finished = this.scheduler.enqueue(new ComputeRequest<>(
            Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.NORMAL, Fixtures.NOOP));
        Fixtures.awaitState(this.finished, ComputeTaskHandle.State.COMPLETED);
    }

    /**
     * Stops the last spinning task and shuts the pool down.
     *
     * @throws InterruptedException if the wait for termination is interrupted
     */
    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        if (this.gate != null) {
            this.gate.set(true);
        }
        this.scheduler.shutdown(Duration.ZERO);
        this.scheduler.awaitTermination(Duration.ofSeconds(10L));
    }

    /**
     * Prints the distribution of the timed calls.
     */
    @TearDown(Level.Iteration)
    public void report() {
        final long[] samples = this.sampler.snapshot();
        System.out.printf(Locale.ROOT,
            "CANCEL-RUNNING n=%d p50=%dns p99=%dns max=%dns%n",
            samples.length,
            QueueWaitSampler.percentile(samples, 0.50d),
            QueueWaitSampler.percentile(samples, 0.99d),
            QueueWaitSampler.percentile(samples, 1.0d));
    }

    /**
     * Runs fresh spinning tasks one after another, times one cancellation of each and stops the task.
     *
     * @param blackhole consumes the sum of the measured nanoseconds
     */
    @Benchmark
    public void cancelRunning(final Blackhole blackhole) {
        long total = 0L;
        for (int generation = 0; generation < GENERATIONS; generation++) {
            final AtomicBoolean previousGate = this.gate;
            if (previousGate != null) {
                // let the previous body return; the new task waits in the queue until it does, and
                // the producer spins on the handle state, so nothing in this loop parks
                previousGate.set(true);
            }
            final AtomicBoolean nextGate = new AtomicBoolean();
            final ComputeTaskHandle<Boolean> next = this.scheduler.enqueue(new ComputeRequest<>(
                Fixtures.CONSUMER, ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                    while (!nextGate.get()) {
                        LockSupport.parkNanos(100_000L);
                    }
                    return Boolean.TRUE;
                }
            ));
            Fixtures.awaitState(next, ComputeTaskHandle.State.RUNNING);
            this.gate = nextGate;
            final long start = System.nanoTime();
            final boolean effective = next.cancel();
            final long elapsed = System.nanoTime() - start;
            this.sampler.record(elapsed);
            total += elapsed;
            if (!effective) {
                throw new IllegalStateException("cancel on a running task was not effective");
            }
        }
        blackhole.consume(total);
    }

    /**
     * Times cancellations of an already completed handle with the same loop, as a floor.
     *
     * @param blackhole consumes the sum of the measured nanoseconds
     */
    @Benchmark
    public void cancelFinished(final Blackhole blackhole) {
        long total = 0L;
        for (int generation = 0; generation < GENERATIONS; generation++) {
            final long start = System.nanoTime();
            final boolean effective = this.finished.cancel();
            final long elapsed = System.nanoTime() - start;
            this.sampler.record(elapsed);
            total += elapsed;
            if (effective) {
                throw new IllegalStateException("cancel on a finished task was effective");
            }
        }
        blackhole.consume(total);
    }
}
