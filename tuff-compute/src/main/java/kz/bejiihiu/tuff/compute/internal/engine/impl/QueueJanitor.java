package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveConcurrencyController;
import kz.bejiihiu.tuff.compute.internal.metrics.MetricsCollector;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.Durations;
import kz.bejiihiu.tuff.compute.internal.worker.WorkerPool;
import org.jspecify.annotations.Nullable;

/**
 * Periodic housekeeping thread of the engine.
 *
 * <p>One small daemon thread does four things on one cadence, which is cheaper and easier to reason
 * about than four timers:
 * <ul>
 *     <li><b>queue expiry</b> — {@link ComputeQueue#hasExpired(long)} is an {@code O(consumers)}
 *     probe; only when it answers {@code true} does the janitor drain the expired prefix and cancel
 *     each entry with {@link ComputeCancelledException.Reason#QUEUE_EXPIRED}. No quota slot is
 *     released, because a queued entry never held one;</li>
 *     <li><b>carried expiry</b> — entries that left the queue but never started are the dispatcher's
 *     job, so the same sweep covers them;</li>
 *     <li><b>gauges</b> — queue depth and worker counts are pushed to the metrics collector and the
 *     adaptive controller once per configured sample interval;</li>
 *     <li><b>autoscaling</b> — the engine's reconciliation callback runs every tick when the static
 *     autoscaler owns pool sizing.</li>
 * </ul>
 *
 * <p>The thread never touches task bodies, never interrupts a worker and never runs user code. An
 * expired entry is terminalized with a deferred future completion, so a dependent stage that blocks
 * cannot park this thread. Its {@link #stop(Duration)} is bounded and reports a miss instead of
 * waiting forever.
 */
final class QueueJanitor implements Runnable {

    private static final System.Logger LOG = System.getLogger(QueueJanitor.class.getName());

    private final ComputeQueue queue;
    private final DispatcherImpl dispatcher;
    private final MetricsCollector metrics;
    private final WorkerPool pool;
    private final ComputeClock clock;
    private final @Nullable AdaptiveConcurrencyController adaptive;
    private final @Nullable Runnable autoscale;
    private final IntSupplier platformDepth;
    private final long tickMillis;
    private final long sampleIntervalNanos;

    private final List<QueueEntry> expired = new ArrayList<>();
    private final CountDownLatch stopped = new CountDownLatch(1);

    private volatile boolean running = true;
    private volatile @Nullable Thread thread;

    /**
     * Creates the janitor.
     *
     * @param queue               queue to sweep
     * @param dispatcher          dispatcher that owns carried entries
     * @param metrics             metrics collector for periodic gauges
     * @param pool                worker pool whose gauges are sampled
     * @param clock               scheduler clock used for expiry and adaptive queue state
     * @param adaptive            adaptive controller, or {@code null} when disabled
     * @param autoscale           static autoscaler reconciliation callback, or {@code null}
     * @param platformDepth       queue depth the platform pool can act on
     * @param tickMillis          how often the sweep runs, at least {@code 1}
     * @param sampleIntervalNanos how often gauges are sampled, at least {@code 1}
     */
    QueueJanitor(
        final ComputeQueue queue,
        final DispatcherImpl dispatcher,
        final MetricsCollector metrics,
        final WorkerPool pool,
        final ComputeClock clock,
        final @Nullable AdaptiveConcurrencyController adaptive,
        final @Nullable Runnable autoscale,
        final IntSupplier platformDepth,
        final long tickMillis,
        final long sampleIntervalNanos
    ) {
        this.queue = Objects.requireNonNull(queue, "queue");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.pool = Objects.requireNonNull(pool, "pool");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.adaptive = adaptive;
        this.autoscale = autoscale;
        this.platformDepth = Objects.requireNonNull(platformDepth, "platformDepth");
        this.tickMillis = Math.max(1L, tickMillis);
        this.sampleIntervalNanos = Math.max(1L, sampleIntervalNanos);
    }

    /**
     * Stores the thread that drives this janitor so {@link #stop()} can join it.
     *
     * @param janitorThread the thread running this runnable
     */
    void attach(final Thread janitorThread) {
        this.thread = janitorThread;
    }

    /**
     * Runs one sweep and one gauge sample immediately.
     *
     * <p>Called after the pool started, so the first snapshot already shows the real worker count,
     * and by tests that want deterministic expiry without waiting for the tick.
     */
    void sampleNow() {
        this.sweep();
        this.sampleGauges();
    }

    /**
     * Runs the expiry sweep once.
     *
     * <p>Called by the tick and available to tests that drive the clock manually.
     */
    void sweepNow() {
        this.sweep();
    }

    @Override
    public void run() {
        final Thread self = Thread.currentThread();
        this.thread = self;
        long lastSample = System.nanoTime();
        try {
            while (this.running) {
                try {
                    this.sweep();
                    final long now = System.nanoTime();
                    if (now - lastSample >= this.sampleIntervalNanos) {
                        this.sampleGauges();
                        lastSample = now;
                    }
                } catch (Throwable failure) {
                    // housekeeping must never die on one bad entry; log and keep the cadence
                    LOG.log(System.Logger.Level.WARNING, "compute janitor tick failed", failure);
                }
                try {
                    Thread.sleep(this.tickMillis);
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } finally {
            this.stopped.countDown();
        }
    }

    /**
     * Stops the janitor and waits for its thread to leave the loop, bounded by a timeout.
     *
     * <p>Expiry no longer terminalizes futures inline, so this should return almost immediately. The
     * bound exists anyway: a stuck sweep must not turn into a scheduler that never reaches
     * {@code TERMINATED}.
     *
     * @param timeout maximum time to wait for the thread, must not be negative
     * @return {@code true} if the janitor left its loop in time
     */
    boolean stop(final Duration timeout) {
        this.running = false;
        final Thread current = this.thread;
        if (current != null) {
            current.interrupt();
        }
        try {
            final boolean stoppedInTime = this.stopped.await(
                Math.max(0L, Durations.toNanosSaturated(timeout)),
                TimeUnit.NANOSECONDS
            );
            if (!stoppedInTime) {
                LOG.log(System.Logger.Level.WARNING,
                    "compute janitor did not stop within " + timeout + "; continuing shutdown without it");
            }
            return stoppedInTime;
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            LOG.log(System.Logger.Level.WARNING, "compute janitor stop was interrupted");
            return false;
        }
    }

    private void sweep() {
        final long now = this.clock.nanoTime();
        // a manual sweep from diagnostics or a test runs on the calling thread; only the janitor's own
        // tick must push user continuations off the service thread
        final boolean defer = Thread.currentThread() == this.thread;
        if (this.queue.hasExpired(now)) {
            this.expired.clear();
            if (this.queue.drainExpired(now, this.expired) > 0) {
                for (final QueueEntry entry : this.expired) {
                    // completion goes through the completion path, never inline here
                    this.dispatcher.cancelQueued(entry, ComputeCancelledException.Reason.QUEUE_EXPIRED, defer);
                }
                this.expired.clear();
            }
        }
        this.dispatcher.sweepCarried(now, defer);
        final AdaptiveConcurrencyController controller = this.adaptive;
        if (controller != null) {
            // the adaptive controller models the platform pool only; virtual lane backlog must not
            // make it grow
            controller.onQueueState(this.platformDepth.getAsInt(), this.queue.capacity());
        }
        final Runnable scaler = this.autoscale;
        if (scaler != null) {
            scaler.run();
        }
    }

    private void sampleGauges() {
        this.metrics.onQueueState(this.queue.size(), this.queue.capacity());
        this.metrics.onWorkerState(
            this.pool.workerCount(),
            this.pool.activeWorkers(),
            this.pool.idleWorkers(),
            this.pool.concurrencyLimit()
        );
    }
}
