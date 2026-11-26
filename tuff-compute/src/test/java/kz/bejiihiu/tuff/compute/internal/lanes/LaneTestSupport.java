package kz.bejiihiu.tuff.compute.internal.lanes;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;

/**
 * Fixtures for the end-to-end lane tests.
 *
 * <p>Everything drives the public {@link AsyncComputeScheduler} built by
 * {@link ComputeSchedulers#create(ComputeSettings, ComputeClock, int)}: the real queue, pool,
 * dispatcher, janitor and virtual lane run underneath. The only test double is the clock, which is
 * a documented extension point and the only way to age, expire and rescale without sleeping.
 */
final class LaneTestSupport {

    /** Deterministic processor budget so pool sizing never depends on the host. */
    static final int PROCESSORS = 8;

    static final Duration NEVER = Duration.ofSeconds(30L);

    private static final AtomicInteger PREFIX_SEQUENCE = new AtomicInteger();

    private LaneTestSupport() {
    }

    static String uniquePrefix(final String tag) {
        return "tuff-lane-" + tag + '-' + PREFIX_SEQUENCE.incrementAndGet();
    }

    static Duration millis(final long value) {
        return Duration.ofMillis(value);
    }

    static AsyncComputeScheduler start(final SettingsBuilder settings) {
        return start(settings, new FakeClock());
    }

    static AsyncComputeScheduler start(final SettingsBuilder settings, final ComputeClock clock) {
        final AsyncComputeScheduler scheduler =
            ComputeSchedulers.create(settings.build(), clock, settings.processorBudget());
        scheduler.start();
        return scheduler;
    }

    static ConsumerPolicy weighted(final double weight) {
        return new ConsumerPolicy(true, ConsumerPolicy.ConcurrencyLimit.automatic(), -1, weight,
            OverflowPolicy.REJECT, Duration.ZERO);
    }

    static ConsumerPolicy absolute(final int permits) {
        return ConsumerPolicy.absolute(permits);
    }

    static ConsumerPolicy bounded(final int permits, final int maxQueued, final OverflowPolicy overflow) {
        return new ConsumerPolicy(true, ConsumerPolicy.ConcurrencyLimit.absolute(permits), maxQueued, 1.0d,
            overflow, Duration.ZERO);
    }

    static ConsumerPolicy expiring(final int permits, final Duration maxQueueWait) {
        return new ConsumerPolicy(true, ConsumerPolicy.ConcurrencyLimit.absolute(permits), -1, 1.0d,
            OverflowPolicy.REJECT, maxQueueWait);
    }

    static void assertEventually(final String what, final Duration timeout, final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(2L);
        }
        if (condition.getAsBoolean()) {
            return;
        }
        throw new AssertionError("timed out waiting for: " + what);
    }

    static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting", interrupted);
        }
    }

    static List<String> threadNames(final String prefix) {
        return Thread.getAllStackTraces().keySet().stream()
            .map(Thread::getName)
            .filter(name -> name.startsWith(prefix))
            .toList();
    }

    static void awaitNoThreads(final String prefix, final Duration timeout) {
        assertEventually("no engine threads remain for " + prefix, timeout,
            () -> threadNames(prefix).isEmpty());
    }

    static ComputeCancelledException cancellationOf(final Throwable thrown) {
        Throwable current = thrown;
        while (current != null && !(current instanceof ComputeCancelledException)) {
            current = current.getCause();
        }
        if (current == null) {
            throw new AssertionError("expected a compute cancellation, got " + thrown);
        }
        return (ComputeCancelledException) current;
    }

    static ComputeCancelledException cancellationOf(final CompletableFuture<?> future) {
        try {
            future.get();
        } catch (final CancellationException cancelled) {
            return cancellationOf(cancelled);
        } catch (final ExecutionException failed) {
            return cancellationOf(failed);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for cancellation", interrupted);
        }
        throw new AssertionError("expected a cancelled future, but it completed normally");
    }

    static void assertCancelledWith(final ComputeTaskHandle<?> handle, final ComputeCancelledException.Reason reason) {
        assertEventually("task " + handle.taskId() + " is cancelled", NEVER, handle::isCancelled);
        final ComputeCancelledException cancellation = cancellationOf(handle.future());
        if (cancellation.reason() != reason) {
            throw new AssertionError("expected " + reason + " but was " + cancellation.reason());
        }
    }

    /**
     * Asserts the lane contract invariant: submitted is exactly the sum of terminal and running
     * virtual tasks. Queued work is deliberately not part of it because a task only enters a lane
     * when it is dispatched.
     *
     * @param metrics snapshot to check
     * @throws AssertionError if the invariant does not hold
     */
    static void assertLaneInvariant(final SchedulerMetrics metrics) {
        final SchedulerMetrics.VirtualLane lane = metrics.lanes().virtual();
        final long sum = lane.completed() + lane.failed() + lane.cancelled() + lane.running();
        if (lane.submitted() != sum) {
            throw new AssertionError("lane invariant broken: submitted=" + lane.submitted()
                + " completed=" + lane.completed() + " failed=" + lane.failed()
                + " cancelled=" + lane.cancelled() + " running=" + lane.running());
        }
        if (lane.spilled() > lane.submitted()) {
            throw new AssertionError("spilled=" + lane.spilled() + " exceeds submitted=" + lane.submitted());
        }
    }

    /**
     * Asserts the per consumer invariant against live counters. Unlike the scheduler wide gauge it
     * does not depend on the sampling tick, so it can be checked while work is in flight.
     *
     * @param metrics snapshot to check
     * @return tasks that were accepted and are not terminal yet, summed over all consumers
     * @throws AssertionError if a consumer or the consumer sum does not add up
     */
    static long assertConsumerInvariant(final SchedulerMetrics metrics) {
        long inFlight = 0L;
        long submitted = 0L;
        for (final ConsumerMetrics consumer : metrics.consumers().values()) {
            final long terminal = consumer.completedTasks() + consumer.failedTasks() + consumer.cancelledTasks();
            final long open = consumer.queuedTasks() + consumer.activeTasks();
            if (consumer.submittedTasks() != terminal + open) {
                throw new AssertionError("consumer " + consumer.consumerId() + " invariant broken: submitted="
                    + consumer.submittedTasks() + " completed=" + consumer.completedTasks()
                    + " failed=" + consumer.failedTasks() + " cancelled=" + consumer.cancelledTasks()
                    + " queued=" + consumer.queuedTasks() + " active=" + consumer.activeTasks());
            }
            inFlight += open;
            submitted += consumer.submittedTasks();
        }
        if (metrics.submittedTasks() != submitted) {
            throw new AssertionError("scheduler submitted=" + metrics.submittedTasks()
                + " does not match the consumer sum=" + submitted);
        }
        return inFlight;
    }

    /**
     * Asserts the scheduler wide invariant in its settled form, where the sampled gauges are zero
     * and therefore cannot hide or fake a term.
     *
     * @param scheduler scheduler whose current snapshot is checked
     * @throws AssertionError if the scheduler is not settled or its counters do not add up
     */
    static void assertSettled(final AsyncComputeScheduler scheduler) {
        final SchedulerMetrics metrics = scheduler.metrics();
        assertLaneInvariant(metrics);
        if (metrics.queuedTasks() != 0) {
            throw new AssertionError("queue is not drained: queued=" + metrics.queuedTasks());
        }
        if (metrics.lanes().virtual().running() != 0) {
            throw new AssertionError("virtual lane still runs " + metrics.lanes().virtual().running() + " tasks");
        }
        final long terminal = metrics.completedTasks() + metrics.failedTasks() + metrics.cancelledTasks();
        if (metrics.submittedTasks() != terminal) {
            throw new AssertionError("settled scheduler invariant broken: submitted=" + metrics.submittedTasks()
                + " completed=" + metrics.completedTasks() + " failed=" + metrics.failedTasks()
                + " cancelled=" + metrics.cancelledTasks());
        }
        assertConsumerInvariant(metrics);
    }

    static void assertStateTerminal(final AsyncComputeScheduler scheduler, final Duration timeout) {
        try {
            if (!scheduler.awaitTermination(timeout)) {
                throw new AssertionError("scheduler did not terminate in " + timeout
                    + ", state=" + scheduler.state());
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for termination", interrupted);
        }
        if (scheduler.state() != AsyncComputeScheduler.SchedulerState.TERMINATED) {
            throw new AssertionError("state is " + scheduler.state());
        }
    }

    /**
     * Records which threads actually ran task bodies, how many at once and whether any body was
     * interrupted. Virtual threads never show up in {@link Thread#getAllStackTraces()}, so this is
     * the leak detector for the virtual lane.
     */
    static final class TaskProbe {

        private final Set<Thread> live = ConcurrentHashMap.newKeySet();
        private final Set<String> names = ConcurrentHashMap.newKeySet();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final AtomicBoolean virtual = new AtomicBoolean();
        private final AtomicBoolean platform = new AtomicBoolean();
        private final AtomicBoolean interrupted = new AtomicBoolean();

        void begin() {
            final Thread thread = Thread.currentThread();
            this.live.add(thread);
            this.names.add(thread.getName());
            if (thread.isVirtual()) {
                this.virtual.set(true);
            } else {
                this.platform.set(true);
            }
            this.active.incrementAndGet();
            this.peak.accumulateAndGet(this.active.get(), Math::max);
        }

        void end() {
            if (Thread.currentThread().isInterrupted()) {
                this.interrupted.set(true);
            }
            this.active.decrementAndGet();
            this.live.remove(Thread.currentThread());
        }

        int active() {
            return this.active.get();
        }

        int peak() {
            return this.peak.get();
        }

        boolean sawVirtual() {
            return this.virtual.get();
        }

        boolean sawPlatform() {
            return this.platform.get();
        }

        boolean wasInterrupted() {
            return this.interrupted.get();
        }

        int liveThreads() {
            return this.live.size();
        }

        Set<String> names() {
            return Set.copyOf(this.names);
        }
    }

    /**
     * Clock driven by tests; monotonic and wall clock readings advance together.
     */
    static final class FakeClock implements ComputeClock {

        private final AtomicLong nanos = new AtomicLong(1_000_000L);
        private volatile Instant instant = Instant.parse("2026-01-01T00:00:00Z");

        @Override
        public long nanoTime() {
            return this.nanos.get();
        }

        @Override
        public Instant instant() {
            return this.instant;
        }

        void advance(final Duration duration) {
            this.nanos.addAndGet(duration.toNanos());
            this.instant = this.instant.plus(duration);
        }
    }

    /**
     * Settings builder with lane-aware defaults: a small fixed platform pool, a roomy queue and
     * the production lane shape.
     */
    static final class SettingsBuilder {

        private final String prefix;
        private ComputeSettings.Workers workers;
        private int processorBudget = PROCESSORS;
        private ComputeSettings.QueueSettings queue =
            new ComputeSettings.QueueSettings(4_096, OverflowPolicy.REJECT);
        private ComputeSettings.Scheduling scheduling = ComputeSettings.Scheduling.defaults();
        private ComputeSettings.Consumers consumers = new ComputeSettings.Consumers(
            ConsumerPolicy.defaults(),
            Map.of(),
            true
        );
        private ComputeSettings.Metrics metrics =
            new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10L), false, millis(10L));
        private ComputeSettings.Adaptive adaptive = ComputeSettings.Adaptive.defaults();
        private ComputeSettings.Lanes lanes = new ComputeSettings.Lanes(
            new ComputeSettings.Lanes.Virtual(true, true, true, 8, millis(50L), 64));
        private Duration grace = Duration.ofSeconds(5L);

        SettingsBuilder(final String tag) {
            this.prefix = uniquePrefix(tag);
            this.workers = new ComputeSettings.Workers(4, 1, 4, Duration.ofSeconds(30L), this.prefix);
        }

        static SettingsBuilder of(final String tag) {
            return new SettingsBuilder(tag);
        }

        String prefix() {
            return this.prefix;
        }

        int processorBudget() {
            return this.processorBudget;
        }

        SettingsBuilder processorBudget(final int value) {
            this.processorBudget = value;
            return this;
        }

        SettingsBuilder fixedWorkers(final int threads) {
            this.workers = new ComputeSettings.Workers(threads, 1, threads, Duration.ofSeconds(30L), this.prefix);
            return this;
        }

        SettingsBuilder autoWorkers(final int min, final int max, final Duration keepAlive) {
            this.workers = new ComputeSettings.Workers(-1, min, max, keepAlive, this.prefix);
            return this;
        }

        SettingsBuilder capacity(final int value) {
            this.queue = new ComputeSettings.QueueSettings(value, this.queue.overflowPolicy());
            return this;
        }

        SettingsBuilder overflow(final OverflowPolicy value) {
            this.queue = new ComputeSettings.QueueSettings(this.queue.capacity(), value);
            return this;
        }

        SettingsBuilder scheduling(final ComputeSettings.Scheduling value) {
            this.scheduling = value;
            return this;
        }

        SettingsBuilder defaultPolicy(final ConsumerPolicy value) {
            this.consumers = new ComputeSettings.Consumers(value, this.consumers.overrides(),
                this.consumers.allowDynamicRegistration());
            return this;
        }

        SettingsBuilder metrics(final ComputeSettings.Metrics value) {
            this.metrics = value;
            return this;
        }

        SettingsBuilder adaptive(final ComputeSettings.Adaptive value) {
            this.adaptive = value;
            return this;
        }

        SettingsBuilder virtualLane(
            final boolean lowPriority,
            final boolean normalSpill,
            final int maxConcurrent,
            final Duration spillWait,
            final int spillQueueDepth
        ) {
            this.lanes = new ComputeSettings.Lanes(
                new ComputeSettings.Lanes.Virtual(true, lowPriority, normalSpill, maxConcurrent, spillWait,
                    spillQueueDepth));
            return this;
        }

        SettingsBuilder lanes(final ComputeSettings.Lanes value) {
            this.lanes = value;
            return this;
        }

        SettingsBuilder noVirtualLane() {
            this.lanes = ComputeSettings.Lanes.withoutVirtual();
            return this;
        }

        SettingsBuilder grace(final Duration value) {
            this.grace = value;
            return this;
        }

        ComputeSettings build() {
            return new ComputeSettings(
                true,
                this.workers,
                this.queue,
                this.scheduling,
                this.consumers,
                this.metrics,
                this.adaptive,
                this.lanes,
                this.grace
            );
        }
    }
}
