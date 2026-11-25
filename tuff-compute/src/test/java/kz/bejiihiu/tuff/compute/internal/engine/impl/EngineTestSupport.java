package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;

/**
 * Shared fixtures for the engine tests.
 *
 * <p>Everything here drives the real engine built from the real queue, pool, metrics and adaptive
 * implementations. The only test double is the clock, which is a documented extension point of the
 * scheduler and is required to drive aging, expiry and the adaptive window without sleeping.
 */
final class EngineTestSupport {

    /** Processor count used for deterministic automatic sizing. */
    static final int PROCESSORS = 8;

    private static final AtomicInteger PREFIX_SEQUENCE = new AtomicInteger();

    private static final long MILLIS = TimeUnit.MILLISECONDS.toNanos(1L);

    private EngineTestSupport() {
    }

    /**
     * Returns a thread name prefix unique to one test, so a scan cannot see threads of another test
     * that is still finishing in the same JVM.
     *
     * @param tag short label
     * @return a unique prefix
     */
    static String uniquePrefix(final String tag) {
        return "tuff-engine-" + tag + '-' + PREFIX_SEQUENCE.incrementAndGet();
    }

    static SettingsBuilder settings() {
        return new SettingsBuilder();
    }

    static Duration millis(final long value) {
        return Duration.ofMillis(value);
    }

    static long millisNanos(final long value) {
        return value * MILLIS;
    }

    static AsyncComputeScheduler scheduler(final ComputeEngineImpl engine) {
        return new AsyncComputeSchedulerImpl(engine);
    }

    static ComputeEngineImpl engine(final ComputeSettings settings, final ComputeClock clock) {
        return new ComputeEngineImpl(settings, clock, PROCESSORS);
    }

    static AsyncComputeScheduler startedScheduler(final ComputeSettings settings, final ComputeClock clock) {
        final AsyncComputeScheduler scheduler = new AsyncComputeSchedulerImpl(engine(settings, clock));
        scheduler.start();
        return scheduler;
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
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting", interrupted);
        }
    }

    static List<String> engineThreadNames(final String prefix) {
        return Thread.getAllStackTraces().keySet().stream()
            .map(Thread::getName)
            .filter(name -> name.startsWith(prefix))
            .toList();
    }

    static void awaitNoEngineThreads(final String prefix, final Duration timeout) {
        assertEventually("no engine threads remain for " + prefix, timeout, () -> engineThreadNames(prefix).isEmpty());
    }

    static ComputeCancelledException cancellationOf(final Exception thrown) {
        Throwable current = thrown;
        while (current != null && !(current instanceof ComputeCancelledException)) {
            current = current.getCause();
        }
        if (current == null) {
            throw new AssertionError("expected a compute cancellation, got " + thrown);
        }
        return (ComputeCancelledException) current;
    }

    /**
     * Waits for a future that must end cancelled and returns the cancellation.
     *
     * <p>{@link CompletableFuture#join()} wraps a cancellation in a fresh
     * {@link java.util.concurrent.CancellationException} on modern JDKs, so the tests inspect the
     * cause chain instead of the thrown type.
     *
     * @param future future to wait for
     * @return the compute cancellation carried by the future
     * @throws AssertionError if the future completed normally or the thread was interrupted
     */
    static ComputeCancelledException cancellationOf(final CompletableFuture<?> future) {
        try {
            future.get();
        } catch (CancellationException cancelled) {
            return cancellationOf(cancelled);
        } catch (ExecutionException failed) {
            return cancellationOf(failed);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for cancellation", interrupted);
        }
        throw new AssertionError("expected a cancelled future, but it completed normally");
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
     * Small settings builder so each test only states the knob it actually cares about.
     */
    static final class SettingsBuilder {

        private final String prefix = uniquePrefix("test");
        private boolean enabled = true;
        private ComputeSettings.Workers workers =
            new ComputeSettings.Workers(4, 1, 4, Duration.ofSeconds(30), this.prefix);
        private ComputeSettings.QueueSettings queue =
            new ComputeSettings.QueueSettings(4096, OverflowPolicy.REJECT);
        private ComputeSettings.Scheduling scheduling = ComputeSettings.Scheduling.defaults();
        private ComputeSettings.Consumers consumers = new ComputeSettings.Consumers(
            ConsumerPolicy.defaults(),
            Map.of(),
            true
        );
        private ComputeSettings.Metrics metrics = new ComputeSettings.Metrics(
            true,
            false,
            Duration.ofSeconds(10),
            false,
            Duration.ofMillis(20)
        );
        private ComputeSettings.Adaptive adaptive = ComputeSettings.Adaptive.defaults();
        private Duration grace = Duration.ofSeconds(3);

        private SettingsBuilder() {
        }

        SettingsBuilder workers(final ComputeSettings.Workers value) {
            this.workers = value;
            return this;
        }

        SettingsBuilder fixedWorkers(final int threads) {
            this.workers = new ComputeSettings.Workers(threads, 1, threads, Duration.ofSeconds(30), this.prefix);
            return this;
        }

        SettingsBuilder autoWorkers(final int min, final int max, final Duration keepAlive) {
            this.workers = new ComputeSettings.Workers(-1, min, max, keepAlive, this.prefix);
            return this;
        }

        String prefix() {
            return this.prefix;
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
            this.consumers = new ComputeSettings.Consumers(value, this.consumers.overrides(), this.consumers.allowDynamicRegistration());
            return this;
        }

        SettingsBuilder overrides(final Map<String, ConsumerPolicy> value) {
            this.consumers = new ComputeSettings.Consumers(this.consumers.defaultPolicy(), value, this.consumers.allowDynamicRegistration());
            return this;
        }

        SettingsBuilder dynamicRegistration(final boolean value) {
            this.consumers = new ComputeSettings.Consumers(this.consumers.defaultPolicy(), this.consumers.overrides(), value);
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

        SettingsBuilder grace(final Duration value) {
            this.grace = value;
            return this;
        }

        SettingsBuilder enabled(final boolean value) {
            this.enabled = value;
            return this;
        }

        ComputeSettings build() {
            return new ComputeSettings(
                this.enabled,
                this.workers,
                this.queue,
                this.scheduling,
                this.consumers,
                this.metrics,
                this.adaptive,
                this.grace
            );
        }
    }
}
