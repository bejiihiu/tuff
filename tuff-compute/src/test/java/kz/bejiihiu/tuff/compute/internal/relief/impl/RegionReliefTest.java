package kz.bejiihiu.tuff.compute.internal.relief.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.RegionRelief;
import kz.bejiihiu.tuff.compute.RegionReliefMetrics;
import kz.bejiihiu.tuff.compute.RegionReliefOwner;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import kz.bejiihiu.tuff.compute.RegionReliefWork;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeBudgetPort;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behaviour of the relief path against the real engine: real queue, real workers, real cancellation.
 *
 * <p>The only test doubles are the engine clock (a documented seam) and the owner, which is a real
 * single threaded executor standing in for a region thread. Waits are latches and convergence polls,
 * never fixed sleeps.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class RegionReliefTest {

    private static final AtomicInteger PREFIXES = new AtomicInteger();

    private AsyncComputeScheduler scheduler;
    private ExecutorService region;
    private Thread regionThread;
    private RegionRelief relief;
    private FakeClock clock;

    @AfterEach
    void stopEverything() throws InterruptedException {
        if (this.scheduler != null) {
            this.scheduler.shutdown(Duration.ZERO);
            this.scheduler.awaitTermination(Duration.ofSeconds(5L));
            this.scheduler = null;
        }
        if (this.region != null) {
            this.region.shutdownNow();
            this.region.awaitTermination(5L, TimeUnit.SECONDS);
            this.region = null;
        }
        this.regionThread = null;
        this.relief = null;
        this.clock = null;
    }

    @Test
    void anAcceptedRequestIsAppliedExactlyOnceOnTheOwnerThread() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();
        final CountDownLatch applied = new CountDownLatch(1);
        final AtomicInteger applies = new AtomicInteger();
        final AtomicReference<Thread> appliedOn = new AtomicReference<>();

        final boolean accepted = this.onRegion(() -> this.relief.submit(
            owner, new TestWork("snapshot", 7L, applied, applies, appliedOn, null)
        ));

        assertTrue(accepted, "a healthy pool must take the request");
        assertTrue(applied.await(10L, TimeUnit.SECONDS), "the applicator must run");
        final RegionReliefMetrics metrics = this.relief.metrics();
        assertEquals(1L, metrics.requests());
        assertEquals(1L, metrics.accepted());
        assertEquals(1L, metrics.completed());
        assertEquals(1, applies.get(), "the applicator must run exactly once");
        assertSame(this.regionThread, appliedOn.get(), "the applicator must run on the owner thread");
        assertEquals(0, metrics.inFlight());
    }

    @Test
    void aStaleResultIsDroppedAndCounted() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();
        final AtomicInteger applies = new AtomicInteger();
        owner.currentEpoch = 99L;

        assertTrue(this.onRegion(() -> this.relief.submit(
            owner, new TestWork("snapshot", 7L, new CountDownLatch(1), applies, new AtomicReference<>(), null)
        )));

        awaitUntil("the stale result is counted", Duration.ofSeconds(10L),
            () -> this.relief.metrics().stale() == 1L);
        assertEquals(0, applies.get(), "a stale result must never be applied");
        assertEquals(0L, this.relief.metrics().completed());
        assertEquals(0, this.relief.metrics().inFlight());
    }

    @Test
    void aDisabledReliefPathRunsTheWorkInline() throws Exception {
        this.start(reliefSettings(false, 8), 2);
        final TestOwner owner = new TestOwner();
        final AtomicInteger applies = new AtomicInteger();

        final boolean accepted = this.onRegion(() -> this.relief.submit(
            owner,
            new TestWork("snapshot", 7L, new CountDownLatch(1), applies, new AtomicReference<>(), null)
        ));

        assertFalse(accepted, "relief is off, so the work runs inline");
        assertEquals(1, applies.get(), "inline work runs before submit returns");
        assertEquals(1L, this.relief.metrics().inlineFallbacks());
        assertEquals(0L, this.relief.metrics().accepted());
    }

    @Test
    void aDisabledComputeEngineRunsTheWorkInline() throws Exception {
        this.startWithCompute(this.compute(false, 2, Map.of()), reliefSettings(true, 8));
        final TestOwner owner = new TestOwner();
        final AtomicInteger applies = new AtomicInteger();

        assertFalse(this.onRegion(() -> this.relief.submit(
            owner,
            new TestWork("snapshot", 7L, new CountDownLatch(1), applies, new AtomicReference<>(), null)
        )));
        assertEquals(1, applies.get());
        assertEquals(1L, this.relief.metrics().inlineFallbacks());
    }

    @Test
    void aRefusedSubmissionRunsTheWorkInline() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        // a disabled consumer is exactly what a refusal looks like from the submit path
        this.scheduler.updateConsumerPolicy(RegionRelief.CONSUMER_ID, disabledPolicy());
        final TestOwner owner = new TestOwner();
        final AtomicInteger applies = new AtomicInteger();

        assertFalse(this.onRegion(() -> this.relief.submit(
            owner,
            new TestWork("snapshot", 7L, new CountDownLatch(1), applies, new AtomicReference<>(), null)
        )));
        assertEquals(1, applies.get());
        assertEquals(1L, this.relief.metrics().inlineFallbacks());
        assertEquals(0L, this.relief.metrics().accepted());
    }

    @Test
    void theInFlightCapTurnsTheNextRequestInline() throws Exception {
        final ConsumerPolicy singleSlot = new ConsumerPolicy(
            true, ConsumerPolicy.ConcurrencyLimit.absolute(1), 8, 1.0d, OverflowPolicy.REJECT, Duration.ZERO
        );
        this.start(reliefSettings(true, 2), 1, singleSlot);
        final TestOwner owner = new TestOwner();
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger applies = new AtomicInteger();

        assertTrue(this.onRegion(() -> this.relief.submit(owner, this.blockingWork(release))));
        assertTrue(this.onRegion(() -> this.relief.submit(owner, this.blockingWork(release))));
        assertEquals(2L, this.relief.metrics().accepted());
        assertEquals(2, this.relief.metrics().inFlight());

        final boolean third = this.onRegion(() -> this.relief.submit(
            owner, new TestWork("snapshot", 7L, new CountDownLatch(1), applies, new AtomicReference<>(), null)
        ));

        assertFalse(third, "the third request is over the per-region cap and runs inline");
        assertEquals(1, applies.get());
        assertEquals(1L, this.relief.metrics().inlineFallbacks());
        release.countDown();
        awaitUntil("the accepted requests finish", Duration.ofSeconds(10L),
            () -> this.relief.metrics().inFlight() == 0);
    }

    @Test
    void cancellingAnOwnerDropsItsPendingResults() throws Exception {
        this.start(reliefSettings(true, 4), 1);
        final TestOwner owner = new TestOwner();
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger applies = new AtomicInteger();

        assertTrue(this.onRegion(() -> this.relief.submit(
            owner, new TestWork("snapshot", 7L, new CountDownLatch(1), applies, new AtomicReference<>(), release)
        )));
        assertEquals(1, this.relief.cancel(owner));
        release.countDown();

        awaitUntil("the cancelled result is counted as dropped", Duration.ofSeconds(10L),
            () -> this.relief.metrics().dropped() == 1L);
        assertEquals(0, applies.get(), "a cancelled region must not receive a result");
        assertEquals(0, this.relief.metrics().inFlight());
    }

    @Test
    void anApplyScheduledOffTheOwnerThreadIsRefused() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();
        // the owner hands the applicator to the compute worker instead of the region thread
        owner.runInline = true;
        final AtomicInteger applies = new AtomicInteger();
        final CountDownLatch release = new CountDownLatch(1);

        assertTrue(this.onRegion(() -> this.relief.submit(
            owner, new TestWork("snapshot", 7L, new CountDownLatch(1), applies, new AtomicReference<>(), release)
        )));
        // the result has to arrive after the completion callback is attached: an already completed
        // future runs it on the calling region thread, which is a legitimate context
        release.countDown();

        awaitUntil("the off-region apply is dropped", Duration.ofSeconds(10L),
            () -> this.relief.metrics().dropped() == 1L);
        assertEquals(0, applies.get(), "ownership is absolute: an off-region applicator never runs");
    }

    @Test
    void anExpiredRequestIsRerunInlineOnTheOwnerThread() throws Exception {
        this.start(reliefSettings(true, 8), 1);
        final CountDownLatch release = new CountDownLatch(1);
        // the only worker is busy, so the relief request waits in the queue and can expire there
        final CompletableFuture<Boolean> blocker = this.scheduler.submit(
            ComputePriority.NORMAL, () -> {
                if (!release.await(10L, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the blocker was never released");
                }
                return Boolean.TRUE;
            }
        );
        awaitUntil("the blocker occupies the worker", Duration.ofSeconds(10L),
            () -> this.scheduler.metrics().activeWorkers() == 1);

        final TestOwner owner = new TestOwner();
        final CountDownLatch applied = new CountDownLatch(1);
        final AtomicInteger applies = new AtomicInteger();
        assertTrue(this.onRegion(() -> this.relief.submit(
            owner, new TestWork("snapshot", 7L, applied, applies, new AtomicReference<>(), null)
        )));

        // past the consumer's max-queue-wait; the janitor sweeps on the frozen clock's new time
        this.clock.advance(Duration.ofSeconds(3L));

        awaitUntil("the expired request runs inline", Duration.ofSeconds(10L),
            () -> this.relief.metrics().expired() == 1L);
        assertTrue(applied.await(10L, TimeUnit.SECONDS), "the same work must be applied after expiry");
        assertEquals(1, applies.get());
        assertEquals(1L, this.relief.metrics().inlineFallbacks());
        assertEquals(1L, this.relief.metrics().completed());

        release.countDown();
        blocker.get(10L, TimeUnit.SECONDS);
    }

    @Test
    void submittingFromAnotherThreadIsRejected() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();

        assertThrows(IllegalStateException.class, () -> this.relief.submit(
            owner,
            new TestWork("snapshot", 7L, new CountDownLatch(1), new AtomicInteger(), new AtomicReference<>(), null)
        ));
    }

    @Test
    void theSavedTimeEstimateCountsAppliedComputeTime() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();
        final CountDownLatch applied = new CountDownLatch(1);

        assertTrue(this.onRegion(() -> this.relief.submit(
            owner, new TestWork("snapshot", 7L, applied, new AtomicInteger(), new AtomicReference<>(), null)
        )));
        assertTrue(applied.await(10L, TimeUnit.SECONDS));

        final RegionReliefMetrics metrics = this.relief.metrics();
        assertEquals(1L, metrics.completed());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(5L), metrics.estimatedSavedNanos(),
            "the estimate is the compute body's time on the worker");
    }

    @Test
    void theConsumerIsRegisteredAtNormalPriorityOnly() throws Exception {
        this.start(reliefSettings(false, 8), 2);
        final var consumer = this.scheduler.consumer(RegionRelief.CONSUMER_ID).orElseThrow();
        assertEquals(ComputePriority.NORMAL, consumer.maxPriority());
        assertTrue(consumer.policy().enabled());
        assertTrue(this.relief.metrics().consumerRegistered());
    }

    private void start(final RegionReliefSettings settings, final int workers) throws Exception {
        this.start(settings, workers, null);
    }

    private void start(
        final RegionReliefSettings settings,
        final int workers,
        final @Nullable ConsumerPolicy reliefPolicy
    ) throws Exception {
        final Map<String, ConsumerPolicy> overrides = reliefPolicy == null
            ? Map.of()
            : Map.of(RegionRelief.CONSUMER_ID, reliefPolicy);
        this.startWithCompute(this.compute(true, workers, overrides), settings);
    }

    private void startWithCompute(final ComputeSettings compute, final RegionReliefSettings settings) throws Exception {
        this.clock = new FakeClock();
        this.scheduler = ComputeSchedulers.create(
            compute,
            this.clock,
            ComputeBudgetPort.fixed(8, "test"),
            ComputeThreadFactoryPort.legacy(),
            settings
        );
        this.scheduler.start();
        this.relief = this.scheduler.regionRelief().orElseThrow();
        this.region = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, this.prefix() + "-region"));
        this.regionThread = this.region.submit(Thread::currentThread).get(5L, TimeUnit.SECONDS);
    }

    private ComputeSettings compute(final boolean enabled, final int workers, final Map<String, ConsumerPolicy> overrides) {
        final String prefix = this.prefix();
        return new ComputeSettings(
            enabled,
            new ComputeSettings.Workers(workers, 1, workers, Duration.ofSeconds(30), prefix),
            new ComputeSettings.QueueSettings(64, OverflowPolicy.REJECT),
            ComputeSettings.Scheduling.defaults(),
            new ComputeSettings.Consumers(ConsumerPolicy.defaults(), overrides, true),
            new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, Duration.ofMillis(20)),
            ComputeSettings.Adaptive.defaults(),
            ComputeSettings.Lanes.withoutVirtual(),
            Duration.ofSeconds(3)
        );
    }

    private static RegionReliefSettings reliefSettings(final boolean enabled, final int cap) {
        return new RegionReliefSettings(enabled, cap, 0, RegionReliefSettings.defaultConsumerPolicy());
    }

    private static ConsumerPolicy disabledPolicy() {
        return new ConsumerPolicy(
            false, ConsumerPolicy.ConcurrencyLimit.automatic(), -1, 1.0d, OverflowPolicy.REJECT, Duration.ZERO
        );
    }

    private <T> T onRegion(final Callable<T> action) throws Exception {
        return this.region.submit(action).get(10L, TimeUnit.SECONDS);
    }

    private String prefix() {
        return "tuff-relief-" + PREFIXES.incrementAndGet();
    }

    private RegionReliefWork<String, String> blockingWork(final CountDownLatch release) {
        return new TestWork("blocked", 1L, new CountDownLatch(1), new AtomicInteger(), new AtomicReference<>(), release);
    }

    private final class TestWork implements RegionReliefWork<String, String> {

        private final String snapshot;
        private final long epoch;
        private final CountDownLatch applied;
        private final AtomicInteger applies;
        private final AtomicReference<Thread> appliedOn;
        private final @Nullable CountDownLatch computeGate;

        private TestWork(
            final String snapshot,
            final long epoch,
            final CountDownLatch applied,
            final AtomicInteger applies,
            final AtomicReference<Thread> appliedOn,
            final @Nullable CountDownLatch computeGate
        ) {
            this.snapshot = snapshot;
            this.epoch = epoch;
            this.applied = applied;
            this.applies = applies;
            this.appliedOn = appliedOn;
            this.computeGate = computeGate;
        }

        @Override
        public String snapshot() {
            return this.snapshot;
        }

        @Override
        public long epoch() {
            return this.epoch;
        }

        @Override
        public String compute(final String value) throws Exception {
            if (this.computeGate != null && !this.computeGate.await(10L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("compute gate was never released");
            }
            // advancing the frozen clock inside the body is what makes the saved time estimate
            // deterministic without sleeping in the test
            RegionReliefTest.this.clock.advance(Duration.ofMillis(5L));
            return value.toUpperCase(Locale.ROOT);
        }

        @Override
        public void apply(final String result) {
            this.applies.incrementAndGet();
            this.appliedOn.set(Thread.currentThread());
            this.applied.countDown();
        }
    }

    private final class TestOwner implements RegionReliefOwner {

        private volatile long currentEpoch = 7L;
        private volatile boolean runInline;

        @Override
        public Thread thread() {
            return RegionReliefTest.this.regionThread;
        }

        @Override
        public boolean isCurrent(final long epoch) {
            return epoch == this.currentEpoch;
        }

        @Override
        public boolean isStopped() {
            return false;
        }

        @Override
        public void runOnOwner(final Runnable task) {
            if (this.runInline) {
                task.run();
                return;
            }
            RegionReliefTest.this.region.execute(task);
        }
    }

    /**
     * Monotonic clock driven by the work bodies, so no test has to sleep for time to pass.
     */
    private static final class FakeClock implements ComputeClock {

        private final AtomicLong nanos = new AtomicLong(1_000_000L);

        @Override
        public long nanoTime() {
            return this.nanos.get();
        }

        @Override
        public Instant instant() {
            return Instant.EPOCH;
        }

        private void advance(final Duration duration) {
            this.nanos.addAndGet(duration.toNanos());
        }
    }

    private static void awaitUntil(final String what, final Duration timeout, final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(2L);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for " + what, interrupted);
            }
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("timed out waiting for " + what);
        }
    }
}
