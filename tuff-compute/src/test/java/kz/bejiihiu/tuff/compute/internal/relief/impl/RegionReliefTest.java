package kz.bejiihiu.tuff.compute.internal.relief.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.RegionRelief;
import kz.bejiihiu.tuff.compute.RegionReliefMetrics;
import kz.bejiihiu.tuff.compute.RegionReliefOwner;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import kz.bejiihiu.tuff.compute.RegionReliefWork;
import kz.bejiihiu.tuff.compute.RegionReliefWork.Reason;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.engine.ComputeEngine;
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
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
        awaitCompleted(1L);
        awaitInFlightZero();
        final RegionReliefMetrics metrics = this.relief.metrics();
        assertEquals(1L, metrics.requests());
        assertEquals(1L, metrics.accepted());
        assertEquals(1L, metrics.completed());
        assertEquals(1, applies.get(), "the applicator must run exactly once");
        assertSame(this.regionThread, appliedOn.get(), "the applicator must run on the owner thread");
        assertEquals(0, metrics.inFlight());
    }

    @Test
    void aStaleResultIsReportedAsDiscardedOnTheOwnerThread() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();
        owner.currentEpoch = 99L;
        final ProbeWork work = new ProbeWork(7L);

        assertTrue(this.onRegion(() -> this.relief.submit(owner, work)));

        awaitUntil("the stale result is counted", Duration.ofSeconds(10L),
            () -> this.relief.metrics().stale() == 1L);
        assertTrue(work.terminal.await(10L, TimeUnit.SECONDS), "the discard callback must run");
        awaitInFlightZero();
        assertEquals(0, work.applies.get(), "a stale result must never be applied");
        assertEquals(1, work.discards.get(), "the work must hear about the discard exactly once");
        assertEquals(Reason.STALE, work.discardReason.get());
        assertEquals(0, work.failures.get());
        assertSame(this.regionThread, work.callbackThread.get(), "the callback runs on the owner thread");
        assertEquals(0L, this.relief.metrics().completed());
        assertEquals(0, this.relief.metrics().inFlight());
        assertEquals(1, work.computes.get(), "the compute body ran once on the worker");
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
    void cancellingAnOwnerReportsTheDiscardAndDropsTheResult() throws Exception {
        this.start(reliefSettings(true, 4), 1);
        final TestOwner owner = new TestOwner();
        final CountDownLatch release = new CountDownLatch(1);
        final ProbeWork work = new ProbeWork(7L, release);

        assertTrue(this.onRegion(() -> this.relief.submit(owner, work)));
        assertEquals(1, this.relief.cancel(owner));
        release.countDown();

        awaitUntil("the cancelled result is counted as dropped", Duration.ofSeconds(10L),
            () -> this.relief.metrics().dropped() == 1L);
        assertTrue(work.terminal.await(10L, TimeUnit.SECONDS), "the discard callback must run");
        awaitInFlightZero();
        assertEquals(0, work.applies.get(), "a cancelled region must not receive a result");
        assertEquals(1, work.discards.get());
        assertEquals(Reason.DROPPED, work.discardReason.get());
        assertSame(this.regionThread, work.callbackThread.get());
        assertEquals(0, this.relief.metrics().inFlight());
    }

    @Test
    void aFailedComputationIsReportedOnceAndNeverRerun() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();
        final ProbeWork work = new ProbeWork(7L, null, true);

        assertTrue(this.onRegion(() -> this.relief.submit(owner, work)));

        assertTrue(work.terminal.await(10L, TimeUnit.SECONDS), "the failure callback must run");
        awaitInFlightZero();
        assertEquals(0, work.applies.get(), "a failed computation must not be applied");
        assertEquals(1, work.failures.get(), "the failure must be reported exactly once");
        assertEquals(0, work.discards.get(), "a failure is not a discard");
        assertTrue(work.failure.get() instanceof IllegalStateException, String.valueOf(work.failure.get()));
        assertSame(this.regionThread, work.callbackThread.get(), "the callback runs on the owner thread");
        assertEquals(1, work.computes.get(), "the failed body is not rerun inline");
        assertEquals(1L, this.relief.metrics().failed());
        assertEquals(0L, this.relief.metrics().inlineFallbacks());
        assertEquals(0, this.relief.metrics().inFlight());
    }

    @Test
    void aStoppedOwnerIsRefusedBeforeAnyReservation() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();
        owner.stopped = true;
        final ProbeWork work = new ProbeWork(7L);

        assertFalse(this.onRegion(() -> this.relief.submit(owner, work)), "a stopped region gets nothing");

        assertEquals(0, work.computes.get(), "the work must not run for a stopped region");
        assertEquals(0, work.applies.get());
        assertEquals(1, work.discards.get(), "the refusal is reported");
        assertEquals(Reason.DROPPED, work.discardReason.get());
        assertSame(this.regionThread, work.callbackThread.get());
        assertEquals(1L, this.relief.metrics().requests());
        assertEquals(1L, this.relief.metrics().dropped());
        assertEquals(0L, this.relief.metrics().accepted());
        assertEquals(0, this.relief.metrics().inFlight());
    }

    @Test
    void aCancelledOwnerIsRefusedWithoutCreatingFreshState() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();
        this.relief.cancel(owner);
        final ProbeWork work = new ProbeWork(7L);

        assertFalse(this.onRegion(() -> this.relief.submit(owner, work)),
            "a cancelled owner must not start a new in-flight state");

        assertEquals(0, work.computes.get());
        assertEquals(0, work.applies.get());
        assertEquals(1, work.discards.get());
        assertEquals(Reason.DROPPED, work.discardReason.get());
        assertEquals(0L, this.relief.metrics().accepted());
        assertEquals(1L, this.relief.metrics().dropped());
        assertEquals(0, this.relief.metrics().inFlight());
    }

    @Test
    void anOwnerPortThatRefusesTheTaskStillGetsTheDiscard() throws Exception {
        this.start(reliefSettings(true, 8), 2);
        final TestOwner owner = new TestOwner();
        owner.refusingPort = true;
        final CountDownLatch release = new CountDownLatch(1);
        final ProbeWork work = new ProbeWork(7L, release);

        assertTrue(this.onRegion(() -> this.relief.submit(owner, work)));
        release.countDown();

        assertTrue(work.terminal.await(10L, TimeUnit.SECONDS), "the terminal callback must still run");
        awaitInFlightZero();
        assertEquals(1, work.discards.get(), "the discard is delivered exactly once");
        assertEquals(Reason.DROPPED, work.discardReason.get());
        assertEquals(0, work.applies.get(), "a refused port must not receive the result");
        assertEquals(1L, this.relief.metrics().dropped());
        assertEquals(0, this.relief.metrics().inFlight(), "the slot must be released");
        assertNotSame(
            this.regionThread,
            work.callbackThread.get(),
            "a port that refuses the task cannot be reached from the owner thread"
        );
    }

    @Test
    void anEnqueueRuntimeExceptionReleasesTheSlotAndRunsInline() {
        final ComputeSettings compute = this.compute(true, 2, Map.of());
        final FlakyEngine engine = new FlakyEngine(compute);
        this.clock = new FakeClock();
        final RegionReliefImpl service = new RegionReliefImpl(engine, reliefSettings(true, 1), this.clock);
        final TestOwner owner = new TestOwner(Thread.currentThread());
        engine.relief(service);

        final ProbeWork first = new ProbeWork(7L);
        assertFalse(service.submit(owner, first), "a request the engine never took runs inline");
        assertEquals(1, first.applies.get());
        assertEquals(1L, service.metrics().inlineFallbacks());

        // the per-region slot is one, so a leak would make this second request fall back too
        final ProbeWork second = new ProbeWork(7L);
        assertTrue(service.submit(owner, second), "the reservation must have been returned");
        assertEquals(1, second.applies.get(), "the accepted request is applied on the owner thread");
        assertEquals(1L, service.metrics().accepted());
        assertEquals(1L, service.metrics().inlineFallbacks(), "only the failed submission fell back");
        assertEquals(0, service.metrics().inFlight());
    }

    @Test
    void anUntrackedSubmissionIsNotCountedAsAccepted() {
        final ComputeSettings compute = this.compute(true, 2, Map.of());
        final CancellingEngine engine = new CancellingEngine(compute);
        final RegionReliefImpl service = new RegionReliefImpl(engine, reliefSettings(true, 8), new FakeClock());
        final TestOwner owner = new TestOwner(Thread.currentThread());
        engine.cancelDuringEnqueue(service, owner);
        final ProbeWork work = new ProbeWork(7L);

        // the fake engine cancels the owner inside enqueue, which is the exact window fix 3 is about
        assertTrue(service.submit(owner, work));

        assertEquals(0L, service.metrics().accepted(),
            "a submission cancelled before it was tracked was never accepted");
        assertEquals(1L, service.metrics().requests());
        assertEquals(1L, service.metrics().dropped());
        assertEquals(1, work.discards.get());
        assertEquals(Reason.DROPPED, work.discardReason.get());
        assertEquals(0, work.computes.get());
        assertEquals(0, service.metrics().inFlight());
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
        awaitUntil("the applied request is counted", Duration.ofSeconds(10L),
            () -> this.relief.metrics().completed() == 1L);

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

    private void awaitCompleted(final long expected) {
        awaitUntil("the relief service counted " + expected + " completed request(s)", Duration.ofSeconds(10L),
            () -> this.relief.metrics().completed() == expected);
    }

    private void awaitInFlightZero() {
        awaitUntil("the relief service released every in-flight slot", Duration.ofSeconds(10L),
            () -> this.relief.metrics().inFlight() == 0);
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

        private final Thread ownerThread;
        private volatile long currentEpoch = 7L;
        private volatile boolean runInline;
        private volatile boolean refusingPort;
        private volatile boolean stopped;

        private TestOwner() {
            this(RegionReliefTest.this.regionThread);
        }

        private TestOwner(final Thread ownerThread) {
            this.ownerThread = ownerThread;
        }

        @Override
        public Thread thread() {
            return this.ownerThread;
        }

        @Override
        public boolean isCurrent(final long epoch) {
            return epoch == this.currentEpoch;
        }

        @Override
        public boolean isStopped() {
            return this.stopped;
        }

        @Override
        public void runOnOwner(final Runnable task) {
            if (this.refusingPort) {
                throw new IllegalStateException("the region refused the task");
            }
            if (this.runInline || Thread.currentThread() == this.ownerThread) {
                task.run();
                return;
            }
            RegionReliefTest.this.region.execute(task);
        }
    }

    /**
     * Work that records which terminal callback ran and on which thread.
     */
    private final class ProbeWork implements RegionReliefWork<String, String> {

        private final long epoch;
        private final @Nullable CountDownLatch computeGate;
        private final boolean fail;
        private final AtomicInteger computes = new AtomicInteger();
        private final AtomicInteger applies = new AtomicInteger();
        private final AtomicInteger failures = new AtomicInteger();
        private final AtomicInteger discards = new AtomicInteger();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicReference<Reason> discardReason = new AtomicReference<>();
        private final AtomicReference<Thread> callbackThread = new AtomicReference<>();
        private final CountDownLatch terminal = new CountDownLatch(1);

        private ProbeWork(final long epoch) {
            this(epoch, null, false);
        }

        private ProbeWork(final long epoch, final @Nullable CountDownLatch computeGate) {
            this(epoch, computeGate, false);
        }

        private ProbeWork(final long epoch, final @Nullable CountDownLatch computeGate, final boolean fail) {
            this.epoch = epoch;
            this.computeGate = computeGate;
            this.fail = fail;
        }

        @Override
        public String snapshot() {
            return "snapshot";
        }

        @Override
        public long epoch() {
            return this.epoch;
        }

        @Override
        public String compute(final String value) throws Exception {
            this.computes.incrementAndGet();
            if (this.computeGate != null && !this.computeGate.await(10L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("compute gate was never released");
            }
            if (this.fail) {
                throw new IllegalStateException("boom");
            }
            RegionReliefTest.this.clock.advance(Duration.ofMillis(5L));
            return value.toUpperCase(Locale.ROOT);
        }

        @Override
        public void apply(final String result) {
            this.applies.incrementAndGet();
            this.callbackThread.set(Thread.currentThread());
            this.terminal.countDown();
        }

        @Override
        public void onFailure(final Throwable thrown) {
            this.failures.incrementAndGet();
            this.failure.set(thrown);
            this.callbackThread.set(Thread.currentThread());
            this.terminal.countDown();
        }

        @Override
        public void onDiscard(final Reason reason) {
            this.discards.incrementAndGet();
            this.discardReason.set(reason);
            this.callbackThread.set(Thread.currentThread());
            this.terminal.countDown();
        }
    }

    /**
     * Engine seam that cancels the owner inside {@code enqueue}, reproducing the exact window between
     * the in-flight reservation and the request being tracked.
     */
    private static final class CancellingEngine implements ComputeEngine {

        private final ComputeSettings settings;
        private final AtomicReference<RegionReliefOwner> ownerToCancel = new AtomicReference<>();
        private volatile RegionRelief relief;

        private CancellingEngine(final ComputeSettings settings) {
            this.settings = settings;
        }

        private void cancelDuringEnqueue(final RegionRelief service, final RegionReliefOwner owner) {
            this.relief = service;
            this.ownerToCancel.set(owner);
        }

        @Override
        public <T> ComputeTaskHandle<T> enqueue(final ComputeRequest<T> request) {
            this.relief.cancel(this.ownerToCancel.get());
            return new PendingHandle<>(request);
        }

        @Override
        public AsyncComputeScheduler.SchedulerState state() {
            return AsyncComputeScheduler.SchedulerState.RUNNING;
        }

        @Override
        public ComputeSettings settings() {
            return this.settings;
        }

        @Override
        public Optional<ComputeConsumer> consumer(final String consumerId) {
            return Optional.empty();
        }

        @Override
        public void start() {
            throw new AssertionError("the fake engine is started by construction");
        }

        @Override
        public void shutdown(final Duration gracePeriod) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public void shutdownNow() {
            throw new AssertionError("not used by this test");
        }

        @Override
        public void applyProcessorBudget(final int processors) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public boolean awaitTermination(final Duration timeout) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public boolean cancel(final long taskId) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public ComputeConsumer registerConsumer(final ComputeConsumer consumer) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public boolean unregisterConsumer(final String consumerId) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public void updateConsumerPolicy(final String consumerId, final ConsumerPolicy policy) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public Set<String> consumerIds() {
            return Set.of();
        }

        @Override
        public SchedulerMetrics metrics() {
            throw new AssertionError("not used by this test");
        }

        @Override
        public ComputeClock clock() {
            throw new AssertionError("not used by this test");
        }

        @Override
        public RegionRelief regionRelief() {
            return this.relief;
        }
    }

    /**
     * Engine seam whose first submission fails with a plain runtime exception, which is what a queue
     * hook or an eviction listener throwing looks like from the relief service.
     */
    private static final class FlakyEngine implements ComputeEngine {

        private final ComputeSettings settings;
        private final AtomicInteger failures = new AtomicInteger(1);
        private volatile RegionRelief relief;

        private FlakyEngine(final ComputeSettings settings) {
            this.settings = settings;
        }

        private void relief(final RegionRelief service) {
            this.relief = service;
        }

        @Override
        public <T> ComputeTaskHandle<T> enqueue(final ComputeRequest<T> request) {
            if (this.failures.getAndDecrement() > 0) {
                throw new IllegalStateException("a queue hook rejected the submission");
            }
            // completes on the calling region thread, so the terminal decision runs inline there
            final PendingHandle<T> handle = new PendingHandle<>(request);
            try {
                handle.complete(request.task().execute());
            } catch (final Exception failure) {
                handle.future().completeExceptionally(failure);
            }
            return handle;
        }

        @Override
        public AsyncComputeScheduler.SchedulerState state() {
            return AsyncComputeScheduler.SchedulerState.RUNNING;
        }

        @Override
        public ComputeSettings settings() {
            return this.settings;
        }

        @Override
        public Optional<ComputeConsumer> consumer(final String consumerId) {
            return Optional.empty();
        }

        @Override
        public void start() {
            throw new AssertionError("the fake engine is started by construction");
        }

        @Override
        public void shutdown(final Duration gracePeriod) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public void shutdownNow() {
            throw new AssertionError("not used by this test");
        }

        @Override
        public void applyProcessorBudget(final int processors) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public boolean awaitTermination(final Duration timeout) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public boolean cancel(final long taskId) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public ComputeConsumer registerConsumer(final ComputeConsumer consumer) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public boolean unregisterConsumer(final String consumerId) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public void updateConsumerPolicy(final String consumerId, final ConsumerPolicy policy) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public Set<String> consumerIds() {
            return Set.of();
        }

        @Override
        public SchedulerMetrics metrics() {
            throw new AssertionError("not used by this test");
        }

        @Override
        public ComputeClock clock() {
            throw new AssertionError("not used by this test");
        }

        @Override
        public RegionRelief regionRelief() {
            return this.relief;
        }
    }

    /**
     * Handle of a task that never starts; cancellation completes it the way the engine would.
     *
     * @param <T> type of the produced value
     */
    private static final class PendingHandle<T> implements ComputeTaskHandle<T> {

        private final ComputeRequest<T> request;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        private PendingHandle(final ComputeRequest<T> request) {
            this.request = request;
        }

        @Override
        public long taskId() {
            return 1L;
        }

        @Override
        public String consumerId() {
            return this.request.consumerId();
        }

        @Override
        public ComputeTaskType taskType() {
            return this.request.taskType();
        }

        @Override
        public ComputePriority priority() {
            return this.request.priority();
        }

        @Override
        public Instant submittedAt() {
            return Instant.EPOCH;
        }

        @Override
        public CompletableFuture<T> future() {
            return this.future;
        }

        private void complete(final T value) {
            this.future.complete(value);
        }

        @Override
        public State state() {
            return this.future.isDone() ? State.CANCELLED : State.QUEUED;
        }

        @Override
        public boolean cancel() {
            return this.future.completeExceptionally(new ComputeCancelledException(
                1L, ComputeCancelledException.Reason.CALLER, "cancelled by the test engine"
            ));
        }

        @Override
        public boolean isCancellationRequested() {
            return this.future.isDone();
        }

        @Override
        public boolean isCancelled() {
            return this.future.isCompletedExceptionally();
        }

        @Override
        public boolean isDone() {
            return this.future.isDone();
        }

        @Override
        public long queueWaitNanos() {
            return -1L;
        }

        @Override
        public long executionNanos() {
            return -1L;
        }

        @Override
        public @Nullable Throwable failure() {
            return null;
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
