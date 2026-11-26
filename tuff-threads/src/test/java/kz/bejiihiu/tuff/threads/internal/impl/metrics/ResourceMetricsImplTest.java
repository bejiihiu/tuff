package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResourceMetricsImplTest {

    private static final PoolId REGION = PoolId.REGION_EXECUTION;
    private static final PoolId COMPUTE = PoolId.COMPUTE;

    private final MetricsFixtures.FakeClock clock = new MetricsFixtures.FakeClock();
    private final MetricsFixtures.FakeCounters counters = new MetricsFixtures.FakeCounters();
    private final MetricsFixtures.FakeRegistry registry = new MetricsFixtures.FakeRegistry();
    private final MetricsFixtures.RecordingSink sink = new MetricsFixtures.RecordingSink();

    @Test
    void countsThreadsPerPoolAndGlobally() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(REGION, 0), spec(COMPUTE, 2));
        this.registry.put(
            REGION,
            thread(1L, "region-1", REGION, Thread.State.RUNNABLE),
            thread(2L, "region-2", REGION, Thread.State.WAITING),
            thread(3L, "region-3", REGION, Thread.State.TIMED_WAITING)
        );
        this.registry.put(
            COMPUTE,
            thread(4L, "compute-1", COMPUTE, Thread.State.RUNNABLE),
            thread(5L, "compute-2", COMPUTE, Thread.State.BLOCKED)
        );

        final ResourceSnapshot snapshot = this.metrics(pools, MetricsConfig.defaults()).snapshot();

        assertEquals(5, snapshot.totalThreads());
        assertEquals(2, snapshot.activeThreads());
        assertEquals(2, snapshot.idleThreads());
        assertEquals(5, snapshot.peakThreads());

        final PoolSnapshot region = snapshot.pool(REGION).orElseThrow();
        assertEquals(3, region.aliveThreads());
        assertEquals(1, region.activeThreads());
        assertEquals(2, region.idleThreads());
        assertEquals(0, region.coreThreads());

        final PoolSnapshot compute = snapshot.pool(COMPUTE).orElseThrow();
        assertEquals(2, compute.aliveThreads());
        assertEquals(2, compute.coreThreads());
        assertEquals(1, compute.activeThreads());
        assertEquals(0, compute.idleThreads());
        assertEquals(-1L, compute.queuedTasks());
    }

    @Test
    void computesPoolCpuShareFromTheDeltaAndTheWindow() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 2));
        this.registry.put(COMPUTE, thread(7L, "compute-1", COMPUTE, Thread.State.RUNNABLE));
        this.counters.cpu(7L, 0L);
        this.counters.threads(10L, 10L);
        final ResourceMetricsImpl metrics = this.metrics(pools, MetricsConfig.defaults());

        final ResourceSnapshot first = metrics.snapshot();
        assertEquals(-1.0d, first.pool(COMPUTE).orElseThrow().cpuLoad(), 0.0d);

        this.counters.cpu(7L, 1_000_000_000L);
        this.clock.advance(Duration.ofSeconds(1));
        final ResourceSnapshot second = metrics.snapshot();

        // one core second over a one second window on a two core machine
        assertEquals(0.5d, second.pool(COMPUTE).orElseThrow().cpuLoad(), 1.0e-9d);
        assertEquals(0.25d, second.processCpuLoad(), 0.0d);
    }

    @Test
    void cpuUnsupportedReportsUnknownInsteadOfZero() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 1));
        this.registry.put(COMPUTE, thread(7L, "compute-1", COMPUTE, Thread.State.RUNNABLE));
        this.counters.unsupported();

        final ResourceSnapshot snapshot = this.metrics(pools, MetricsConfig.defaults()).snapshot();

        assertEquals(-1.0d, snapshot.pool(COMPUTE).orElseThrow().cpuLoad(), 0.0d);
        assertEquals(-1.0d, snapshot.processCpuLoad(), 0.0d);
        assertEquals(List.of(DegradedReason.METRICS_UNAVAILABLE), snapshot.pool(COMPUTE).orElseThrow().degradedReasons());
        assertTrue(snapshot.degraded());
    }

    @Test
    void rateIsDeltaOverTheFakeWindow() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 1));
        this.counters.threads(10L, 8L);
        final ResourceMetricsImpl metrics = this.metrics(pools, MetricsConfig.defaults());

        assertEquals(0.0d, metrics.snapshot().creationRatePerSecond(), 0.0d);

        this.counters.threads(18L, 9L);
        this.clock.advance(Duration.ofSeconds(2));
        final ResourceSnapshot second = metrics.snapshot();

        assertEquals(4.0d, second.creationRatePerSecond(), 1.0e-9d);
        assertEquals(3.5d, second.terminationRatePerSecond(), 1.0e-9d);
    }

    @Test
    void disabledMetricsPublishZerosAndNoPools() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 1));
        this.registry.put(COMPUTE, thread(7L, "compute-1", COMPUTE, Thread.State.RUNNABLE));
        final MetricsConfig off = new MetricsConfig(false, true, true, Duration.ofSeconds(1), Duration.ofSeconds(30));
        final ResourceMetricsImpl metrics = this.metrics(pools, off);

        assertFalse(metrics.enabled());
        final ResourceSnapshot snapshot = metrics.snapshot();
        assertEquals(0, snapshot.totalThreads());
        assertTrue(snapshot.pools().isEmpty());
        assertTrue(snapshot.threads().isEmpty());
        assertTrue(metrics.pool(COMPUTE).isEmpty());
        assertTrue(metrics.threads(COMPUTE).isEmpty());
        assertEquals(SaturationReport.Level.UNKNOWN, metrics.saturation(COMPUTE).level());
        assertEquals(2, snapshot.usableCores());
    }

    @Test
    void perThreadAndPerPoolSwitchesOnlyTrimTheOutput() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 2));
        this.registry.put(COMPUTE, thread(7L, "compute-1", COMPUTE, Thread.State.RUNNABLE));
        final MetricsConfig countsOnly = new MetricsConfig(true, false, false, Duration.ofSeconds(1),
            Duration.ofSeconds(30));
        final ResourceSnapshot snapshot = this.metrics(pools, countsOnly).snapshot();

        assertEquals(1, snapshot.totalThreads());
        assertTrue(snapshot.pools().isEmpty());
        assertTrue(snapshot.threads().isEmpty());
        assertTrue(this.sink.poolStates.isEmpty());
    }

    @Test
    void probeValuesWinAndFallBackToThreadStates() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 4));
        final PoolProbe probe = new MetricsFixtures.FakeProbe(
            COMPUTE,
            new MetricsFixtures.FakeObservation(4, 4, 3, 1, 40L, 80L, List.of())
        );
        final ResourceMetricsImpl metrics = this.metricsWithProbes(pools, List.of(probe));

        final PoolSnapshot pool = metrics.snapshot().pool(COMPUTE).orElseThrow();
        assertEquals(4, pool.aliveThreads());
        assertEquals(4, pool.coreThreads());
        assertEquals(3, pool.activeThreads());
        assertEquals(1, pool.idleThreads());
        assertEquals(40L, pool.queuedTasks());
        assertEquals(80L, pool.queueCapacity());
        assertEquals(SaturationReport.Level.BUSY, pool.saturation().level());
        assertEquals(0.5d, pool.saturation().queuePressure(), 1.0e-9d);
        assertEquals(1, this.sink.saturations.size());
    }

    @Test
    void probeWithUnknownClassificationFallsBackToStates() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 2));
        this.registry.put(
            COMPUTE,
            thread(7L, "compute-1", COMPUTE, Thread.State.RUNNABLE),
            thread(8L, "compute-2", COMPUTE, Thread.State.WAITING)
        );
        final PoolProbe probe = new MetricsFixtures.FakeProbe(
            COMPUTE,
            new MetricsFixtures.FakeObservation(2, 2, -1, -1, -1L, -1L, List.of())
        );

        final PoolSnapshot pool = this.metricsWithProbes(pools, List.of(probe)).snapshot().pool(COMPUTE).orElseThrow();
        assertEquals(1, pool.activeThreads());
        assertEquals(1, pool.idleThreads());
        assertEquals(-1L, pool.queuedTasks());
        assertEquals(SaturationReport.Level.NORMAL, pool.saturation().level());
    }

    @Test
    void probeThreadsAreCountedAndListed() throws InterruptedException {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 1));
        final CountDownLatch latch = new CountDownLatch(1);
        final Thread worker = Thread.ofPlatform().daemon().name("probe-worker-1").start(() -> await(latch));
        try {
            final PoolProbe probe = new MetricsFixtures.FakeProbe(
                COMPUTE,
                new MetricsFixtures.FakeObservation(1, 1, 1, 0, 0L, 10L, List.of(worker))
            );
            final ResourceMetricsImpl metrics = this.metricsWithProbes(pools, List.of(probe));

            assertEquals(1, metrics.totalThreads());
            final ThreadSnapshot snapshot = metrics.threads(COMPUTE).getFirst();
            assertEquals(worker.threadId(), snapshot.threadId());
            assertEquals("probe-worker-1", snapshot.name());
            assertEquals(COMPUTE, snapshot.pool());
        } finally {
            latch.countDown();
            worker.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @Test
    void unaccountedThreadsAreReportedAndNeverCountedInTotals() throws InterruptedException {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 1));
        final ResourceMetricsImpl metrics = this.metrics(pools, MetricsConfig.defaults());
        final CountDownLatch latch = new CountDownLatch(1);
        final Thread stray = Thread.ofPlatform().daemon().name("stray-metrics-worker").start(() -> await(latch));
        try {
            final List<ThreadSnapshot> found = metrics.unregisteredThreads();
            final ThreadSnapshot report = found.stream()
                .filter(thread -> thread.threadId() == stray.threadId())
                .findFirst()
                .orElseThrow();
            assertEquals("stray-metrics-worker", report.name());
            assertEquals(ResourceSampler.UNREGISTERED_POOL, report.pool());

            assertEquals(0, metrics.totalThreads());
            assertTrue(metrics.snapshot().threads().isEmpty());
            assertTrue(
                this.sink.unregistered.stream().anyMatch(entry -> entry.endsWith(":stray-metrics-worker")),
                this.sink.unregistered.toString()
            );
        } finally {
            latch.countDown();
            stray.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @Test
    void budgetChangeIsEmittedOnce() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 2));
        final ResourceMetricsImpl metrics = this.metrics(pools, MetricsConfig.defaults());
        metrics.snapshot();
        assertTrue(this.sink.budgetChanges.isEmpty());

        final PoolSpec changed = new PoolSpec(
            COMPUTE,
            PoolKind.COMPUTE,
            "compute",
            "test pool",
            2,
            -1,
            CoverageLevel.L2,
            ResourceBudget.fixed(3, 1)
        );
        pools.register(changed);
        this.clock.advance(Duration.ofSeconds(1));
        metrics.snapshot();
        assertEquals(List.of("compute:-1->3"), this.sink.budgetChanges);

        this.clock.advance(Duration.ofSeconds(1));
        metrics.snapshot();
        assertEquals(1, this.sink.budgetChanges.size());
    }

    @Test
    void brokenSinkDoesNotBreakSampling() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 1));
        this.registry.put(COMPUTE, thread(7L, "compute-1", COMPUTE, Thread.State.RUNNABLE));
        this.sink.explode();

        final ResourceSnapshot snapshot = this.metrics(pools, MetricsConfig.defaults()).snapshot();
        assertEquals(1, snapshot.totalThreads());
    }

    @Test
    void cpuBaselinesFollowLiveThreadsAndDoNotLeak() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 2));
        this.registry.put(COMPUTE, thread(7L, "compute-1", COMPUTE, Thread.State.RUNNABLE));
        this.counters.cpu(7L, 0L);
        final ResourceSampler sampler = this.sampler(pools, PoolProbeSource.empty());
        sampler.sampleNow();
        assertEquals(1, sampler.trackedCpuThreads());

        this.registry.put(COMPUTE, thread(8L, "compute-2", COMPUTE, Thread.State.RUNNABLE));
        this.counters.cpu(8L, 1_000_000_000L);
        this.clock.advance(Duration.ofSeconds(1));
        final ResourceSnapshot second = sampler.sampleNow();

        assertEquals(1, sampler.trackedCpuThreads());
        // a brand new thread has no baseline, so its lifetime CPU is not mistaken for a pool burst
        assertEquals(-1.0d, second.pool(COMPUTE).orElseThrow().cpuLoad(), 0.0d);
    }

    @Test
    void closedSamplerStopsSampling() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 1));
        this.registry.put(COMPUTE, thread(7L, "compute-1", COMPUTE, Thread.State.RUNNABLE));
        final ResourceSampler sampler = this.sampler(pools, PoolProbeSource.empty());
        sampler.sampleNow();
        sampler.close();

        assertFalse(sampler.enabled());
        assertEquals(0, sampler.current().totalThreads());
        assertEquals(0, new ResourceMetricsImpl(sampler).totalThreads());
    }

    @Test
    void snapshotOrderAndUniqueness() {
        final MetricsFixtures.FakePools pools = new MetricsFixtures.FakePools(spec(COMPUTE, 1), spec(REGION, 0));
        this.registry.put(
            COMPUTE,
            thread(9L, "compute-1", COMPUTE, Thread.State.RUNNABLE),
            thread(3L, "compute-2", COMPUTE, Thread.State.RUNNABLE)
        );
        final ResourceSnapshot snapshot = this.metrics(pools, MetricsConfig.defaults()).snapshot();

        final List<PoolId> order = new ArrayList<>();
        for (final PoolSnapshot pool : snapshot.pools()) {
            order.add(pool.id());
        }
        assertEquals(List.of(COMPUTE, REGION), order);
        assertEquals(List.of(3L, 9L), snapshot.threads().stream().map(ThreadSnapshot::threadId).toList());
        assertNotNull(snapshot.capturedAt());
        assertEquals(Set.of(), snapshot.degradedReasons());
    }

    private ResourceMetricsImpl metrics(final MetricsFixtures.FakePools pools, final MetricsConfig config) {
        return new ResourceMetricsImpl(this.sampler(pools, PoolProbeSource.empty(), config));
    }

    private ResourceMetricsImpl metricsWithProbes(final MetricsFixtures.FakePools pools, final List<PoolProbe> probes) {
        return new ResourceMetricsImpl(this.sampler(pools, () -> probes, MetricsConfig.defaults()));
    }

    private ResourceSampler sampler(final MetricsFixtures.FakePools pools, final PoolProbeSource probes) {
        return this.sampler(pools, probes, MetricsConfig.defaults());
    }

    private ResourceSampler sampler(
        final MetricsFixtures.FakePools pools,
        final PoolProbeSource probes,
        final MetricsConfig config
    ) {
        return new ResourceSampler(
            pools,
            this.registry,
            probes,
            this.counters,
            new MetricsFixtures.FakeTopology(2),
            this.clock,
            this.sink,
            config,
            0
        );
    }

    private static PoolSpec spec(final PoolId id, final int minThreads) {
        final PoolKind kind = id.equals(REGION) ? PoolKind.REGION_EXECUTION : PoolKind.COMPUTE;
        final CoverageLevel coverage = minThreads > 0 ? CoverageLevel.L2 : CoverageLevel.L1;
        return new PoolSpec(id, kind, "test", "test pool", minThreads, -1, coverage, ResourceBudget.defaults());
    }

    private static ThreadSnapshot thread(final long id, final String name, final PoolId pool, final Thread.State state) {
        return new ThreadSnapshot(id, name, pool, state, true, Thread.NORM_PRIORITY, -1L);
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }
}
