package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.config.ComputeThreadBudget;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.LeaseTimeoutException;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.ThreadResourceController;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.internal.impl.controller.ResourceControllerImpl;
import kz.bejiihiu.tuff.threads.internal.impl.events.JfrThreadSink;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.SystemMonotonicClock;
import kz.bejiihiu.tuff.threads.internal.runtime.DefaultThreadManagerFactory;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;
import kz.bejiihiu.tuff.threads.pressure.TickPressureMonitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ComputeUtmiAdapterTest {

    /**
     * Processor count every test that does not name its own topology runs with.
     *
     * <p>Never read the host here. CI once had four cores while the dev box had sixteen, and the pool
     * budget arithmetic (and the min-threads floor on top of it) came out differently, so the same test
     * was green locally and red on the runner. A fixed topology gives identical numbers whether the
     * machine has 2, 4, 8, 16 or 32 cores.
     */
    private static final int FIXED_CORES = 8;

    private ThreadManagerRuntime runtime;
    private AsyncComputeScheduler engine;

    @AfterEach
    void stopEverything() {
        if (this.engine != null) {
            this.engine.shutdown(Duration.ZERO);
            try {
                this.engine.awaitTermination(Duration.ofSeconds(5L));
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
            this.engine = null;
        }
        if (this.runtime != null) {
            this.runtime.shutdown(Duration.ZERO);
            this.runtime = null;
        }
    }

    @Test
    void anObserveOnlyLayerLeavesTheEngineOnItsOwnThreads() {
        final ThreadManagerRuntime started = this.start(false);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);

        assertFalse(adapter.isManaged());
        final PoolSpec compute = started.manager().pools().spec(PoolId.COMPUTE).orElseThrow();
        assertEquals(CoverageLevel.L1, compute.coverage(), "without the flag the pool stays observed");

        final String prefix = settings.workers().threadNamePrefix();
        final Thread thread = adapter.platform(prefix, true).newThread(() -> { });
        assertEquals(prefix + "-1", thread.getName(), "observation mode keeps the legacy name");
        assertTrue(thread.isDaemon());
        assertTrue(started.manager().poolOf(thread).isEmpty(), "the legacy factory does not register threads");
        startAndJoin(thread);
    }

    @Test
    void aManagedLayerCreatesTheWorkersAndKeepsTheLegacyNames() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);
        assertTrue(adapter.isManaged());

        final PoolSpec compute = started.manager().pools().spec(PoolId.COMPUTE).orElseThrow();
        assertEquals(CoverageLevel.L4, compute.coverage());
        assertEquals(settings.workers().minThreads(), compute.minThreads());
        if (started.manager().resources().usableCores() > settings.workers().minThreads()) {
            assertFalse(
                started.manager().degraded(),
                () -> "a managed compute pool must not degrade the layer: " + started.manager().degradedReasons()
            );
        }

        final String prefix = settings.workers().threadNamePrefix();
        final Thread legacy = ComputeThreadFactoryPort.legacy().platform(prefix, true).newThread(() -> { });
        final Thread managed = adapter.platform(prefix, true).newThread(() -> { });

        assertEquals(legacy.getName(), managed.getName(), "the operational thread name must not change");
        assertEquals(prefix + "-1", managed.getName());
        assertEquals(Optional.of(PoolId.COMPUTE), started.manager().poolOf(managed), "the layer accounts the worker");
        startAndJoin(legacy);
        startAndJoin(managed);
        assertTrue(started.manager().poolOf(managed).isEmpty(), "a finished worker is unregistered");
    }

    @Test
    void aManagedThreadKeepsItsExactNameAndIsAccounted() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);

        final String name = settings.workers().threadNamePrefix() + "-janitor";
        final CountDownLatch inside = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread thread = adapter.named(name, true).newThread(() -> {
            inside.countDown();
            await(release);
        });
        thread.start();
        assertTrue(await(inside), "the named thread must start");
        assertEquals(name, thread.getName(), "a service thread keeps its exact name");
        assertEquals(Optional.of(PoolId.COMPUTE), started.manager().poolOf(thread));
        release.countDown();
        join(thread);
    }

    @Test
    void theVirtualLaneIsAccountedThroughTheLayer() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);

        final PoolSpec virtual = started.manager().pools().spec(PoolId.COMPUTE_VIRTUAL).orElseThrow();
        assertEquals(PoolKind.VIRTUAL, virtual.kind());
        assertEquals(CoverageLevel.L2, virtual.coverage(), "the lane's threads are accounted, its lane stays the engine's");
        assertEquals(settings.lanes().virtual().maxConcurrent(), virtual.maxThreads());

        final String name = settings.workers().threadNamePrefix() + "-virtual";
        final CountDownLatch inside = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread thread = adapter.virtual(name).newThread(() -> {
            inside.countDown();
            await(release);
        });
        assertTrue(thread.isVirtual());
        assertEquals(name + "-1", thread.getName(), "virtual threads keep the legacy numbering");
        thread.start();
        assertTrue(await(inside), "the virtual thread must start");
        assertEquals(Optional.of(PoolId.COMPUTE_VIRTUAL), started.manager().poolOf(thread));
        release.countDown();
        join(thread);
    }

    /**
     * The engine's budget is the ledger's number, floored only by {@code workers.min-threads}.
     *
     * <p>Two numbers that used to be asserted as one: the ledger capacity compute was given and what
     * the engine actually applies. They coincide above the floor, but on a small machine the ledger can
     * hand compute a single core while {@code min-threads} is two - the adapter keeps two workers
     * because the engine will not run fewer. That floor was the CI failure: a four core runner divided
     * two usable cores into one for the pump and one for compute, the test expected
     * {@code usable - 1 == 1} from the adapter, and got the floored {@code 2}. Both are correct, the
     * assertion was not.
     *
     * <p>The topology is a parameter, so this test proves the same arithmetic on 2, 4, 8, 16 and 32
     * cores instead of on whatever the runner happens to have.
     *
     * @param cores fixed processor count for this run
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 6, 8, 12, 16, 32, 64})
    void theBudgetIsTheControllersNumberWithoutASecondSubtraction(final int cores) {
        final ThreadManagerRuntime started = this.start(true, cores);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);

        final int usable = started.manager().resources().usableCores();
        final int capacity = started.manager().resources().budget(PoolId.COMPUTE).orElseThrow().cores();
        final int floor = settings.workers().minThreads();
        final String context = "fixed topology: cores=" + cores + ", usable=" + usable + ", capacity=" + capacity;

        final int applied = adapter.processors();
        assertEquals("utmi", adapter.source());
        // the compute pool and the virtual lane's pump share the remainder; nothing else is subtracted
        assertEquals(Math.max(0, usable - 1), capacity,
            "compute owns the ledger remainder after the one core pump: " + context);
        assertEquals(Math.max(floor, capacity), applied,
            "the applied budget is the ledger capacity, floored at min-threads: " + context);
        if (capacity >= floor) {
            assertEquals(capacity, applied, "no second subtraction above the floor: " + context);
        }
        assertTrue(
            applied >= ComputeThreadBudget.resolve(cores),
            "the managed budget must not be lower than the legacy one it replaces: " + context
        );
    }

    @Test
    void aRefusedProviderFallsBackToTheLegacyFactory() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);
        assertTrue(adapter.isManaged());

        // a stopped provider refuses both the factory and the wrapper; the engine must survive that
        started.shutdown(Duration.ZERO);
        this.runtime = null;

        final String prefix = settings.workers().threadNamePrefix();
        final Thread thread = adapter.platform(prefix, true).newThread(() -> { });
        assertEquals(prefix + "-1", thread.getName(), "the fallback keeps the legacy naming scheme");
        startAndJoin(thread);
    }

    @Test
    void theProbeReportsTheEnginesOwnNumbers() {
        final ThreadManagerRuntime started = this.start(true);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);
        this.engine = ComputeSchedulers.create(settings, ComputeClock.system(), adapter, adapter);
        this.engine.start();
        adapter.bind(this.engine);

        this.awaitMinimumWorkers();
        final SchedulerMetrics metrics = this.engine.metrics();
        final PoolObservation reading = adapter.platformProbe().observe();

        assertEquals(metrics.totalWorkers(), reading.aliveThreads());
        assertEquals(metrics.activeWorkers(), reading.activeThreads());
        assertEquals(metrics.idleWorkers(), reading.idleThreads());
        assertEquals(metrics.activeWorkers() + metrics.idleWorkers(), reading.aliveThreads(),
            "busy and idle must come from the worker bookkeeping, not from thread states");
        assertEquals(metrics.queuedTasks(), reading.queuedTasks());
        assertEquals(metrics.queueCapacity(), reading.queueCapacity());
        assertTrue(reading.aliveThreads() >= 1);
        assertFalse(reading.threads().isEmpty(), "the probe must list the workers it counted");
        for (final Thread thread : reading.threads()) {
            // workers and the engine's own metrics sampler are the platform pool; the lane threads are
            // filtered out by the scan. Anything else is a foreign thread and a real bug
            final String name = thread.getName();
            assertTrue(
                name.startsWith(settings.workers().threadNamePrefix())
                    || name.startsWith(ComputeSettings.METRICS_THREAD_PREFIX),
                () -> "a foreign thread leaked into the compute pool listing: " + name
            );
        }
    }

    @ParameterizedTest
    @CsvSource({"8, 3, 2, 1", "16, 9, 7, 2"})
    void tickPressureNeverDropsBelowMinThreadsAndReportsTheAppliedBudget(
        final int cores,
        final int poolBudget,
        final int appliedAtCeiling,
        final int reserveCeiling
    ) {
        final ThreadManagerRuntime started = this.start(true, cores);
        this.registerService(started);
        final TickPressureMonitor monitor = new TickPressureMonitor(
            pressurePolicy(true), started.manager().resources()::usableCores
        );
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, ComputeSettings.defaults(), monitor);
        monitor.bind(adapter.pressureSink());

        assertEquals(poolBudget, started.manager().resources().budget(PoolId.COMPUTE).orElseThrow().cores(),
            "the pool budget is what the division actually gave compute");

        monitor.onSample(pass(calm()), 0L);
        assertEquals(poolBudget, monitor.snapshot().appliedBudget(),
            "without pressure the applied budget is the whole pool budget");

        for (int second = 1; second <= 20; second++) {
            monitor.onSample(pass(lagging()), TimeUnit.SECONDS.toNanos(second));
        }

        assertEquals(reserveCeiling, monitor.snapshot().reserve(),
            "the configured ceiling is lowered by the min-threads floor");
        assertEquals(appliedAtCeiling, monitor.snapshot().appliedBudget());
        assertEquals(appliedAtCeiling, adapter.processors(),
            "one arithmetic: the report and the engine get the same number");
        assertTrue(adapter.processors() >= ComputeSettings.defaults().workers().minThreads(),
            "the pool never falls below its own min-threads");
        assertTrue(monitor.snapshot().poolBudget() >= adapter.processors());
    }

    @Test
    void theReliefReserveSurvivesTickPressure() {
        final ThreadManagerRuntime started = this.start(true, 16);
        this.registerService(started);
        // a policy that would happily take every core the pool has: max reserve 16, two cores per step
        final ThreadManagementSettings.AsyncCompute.TickPressure policy =
            new ThreadManagementSettings.AsyncCompute.TickPressure(true, 0.25d, 19.0d, 19.5d, 3, 2, 16, 2);
        final TickPressureMonitor monitor = new TickPressureMonitor(policy, started.manager().resources()::usableCores);
        final RegionReliefSettings relief = new RegionReliefSettings(
            true, 8, 1, RegionReliefSettings.defaultConsumerPolicy()
        );
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(
            started, ComputeSettings.defaults(), monitor, relief
        );
        monitor.bind(adapter.pressureSink());

        final int poolBudget = started.manager().resources().budget(PoolId.COMPUTE).orElseThrow().cores();
        assertEquals(1, adapter.reliefReserveCores());
        for (int second = 1; second <= 30; second++) {
            monitor.onSample(pass(lagging()), TimeUnit.SECONDS.toNanos(second));
        }

        final int minThreads = ComputeSettings.defaults().workers().minThreads();
        assertEquals(poolBudget - minThreads - 1, monitor.snapshot().reserve(),
            "pressure may take everything except the relief core");
        assertEquals(minThreads + 1, monitor.snapshot().appliedBudget(),
            "the pool keeps one core for relief even at the pressure ceiling");
        assertEquals(minThreads + 1, adapter.processors(),
            "the budget the engine reads carries the same reservation");
    }

    @Test
    void aDisabledReliefPathLeavesThePressureCeilingAlone() {
        final ThreadManagerRuntime started = this.start(true, 16);
        this.registerService(started);
        final ThreadManagementSettings.AsyncCompute.TickPressure policy =
            new ThreadManagementSettings.AsyncCompute.TickPressure(true, 0.25d, 19.0d, 19.5d, 3, 2, 16, 2);
        final TickPressureMonitor monitor = new TickPressureMonitor(policy, started.manager().resources()::usableCores);
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(
            started, ComputeSettings.defaults(), monitor, new RegionReliefSettings(
                false, 8, 5, RegionReliefSettings.defaultConsumerPolicy()
            )
        );
        monitor.bind(adapter.pressureSink());

        final int poolBudget = started.manager().resources().budget(PoolId.COMPUTE).orElseThrow().cores();
        for (int second = 1; second <= 30; second++) {
            monitor.onSample(pass(lagging()), TimeUnit.SECONDS.toNanos(second));
        }

        assertEquals(poolBudget - ComputeSettings.defaults().workers().minThreads(), monitor.snapshot().reserve(),
            "a relief path that is off must not withhold a core from the regions");
        assertEquals(0, adapter.reliefReserveCores());
    }

    @Test
    void aDisabledVirtualLaneDoesNotClaimAnyOfTheComputeBudget() {
        final ThreadManagerRuntime started = this.start(true, 16);
        final ComputeSettings settings = withVirtualLane(ComputeSettings.defaults(), false);
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);

        assertTrue(adapter.isManaged());
        assertEquals(started.manager().resources().usableCores(),
            started.manager().resources().budget(PoolId.COMPUTE).orElseThrow().cores(),
            "a lane that is off owns nothing, so compute keeps the whole remainder");
        final PoolSpec virtual = started.manager().pools().spec(PoolId.COMPUTE_VIRTUAL).orElseThrow();
        assertEquals(CoverageLevel.L1, virtual.coverage());
        assertEquals(0, virtual.budget().resolveCores(started.manager().resources().usableCores()),
            "the disabled lane declares no cores, so it never joins the shared demand");
        assertEquals(0, virtual.minThreads());
    }

    @Test
    void aShedReachesTheEngineOnTheAdmissionDecisionThatCausedIt() throws InterruptedException {
        final ThreadManagerRuntime started = this.start(true, 16, true);
        started.manager().pools().register(new PoolSpec(
            PoolId.NETWORK, PoolKind.NETWORK, "netty", "event loops", 1, -1, CoverageLevel.L3,
            ResourceBudget.fixed(1, 0)
        ));
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);
        final RecordingEngine engine = new RecordingEngine(settings);
        adapter.bind(engine);

        final int capacity = started.manager().resources().budget(PoolId.COMPUTE).orElseThrow().cores();
        final int before = adapter.processors();
        assertEquals(capacity, before, "the pool budget is the engine budget while nothing is shed");
        assertTrue(engine.budgets().isEmpty(), "binding alone does not resize anything");

        final List<Lease> held = new ArrayList<>(capacity);
        for (int core = 0; core < capacity; core++) {
            held.add(started.manager().resources().acquire(
                LeaseRequest.immediate(PoolId.COMPUTE, System.nanoTime() + TimeUnit.MINUTES.toNanos(1L))
            ));
        }
        final Lease network = started.manager().resources().acquire(
            LeaseRequest.immediate(PoolId.NETWORK, System.nanoTime() + TimeUnit.MINUTES.toNanos(1L))
        );
        final Thread waiter = new Thread(() -> {
            try {
                started.manager().resources().acquire(new LeaseRequest(
                    PoolId.NETWORK,
                    System.nanoTime() + TimeUnit.MINUTES.toNanos(1L),
                    Duration.ofMinutes(1L)
                ));
            } catch (RuntimeException _) {
                // the test owns the outcome
            }
        }, "adapter-shed-waiter");
        waiter.setDaemon(true);
        waiter.start();
        this.awaitWaiting(started.manager().resources(), PoolId.NETWORK);

        assertThrows(
            LeaseTimeoutException.class,
            () -> started.manager().resources().acquire(LeaseRequest.immediate(
                PoolId.COMPUTE, System.nanoTime() + TimeUnit.MINUTES.toNanos(1L)
            )),
            "the pool is at its capacity"
        );

        assertEquals(List.of(before - 1), engine.budgets(),
            "the shed must reach the engine without waiting for the next pressure sample");
        assertEquals(before - 1, adapter.processors());

        ((ResourceControllerImpl) started.manager().resources()).close();
        waiter.join(5_000L);
        network.release();
        for (final Lease lease : held) {
            lease.release();
        }
    }

    @Test
    void theVirtualPumpIsAccountedOnceUnderTheVirtualPool() {
        final ThreadManagerRuntime started = this.start(true, 8);
        final ComputeSettings settings = ComputeSettings.defaults();
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(started, settings);
        this.engine = ComputeSchedulers.create(settings, ComputeClock.system(), adapter, adapter);
        this.engine.start();
        adapter.bind(this.engine);
        this.awaitMinimumWorkers();

        // the pump is registered when its wrapped body starts running, not when the Thread is created,
        // so reading the snapshot straight after start() is a race the busy CI runner loses
        final ResourceSnapshot snapshot = this.awaitSnapshotWith(started, "-virtual-pump");
        final List<ThreadSnapshot> pumps = snapshot.threads().stream()
            .filter(thread -> thread.name().contains("-virtual-pump"))
            .toList();

        assertEquals(1, pumps.size(), "one pump, accounted once: " + snapshot.threads());
        assertEquals(PoolId.COMPUTE_VIRTUAL, pumps.getFirst().pool(), "the pump belongs to the virtual pool");
        assertEquals(snapshot.threads().size(), snapshot.totalThreads(),
            "the header must agree with the published thread list");
    }

    private ThreadManagerRuntime start(final boolean managed) {
        return this.start(managed, FIXED_CORES);
    }

    private ThreadManagerRuntime start(final boolean managed, final int cores) {
        return this.start(managed, cores, false);
    }

    private ThreadManagerRuntime start(final boolean managed, final int cores, final boolean loadShedding) {
        this.runtime = new DefaultThreadManagerFactory(
            new SystemMonotonicClock(), topology(cores), new JfrThreadSink()
        ).create(settings(managed, loadShedding));
        this.runtime.start();
        return this.runtime;
    }

    private static void registerService(final ThreadManagerRuntime runtime) {
        runtime.manager().pools().register(new PoolSpec(
            PoolId.SERVICE, PoolKind.SERVICE, "tuff", "service refresher", 1, -1, CoverageLevel.L2,
            ResourceBudget.fixed(1, 0)
        ));
    }

    private static TopologyView topology(final int cores) {
        return new TopologyView() {
            @Override
            public int availableProcessors() {
                return cores;
            }

            @Override
            public int totalCores() {
                return cores;
            }

            @Override
            public boolean numaAvailable() {
                return false;
            }

            @Override
            public int numaNodeCount() {
                return 1;
            }
        };
    }

    private static ComputeSettings withVirtualLane(final ComputeSettings base, final boolean enabled) {
        return new ComputeSettings(
            base.enabled(),
            base.workers(),
            base.queue(),
            base.scheduling(),
            base.consumers(),
            base.metrics(),
            base.adaptive(),
            enabled ? ComputeSettings.Lanes.defaults() : ComputeSettings.Lanes.withoutVirtual(),
            base.shutdownGracePeriod()
        );
    }

    private static ThreadManagementSettings.AsyncCompute.TickPressure pressurePolicy(final boolean enabled) {
        return new ThreadManagementSettings.AsyncCompute.TickPressure(
            enabled, 0.25d, 19.0d, 19.5d, 3, 5, 2, 1
        );
    }

    private static RegionPoolMetrics lagging() {
        return new RegionPoolMetrics(4, 4.0d, 12.0d, 4.0d, 11.0d, 16.0d, 2, 0, 0, 16);
    }

    private static RegionPoolMetrics calm() {
        return new RegionPoolMetrics(4, 2.0d, 2.0d, 2.0d, 2.0d, 20.0d, 0, 0, 0, 16);
    }

    private static ResourceSnapshot pass(final RegionPoolMetrics regions) {
        final PoolSpec spec = PoolSpec.observeOnly(
            PoolId.REGION_EXECUTION, PoolKind.REGION_EXECUTION, "folia", "region tick execution"
        );
        final PoolSnapshot pool = new PoolSnapshot(
            spec, 4, 0, 0, 4, -1L, -1L, -1L, -1L, -1L, -1.0d,
            SaturationReport.unknown(PoolId.REGION_EXECUTION),
            List.of(DegradedReason.OBSERVE_ONLY),
            Optional.of(regions)
        );
        return new ResourceSnapshot(
            Instant.EPOCH, 10, 1, 4, 0, 4, 4, 0.0d, 0.0d, -1.0d, List.of(pool), List.of(),
            Set.of(DegradedReason.OBSERVE_ONLY)
        );
    }

    private void awaitWaiting(final ThreadResourceController controller, final PoolId pool) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (System.nanoTime() < deadline) {
            if (controller.saturation(pool).queuedTasks() >= 1) {
                return;
            }
            Thread.yield();
        }
        fail("no request parked in pool '" + pool + "'");
    }

    private void awaitMinimumWorkers() {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (System.nanoTime() < deadline) {
            if (this.engine.metrics().totalWorkers() >= this.engine.settings().workers().minThreads()) {
                return;
            }
            try {
                Thread.sleep(20L);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        fail("the engine did not spawn its minimum workers in time");
    }

    /**
     * Waits until the thread registry has published a thread whose name contains a fragment.
     *
     * <p>The registry accounts a thread when its wrapped body starts, which is after {@code start()}
     * returned and after the thread object exists. A busy runner can therefore take the snapshot
     * before the pump appears; waiting for it keeps the assertion about accounting instead of about
     * scheduling luck.
     *
     * @param started  running thread manager
     * @param fragment part of the thread name to wait for
     * @return the snapshot that contained the fragment, or the last one when the wait timed out
     */
    private ResourceSnapshot awaitSnapshotWith(final ThreadManagerRuntime started, final String fragment) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        ResourceSnapshot last = started.manager().metrics().snapshot();
        while (System.nanoTime() < deadline) {
            last = started.manager().metrics().snapshot();
            if (last.threads().stream().anyMatch(thread -> thread.name().contains(fragment))) {
                return last;
            }
            try {
                Thread.sleep(20L);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return last;
    }

    private static ThreadManagementSettings settings(final boolean computeManaged) {
        return settings(computeManaged, false);
    }

    private static ThreadManagementSettings settings(final boolean computeManaged, final boolean loadShedding) {
        return new ThreadManagementSettings(
            true,
            new ThreadManagementSettings.Resources(true, 1, -1, loadShedding),
            ThreadManagementSettings.Lifecycle.defaults(),
            ThreadManagementSettings.RegionExecution.defaults(),
            ThreadManagementSettings.AsyncCompute.defaults(),
            new ThreadManagementSettings.Pools(
                ThreadManagementSettings.PoolSettings.defaults(),
                ThreadManagementSettings.PoolSettings.defaults(),
                new ThreadManagementSettings.PoolSettings(computeManaged, true)
            ),
            ThreadManagementSettings.Metrics.defaults()
        );
    }

    private static void startAndJoin(final Thread thread) {
        thread.start();
        join(thread);
    }

    private static void join(final Thread thread) {
        try {
            thread.join(5_000L);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean await(final CountDownLatch latch) {
        try {
            return latch.await(5L, TimeUnit.SECONDS);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Engine that only records the budgets the adapter applies.
     *
     * <p>The real engine resizes its gate to the minimum while idle, so its concurrency limit is not a
     * record of what was applied; this double records the calls themselves.
     */
    private static final class RecordingEngine implements AsyncComputeScheduler {

        private final List<Integer> budgets = new ArrayList<>();
        private final ComputeSettings settings;

        private RecordingEngine(final ComputeSettings settings) {
            this.settings = settings;
        }

        private List<Integer> budgets() {
            return List.copyOf(this.budgets);
        }

        @Override
        public void applyProcessorBudget(final int processors) {
            if (processors < 1) {
                throw new IllegalArgumentException("budget must be at least one, got " + processors);
            }
            this.budgets.add(processors);
        }

        @Override
        public ComputeSettings settings() {
            return this.settings;
        }

        @Override
        public SchedulerMetrics metrics() {
            throw new UnsupportedOperationException("the recording engine has no metrics");
        }

        @Override
        public SchedulerState state() {
            return SchedulerState.RUNNING;
        }

        @Override
        public void start() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void shutdown(final Duration gracePeriod) {
        }

        @Override
        public boolean awaitTermination(final Duration timeout) {
            return true;
        }

        @Override
        public <T> CompletableFuture<T> submit(
            final ComputeTask<T> task
        ) {
            throw new UnsupportedOperationException("the recording engine accepts no work");
        }

        @Override
        public <T> CompletableFuture<T> submit(
            final ComputePriority priority,
            final ComputeTask<T> task
        ) {
            throw new UnsupportedOperationException("the recording engine accepts no work");
        }

        @Override
        public <T> CompletableFuture<T> submit(
            final String consumerId,
            final ComputeTaskType taskType,
            final ComputePriority priority,
            final ComputeTask<T> task
        ) {
            throw new UnsupportedOperationException("the recording engine accepts no work");
        }

        @Override
        public <T> ComputeTaskHandle<T> enqueue(
            final ComputeRequest<T> request
        ) {
            throw new UnsupportedOperationException("the recording engine accepts no work");
        }

        @Override
        public ComputeConsumer registerConsumer(
            final ComputeConsumer consumer
        ) {
            throw new UnsupportedOperationException("the recording engine has no consumers");
        }

        @Override
        public boolean unregisterConsumer(final String consumerId) {
            return false;
        }

        @Override
        public Optional<ComputeConsumer> consumer(final String consumerId) {
            return Optional.empty();
        }

        @Override
        public Set<String> consumerIds() {
            return Set.of();
        }

        @Override
        public void updateConsumerPolicy(
            final String consumerId,
            final ConsumerPolicy policy
        ) {
            throw new UnsupportedOperationException("the recording engine has no consumers");
        }
    }
}
