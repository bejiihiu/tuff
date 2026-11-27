package kz.bejiihiu.tuff.threads.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ResourceMetrics;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadFactoryProvider;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.ThreadResourceController;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.internal.impl.budget.BudgetDeductions;
import kz.bejiihiu.tuff.threads.internal.impl.controller.ResourceControllerImpl;
import kz.bejiihiu.tuff.threads.internal.impl.events.MetricsEventSink;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.MetricsConfig;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceSampler;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ThreadCounters;
import kz.bejiihiu.tuff.threads.internal.impl.registry.ThreadRegistryImpl;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;
import org.jspecify.annotations.Nullable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * In-memory pieces the scenario tests wire together.
 *
 * <p>Nothing here is a mock of the component under test: the controller, the monitor and the adapter
 * are the production classes. The doubles are the environment - time, processors, the pool registry,
 * the engine and the probes - so a scenario can state "ten cores, one core reserved, two regions
 * late" and get exactly that.
 */
final class ScenarioFixtures {

    static final long SECOND = TimeUnit.SECONDS.toNanos(1L);

    private static final String OWNER = "scenario";

    private ScenarioFixtures() {
    }

    /**
     * Clock that only moves when the test moves it.
     */
    static final class FakeClock implements MonotonicClock {

        private long nanos = SECOND;
        private Instant instant = Instant.EPOCH.plusNanos(SECOND);

        void advanceSeconds(final int seconds) {
            this.nanos += seconds * SECOND;
            this.instant = this.instant.plusNanos(seconds * SECOND);
        }

        @Override
        public long nanoTime() {
            return this.nanos;
        }

        @Override
        public Instant instant() {
            return this.instant;
        }

        long deadlineNanos() {
            return this.nanos + Duration.ofMinutes(2).toNanos();
        }
    }

    /**
     * Topology with a fixed processor count, so every budget number is deterministic.
     */
    static final class FixedTopology implements TopologyView {

        private final int processors;

        FixedTopology(final int processors) {
            this.processors = processors;
        }

        @Override
        public int availableProcessors() {
            return this.processors;
        }

        @Override
        public int totalCores() {
            return this.processors;
        }

        @Override
        public boolean numaAvailable() {
            return false;
        }

        @Override
        public int numaNodeCount() {
            return 1;
        }
    }

    /**
     * Pool registry the scenario owns, with the replace-by-id behaviour the real one documents.
     */
    static final class MemoryPoolRegistry implements ExecutionPoolRegistry {

        private final ConcurrentHashMap<PoolId, PoolSpec> specs = new ConcurrentHashMap<>();

        @Override
        public PoolSpec register(final PoolSpec spec) {
            Objects.requireNonNull(spec, "spec");
            final @Nullable PoolSpec previous = this.specs.get(spec.id());
            if (previous != null && previous.kind() != spec.kind()) {
                throw new IllegalStateException("pool '" + spec.id() + "' already has kind " + previous.kind());
            }
            this.specs.put(spec.id(), spec);
            return spec;
        }

        @Override
        public boolean unregister(final PoolId id) {
            return this.specs.remove(Objects.requireNonNull(id, "id")) != null;
        }

        @Override
        public Optional<PoolSpec> spec(final PoolId id) {
            return Optional.ofNullable(this.specs.get(Objects.requireNonNull(id, "id")));
        }

        @Override
        public boolean contains(final PoolId id) {
            return this.specs.containsKey(Objects.requireNonNull(id, "id"));
        }

        @Override
        public Set<PoolId> ids() {
            final List<PoolId> sorted = new ArrayList<>(this.specs.keySet());
            Collections.sort(sorted);
            return Collections.unmodifiableSet(new LinkedHashSet<>(sorted));
        }

        @Override
        public List<PoolSpec> specs() {
            final List<PoolSpec> sorted = new ArrayList<>(this.specs.values());
            sorted.sort((left, right) -> left.id().compareTo(right.id()));
            return List.copyOf(sorted);
        }

        @Override
        public List<PoolSpec> specs(final PoolKind kind) {
            Objects.requireNonNull(kind, "kind");
            return this.specs().stream().filter(spec -> spec.kind() == kind).toList();
        }
    }

    /**
     * Sink that records nothing; the scenarios assert on the controller's own API.
     */
    static final class SilentSink implements MetricsEventSink {

        @Override
        public void threadCreated(final ThreadSnapshot thread) {
        }

        @Override
        public void threadTerminated(final ThreadSnapshot thread) {
        }

        @Override
        public void poolState(final PoolSnapshot pool) {
        }

        @Override
        public void saturation(final SaturationReport report) {
        }

        @Override
        public void leaseTimedOut(final LeaseRequest request) {
        }

        @Override
        public void unregisteredThread(final long threadId, final String name) {
        }

        @Override
        public void budgetChanged(final PoolId pool, final ResourceBudget previous, final ResourceBudget current) {
        }
    }

    /**
     * Fake engine: it only records the budgets the adapter applies.
     *
     * <p>Everything else is deliberately unsupported, so a scenario that accidentally starts using the
     * engine fails loudly instead of silently measuring a stub.
     */
    static final class RecordingEngine implements AsyncComputeScheduler {

        private final List<Integer> budgets = new ArrayList<>();
        private final ComputeSettings settings;

        RecordingEngine(final ComputeSettings settings) {
            this.settings = settings;
        }

        List<Integer> budgets() {
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
        public <T> CompletableFuture<T> submit(final ComputeTask<T> task) {
            throw new UnsupportedOperationException("the recording engine accepts no work");
        }

        @Override
        public <T> CompletableFuture<T> submit(final ComputePriority priority, final ComputeTask<T> task) {
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
        public <T> ComputeTaskHandle<T> enqueue(final ComputeRequest<T> request) {
            throw new UnsupportedOperationException("the recording engine accepts no work");
        }

        @Override
        public ComputeConsumer registerConsumer(final ComputeConsumer consumer) {
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
        public void updateConsumerPolicy(final String consumerId, final ConsumerPolicy policy) {
            throw new UnsupportedOperationException("the recording engine has no consumers");
        }
    }

    /**
     * Thread factory provider that refuses every call; the scenarios never create a thread.
     */
    static final class RefusingProvider implements ThreadFactoryProvider {

        @Override
        public Thread newThread(final ThreadSpec spec, final Runnable body) {
            throw new UnsupportedOperationException("the scenario must not create threads");
        }

        @Override
        public ThreadFactory factory(final ThreadSpec spec) {
            throw new UnsupportedOperationException("the scenario must not create threads");
        }

        @Override
        public Runnable wrap(final ThreadSpec spec, final Runnable body) {
            throw new UnsupportedOperationException("the scenario must not create threads");
        }

        @Override
        public Optional<PoolId> poolOf(final Thread thread) {
            return Optional.empty();
        }
    }

    /**
     * Metrics view that is never read; a scenario that asks for it fails loudly.
     */
    static final class RefusingMetrics implements ResourceMetrics {

        @Override
        public boolean enabled() {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public ResourceSnapshot snapshot() {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public Optional<PoolSnapshot> pool(final PoolId pool) {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public SaturationReport saturation(final PoolId pool) {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public List<ThreadSnapshot> threads(final PoolId pool) {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public List<ThreadSnapshot> unregisteredThreads() {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public double creationRatePerSecond() {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public double terminationRatePerSecond() {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public int totalThreads() {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public int activeThreads() {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }

        @Override
        public int idleThreads() {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager metrics");
        }
    }

    /**
     * Manager facade over the scenario's registry and controller.
     */
    static final class FakeManager implements UnifiedThreadManager {

        private final ThreadManagementSettings settings;
        private final ExecutionPoolRegistry pools;
        private final ThreadResourceController resources;
        private final RefusingProvider provider = new RefusingProvider();
        private final RefusingMetrics metrics = new RefusingMetrics();

        FakeManager(
            final ThreadManagementSettings settings,
            final ExecutionPoolRegistry pools,
            final ThreadResourceController resources
        ) {
            this.settings = settings;
            this.pools = pools;
            this.resources = resources;
        }

        @Override
        public ThreadManagementSettings settings() {
            return this.settings;
        }

        @Override
        public ExecutionPoolRegistry pools() {
            return this.pools;
        }

        @Override
        public ThreadResourceController resources() {
            return this.resources;
        }

        @Override
        public ThreadFactoryProvider threadFactory() {
            return this.provider;
        }

        @Override
        public ResourceMetrics metrics() {
            return this.metrics;
        }

        @Override
        public State state() {
            return State.RUNNING;
        }

        @Override
        public ResourceSnapshot snapshot() {
            throw new UnsupportedOperationException("the scenario reads the sampler, not the manager");
        }

        @Override
        public Optional<PoolId> poolOf(final Thread thread) {
            return Optional.empty();
        }

        @Override
        public Set<DegradedReason> degradedReasons() {
            return Set.of();
        }
    }

    /**
     * Running runtime around a fake manager; probes are accepted and dropped.
     */
    static final class FakeRuntime implements ThreadManagerRuntime {

        private final FakeManager manager;

        FakeRuntime(final FakeManager manager) {
            this.manager = manager;
        }

        @Override
        public UnifiedThreadManager manager() {
            return this.manager;
        }

        @Override
        public void start() {
        }

        @Override
        public void shutdown(final Duration gracePeriod) {
        }

        @Override
        public boolean awaitTermination(final Duration timeout) {
            return true;
        }

        @Override
        public void attachProbe(final PoolProbe probe) {
        }
    }

    /**
     * Real controller, registry, thread registry and time source wired together.
     */
    static final class Harness {

        final FakeClock clock = new FakeClock();
        final MemoryPoolRegistry registry = new MemoryPoolRegistry();
        final SilentSink sink = new SilentSink();
        final FixedTopology topology;
        final ThreadRegistryImpl threads;
        final ResourceControllerImpl controller;
        final ThreadManagementSettings settings;
        final FakeManager manager;
        final FakeRuntime runtime;

        private Harness(final int processors, final ThreadManagementSettings settings, final PoolSpec... specs) {
            this.topology = new FixedTopology(processors);
            for (final PoolSpec spec : specs) {
                this.registry.register(spec);
            }
            this.threads = new ThreadRegistryImpl(this.sink);
            this.settings = settings;
            this.controller = new ResourceControllerImpl(
                this.topology,
                this.registry,
                this.threads,
                settings.resources(),
                BudgetDeductions.none(),
                this.clock,
                this.sink
            );
            this.manager = new FakeManager(settings, this.registry, this.controller);
            this.runtime = new FakeRuntime(this.manager);
        }

        static Harness create(final int processors, final ThreadManagementSettings settings, final PoolSpec... specs) {
            return new Harness(processors, settings, specs);
        }

        /**
         * Rebuilds the ledger after a pool spec moved (the adapter registers compute at L4).
         */
        void refresh() {
            this.controller.refresh();
        }

        long deadline() {
            return this.clock.nanoTime() + Duration.ofMinutes(2).toNanos();
        }
    }

    static ThreadManagementSettings settings(
        final boolean computeManaged,
        final int reserveCores,
        final boolean loadShedding,
        final ThreadManagementSettings.AsyncCompute.TickPressure pressure,
        final boolean metricsEnabled
    ) {
        return new ThreadManagementSettings(
            true,
            new ThreadManagementSettings.Resources(true, reserveCores, -1, loadShedding),
            ThreadManagementSettings.Lifecycle.defaults(),
            ThreadManagementSettings.RegionExecution.defaults(),
            new ThreadManagementSettings.AsyncCompute(computeManaged, -1, pressure),
            new ThreadManagementSettings.Pools(
                ThreadManagementSettings.PoolSettings.defaults(),
                ThreadManagementSettings.PoolSettings.defaults(),
                new ThreadManagementSettings.PoolSettings(computeManaged, true)
            ),
            new ThreadManagementSettings.Metrics(metricsEnabled, true, true)
        );
    }

    static ThreadManagementSettings.AsyncCompute.TickPressure pressure(final boolean enabled) {
        return new ThreadManagementSettings.AsyncCompute.TickPressure(enabled, 0.25d, 19.0d, 19.5d, 3, 5, 2, 1);
    }

    static PoolSpec compute(final int minThreads, final int maxThreads, final int cores) {
        return new PoolSpec(
            PoolId.COMPUTE,
            PoolKind.COMPUTE,
            OWNER,
            "compute workers",
            minThreads,
            maxThreads,
            CoverageLevel.L3,
            ResourceBudget.fixed(cores, 0)
        );
    }

    static PoolSpec network(final int minThreads, final int cores) {
        return new PoolSpec(
            PoolId.NETWORK,
            PoolKind.NETWORK,
            OWNER,
            "network event loops",
            minThreads,
            -1,
            CoverageLevel.L3,
            ResourceBudget.fixed(cores, 0)
        );
    }

    static PoolSpec service() {
        return new PoolSpec(
            PoolId.SERVICE,
            PoolKind.SERVICE,
            OWNER,
            "short lived services",
            0,
            -1,
            CoverageLevel.L1,
            ResourceBudget.fixed(0, 0)
        );
    }

    static ResourceSnapshot pass(final RegionPoolMetrics regions) {
        final PoolSpec spec = PoolSpec.observeOnly(
            PoolId.REGION_EXECUTION, PoolKind.REGION_EXECUTION, "folia", "region tick execution"
        );
        final PoolSnapshot pool = new PoolSnapshot(
            spec,
            4,
            0,
            0,
            4,
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            -1.0d,
            SaturationReport.unknown(PoolId.REGION_EXECUTION),
            List.of(DegradedReason.OBSERVE_ONLY),
            Optional.of(regions)
        );
        return new ResourceSnapshot(
            Instant.EPOCH,
            10,
            1,
            4,
            0,
            4,
            4,
            0.0d,
            0.0d,
            -1.0d,
            List.of(pool),
            List.of(),
            Set.of(DegradedReason.OBSERVE_ONLY)
        );
    }

    static RegionPoolMetrics lagging() {
        // two of four regions below the lag threshold, worst one well below the entry TPS
        return new RegionPoolMetrics(4, 4.0d, 12.0d, 4.0d, 11.0d, 16.0d, 2, 0, 0, 16);
    }

    static RegionPoolMetrics calm() {
        return new RegionPoolMetrics(4, 2.0d, 2.0d, 2.0d, 2.0d, 20.0d, 0, 0, 0, 16);
    }

    /**
     * Probe that reports a fixed reading and counts how often the sampler asked.
     */
    static final class CountingProbe implements PoolProbe {

        private final PoolId pool;
        private final Supplier<PoolObservation> reading;
        private int calls;

        CountingProbe(final PoolId pool, final Supplier<PoolObservation> reading) {
            this.pool = pool;
            this.reading = reading;
        }

        int calls() {
            return this.calls;
        }

        @Override
        public PoolId pool() {
            return this.pool;
        }

        @Override
        public PoolObservation observe() {
            this.calls++;
            return this.reading.get();
        }
    }

    record Reading(
        int alive,
        int active,
        int idle,
        long queued,
        long capacity,
        Optional<RegionPoolMetrics> regions
    ) implements PoolObservation {

        static Reading of(final long queued, final long capacity) {
            return new Reading(2, 0, 2, queued, capacity, Optional.empty());
        }

        @Override
        public int aliveThreads() {
            return this.alive;
        }

        @Override
        public int coreThreads() {
            return 0;
        }

        @Override
        public int activeThreads() {
            return this.active;
        }

        @Override
        public int idleThreads() {
            return this.idle;
        }

        @Override
        public long queuedTasks() {
            return this.queued;
        }

        @Override
        public long queueCapacity() {
            return this.capacity;
        }

        @Override
        public List<Thread> threads() {
            return List.of();
        }

        @Override
        public Optional<RegionPoolMetrics> regionMetrics() {
            return this.regions;
        }
    }

    /**
     * JVM counters the sampler can read, without a real runtime.
     */
    static final class FakeCounters implements ThreadCounters {

        private long started = 4L;
        private long live = 4L;

        @Override
        public boolean cpuTimeSupported() {
            return true;
        }

        @Override
        public long cpuTimeNanos(final long threadId) {
            return 0L;
        }

        @Override
        public long startedTotal() {
            return this.started;
        }

        @Override
        public long liveThreads() {
            return this.live;
        }

        @Override
        public double processCpuLoad() {
            return 0.1d;
        }
    }

    static ResourceSampler sampler(
        final Harness harness,
        final MetricsConfig config,
        final List<PoolProbe> probes,
        final ThreadCounters counters
    ) {
        return new ResourceSampler(
            harness.registry,
            harness.threads,
            () -> List.copyOf(probes),
            counters,
            harness.clock,
            harness.sink,
            config,
            harness.settings.resources().reserveCores(),
            harness.controller
        );
    }

    static void awaitWaiting(
        final ResourceControllerImpl controller,
        final PoolId pool,
        final int expected
    ) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (controller.waiting(pool) != expected) {
            if (System.nanoTime() > deadline) {
                fail("waiter never parked in pool '" + pool + "', expected " + expected
                    + " got " + controller.waiting(pool));
            }
            Thread.yield();
        }
        assertEquals(expected, controller.waiting(pool));
    }

    static Thread waiter(
        final ResourceControllerImpl controller,
        final FakeClock clock,
        final PoolId pool,
        final Duration maxWait
    ) {
        final Thread thread = new Thread(() -> {
            try {
                controller.acquire(new LeaseRequest(pool, clock.deadlineNanos(), maxWait));
            } catch (RuntimeException _) {
                // the scenario owns the outcome; close() or a grant ends the wait
            }
        }, "scenario-waiter-" + pool);
        thread.setDaemon(true);
        return thread;
    }

    static void awaitFinished(final Thread thread) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (thread.isAlive()) {
            if (System.nanoTime() > deadline) {
                fail("thread '" + thread.getName() + "' did not finish");
            }
            Thread.yield();
        }
    }
}
